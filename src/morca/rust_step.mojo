"""Steps written in Rust: a worker library, called from a step of a Mojo
proxy pipeline, whose record batches morca writes into the step's table.

```mojo
from iceberg.schema import Schema
from morca import Ctx, Mapped, Month, Pipeline, Step, morca_main
from morca.rust_step import rust_step
from morca.tables import ensure_table

comptime NUMBERS = "demo.numbers"
comptime SCHEMA = "schemas/numbers.json"  # read by the worker too

def numbers(mut ctx: Ctx, m: Month) raises -> Int64:
    var schema = Schema.parse(open(SCHEMA, "r").read())
    var t = ensure_table(ctx, NUMBERS, schema)
    return rust_step(ctx, "numbers_worker", t, params='{"rows": 100}').snapshot_id

comptime Numbers = Step["numbers", Month, Int64, numbers, Mapped, writes=[NUMBERS]]
```

`rust_step` runs the worker once, in this process (docs/rust-workers.md is
the contract, and the `morca-worker` crate its Rust side):

1. **A retry that finds its commit** returns it: if an earlier attempt of
   the step committed to the table in this run (`find_commit`), the worker
   doesn't run again, and nothing is written.
2. It **loads the library** now, not at program start, and refuses one
   built for another ABI version. `library` is a path, a file name
   (`libx.so`), or a bare name `x`, found as `libx.so` (`libx.dylib` on
   macOS) in `$CONDA_PREFIX/lib`, next to the executable or in its
   `../lib`, then on the dynamic linker's path.
3. It **calls the worker** with the step's context: pipeline, step,
   partition, run, attempt, the registry hash, `params` (a JSON object for
   the worker), `step_version`, and the run's secrets. The worker's log
   lines, progress and metrics go to the step's log.
4. It **checks the stream's schema** against the table's before writing
   anything: every column of the table must be in it, matched by name at
   any depth, and no other; a required column mustn't be nullable; a type
   must be the table's or one Iceberg widens to it (Int8–Int32 to `int`,
   any integer to `long`, Float32 to `double`, a large string or binary,
   a decimal of the same scale and no more precision, a timestamp in
   another unit). The error names the column. A nested column is checked
   for its kind here, and to the leaf as each batch is aligned.
5. It **writes Parquet as batches arrive**, each aligned to the table's
   schema, gathering small batches into files of about `flush_bytes`, so
   the output never sits in memory whole and the worker produces at the
   rate the writer consumes.
6. At the end of the stream it **commits once**: `append_files`, which is
   `replace_partition_files` for a dataset the step declared in
   `replaces=`. A step's commits keep their numbers, so a crash after this
   commit is recognised by the retry (1).

**Stopping.** While the worker runs, a thread polls `stop_when()` (by
default `morca.outdated()`: a newer version of the pipeline leads) every
50 ms and, once it's true, sets the worker's stop flag. A worker that
honours it ends its stream early; what came is committed, and
`RustRun.stopped` says the flag was set. The step decides what that
means, as with `outdated()` in Mojo.

A failure anywhere (the library, the worker's error or panic, a schema
that doesn't fit, a batch that can't be aligned) fails the attempt before
the commit; files already written are left unreferenced in `data/`, as
with an interrupted `append`.

Not yet: a step's input stream (the worker gets none), several outputs,
and Rust steps as dista tasks (`Mapped` over tasks); see
docs/rust-workers.md. This module imports iceberg, like `morca.tables`.
"""

from iceberg.catalog.filesystem import Table
from iceberg.nested import ColumnTree, arrow_type_of, concat_tree, subtree_copy
from iceberg.schema import Schema
from iceberg.types import (
    TK_LIST,
    TK_MAP,
    TK_PRIMITIVE,
    TK_STRUCT,
    NestedField,
    TypeStore,
    primitive_name,
)
from iceberg.write import align_batch
from arrow_mlake.arrow import (
    AT_BINARY,
    AT_DECIMAL128,
    AT_FIXED_SIZE_BINARY,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_INT8,
    AT_INT16,
    AT_INT32,
    AT_INT64,
    AT_LARGE_BINARY,
    AT_LARGE_LIST,
    AT_LARGE_UTF8,
    AT_LIST,
    AT_MAP,
    AT_STRUCT,
    AT_TIME64,
    AT_TIMESTAMP,
    AT_UTF8,
    ArrowType,
)
from arrow_mlake.carrow import ARROW_FLAG_NULLABLE, CArrowSchema
from arrow_mlake.carrow_import import parquet_field_id, parse_format
from parquet import RecordBatch

from morca.ctx import Ctx
from morca._internal.handover import outdated
from morca._internal.rust_ffi import StopFn, WorkerCall, WorkerLib
from morca.tables import (
    DataFiles,
    FileWriter,
    append_files,
    dataset_of,
    find_commit,
)

comptime DEFAULT_FLUSH_BYTES = 64 * 1024 * 1024
"""About how much of the worker's output `rust_step` gathers into a file."""


@fieldwise_init
struct RustRun(Copyable, Movable, Writable):
    """What `rust_step` did."""

    var snapshot_id: Int64
    """The step's commit to the table."""
    var rows: Int
    """The rows written; 0 when `resumed`."""
    var files: Int
    """The data files written."""
    var stopped: Bool
    """Whether the stop flag was set when the stream ended."""
    var resumed: Bool
    """Whether an earlier attempt's commit was found, and the worker didn't
    run."""


def rust_step(
    mut ctx: Ctx,
    library: String,
    mut table: Table,
    params: String = "{}",
    step_version: Int = 0,
    stop_when: StopFn = outdated,
    flush_bytes: Int = DEFAULT_FLUSH_BYTES,
) raises -> RustRun:
    """Runs the Rust worker in `library` and commits what it streams to
    `table` in one snapshot (the module's doc says how)."""
    table.refresh()
    if find_commit(ctx, table) >= 0:
        var id = append_files(ctx, table, DataFiles())
        ctx.log(
            "the worker's output is committed already; it doesn't run again"
        )
        return RustRun(id, 0, 0, False, True)
    var dataset = dataset_of(table.metadata.location)
    var name = dataset if dataset else table.metadata.location
    var schema = table.metadata.schema()

    var call = WorkerCall(
        WorkerLib(library), ctx, params, step_version, stop_when
    )
    ctx.log(
        String(
            "running the Rust worker ", call.lib.path, " ", call.lib.describe
        )
    )
    call.start()
    ref stream = call.stream.value()
    try:
        check_schema(stream.schema_address(), schema)
    except e:
        raise Error(
            "morca: the Rust worker's output doesn't fit ", name, ": ", e
        )

    var writer = FileWriter(table)
    var pending = List[ColumnTree]()
    var pending_rows = 0
    var pending_bytes = 0
    var rows = 0
    while True:
        var got: Optional[RecordBatch]
        try:
            got = stream.next()
        except e:
            raise Error(
                "morca: the Rust worker ", call.lib.path, " failed: ", e
            )
        if not got:
            break
        var batch = got.take()
        if batch.num_rows == 0:
            continue
        var cols: List[ColumnTree]
        try:
            cols = align_batch(batch, schema)
        except e:
            raise Error(
                "morca: a batch of the Rust worker doesn't fit ", name, ": ", e
            )
        if pending_rows == 0:
            pending = cols^
        else:
            for k in range(len(pending)):
                concat_tree(
                    pending[k].arena,
                    pending[k].root,
                    cols[k].arena,
                    cols[k].root,
                )
        pending_rows += batch.num_rows
        pending_bytes += _bytes(batch)
        rows += batch.num_rows
        if pending_bytes >= flush_bytes:
            writer.put(_batch_of(pending, pending_rows))
            pending = List[ColumnTree]()
            pending_rows = 0
            pending_bytes = 0
    if pending_rows > 0:
        writer.put(_batch_of(pending, pending_rows))
    var stopped = call.stopping()
    _ = call^
    var files = writer^.finish()
    var n_files = len(files)
    var id = append_files(ctx, table, files)
    ctx.log(
        String(
            "the Rust worker wrote ",
            rows,
            " rows in ",
            n_files,
            " data file" if n_files == 1 else " data files",
            " to ",
            name,
            " (stopped early)" if stopped else "",
        )
    )
    return RustRun(id, rows, n_files, stopped, False)


def _bytes(batch: RecordBatch) -> Int:
    """About how many bytes `batch`'s buffers hold."""
    var n = 0
    for a in batch.arena.nodes:
        n += len(a.values) + len(a.validity)
        n += 4 * len(a.offsets) + 8 * len(a.large_offsets)
    return n


def _batch_of(cols: List[ColumnTree], rows: Int) raises -> RecordBatch:
    """Aligned columns as one batch, for the writer."""
    var b = RecordBatch()
    for c in cols:
        b.roots.append(subtree_copy(c.arena, c.root, b.arena))
    b.num_rows = rows
    return b^


# --- the stream's schema against the table's ---


@fieldwise_init
struct _Field(Copyable, Movable):
    var name: String
    var type: ArrowType
    var nullable: Bool
    var field_id: Int
    var children: List[Int]
    """The children's `ArrowSchema` addresses."""


def _cstr(addr: Int) -> String:
    if addr == 0:
        return String()
    return String(
        unsafe_from_utf8_ptr=Pointer[UInt8, ImmUntrackedOrigin](
            unsafe_from_address=addr
        )
    )


def _read(addr: Int, path: String) raises -> _Field:
    var s = Pointer[CArrowSchema, MutUntrackedOrigin](unsafe_from_address=addr)
    var name = _cstr(s[].name)
    var at = String(path, ".", name) if path else name
    var fmt = _cstr(s[].format)
    var t: ArrowType
    try:
        t = parse_format(fmt)
    except e:
        raise Error("column ", at, " has a type morca can't read: ", e)
    var kids = List[Int]()
    for i in range(Int(s[].n_children)):
        kids.append(
            Pointer[Int, MutUntrackedOrigin](
                unsafe_from_address=s[].children + 8 * i
            )[]
        )
    return _Field(
        name,
        t^,
        (s[].flags & ARROW_FLAG_NULLABLE) != 0,
        Int(parquet_field_id(s[].metadata)),
        kids^,
    )


def check_schema(stream_schema: Int, table: Schema) raises:
    """Raises, naming the column, unless the stream's schema (an
    `ArrowSchema` address) fits `table` by the rule in the module's doc."""
    var root = _read(stream_schema, "")
    if root.type.id != AT_STRUCT:
        raise Error("the stream's schema isn't a struct of columns")
    _fields_fit(table.store, table.columns(), root.children, "")


def _fields_fit(
    store: TypeStore, want: List[NestedField], kids: List[Int], path: String
) raises:
    var got = List[_Field]()
    for k in kids:
        got.append(_read(k, path))
    for f in want:
        var at = String(path, ".", f.name) if path else f.name
        var found = -1
        for i in range(len(got)):
            if got[i].name == f.name:
                found = i
                break
        if found < 0:
            raise Error(
                "column ", at, " is in the table but not in the worker's schema"
            )
        ref g = got[found]
        if g.field_id >= 0 and g.field_id != f.id:
            raise Error(
                "column ",
                at,
                " has field id ",
                g.field_id,
                ", and the table's is ",
                f.id,
            )
        _nullable_fits(f.required, g.nullable, at)
        _fits(store, f.type, g, at)
    for g in got:
        var known = False
        for f in want:
            if f.name == g.name:
                known = True
                break
        if not known:
            var at = String(path, ".", g.name) if path else g.name
            raise Error(
                "column ", at, " is in the worker's schema but not in the table"
            )


def _nullable_fits(required: Bool, nullable: Bool, at: String) raises:
    if required and nullable:
        raise Error(
            "column ",
            at,
            " is required in the table but nullable in the worker's schema",
        )


def _fits(store: TypeStore, ti: Int, g: _Field, at: String) raises:
    ref t = store.nodes[ti]
    if t.kind == TK_PRIMITIVE:
        if not prim_fits(arrow_type_of(store, ti), g.type):
            raise Error(
                "column ",
                at,
                " is ",
                g.type,
                " in the worker's schema, which doesn't fit the table's ",
                primitive_name(t.prim),
            )
        return
    if t.kind == TK_STRUCT:
        if g.type.id != AT_STRUCT:
            raise Error(
                "column ",
                at,
                " is ",
                g.type,
                " in the worker's schema, not a struct",
            )
        _fields_fit(store, t.fields, g.children, at)
        return
    if t.kind == TK_LIST:
        if (g.type.id != AT_LIST and g.type.id != AT_LARGE_LIST) or len(
            g.children
        ) != 1:
            raise Error(
                "column ",
                at,
                " is ",
                g.type,
                " in the worker's schema, not a list",
            )
        var el = _read(g.children[0], at)
        var p = String(at, ".element")
        _nullable_fits(t.element_required, el.nullable, p)
        _fits(store, t.element, el, p)
        return
    if g.type.id != AT_MAP or len(g.children) != 1:
        raise Error(
            "column ", at, " is ", g.type, " in the worker's schema, not a map"
        )
    var kv = _read(g.children[0], at)
    if len(kv.children) != 2:
        raise Error(
            "column ", at, " is a map whose entries aren't key and value"
        )
    var k = _read(kv.children[0], at)
    var v = _read(kv.children[1], at)
    _nullable_fits(True, k.nullable, String(at, ".key"))
    _fits(store, t.key, k, String(at, ".key"))
    _nullable_fits(t.value_required, v.nullable, String(at, ".value"))
    _fits(store, t.value, v, String(at, ".value"))


def prim_fits(want: ArrowType, got: ArrowType) -> Bool:
    """Whether a primitive column of type `got` can be written to one the
    table stores as `want`: the same type, or one Iceberg widens to it."""
    var w = want.id
    var g = got.id
    if w == AT_INT32:
        return g == AT_INT8 or g == AT_INT16 or g == AT_INT32
    if w == AT_INT64:
        return g == AT_INT8 or g == AT_INT16 or g == AT_INT32 or g == AT_INT64
    if w == AT_FLOAT64:
        return g == AT_FLOAT32 or g == AT_FLOAT64
    if w == AT_UTF8:
        return g == AT_UTF8 or g == AT_LARGE_UTF8
    if w == AT_BINARY:
        return g == AT_BINARY or g == AT_LARGE_BINARY
    if w == AT_DECIMAL128:
        return (
            g == AT_DECIMAL128
            and got.scale == want.scale
            and got.precision <= want.precision
        )
    if w == AT_TIMESTAMP:
        return g == AT_TIMESTAMP and (got.tz.byte_length() > 0) == (
            want.tz.byte_length() > 0
        )
    if w == AT_TIME64:
        return g == AT_TIME64
    if w == AT_FIXED_SIZE_BINARY:
        return g == AT_FIXED_SIZE_BINARY and got.byte_width == want.byte_width
    return g == w
