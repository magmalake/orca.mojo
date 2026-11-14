"""Steps written in Rust: a worker library, called from a step of a Mojo
proxy pipeline, whose record batches morca writes into the step's table,
and whose own Parquet files it commits with them.

```mojo
from morca import Ctx, Mapped, Month, Pipeline, Step, morca_main
from morca.rust_step import rust_step
from morca.tables import Schema, ensure_table

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
6. It **takes the worker's own Parquet files**, if it writes any: files
   the worker wrote into the directory the context names, and reported
   (`ctx.add_file` in the crate). When `run` returns and after each batch,
   morca checks each reported file against the table (every column with
   the table's field id and a type iceberg reads as the table's, and its
   rows in one partition, from its statistics), naming the column, uploads
   it under the table's `data/` with the table's `FileIO`, and keeps its
   entry. A worker may stream, write files, or both; a stream of no
   columns says it only writes files. A file reported that isn't there
   fails the attempt.
7. At the end of the stream it **commits once**: `append_files`, which is
   `replace_partition_files` for a dataset the step declared in
   `replaces=`. A step's commits keep their numbers, so a crash after this
   commit is recognised by the retry (1). The worker's files and those
   morca wrote from the stream are in the same commit.

**Stopping.** While the worker runs, a thread polls `stop_when()` (by
default `morca.outdated()`: a newer version of the pipeline leads) every
50 ms and, once it's true, sets the worker's stop flag. A worker that
honours it ends its stream early; what came is committed, and
`RustRun.stopped` says the flag was set. The step decides what that
means, as with `outdated()` in Mojo.

A failure anywhere (the library, the worker's error or panic, a schema
or a file that doesn't fit, a batch that can't be aligned) fails the
attempt before the commit; files already written or uploaded are left
unreferenced in `data/`, as with an interrupted `append`. The output
directory goes with the call, whatever's in it.

Not yet: a step's input stream (the worker gets none), several outputs,
and Rust steps as dista tasks (`Mapped` over tasks); see
docs/rust-workers.md. This module compiles iceberg into the program: it
writes the stream itself, rather than through liblake as `morca.tables`
does.
"""

from iceberg.catalog.filesystem import Table as IcebergTable
from iceberg.io import FileIO
from iceberg.nested import ColumnTree, concat_tree, subtree_copy
from iceberg.write import align_batch
from arrow_mlake.carrow import CArrowSchema
from parquet import RecordBatch

from morca.ctx import Ctx
from morca._internal.handover import outdated
from morca._internal.rust_ffi import StopFn, WorkerCall, WorkerLib
from morca._internal.rust_files import WorkerFiles
from morca._internal.rust_schema import check_schema, prim_fits
from morca._lake.client import HostBlock
from morca._lake.host import Host
from morca._lake.impl import DataFiles, FileWriter, append_files, find_commit
from morca._lake.names import dataset_of
from morca.tables import Table

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
    """The data files committed: those morca wrote from the stream, and
    the worker's own."""
    var worker_files: Int
    """Of `files`, those the worker wrote itself."""
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
    var location = table.metadata.location
    var ice = _load(location)
    var host = HostBlock(ctx)
    var run = _rust_step(
        Host(host.addr),
        ctx,
        library,
        ice,
        params,
        step_version,
        stop_when,
        flush_bytes,
    )
    _ = host^
    table.refresh()
    return run^


def _load(location: String) raises -> IcebergTable:
    if "://" in location and not location.startswith("file://"):
        return IcebergTable.load(location, FileIO())
    return IcebergTable.load_local(String(location.removeprefix("file://")))


def _rust_step(
    var hctx: Host,
    mut ctx: Ctx,
    library: String,
    mut table: IcebergTable,
    params: String,
    step_version: Int,
    stop_when: StopFn,
    flush_bytes: Int,
) raises -> RustRun:
    table.refresh()
    if find_commit(hctx, table) >= 0:
        var id = append_files(hctx, table, DataFiles())
        ctx.log(
            "the worker's output is committed already; it doesn't run again"
        )
        return RustRun(id, 0, 0, 0, False, True)
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
    var own = WorkerFiles(table, name, call.output_dir)
    try:
        call.start()
    except e:
        # A file morca refused is why the worker failed; say it first.
        _refused(call)
        raise e^
    _take(call, own)
    ref stream = call.stream.value()
    # A stream of no columns carries no rows: the worker's are in files.
    var streams = _columns(stream.schema_address()) > 0
    if streams:
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
            _refused(call)
            var plain = call.last_error()
            raise Error(
                "morca: the Rust worker ",
                call.lib.path,
                " failed: ",
                plain if plain else String(e),
            )
        _take(call, own)
        if not got:
            break
        var batch = got.take()
        if batch.num_rows == 0:
            continue
        if not streams:
            raise Error(
                "morca: the Rust worker streamed rows of no columns to ", name
            )
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
    var n_own = len(own.files)
    files.add(DataFiles(own.files.copy()))
    rows += own.rows
    var n_files = len(files)
    var id = append_files(hctx, table, files)
    ctx.log(
        String(
            "the Rust worker wrote ",
            rows,
            " rows in ",
            n_files,
            " data file" if n_files == 1 else " data files",
            String(" (", n_own, " of its own)") if n_own else String(),
            " to ",
            name,
            " (stopped early)" if stopped else "",
        )
    )
    return RustRun(id, rows, n_files, n_own, stopped, False)


def _refused(call: WorkerCall) raises:
    """Raises why morca refused a file the worker reported, if it did."""
    var why = call.refused()
    if why:
        raise Error(why)


def _take(mut call: WorkerCall, mut own: WorkerFiles) raises:
    """Takes the files the worker reported since last time."""
    _refused(call)
    for p in call.take_files():
        own.take(p)


def _columns(schema: Int) -> Int:
    """How many columns the `ArrowSchema` at `schema` has."""
    return Int(
        Pointer[CArrowSchema, MutUntrackedOrigin](
            unsafe_from_address=schema
        )[].n_children
    )


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
