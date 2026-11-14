"""The lake lib-tin's library, liblake (#454): morca's Iceberg tables behind
a C ABI. This file is the library's root, built by
scripts/build-liblake.sh, roughly:

    mojo build --emit shared-lib -g1 -I src $DEP_INC src/morca/_lake/lib.mojo

`@export` only exports from the root module, so these are the library's
symbols when it is built, and plain `abi("C")` functions when the wrapper
imports this module in a build with `-D MORCA_LAKE_SOURCE=1`, which
compiles the library into the program instead (the fallback, and what CI
compares against).

**Handles.** A table is the address of a heap `Table` (`lake_table_free`),
a file writer of a heap `FileWriter`. Written files cross as bytes, iceberg's
`encode_data_files`, which is also how a dista task carries them.

The ABI (`morca._lake.abi`) says how text, maps, out blocks, errors and the
host block cross; each function's doc says what its failure value is.
"""

from std.memory.alloc import unsafe_alloc

from arrow_mlake import AT_STRUCT, ArrayArena, ArrayData, ArrowType
from arrow_mlake.carrow import ExportedArray, export_c
from arrow_mlake.carrow_import import import_batch_c
from arrow_mlake.carrow_stream import export_stream_of
from iceberg.catalog.filesystem import Table
from iceberg.commit import FileChanges
from iceberg.io import FileIO
from iceberg.json import parse_json
from iceberg.manifest import DataFile, decode_data_files, encode_data_files
from iceberg.nested import concat_tree, empty_tree
from iceberg.scan import TableScan
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.write import _partition_key
from parquet import RecordBatch

from morca._lake.abi import (
    ABI_VERSION,
    MODE_APPEND,
    MODE_BY_STEP,
    MODE_REPLACE,
    SNAP_CURRENT,
    SNAP_NONE,
    Dec,
    Enc,
    bytes_of,
    free_text,
    give,
    give_text,
    get_int,
    put_int,
    text_of,
)
from morca._lake.host import Host
from morca._lake.names import dataset_of
import morca._lake.impl as tables


# ── handles, errors, batches ────────────────────────────────────────────────


def _new_table(var t: Table) -> Int:
    var p = unsafe_alloc[Table](1)
    p.unsafe_write(t^)
    return Int(p)


def _table(h: Int) -> Pointer[Table, MutAnyOrigin]:
    return Pointer[Table, MutAnyOrigin](unsafe_from_address=h)


def _writer(h: Int) -> Pointer[tables.FileWriter, MutAnyOrigin]:
    return Pointer[tables.FileWriter, MutAnyOrigin](unsafe_from_address=h)


def _fail(err: Int, e: Error):
    if err != 0:
        give_text(err, String(e))


def _batches(
    n: Int, arrays: Int, schemas: Int, schema: Schema
) raises -> List[RecordBatch]:
    """The `n` borrowed C batches, imported (copied), their columns given the
    table's field ids by name: arrow_mlake's export doesn't write
    `PARQUET:field_id` yet, and a writer matches columns by id.

    TODO(#454): arrow-mlake-mojo 0.3.0 (d2fc489) exports field ids and
    imports without copying. Pin it once parquet-mojo, parquet-full-mojo,
    iceberg-mojo and dista pin it too, then drop `_assign_ids` and the
    copies in `morca.tables`' scans.
    """
    var out = List[RecordBatch]()
    for i in range(n):
        var b = import_batch_c(get_int(arrays, 8 * i), get_int(schemas, 8 * i))
        for r in b.roots.copy():
            var name = b.arena.nodes[r].name.copy()
            _assign_ids(b.arena, r, name, schema)
        out.append(b^)
    return out^


def _assign_ids(mut arena: ArrayArena, node: Int, full: String, schema: Schema):
    for k in range(len(schema.flat)):
        if schema.flat[k].name == full:
            arena.nodes[node].field_id = Int32(schema.flat[k].id)
            break
    var kids = arena.nodes[node].children.copy()
    for c in kids:
        _assign_ids(arena, c, String(full, ".", arena.nodes[c].name), schema)


def _export_batch(b: RecordBatch) raises -> ExportedArray:
    """A batch as one struct array, as a stream carries it."""
    var arena = b.arena.copy()
    var st = ArrayData(ArrowType(AT_STRUCT), String())
    st.nullable = False
    st.length = b.num_rows
    st.children = b.roots.copy()
    var root = arena.add(st^)
    return export_c(arena, root)


def _files_of(addr: Int, n: Int) raises -> List[DataFile]:
    if n <= 0:
        return List[DataFile]()
    var buf = bytes_of(addr, n)
    var pos = 0
    return decode_data_files(Span(buf), pos)


def _give_files(out_block: Int, files: List[DataFile]):
    var buf = List[UInt8]()
    encode_data_files(files, buf)
    give(out_block, Span(buf))


struct _ScanOpts(Movable):
    var snapshot: Int64
    var filter: String
    var columns: List[String]
    var paths: List[String]

    def __init__(out self, addr: Int, n: Int) raises:
        var d = Dec(addr, n)
        self.snapshot = d.int()
        self.filter = d.str()
        self.columns = d.strs()
        self.paths = d.strs()


def _scan_of(t: Table, o: _ScanOpts) raises -> TableScan:
    var sc = t.scan()
    if o.snapshot >= 0:
        sc = sc.use_snapshot(o.snapshot)
    if o.filter:
        sc = sc.filter(o.filter)
    if len(o.columns) > 0:
        sc = sc.select(o.columns.copy())
    return sc^


# ── version ─────────────────────────────────────────────────────────────────


@export("lake_abi_version")
def lake_abi_version() abi("C") -> UInt32:
    return ABI_VERSION


@export("lake_free_text")
def lake_free_text(p: Int) abi("C"):
    free_text(p)


# ── tables ──────────────────────────────────────────────────────────────────


@export("lake_load")
def lake_load(loc: Int, n: Int, err: Int) abi("C") -> Int:
    """The table at a location, local or in object storage; 0 on error."""
    try:
        var place = text_of(loc, n)
        if "://" in place and not place.startswith("file://"):
            return _new_table(Table.load(place, FileIO()))
        return _new_table(
            Table.load_local(String(place.removeprefix("file://")))
        )
    except e:
        _fail(err, e)
        return 0


@export("lake_load_dataset")
def lake_load_dataset(ds: Int, n: Int, err: Int) abi("C") -> Int:
    """`load_dataset`: no step, nothing checked; 0 on error."""
    try:
        return _new_table(tables.load_dataset(text_of(ds, n)))
    except e:
        _fail(err, e)
        return 0


@export("lake_table")
def lake_table(host: Int, ds: Int, n: Int, err: Int) abi("C") -> Int:
    """`table`: a dataset the step declared; 0 on error."""
    try:
        var ctx = Host(host)
        return _new_table(tables.table(ctx, text_of(ds, n)))
    except e:
        _fail(err, e)
        return 0


@export("lake_ensure_table")
def lake_ensure_table(
    host: Int,
    ds: Int,
    ds_n: Int,
    schema: Int,
    schema_n: Int,
    spec: Int,
    spec_n: Int,
    props: Int,
    props_n: Int,
    err: Int,
) abi("C") -> Int:
    """`ensure_table`, with the schema and spec as Iceberg JSON (an empty
    spec is unpartitioned) and the properties an encoded map; 0 on error."""
    try:
        var ctx = Host(host)
        var s = Schema.parse(text_of(schema, schema_n))
        var p = PartitionSpec.unpartitioned()
        var spec_text = text_of(spec, spec_n)
        if spec_text:
            var doc = parse_json(spec_text)
            p = PartitionSpec.from_json(doc, doc.root)
        var d = Dec(props, props_n)
        var properties = d.map() if props_n > 0 else Dict[String, String]()
        return _new_table(
            tables.ensure_table(ctx, text_of(ds, ds_n), s, p, properties)
        )
    except e:
        _fail(err, e)
        return 0


@export("lake_table_free")
def lake_table_free(h: Int) abi("C"):
    if h != 0:
        var p = _table(h)
        p.unsafe_deinit_pointee()
        p.unsafe_free()


@export("lake_table_copy")
def lake_table_copy(h: Int) abi("C") -> Int:
    return _new_table(_table(h)[].copy())


@export("lake_table_refresh")
def lake_table_refresh(h: Int, err: Int) abi("C") -> Int32:
    """`Table.refresh`; 1 on error."""
    try:
        _table(h)[].refresh()
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_table_meta")
def lake_table_meta(h: Int, out_block: Int, err: Int) abi("C") -> Int32:
    """What `morca.tables.TableMeta` mirrors of the table's metadata, encoded
    in its order; 1 on error."""
    try:
        ref t = _table(h)[]
        ref m = t.metadata
        var e = Enc()
        e.str(m.location)
        e.str(t.metadata_location)
        e.int(Int64(m.format_version))
        e.str(m.table_uuid)
        e.bool(m.has_current_snapshot)
        e.int(m.current_snapshot_id)
        e.int(Int64(m.current_schema_id))
        e.int(Int64(m.default_spec_id))
        e.int(Int64(m.default_sort_order_id))
        e.int(m.last_updated_ms)
        e.int(m.last_sequence_number)
        e.map(m.properties)
        e.int(Int64(len(m.schemas)))
        for s in m.schemas:
            e.int(Int64(s.schema_id))
            e.str(s.to_json())
        e.int(Int64(len(m.partition_specs)))
        for p in m.partition_specs:
            e.int(Int64(p.spec_id))
            e.str(p.to_json())
        e.int(Int64(len(m.snapshots)))
        for s in m.snapshots:
            e.int(s.snapshot_id)
            e.bool(s.has_parent)
            e.int(s.parent_snapshot_id)
            e.int(s.sequence_number)
            e.int(s.timestamp_ms)
            e.str(s.manifest_list)
            e.int(Int64(s.schema_id) if s.has_schema_id else -1)
            e.map(s.summary)
        e.int(Int64(len(m.metadata_log)))
        for l in m.metadata_log:
            e.int(l.timestamp_ms)
            e.str(l.metadata_file)
        give(out_block, Span(e.buf))
        return 0
    except e:
        _fail(err, e)
        return 1


# ── reading ─────────────────────────────────────────────────────────────────


@export("lake_pin")
def lake_pin(host: Int, h: Int, out_id: Int, err: Int) abi("C") -> Int32:
    """What `scan(ctx, table)` reads: the step's pinned snapshot of the table,
    to `out_id`, or `SNAP_NONE` when it pinned the table empty; 1 on error."""
    try:
        var ctx = Host(host)
        ref m = _table(h)[].metadata
        var dataset = dataset_of(m.location)
        ctx.check_declared(dataset, write=False)
        var current = m.current_snapshot_id if m.has_current_snapshot else -1
        var id = ctx.pin(m.location, current, dataset)
        put_int(out_id, 0, Int(id) if id >= 0 else Int(SNAP_NONE))
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_scan")
def lake_scan(
    h: Int, opts: Int, opts_n: Int, out_stream: Int, err: Int
) abi("C") -> Int32:
    """The scan's rows as an `ArrowArrayStream`, its address to
    `out_stream` (0 when there are no batches); 1 on error."""
    try:
        var o = _ScanOpts(opts, opts_n)
        put_int(out_stream, 0, 0)
        if o.snapshot == SNAP_NONE:
            return 0
        ref t = _table(h)[]
        if not t.metadata.has_current_snapshot and o.snapshot < 0:
            return 0
        var sc = _scan_of(t, o)
        var batches: List[RecordBatch]
        if len(o.paths) > 0:
            batches = sc.to_batches_for_paths(o.paths)
        else:
            batches = sc.to_batches()
        var exported = List[ExportedArray]()
        for b in batches:
            exported.append(_export_batch(b))
        if len(exported) > 0:
            put_int(out_stream, 0, export_stream_of(exported^))
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_plan_files")
def lake_plan_files(
    h: Int, opts: Int, opts_n: Int, out_block: Int, err: Int
) abi("C") -> Int32:
    """The scan's file tasks, encoded as `morca.tables.FileScanTask` reads
    them; 1 on error."""
    try:
        var o = _ScanOpts(opts, opts_n)
        var e = Enc()
        ref t = _table(h)[]
        if o.snapshot == SNAP_NONE or (
            not t.metadata.has_current_snapshot and o.snapshot < 0
        ):
            e.int(0)
            give(out_block, Span(e.buf))
            return 0
        var tasks = _scan_of(t, o).plan_files()
        e.int(Int64(len(tasks)))
        for task in tasks:
            ref f = task.data_file
            e.str(f.file_path)
            e.int(Int64(f.content))
            e.int(f.record_count)
            e.int(f.file_size_in_bytes)
            e.int(Int64(task.spec_id))
            e.str(_partition_key(f.partition))
            e.str(task.residual)
            e.int(task.start)
            e.int(task.length)
            e.int(Int64(len(task.delete_files)))
        give(out_block, Span(e.buf))
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_count")
def lake_count(
    h: Int, opts: Int, opts_n: Int, out_n: Int, err: Int
) abi("C") -> Int32:
    """How many rows the scan returns, to `out_n`; 1 on error."""
    try:
        var o = _ScanOpts(opts, opts_n)
        ref t = _table(h)[]
        put_int(out_n, 0, 0)
        if o.snapshot == SNAP_NONE or (
            not t.metadata.has_current_snapshot and o.snapshot < 0
        ):
            return 0
        put_int(out_n, 0, Int(_scan_of(t, o).count()))
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_empty_batch")
def lake_empty_batch(
    schema: Int, schema_n: Int, out_array: Int, out_schema: Int, err: Int
) abi("C") -> Int32:
    """A zero-row batch of an Iceberg schema (JSON), every column at the
    Arrow type iceberg writes it with, as one C struct array: what the
    wrapper's `BatchBuilder` fills. 1 on error."""
    try:
        var s = Schema.parse(text_of(schema, schema_n))
        var b = RecordBatch()
        for f in s.columns():
            b.roots.append(
                empty_tree(
                    b.arena, s.store, f.type, f.name, f.id, not f.required
                )
            )
        var ex = _export_batch(b)
        var pair = ex.into_raw()
        put_int(out_array, 0, pair[0])
        put_int(out_schema, 0, pair[1])
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_concat")
def lake_concat(
    n: Int, arrays: Int, schemas: Int, out_array: Int, out_schema: Int, err: Int
) abi("C") -> Int32:
    """`n` borrowed C batches with the same columns as one, exported to the
    out slots as `lake_empty_batch` does; 1 on error."""
    try:
        var out = RecordBatch()
        for i in range(n):
            var b = import_batch_c(
                get_int(arrays, 8 * i), get_int(schemas, 8 * i)
            )
            if i == 0:
                out = b^
                continue
            if b.num_columns() != out.num_columns():
                raise Error(
                    "lake: batch ",
                    i,
                    " has ",
                    b.num_columns(),
                    " columns, the first ",
                    out.num_columns(),
                )
            for c in range(out.num_columns()):
                concat_tree(out.arena, out.roots[c], b.arena, b.roots[c])
            out.num_rows += b.num_rows
        var ex = _export_batch(out)
        var pair = ex.into_raw()
        put_int(out_array, 0, pair[0])
        put_int(out_schema, 0, pair[1])
        return 0
    except e:
        _fail(err, e)
        return 1


# ── writing ─────────────────────────────────────────────────────────────────


@export("lake_find_commit")
def lake_find_commit(host: Int, h: Int) abi("C") -> Int64:
    return tables.find_commit(Host(host), _table(h)[])


@export("lake_append")
def lake_append(
    host: Int, h: Int, n: Int, arrays: Int, schemas: Int, mode: Int, err: Int
) abi("C") -> Int64:
    """`append` (`MODE_BY_STEP`), or `replace_partitions` (`MODE_REPLACE`),
    of `n` borrowed C batches. With no host, iceberg's own `Table.append`,
    which nothing checks or records. -2 on error."""
    try:
        ref t = _table(h)[]
        var bs = _batches(n, arrays, schemas, t.metadata.schema())
        if host == 0:
            return t.append(bs)
        var ctx = Host(host)
        if mode == MODE_REPLACE:
            return tables.replace_partitions(ctx, t, bs)
        if mode == MODE_APPEND:
            return tables._append(ctx, t, bs, False)
        return tables.append(ctx, t, bs)
    except e:
        _fail(err, e)
        return -2


@export("lake_append_files")
def lake_append_files(
    host: Int, h: Int, files: Int, files_n: Int, mode: Int, err: Int
) abi("C") -> Int64:
    """`append_files` (`MODE_BY_STEP`) or `replace_partition_files`
    (`MODE_REPLACE`) of encoded entries; -2 on error."""
    try:
        var ctx = Host(host)
        ref t = _table(h)[]
        var fs = _files_of(files, files_n)
        if mode == MODE_REPLACE:
            return tables.replace_partition_files(ctx, t, fs)
        if mode == MODE_APPEND:
            return tables._append_files(ctx, t, fs, False)
        return tables.append_files(ctx, t, fs)
    except e:
        _fail(err, e)
        return -2


@export("lake_commit")
def lake_commit(
    host: Int,
    h: Int,
    op: Int,
    op_n: Int,
    changes: Int,
    changes_n: Int,
    err: Int,
) abi("C") -> Int64:
    """`commit` of `FileChanges` encoded as added data files, added delete
    files (each `encode_data_files` bytes) and the paths removed; -2 on
    error."""
    try:
        var ctx = Host(host)
        ref t = _table(h)[]
        var d = Dec(changes, changes_n)
        var fc = FileChanges()
        var data = d.bytes()
        var deletes = d.bytes()
        fc.added_data = _files_of(Int(data.unsafe_ptr()), len(data))
        fc.added_deletes = _files_of(Int(deletes.unsafe_ptr()), len(deletes))
        fc.removed = d.strs()
        return tables.commit(ctx, t, text_of(op, op_n), fc^)
    except e:
        _fail(err, e)
        return -2


@export("lake_write_files")
def lake_write_files(
    h: Int, n: Int, arrays: Int, schemas: Int, out_block: Int, err: Int
) abi("C") -> Int32:
    """`write_files` of `n` borrowed C batches: the entries, encoded, to
    `out_block`; 1 on error."""
    try:
        ref t = _table(h)[]
        var bs = _batches(n, arrays, schemas, t.metadata.schema())
        _give_files(out_block, tables.write_files(t, bs).files)
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_writer_new")
def lake_writer_new(h: Int, err: Int) abi("C") -> Int:
    """A `FileWriter` for the table; 0 on error."""
    try:
        var p = unsafe_alloc[tables.FileWriter](1)
        p.unsafe_write(tables.FileWriter(_table(h)[]))
        return Int(p)
    except e:
        _fail(err, e)
        return 0


@export("lake_writer_write")
def lake_writer_write(
    w: Int, n: Int, arrays: Int, schemas: Int, err: Int
) abi("C") -> Int32:
    """`FileWriter.write` of `n` borrowed C batches; 1 on error."""
    try:
        ref fw = _writer(w)[]
        fw.write(_batches(n, arrays, schemas, fw.schema))
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_writer_finish")
def lake_writer_finish(w: Int, out_block: Int) abi("C"):
    """The writer's entries, encoded, to `out_block`; frees the writer."""
    var p = _writer(w)
    var files = p[].files.files.copy()
    p.unsafe_deinit_pointee()
    p.unsafe_free()
    _give_files(out_block, files)


@export("lake_writer_free")
def lake_writer_free(w: Int) abi("C"):
    if w != 0:
        var p = _writer(w)
        p.unsafe_deinit_pointee()
        p.unsafe_free()


# ── written files ───────────────────────────────────────────────────────────


@export("lake_files_info")
def lake_files_info(
    files: Int, files_n: Int, out_block: Int, err: Int
) abi("C") -> Int32:
    """What `morca.tables.DataFile` shows of each entry; 1 on error."""
    try:
        var fs = _files_of(files, files_n)
        var e = Enc()
        e.int(Int64(len(fs)))
        for f in fs:
            e.str(f.file_path)
            e.int(Int64(f.content))
            e.int(f.record_count)
            e.int(f.file_size_in_bytes)
            e.str(_partition_key(f.partition))
        give(out_block, Span(e.buf))
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_files_concat")
def lake_files_concat(
    a: Int, a_n: Int, b: Int, b_n: Int, out_block: Int, err: Int
) abi("C") -> Int32:
    """The entries of two encodings as one; 1 on error."""
    try:
        var fs = _files_of(a, a_n)
        for f in _files_of(b, b_n):
            fs.append(f.copy())
        _give_files(out_block, fs)
        return 0
    except e:
        _fail(err, e)
        return 1


@export("lake_files_decode")
def lake_files_decode(
    buf: Int, n: Int, pos: Int, out_block: Int, err: Int
) abi("C") -> Int32:
    """Reads one encoding of entries starting at the offset in the slot
    `pos`, which it advances, and returns it alone (`DataFiles.decode`);
    1 on error."""
    try:
        var bytes = bytes_of(buf, n)
        var at = get_int(pos, 0)
        var start = at
        _ = decode_data_files(Span(bytes), at)
        put_int(pos, 0, at)
        give(out_block, Span(bytes)[start:at])
        return 0
    except e:
        _fail(err, e)
        return 1
