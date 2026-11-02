"""Iceberg tables from steps: pinned reads, and commits a retry recognises.

```mojo
from orca.tables import append, scan

def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded:
    var src = Table.load_local(raw.table)
    var batches = scan(ctx, src).to_batches()
    var dst = Table.load_local(OUT)
    var id = append(ctx, dst, transform(batches))
    return Loaded(raw.month, id)
```

- **`scan`** reads a table at a pinned snapshot: the one this step's first
  attempt read, or its current snapshot if this is the first attempt, which
  is then recorded. A retry or a resume of the step sees the same data,
  whatever was committed in between.
- **`append`** and **`commit`** put `orca.*` properties in the summary of the
  snapshot they commit (`Ctx.summary`): pipeline, run, step, partition,
  attempt, commit number, code, and the snapshots read. Before writing
  anything, they look through the table's snapshots for one this step
  already made in this run (`Ctx.made`). If an attempt committed and was
  killed before Postgres heard of it, the retry finds that snapshot and
  returns it instead of committing the data a second time.

A table is known by its location, `metadata.location`, and, when it's in
the warehouse, by its dataset name. `ORCA_WAREHOUSE` is the root of a
filesystem catalog: dataset `nyc.trips` is the table at
`<warehouse>/nyc/trips`. **`table`** loads a dataset the step declared, and
**`ensure_table`** creates one it writes if it's missing. `scan` refuses a
dataset the step didn't declare in `reads=`, and `append` and `commit` one it
didn't declare in `writes=`. A table outside the warehouse (or with
`ORCA_WAREHOUSE` unset) is used by location as before, and belongs to no
dataset.

This module imports iceberg, so only pipelines that use tables build it;
`orca` itself doesn't.
"""

from std.os import getenv
from std.random import random_float64
from std.time import sleep

from iceberg.append import prepare_append
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.commit import FileChanges, prepare_commit
from iceberg.scan import TableScan
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.write import WriteOptions, write_data_files
from parquet import RecordBatch

from orca.ctx import Ctx
from orca.proc import absolute

comptime _COMMIT_RETRIES = 8
"""How many times to reload and retry when another writer got there first."""


def _backoff(attempt: Int):
    """Waits before retry `attempt` (1, 2, ...) of a commit that clashed:
    10 ms doubling with each, to 640 ms, times a random half to one and a
    half. Two writers that both wrote the same metadata version both back
    out (iceberg's filesystem catalog deletes both files), so retrying at
    once can clash again and again; the random wait lets one get ahead.
    Each process seeds `std.random` itself (`orca.cli.seed_random`)."""
    var base = 0.01 * Float64(1 << min(attempt - 1, 6))
    sleep(base * random_float64(0.5, 1.5))


def warehouse() -> String:
    """`ORCA_WAREHOUSE`: the root datasets are found under, or empty."""
    return getenv("ORCA_WAREHOUSE")


def _norm(path: String) raises -> String:
    var p = path.removeprefix("file://")
    while p.endswith("/") and p.byte_length() > 1:
        p = p[byte = : p.byte_length() - 1]
    return absolute(String(p))


def dataset_of(location: String) raises -> String:
    """The dataset a table at `location` is, `namespace.table`, or empty if
    it isn't directly under a namespace in the warehouse."""
    var root = warehouse()
    if not root:
        return ""
    var base = String(_norm(root), "/")
    var loc = _norm(location)
    if not loc.startswith(base):
        return ""
    var rel = loc[byte = base.byte_length() :]
    var parts = rel.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return ""
    return String(parts[0], ".", parts[1])


def _split(dataset: String) raises -> Tuple[String, String]:
    var dot = dataset.rfind(".")
    if dot <= 0 or dot == dataset.byte_length() - 1:
        raise Error("orca: a dataset is named namespace.table, not ", dataset)
    return (
        String(dataset[byte=:dot]),
        String(dataset[byte = dot + 1 :]),
    )


def _catalog() raises -> FilesystemCatalog:
    var root = warehouse()
    if not root:
        raise Error(
            "orca: ORCA_WAREHOUSE isn't set, so datasets can't be found"
        )
    return FilesystemCatalog.local(root)


def table(mut ctx: Ctx, dataset: String) raises -> Table:
    """The table of a dataset the step declared it reads or writes."""
    if dataset not in ctx.declared_writes:
        ctx.check_declared(dataset, write=False)
    var name = _split(dataset)
    return _catalog().load_table(name[0], name[1])


def ensure_table(
    mut ctx: Ctx,
    dataset: String,
    schema: Schema,
    spec: PartitionSpec = PartitionSpec.unpartitioned(),
) raises -> Table:
    """The table of a dataset the step writes, created empty with `schema`
    and `spec` if the warehouse doesn't have it yet."""
    ctx.check_declared(dataset, write=True)
    var name = _split(dataset)
    var cat = _catalog()
    if not cat.table_exists(name[0], name[1]):
        try:
            _ = cat.create_table(name[0], name[1], schema, spec)
        except e:
            # Another run, started at the same time, created it first.
            if not cat.table_exists(name[0], name[1]):
                raise e^
    return cat.load_table(name[0], name[1])


def scan(mut ctx: Ctx, table: Table) raises -> TableScan:
    """A scan of `table` at this step's pinned snapshot.

    A table that had no snapshot when it was first read stays empty for the
    step: the scan sees no files even if some were committed since.
    """
    var m = table.metadata.copy()
    var dataset = dataset_of(m.location)
    ctx.check_declared(dataset, write=False)
    var current = m.current_snapshot_id if m.has_current_snapshot else -1
    var id = ctx.pin(m.location, current, dataset)
    if id < 0:
        m.has_current_snapshot = False
        m.current_snapshot_id = -1
        return TableScan(m^, table.io.copy())
    return table.scan().use_snapshot(id)


def find_commit(ctx: Ctx, table: Table) -> Int64:
    """The snapshot this step already committed as its next commit to
    `table` in this run, or -1.

    `append` and `commit` check this themselves. A step that writes files
    before committing them, such as a compaction, checks it first, so a retry
    of an attempt that committed doesn't write the files again.
    """
    var loc = table.metadata.location
    for s in table.metadata.snapshots:
        if ctx.made(loc, s.summary):
            return s.snapshot_id
    return -1


def append(
    mut ctx: Ctx, mut table: Table, batches: List[RecordBatch]
) raises -> Int64:
    """Writes `batches` to `table` as one `append` snapshot and returns its ID,
    or returns the snapshot an earlier attempt of this step committed."""
    table.refresh()
    var loc = table.metadata.location
    var dataset = dataset_of(loc)
    ctx.check_declared(dataset, write=True)
    var found = find_commit(ctx, table)
    if found >= 0:
        ctx.log(String("found its commit ", found, " to ", loc))
        ctx.committed(loc, found, dataset)
        return found
    var files = write_data_files(
        table.io,
        table.metadata.location,
        batches,
        table.metadata.schema(),
        table.metadata.spec(),
        table.metadata.default_sort_order_id,
        WriteOptions.from_properties(table.metadata.properties),
    )
    var attempt = 0
    while True:
        var result = prepare_append(
            table.io, table.metadata, files.copy(), ctx.summary(loc)
        )
        try:
            table.commit(result)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise e^
            _backoff(attempt)
            table.refresh()
            continue
        ctx.committed(loc, result.snapshot_id, dataset)
        table.refresh()
        return result.snapshot_id


def commit(
    mut ctx: Ctx,
    mut table: Table,
    operation: String,
    var files: FileChanges,
) raises -> Int64:
    """Commits `files` (data files added and removed, already written) to
    `table` as one snapshot of `operation`, e.g. `replace` for a compaction,
    and returns its ID, or returns the snapshot an earlier attempt of this
    step committed."""
    table.refresh()
    var loc = table.metadata.location
    var dataset = dataset_of(loc)
    ctx.check_declared(dataset, write=True)
    var found = find_commit(ctx, table)
    if found >= 0:
        ctx.log(String("found its commit ", found, " to ", loc))
        ctx.committed(loc, found, dataset)
        return found
    var attempt = 0
    while True:
        var result = prepare_commit(
            table.io, table.metadata, operation, files.copy(), ctx.summary(loc)
        )
        try:
            table.commit(result)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise e^
            _backoff(attempt)
            table.refresh()
            continue
        ctx.committed(loc, result.snapshot_id, dataset)
        table.refresh()
        return result.snapshot_id
