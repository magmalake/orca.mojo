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

A table is known by its location, `metadata.location`. This module imports
iceberg, so only pipelines that use tables build it; `orca` itself doesn't.
"""

from iceberg.append import prepare_append
from iceberg.catalog.filesystem import Table
from iceberg.commit import FileChanges, prepare_commit
from iceberg.scan import TableScan
from iceberg.write import WriteOptions, write_data_files
from parquet import RecordBatch

from orca.ctx import Ctx

comptime _COMMIT_RETRIES = 4
"""How many times to reload and retry when another writer got there first."""


def scan(mut ctx: Ctx, table: Table) raises -> TableScan:
    """A scan of `table` at this step's pinned snapshot.

    A table that had no snapshot when it was first read stays empty for the
    step: the scan sees no files even if some were committed since.
    """
    var m = table.metadata.copy()
    var current = m.current_snapshot_id if m.has_current_snapshot else -1
    var id = ctx.pin(m.location, current)
    if id < 0:
        m.has_current_snapshot = False
        m.current_snapshot_id = -1
        return TableScan(m^, table.io.copy())
    return table.scan().use_snapshot(id)


def _made(ctx: Ctx, table: Table) -> Int64:
    """The snapshot this step already committed as its next commit to
    `table` in this run, or -1."""
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
    var found = _made(ctx, table)
    if found >= 0:
        ctx.log(String("found its commit ", found, " to ", loc))
        ctx.committed(loc, found)
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
            table.refresh()
            continue
        ctx.committed(loc, result.snapshot_id)
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
    var found = _made(ctx, table)
    if found >= 0:
        ctx.log(String("found its commit ", found, " to ", loc))
        ctx.committed(loc, found)
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
            table.refresh()
            continue
        ctx.committed(loc, result.snapshot_id)
        table.refresh()
        return result.snapshot_id
