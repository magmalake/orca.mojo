"""The NYC yellow-taxi load as an orca pipeline: download each month's TLC
file, load it into a month-partitioned Iceberg table, then compact the table
to one file per month.

    pixi run -e data taxi
    pixi run -e data ./build/taxi run --partitions 2023-01..2024-12

That's phase 1's gate: 2023-2024 loads to 79,478,796 rows, as taxibench's
table does, and a run killed during the load resumes at the month it was on.
With ORCA_DATABASE_URL set the run is recorded, so `taxi resume <run>` can
finish it.

- `TAXI_WAREHOUSE` (default `build/taxi-data/warehouse`): the warehouse that holds
  `taxi.trips`, created by the first load.
- `TAXI_TLC` (default `build/taxi-data/tlc`): where the downloads go. A month whose
  file is already there whole isn't downloaded again.
- `TAXI_SOURCE` (default the TLC's URL): where to download from, a URL or a
  local directory of the same files.

The load appends a month in the reader's batches, which leaves about 46 files
per month; the compaction rewrites each month as one file, in one `replace`
snapshot. Data files written by an attempt killed before it committed aren't
in any snapshot and are left behind; expiring and removing orphans is for
later.
"""

from std.os import getenv

from dista import Seq
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.commit import OP_REPLACE, FileChanges
from iceberg.nested import concat_tree
from iceberg.scan import FileScanTask, TableScan
from iceberg.write import WriteOptions, write_data_files
from parquet import RecordBatch

from orca import Ctx, Gather, Mapped, Month, Pipeline, Step, orca_main
from orca.tables import append, commit, find_commit, scan
from tlc import TLC_URL, download, read_month, schema, spec


def _trips() raises -> Table:
    """`taxi.trips`, created if the warehouse doesn't have it yet."""
    var cat = FilesystemCatalog.local(
        getenv("TAXI_WAREHOUSE", "build/taxi-data/warehouse")
    )
    if not cat.table_exists("taxi", "trips"):
        _ = cat.create_table("taxi", "trips", schema(), spec())
    return cat.load_table("taxi", "trips")


@fieldwise_init
struct RawFile(Copyable, Movable):
    var month: Month
    var path: String
    var bytes: Int


@fieldwise_init
struct Loaded(Copyable, Movable):
    var month: Month
    var snapshot: Int64
    var rows: Int
    var dropped: Int
    """Rows dated outside the month, not loaded."""


@fieldwise_init
struct Compacted(Copyable, Movable):
    var snapshot: Int64
    var rows: Int
    var files_before: Int
    var files_after: Int


def fetch(mut ctx: Ctx, m: Month) raises -> RawFile:
    var got = download(
        m,
        getenv("TAXI_TLC", "build/taxi-data/tlc"),
        getenv("TAXI_SOURCE", TLC_URL),
    )
    ctx.log(String(got[0], ", ", got[1], " bytes"))
    return RawFile(m.copy(), got[0], got[1])


def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded:
    var rows = read_month(raw.path, raw.month)
    var table = _trips()
    var id = append(ctx, table, rows.batches)
    ctx.log(
        String(
            rows.rows,
            " rows in ",
            len(rows.batches),
            " batches, ",
            rows.dropped,
            " dropped as out of month",
        )
    )
    return Loaded(raw.month.copy(), id, rows.rows, rows.dropped)


def _partition(path: String) -> String:
    """A data file's partition directory, `.../data/pickup_month=2024-01`."""
    var at = path.rfind("/")
    return String(path[byte=0:at]) if at >= 0 else String()


def _concat(var batches: List[RecordBatch]) raises -> RecordBatch:
    var out = batches.pop(0)
    for k in range(len(batches)):
        ref b = batches[k]
        for c in range(out.num_columns()):
            concat_tree(out.arena, out.roots[c], b.arena, b.roots[c])
        out.num_rows += b.num_rows
    return out^


def _files(s: TableScan) raises -> Tuple[Int, Int]:
    """How many data files the scan's snapshot has, and their rows."""
    var files = 0
    var rows = 0
    for t in s.plan_files():
        files += 1
        rows += Int(t.data_file.record_count)
    return (files, rows)


def compact(mut ctx: Ctx, loads: Seq[Loaded]) raises -> Compacted:
    """Rewrites every month that has more than one file as one file, all in
    one `replace` snapshot."""
    var table = _trips()
    var s = scan(ctx, table)
    var tasks = s.plan_files()
    var parts = List[String]()
    var groups = List[List[FileScanTask]]()
    for t in tasks:
        var p = _partition(t.data_file.file_path)
        var g = -1
        for k in range(len(parts)):
            if parts[k] == p:
                g = k
                break
        if g < 0:
            parts.append(p)
            groups.append(List[FileScanTask]())
            g = len(groups) - 1
        groups[g].append(t.copy())
    var before = len(tasks)

    var id = find_commit(ctx, table)
    if id < 0:
        var changes = FileChanges()
        var m = table.metadata.copy()
        for g in range(len(groups)):
            if len(groups[g]) < 2:
                continue
            var one = _concat(s.to_batches_for_tasks(groups[g].copy()))
            changes.added_data.extend(
                write_data_files(
                    table.io,
                    m.location,
                    [one^],
                    m.schema(),
                    m.spec(),
                    m.default_sort_order_id,
                    WriteOptions.from_properties(m.properties),
                )
            )
            for t in groups[g]:
                changes.removed.append(t.data_file.file_path)
            ctx.log(String(parts[g], ": ", len(groups[g]), " files to 1"))
        if changes.is_empty():
            var n = _files(s)
            ctx.log(String("nothing to compact: ", n[1], " rows"))
            return Compacted(m.current_snapshot_id, n[1], before, before)
        id = commit(ctx, table, OP_REPLACE, changes^)
    var n = _files(table.scan().use_snapshot(id))
    var loaded = 0
    for i in range(len(loads)):
        loaded += loads[i].rows
    ctx.log(
        String(
            n[1], " rows in ", n[0], " files (", loaded, " loaded by this run)"
        )
    )
    return Compacted(id, n[1], before, n[0])


comptime Fetch = Step["download", Month, RawFile, fetch, Mapped, retries=2]
comptime Load = Step["load", RawFile, Loaded, load, Mapped, after=Fetch]
comptime Compact = Step[
    "compact", Seq[Loaded], Compacted, compact, Gather, after=Load
]
comptime Taxi = Pipeline["taxi", Fetch, Load, Compact]


def main() raises:
    orca_main[Taxi]()
