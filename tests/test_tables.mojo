"""Pinned reads and recognised commits, over local Iceberg tables. Run under
scripts/with-postgres.sh, in the data environment."""

from std.os import getenv, setenv
from std.testing import TestSuite, assert_equal, assert_true

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.schema import Schema
from iceberg.util import now_ms
from iceberg.values import Datum
from parquet import RecordBatch
from postgres import Connection, Params

from orca import (
    Ctx,
    Mapped,
    Month,
    PgStore,
    Pipeline,
    RunResult,
    State,
    Step,
    parse_months,
)
from orca.tables import append, scan

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"month","required":true,"type":"string"}]}'
)


def _rows(month: String, n: Int) raises -> List[RecordBatch]:
    var schema = Schema.parse(SCHEMA)
    var ids = ColumnBuilder.of(schema, 1)
    var months = ColumnBuilder.of(schema, 2)
    for i in range(n):
        ids.add(Datum.long_(Int64(i)))
        months.add(Datum.string_(month))
    return [batch_of([ids^, months^])]


def _count(t: Table) raises -> Int:
    var n = 0
    for b in t.scan().to_batches():
        n += b.num_rows
    return n


def _snapshots(t: Table) -> Int:
    return len(t.metadata.snapshots)


# Where the steps' tables are, and how the next attempt misbehaves: the
# steps are plain functions, so the test talks to them through the
# environment.


def _table(name: String) raises -> Table:
    return Table.load_local(String(getenv("ORCA_TEST_WH"), "/db/", name))


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    """Appends 3 rows for the month to `out`; dies right after committing
    while ORCA_TEST_DIE is set."""
    var out = _table("out")
    var id = append(ctx, out, _rows(String(m), 3))
    if getenv("ORCA_TEST_DIE"):
        raise Error("died after committing")
    return id


def copy_count(mut ctx: Ctx, m: Month) raises -> Int:
    """Counts `src` at the pinned snapshot. While ORCA_TEST_DIE is set,
    another writer appends to `src` first, then the attempt dies."""
    var src = _table("src")
    var n = 0
    for b in scan(ctx, src).to_batches():
        n += b.num_rows
    if getenv("ORCA_TEST_DIE"):
        _ = src.append(_rows("late", 5))
        raise Error("died after reading")
    return n


comptime Load = Step["load", Month, Int64, load, Mapped]
comptime Loads = Pipeline["loads", Load]
comptime CopyCount = Step["copy_count", Month, Int, copy_count, Mapped]
comptime Copies = Pipeline["copies", CopyCount]


def _warehouse() raises -> FilesystemCatalog:
    var root = String("build/wh-", now_ms())
    _ = setenv("ORCA_TEST_WH", root)
    var cat = FilesystemCatalog.local(root)
    _ = cat.create_table("db", "out", Schema.parse(SCHEMA))
    _ = cat.create_table("db", "src", Schema.parse(SCHEMA))
    return cat^


def _store(run_name: String) raises -> PgStore:
    var url = getenv("ORCA_DATABASE_URL")
    var db = State(url)
    db.migrate()
    db.register(run_name, "h", "{}")
    var run = db.create_run(run_name, "h", "x", "manual", "a:1")
    return PgStore(db^, url, run, "a:1", "h")


def _resume(mut store: PgStore) raises -> RunResult:
    """What `resume` does to the store between two processes."""
    _ = store.db.interrupt(store.run)
    return store.db.prior(store.run)


def test_a_commit_is_recognised_on_resume() raises:
    _ = _warehouse()
    var store = _store("loads")
    var months = parse_months("2024-01..2024-02")
    _ = setenv("ORCA_TEST_DIE", "1")
    var res = Loads.execute(store, RunResult(), months, store.run)
    assert_true(not res.succeeded())
    assert_equal(_snapshots(_table("out")), 2)
    # The commits were recorded as they happened, before the attempts died.
    var conn = Connection(getenv("ORCA_DATABASE_URL"))
    assert_equal(
        Int(
            conn.query(
                (
                    "SELECT count(*) AS n FROM orca.commits c JOIN"
                    " orca.attempts a ON a.id = c.attempt JOIN orca.steps s ON"
                    " s.id = a.step WHERE s.run = $1"
                ),
                Params().int64(Int64(store.run)),
            )
            .row(0)
            .int64("n")
        ),
        2,
    )
    _ = setenv("ORCA_TEST_DIE", "")
    res = Loads.execute(store, _resume(store), months, store.run)
    assert_true(res.succeeded())
    var out = _table("out")
    # One snapshot per month, not two, and the step returns it.
    assert_equal(_snapshots(out), 2)
    assert_equal(_count(out), 6)
    assert_equal(
        res.output[Int64]("load", "2024-01"),
        out.metadata.snapshots[0].snapshot_id,
    )
    var s = out.metadata.snapshots[1].summary.copy()
    assert_equal(s["orca.pipeline"], "loads")
    assert_equal(s["orca.run"], String(store.run))
    assert_equal(s["orca.step"], "load")
    assert_equal(s["orca.partition"], "2024-02")
    assert_equal(s["orca.attempt"], "1")
    assert_equal(s["orca.commit"], "0")
    assert_equal(s["orca.code"], hex(Loads.registry_hash()))


def test_in_memory_runs_dont_recognise() raises:
    _ = _warehouse()
    _ = setenv("ORCA_TEST_DIE", "")
    _ = Loads.run(parse_months("2024-01"))
    _ = Loads.run(parse_months("2024-01"))
    assert_equal(_snapshots(_table("out")), 2)


def test_a_resumed_step_reads_its_pinned_snapshot() raises:
    _ = _warehouse()
    var src = _table("src")
    _ = src.append(_rows("2024-01", 4))
    var store = _store("copies")
    var months = parse_months("2024-01")
    _ = setenv("ORCA_TEST_DIE", "1")
    var res = Copies.execute(store, RunResult(), months, store.run)
    assert_true(not res.succeeded())
    assert_equal(_count(_table("src")), 9)  # the late commit landed
    _ = setenv("ORCA_TEST_DIE", "")
    res = Copies.execute(store, _resume(store), months, store.run)
    assert_true(res.succeeded())
    assert_equal(res.output[Int]("copy_count", "2024-01"), 4)
    # Without the pin, a new run sees the late rows.
    res = Copies.run(months)
    assert_equal(res.output[Int]("copy_count", "2024-01"), 9)


def test_an_empty_table_stays_empty_for_the_step() raises:
    _ = _warehouse()
    var store = _store("copies")
    var months = parse_months("2024-01")
    _ = setenv("ORCA_TEST_DIE", "1")
    _ = Copies.execute(store, RunResult(), months, store.run)
    _ = setenv("ORCA_TEST_DIE", "")
    var res = Copies.execute(store, _resume(store), months, store.run)
    assert_equal(res.output[Int]("copy_count", "2024-01"), 0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
