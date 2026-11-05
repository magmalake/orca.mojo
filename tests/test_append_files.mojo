"""Data files written in dista tasks and committed by the step with
`append_files`: once, whether the step is retried or the run resumed. Run
under scripts/with-postgres.sh, in the data environment.

The runtime's workers are forked before any test, so they see the
warehouse `main` sets then (`ORCA_WAREHOUSE`), as a cluster's workers see
theirs; each test has datasets of its own in it.
"""

from std.ffi import external_call
from std.os import getenv, setenv
from std.os.path import exists
from std.testing import assert_equal, assert_true

from dista import ObjectRef, Task3
from dista.codec import decode, encode
from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.io import FileIO
from iceberg.manifest import CONTENT_POSITION_DELETES
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
    Tasks,
    parse_months,
)
from orca.tables import (
    DataFiles,
    FileWriter,
    append_files,
    find_commit,
    load_dataset,
    table,
    write_files,
)

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"month","required":true,"type":"string"}]}'
)
comptime PARTS = 3
"""Tasks per month."""
comptime ROWS = 4
"""Rows per batch; a task writes two batches."""


def _pid() -> Int:
    return Int(external_call["getpid", Int32]())


def _rows(month: String, start: Int, n: Int) raises -> List[RecordBatch]:
    var schema = Schema.parse(SCHEMA)
    var ids = ColumnBuilder.of(schema, 1)
    var months = ColumnBuilder.of(schema, 2)
    for i in range(n):
        ids.add(Datum.long_(Int64(start + i)))
        months.add(Datum.string_(month))
    return [batch_of([ids^, months^])]


struct Written(Copyable, Movable):
    """What a task sends back: its files, and plain fields beside them.
    No codec of its own: dista encodes it field by field, `DataFiles` by
    its `Codec`."""

    var files: DataFiles
    var rows: Int
    var pid: Int

    def __init__(out self, var files: DataFiles, rows: Int, pid: Int):
        self.files = files^
        self.rows = rows
        self.pid = pid


def write_part(dataset: String, month: String, part: Int) raises -> Written:
    """On a worker: two batches of the month's part, one written with
    `FileWriter` a batch at a time and one with `write_files`, uncommitted."""
    var t = load_dataset(dataset)
    var w = FileWriter(t)
    w.put(_rows(month, part * 2 * ROWS, ROWS)[0].copy())
    var rows = w.rows
    var files = w^.finish()
    files.add(write_files(t, _rows(month, (part * 2 + 1) * ROWS, ROWS)))
    return Written(files^, rows + ROWS, _pid())


comptime WritePart = Task3[String, String, Int, Written, write_part]
comptime FileTasks = Tasks[WritePart]


def _die_once(name: String) raises -> Bool:
    """True the first time for `name` while ORCA_TEST_DIE is `once`, and
    every time while it's `always`."""
    var how = getenv("ORCA_TEST_DIE")
    if how == "always":
        return True
    if how != "once":
        return False
    var marker = String(getenv("ORCA_WAREHOUSE"), "/died-", name)
    if exists(marker):
        return False
    with open(marker, "w") as f:
        f.write("1")
    return True


def load[
    dataset: StaticString, check_first: Bool
](mut ctx: Ctx, m: Month) raises -> Int64:
    """The month's parts written by tasks and committed as one append.
    ORCA_TEST_DIE kills the attempt right after it commits. With
    `check_first`, a retry that finds its commit writes nothing; without,
    the tasks write the files again and `append_files` alone keeps them
    from being committed twice."""
    var out = table(ctx, dataset)
    if check_first and find_commit(ctx, out) >= 0:
        ctx.log("already committed")
        return append_files(ctx, out, DataFiles())
    ref rt = ctx.runtime[FileTasks]()
    var refs = List[ObjectRef[Written]]()
    for p in range(PARTS):
        refs.append(rt.remote[WritePart](String(dataset), String(m), p))
    var files = DataFiles()
    for r in refs:
        var got = rt.get(r)
        if got.pid == _pid():
            raise Error("a task ran in the step's own process")
        if got.rows != 2 * ROWS:
            raise Error("a task wrote ", got.rows, " rows")
        files.add(got.files)
    var id = append_files(ctx, out, files)
    if _die_once(String(dataset, "-", m)):
        raise Error("died after committing")
    return id


def undeclared(mut ctx: Ctx, m: Month) raises -> Int64:
    """Commits to db.undeclared, which it doesn't declare."""
    var t = load_dataset("db.undeclared")
    return append_files(ctx, t, write_files(t, _rows(String(m), 0, 1)))


def delete_file(mut ctx: Ctx, m: Month) raises -> Int64:
    """Hands append_files a delete file."""
    var t = table(ctx, "db.deletes")
    var files = write_files(t, _rows(String(m), 0, 1))
    files.files[0].content = CONTENT_POSITION_DELETES
    return append_files(ctx, t, files)


comptime LoadA = Step[
    "load", Month, Int64, load["db.a", True], Mapped, writes=["db.a"]
]
comptime ResumeA = Pipeline["append_files_resume", LoadA, tasks=FileTasks]
comptime LoadB = Step[
    "load",
    Month,
    Int64,
    load["db.b", False],
    Mapped,
    retries=1,
    writes=["db.b"],
]
comptime RetryB = Pipeline["append_files_retry", LoadB, tasks=FileTasks]
comptime Undeclared = Step["undeclared", Month, Int64, undeclared, Mapped]
comptime Undeclareds = Pipeline["append_files_undeclared", Undeclared]
comptime DeleteFile = Step[
    "delete_file", Month, Int64, delete_file, Mapped, writes=["db.deletes"]
]
comptime DeleteFiles = Pipeline["append_files_deletes", DeleteFile]


def _table(name: String) raises -> Table:
    return Table.load_local(String(getenv("ORCA_WAREHOUSE"), "/db/", name))


def _count(t: Table) raises -> Int:
    var n = 0
    for b in t.scan().to_batches():
        n += b.num_rows
    return n


def _parquet_files(name: String) raises -> Int:
    """The data files on disk under the table, committed or not."""
    var n = 0
    var root = String(getenv("ORCA_WAREHOUSE"), "/db/", name, "/data")
    if not exists(root):
        return 0
    for p in FileIO.local().list(root):
        if p.endswith(".parquet"):
            n += 1
    return n


def _store(run_name: String) raises -> PgStore:
    var url = getenv("ORCA_DATABASE_URL")
    var db = State(url)
    db.migrate()
    db.register(run_name, "h", "{}")
    var run = db.create_run(run_name, "h", "x", "manual", "a:1")
    return PgStore(db^, url, run, "a:1", "h")


def _error(res: RunResult) -> String:
    for st in res.steps:
        if st.error:
            return st.error
    return ""


def test_a_resumed_step_doesnt_append_its_files_twice(runtime: Int) raises:
    var cat = FilesystemCatalog.local(getenv("ORCA_WAREHOUSE"))
    _ = cat.create_table("db", "a", Schema.parse(SCHEMA))
    var store = _store("append_files_resume")
    var months = parse_months("2024-01..2024-02")
    _ = setenv("ORCA_TEST_DIE", "always")
    var res = ResumeA.execute(store, RunResult(), months, store.run, runtime)
    assert_true(not res.succeeded())
    assert_true("died after committing" in _error(res), _error(res))
    var a = _table("a")
    assert_equal(len(a.metadata.snapshots), 2)
    assert_equal(_count(a), 2 * PARTS * 2 * ROWS)
    # Each task wrote a file per batch, all committed.
    var written = _parquet_files("a")
    assert_equal(written, 2 * PARTS * 2)
    # Another process resumes the run: it finds both commits, and neither
    # writes nor commits the files again.
    _ = setenv("ORCA_TEST_DIE", "")
    _ = store.db.interrupt(store.run)
    res = ResumeA.execute(
        store, store.db.prior(store.run), months, store.run, runtime
    )
    assert_true(res.succeeded(), _error(res))
    a = _table("a")
    assert_equal(len(a.metadata.snapshots), 2)
    assert_equal(_count(a), 2 * PARTS * 2 * ROWS)
    assert_equal(_parquet_files("a"), written)
    assert_equal(
        res.output[Int64]("load", "2024-01"),
        a.metadata.snapshots[0].snapshot_id,
    )
    var s = a.metadata.snapshots[1].summary.copy()
    assert_equal(s["operation"], "append")
    assert_equal(s["orca.pipeline"], "append_files_resume")
    assert_equal(s["orca.step"], "load")
    assert_equal(s["orca.partition"], "2024-02")
    assert_equal(s["orca.commit"], "0")
    assert_equal(s["added-data-files"], String(PARTS * 2))
    assert_equal(s["added-records"], String(PARTS * 2 * ROWS))
    # The commits are on record for the run, one per month.
    var conn = Connection(getenv("ORCA_DATABASE_URL"))
    var n = conn.query(
        (
            "SELECT count(DISTINCT c.snapshot_id) AS n FROM orca.commits c"
            " JOIN orca.attempts t ON t.id = c.attempt JOIN orca.steps s ON"
            " s.id = t.step WHERE s.run = $1 AND c.dataset = 'db.a'"
        ),
        Params().int64(Int64(store.run)),
    )
    assert_equal(Int(n.row(0).int64("n")), 2)


def test_a_retried_step_doesnt_append_its_files_twice(runtime: Int) raises:
    var cat = FilesystemCatalog.local(getenv("ORCA_WAREHOUSE"))
    _ = cat.create_table("db", "b", Schema.parse(SCHEMA))
    var store = _store("append_files_retry")
    _ = setenv("ORCA_TEST_DIE", "once")
    var res = RetryB.execute(
        store, RunResult(), parse_months("2024-03"), store.run, runtime
    )
    _ = setenv("ORCA_TEST_DIE", "")
    assert_true(res.succeeded(), _error(res))
    assert_equal(res.steps[0].attempt, 2)
    var b = _table("b")
    assert_equal(len(b.metadata.snapshots), 1)
    assert_equal(_count(b), PARTS * 2 * ROWS)
    # The retry's tasks wrote the files again, and they were left out: the
    # snapshot holds the first attempt's.
    assert_equal(_parquet_files("b"), 2 * PARTS * 2)
    assert_equal(b.metadata.snapshots[0].summary["orca.attempt"], "1")
    assert_equal(
        res.output[Int64]("load", "2024-03"),
        b.metadata.snapshots[0].snapshot_id,
    )


def test_append_files_checks_the_declaration() raises:
    var cat = FilesystemCatalog.local(getenv("ORCA_WAREHOUSE"))
    _ = cat.create_table("db", "undeclared", Schema.parse(SCHEMA))
    var res = Undeclareds.run(parse_months("2024-01"))
    assert_equal(
        _error(res),
        (
            "orca: step undeclared of append_files_undeclared writes"
            " db.undeclared, which it doesn't declare in writes="
        ),
    )
    assert_equal(len(_table("undeclared").metadata.snapshots), 0)


def test_append_files_refuses_a_delete_file() raises:
    var cat = FilesystemCatalog.local(getenv("ORCA_WAREHOUSE"))
    _ = cat.create_table("db", "deletes", Schema.parse(SCHEMA))
    var res = DeleteFiles.run(parse_months("2024-01"))
    assert_true("append_files takes data files" in _error(res), _error(res))
    assert_equal(len(_table("deletes").metadata.snapshots), 0)


def test_data_files_cross_dista_inside_a_struct() raises:
    var cat = FilesystemCatalog.local(getenv("ORCA_WAREHOUSE"))
    var t = cat.create_table("db", "codec", Schema.parse(SCHEMA))
    var w = Written(write_files(t, _rows("2024-01", 0, 5)), 5, 7)
    var bytes = List[UInt8]()
    encode(w, bytes)
    var pos = 0
    var back = decode[Written](Span(bytes), pos)
    assert_equal(pos, len(bytes))
    assert_equal(back.rows, 5)
    assert_equal(back.pid, 7)
    assert_equal(len(back.files), 1)
    assert_equal(back.files.files[0].file_path, w.files.files[0].file_path)
    assert_equal(back.files.files[0].record_count, 5)
    assert_equal(
        len(back.files.files[0].metrics), len(w.files.files[0].metrics)
    )


def main() raises:
    var root = String("build/wh-append-files-", now_ms())
    _ = setenv("ORCA_WAREHOUSE", root)
    # Before any connection, as a run starts its runtime.
    var rt = FileTasks.start(2)
    var runtime = Int(Pointer(to=rt))
    test_data_files_cross_dista_inside_a_struct()
    print("PASS test_data_files_cross_dista_inside_a_struct")
    test_append_files_checks_the_declaration()
    print("PASS test_append_files_checks_the_declaration")
    test_append_files_refuses_a_delete_file()
    print("PASS test_append_files_refuses_a_delete_file")
    test_a_resumed_step_doesnt_append_its_files_twice(runtime)
    print("PASS test_a_resumed_step_doesnt_append_its_files_twice")
    test_a_retried_step_doesnt_append_its_files_twice(runtime)
    print("PASS test_a_retried_step_doesnt_append_its_files_twice")
    _ = rt^
