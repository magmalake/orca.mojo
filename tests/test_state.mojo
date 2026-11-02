"""The state in Postgres. Run under scripts/with-postgres.sh, which sets
ORCA_DATABASE_URL to a throwaway database."""

from std.os import getenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from postgres import Connection, Params

from dista import Seq
from orca import (
    Ctx,
    Gather,
    Mapped,
    Month,
    PgStore,
    Pipeline,
    RunResult,
    State,
    Step,
    parse_months,
)


def _db() raises -> State:
    var db = State(getenv("ORCA_DATABASE_URL"))
    db.migrate()
    return db^


def _count(sql: String) raises -> Int:
    var conn = Connection(getenv("ORCA_DATABASE_URL"))
    return Int(conn.query(sql).row(0).int64("n"))


def _new_run(mut db: State, owner: String) raises -> Int:
    db.register("t", "0x1", "{}")
    return db.create_run("t", "0x1", "2024-01..2024-03", "manual", owner)


# --- a pipeline whose second step fails its first attempt ---


def month_no(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


def flaky(mut ctx: Ctx, n: Int) raises -> Int:
    if ctx.attempt == 1 and n == 2:
        raise Error("first try fails")
    return n * 10


def add(mut ctx: Ctx, xs: Seq[Int]) raises -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return t


comptime MonthNo = Step["month_no", Month, Int, month_no, Mapped]
comptime Flaky = Step[
    "flaky", Int, Int, flaky, Mapped, after=MonthNo, retries=1
]
comptime Add = Step["add", Seq[Int], Int, add, Gather, after=Flaky]
comptime Retrying = Pipeline["retrying", MonthNo, Flaky, Add]


def test_migrate_is_idempotent() raises:
    var db = _db()
    db.migrate()
    assert_equal(_count("SELECT max(version) AS n FROM orca.schema_version"), 6)
    assert_equal(_count("SELECT count(*) AS n FROM orca.schema_version"), 6)


def test_claim_needs_a_stale_heartbeat() raises:
    var db = _db()
    var run = _new_run(db, "a:1")
    with assert_raises(contains="is held by a:1"):
        db.claim(run, "b:2", 30.0)
    # A heartbeat older than the limit can be taken over.
    _ = db.conn.execute(
        (
            "UPDATE orca.runs SET heartbeat = now() - interval '1 minute' WHERE"
            " id = $1"
        ),
        Params().int64(Int64(run)),
    )
    db.claim(run, "b:2", 30.0)
    assert_equal(db.run_info(run).owner, "b:2")


def test_a_queued_run_can_be_claimed() raises:
    var db = _db()
    var run = _new_run(db, "a:1")
    _ = db.conn.execute(
        (
            "UPDATE orca.runs SET status = 'queued', owner = NULL, heartbeat ="
            " NULL WHERE id = $1"
        ),
        Params().int64(Int64(run)),
    )
    db.claim(run, "b:2", 30.0)
    assert_equal(db.run_info(run).status, "running")
    assert_equal(db.run_info(run).owner, "b:2")


def test_finished_runs() raises:
    var db = _db()
    var run = _new_run(db, "a:1")
    db.finish_run(run, "a:1", "failed")
    # A failed run's heartbeat is cleared: it can be resumed at once.
    db.claim(run, "b:2", 30.0)
    db.finish_run(run, "b:2", "succeeded")
    with assert_raises(contains="is succeeded"):
        db.claim(run, "c:3", 30.0)
    with assert_raises(contains="no run"):
        db.claim(99999, "c:3", 30.0)


def test_execute_records_attempts_and_retries() raises:
    var db = _db()
    db.register("retrying", hex(Retrying.registry_hash()), Retrying.describe())
    var run = db.create_run("retrying", "h", "x", "manual", "a:1")
    var store = PgStore(db^, getenv("ORCA_DATABASE_URL"), run, "a:1", "h")
    var res = Retrying.execute(
        store, RunResult(), parse_months("2024-01..2024-03"), run
    )
    assert_true(res.succeeded())
    assert_equal(res.output[Int]("add"), 60)
    var steps = store.db.steps(run)
    assert_equal(len(steps), 7)
    assert_equal(steps[0].step, "month_no")
    assert_equal(steps[6].step, "add")
    for s in steps:
        assert_equal(s.status, "succeeded")
        assert_equal(
            s.attempts,
            2 if s.step == "flaky" and s.partition == "2024-02" else 1,
        )
    assert_equal(
        _count(
            "SELECT count(*) AS n FROM orca.attempts WHERE status = 'failed'"
            " AND error = 'first try fails'"
        ),
        1,
    )


def test_resume_skips_what_succeeded() raises:
    var db = _db()
    var run = db.create_run("retrying", "h", "x", "manual", "a:1")
    var store = PgStore(db^, getenv("ORCA_DATABASE_URL"), run, "a:1", "h")
    var months = parse_months("2024-01..2024-02")
    _ = Retrying.execute(store, RunResult(), months, run)
    # Pretend the process died during `add`: its attempt is left running.
    _ = store.db.conn.execute(
        (
            "UPDATE orca.steps SET status = 'running' WHERE run = $1 AND step ="
            " 'add'"
        ),
        Params().int64(Int64(run)),
    )
    _ = store.db.conn.execute(
        (
            "UPDATE orca.attempts a SET status = 'running' FROM orca.steps s"
            " WHERE a.step = s.id AND s.run = $1 AND s.step = 'add'"
        ),
        Params().int64(Int64(run)),
    )
    assert_equal(store.db.interrupt(run), 1)
    var prior = store.db.prior(run)
    assert_equal(len(prior.steps), 4)
    var res = Retrying.execute(store, prior^, months, run)
    assert_true(res.succeeded())
    assert_equal(res.output[Int]("add"), 30)
    var steps = store.db.steps(run)
    for s in steps:
        if s.step == "add":
            assert_equal(s.attempts, 2)
            assert_equal(s.status, "succeeded")
        elif s.step == "month_no":
            assert_equal(s.attempts, 1)


def test_a_store_that_lost_its_run_stops() raises:
    var db = _db()
    var run = db.create_run("retrying", "h", "x", "manual", "a:1")
    _ = db.conn.execute(
        "UPDATE orca.runs SET owner = 'b:2' WHERE id = $1",
        Params().int64(Int64(run)),
    )
    var store = PgStore(db^, getenv("ORCA_DATABASE_URL"), run, "a:1", "h")
    with assert_raises(contains="lost run"):
        _ = Retrying.execute(store, RunResult(), parse_months("2024-01"), run)
    assert_equal(
        _count(
            String("SELECT count(*) AS n FROM orca.steps WHERE run = ", run)
        ),
        0,
    )


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
