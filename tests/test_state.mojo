"""The state in Postgres: `tests/store_suite.mojo` on `PgStore`, and what
only Postgres has. Run under scripts/with-postgres.sh, which sets
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

from orca import PgStore, RunResult, State, parse_months
from store_suite import (
    Touching,
    check_a_store_that_lost_its_run_stops,
    check_claim_needs_a_stale_heartbeat,
    check_execute_records_attempts_and_retries,
    check_finished_runs,
    check_killed_anywhere_then_resumed,
    check_reads_are_pinned_across_attempts,
    check_resume_skips_what_succeeded,
    check_versions,
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


def test_migrate_is_idempotent() raises:
    var db = _db()
    db.migrate()
    assert_equal(
        _count("SELECT max(version) AS n FROM orca.schema_version"), 34
    )
    assert_equal(_count("SELECT count(*) AS n FROM orca.schema_version"), 34)


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


def test_reads_and_commits_go_through_the_store() raises:
    var db = _db()
    db.register("touching", hex(Touching.registry_hash()), "{}")
    var run = db.create_run("touching", "h", "x", "manual", "a:1")
    var store = PgStore(db^, getenv("ORCA_DATABASE_URL"), run, "a:1", "h")
    var res = Touching.execute(store, RunResult(), parse_months("2024-03"), run)
    # The second attempt read the snapshot the first one did.
    assert_equal(res.output[Int]("touch", "2024-03"), 101)
    var att = String(
        " r JOIN orca.attempts a ON a.id = r.attempt JOIN orca.steps s ON",
        " s.id = a.step WHERE s.run = ",
        run,
    )
    assert_equal(
        _count(
            String(
                "SELECT count(*) AS n FROM orca.reads",
                att,
                " AND r.snapshot_id = 101 AND r.dataset = 'ns.in'",
            )
        ),
        2,
    )
    # Both commits are on record, the failed attempt's too.
    assert_equal(
        _count(
            String(
                "SELECT count(*) AS n FROM orca.commits",
                att,
                " AND r.table_name = '/wh/out' AND r.snapshot_id = 3",
            )
        ),
        2,
    )


def _store() raises -> PgStore:
    return PgStore(getenv("ORCA_DATABASE_URL"))


def test_pg_claim_needs_a_stale_heartbeat() raises:
    var s = _store()
    check_claim_needs_a_stale_heartbeat(s)


def test_pg_finished_runs() raises:
    var s = _store()
    check_finished_runs(s)


def test_pg_execute_records_attempts_and_retries() raises:
    var s = _store()
    check_execute_records_attempts_and_retries(s)


def test_pg_resume_skips_what_succeeded() raises:
    var s = _store()
    check_resume_skips_what_succeeded(s)


def test_pg_lost_run_stops() raises:
    var s = _store()
    check_a_store_that_lost_its_run_stops(s)


def test_pg_reads_are_pinned_across_attempts() raises:
    var s = _store()
    check_reads_are_pinned_across_attempts(s)


def test_pg_versions() raises:
    var s = _store()
    check_versions(s)


def test_pg_killed_anywhere_then_resumed() raises:
    var s = _store()
    check_killed_anywhere_then_resumed(s)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
