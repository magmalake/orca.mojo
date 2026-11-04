"""`tests/store_suite.mojo` on the stores that need no server."""

from std.testing import TestSuite, assert_equal, assert_raises, assert_true

from orca import MemoryStore, RunResult, parse_months
from store_suite import (
    Retrying,
    Touching,
    check_a_store_that_lost_its_run_stops,
    check_claim_needs_a_stale_heartbeat,
    check_execute_records_attempts_and_retries,
    check_finished_runs,
    check_killed_anywhere_then_resumed,
    check_reads_are_pinned_across_attempts,
    check_resume_skips_what_succeeded,
    check_versions,
    new_run,
)


def test_memory_claim_needs_a_stale_heartbeat() raises:
    var s = MemoryStore()
    check_claim_needs_a_stale_heartbeat(s)


def test_memory_finished_runs() raises:
    var s = MemoryStore()
    check_finished_runs(s)


def test_memory_execute_records_attempts_and_retries() raises:
    var s = MemoryStore()
    check_execute_records_attempts_and_retries(s)


def test_memory_resume_skips_what_succeeded() raises:
    var s = MemoryStore()
    check_resume_skips_what_succeeded(s)


def test_memory_lost_run_stops() raises:
    var s = MemoryStore()
    check_a_store_that_lost_its_run_stops(s)


def test_memory_reads_are_pinned_across_attempts() raises:
    var s = MemoryStore()
    check_reads_are_pinned_across_attempts(s)


def test_memory_versions() raises:
    var s = MemoryStore()
    check_versions(s)


def test_memory_killed_anywhere_then_resumed() raises:
    var s = MemoryStore()
    check_killed_anywhere_then_resumed(s)


def test_memory_runs_are_never_positive() raises:
    # So `Ctx.made` recognises no commit of theirs: another process's
    # in-memory run would have the same number.
    var s = MemoryStore()
    assert_equal(new_run(s, "a:1"), 0)
    assert_equal(new_run(s, "a:1"), -1)
    assert_equal(s.runs(10)[0].id, -1)
    with assert_raises(contains="no run 1"):
        _ = s.run_info(1)


def test_memory_records_reads_and_commits() raises:
    var s = MemoryStore()
    check_reads_are_pinned_across_attempts(s)
    # Two attempts, each with one read and one commit, in order.
    assert_equal(len(s.records), 4)
    assert_true(not s.records[0].commit and s.records[1].commit)
    assert_equal(s.records[1].table.table, "/wh/out")
    assert_equal(s.records[3].table.snapshot_id, 3)


def test_memory_cancel_stops_before_the_next_attempt() raises:
    var s = MemoryStore()
    var run = new_run(s, "a:1")
    s.request_cancel(run)
    assert_true(s.cancel_requested())
    with assert_raises(contains="was cancelled"):
        _ = Retrying.execute(s, RunResult(), parse_months("2024-01"), run)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
