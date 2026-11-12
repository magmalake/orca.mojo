"""Requests, admission and grants (morca.resources): the parsing, what fits,
the grants and the reason a run waits, and `auto`."""

from std.os import setenv, unsetenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from morca.resources import (
    Grant,
    Request,
    Room,
    admit,
    auto_count,
    canonical_cpus,
    canonical_memory_gb,
    gb_text,
    host_capacity,
    host_room,
    parse_cpus,
    parse_memory_gb,
    resolve_auto,
    run_env,
)


def test_parse_cpus() raises:
    assert_equal(parse_cpus("4")[0], 4)
    assert_equal(parse_cpus("4")[1], 4)
    assert_equal(parse_cpus("1..16")[0], 1)
    assert_equal(parse_cpus("1..16")[1], 16)
    assert_equal(canonical_cpus("2..2"), "2")
    assert_equal(canonical_cpus("1..16"), "1..16")
    for bad in ["0", "0..4", "4..2", "1..2000", "x", "1.5", "", "..4"]:
        with assert_raises(contains="CPUs"):
            _ = parse_cpus(bad)


def test_parse_memory_gb() raises:
    assert_equal(parse_memory_gb("2..24")[0], 2048)
    assert_equal(parse_memory_gb("2..24")[1], 24 * 1024)
    assert_equal(parse_memory_gb("0.5")[0], 512)
    assert_equal(parse_memory_gb("0.5")[1], 512)
    assert_equal(canonical_memory_gb("2..24"), "2..24")
    assert_equal(canonical_memory_gb("2.0..24"), "2..24")
    assert_equal(canonical_memory_gb("1.5..1.5"), "1.5")
    assert_equal(canonical_memory_gb("8"), "8")
    for bad in ["0", "-1", "24..2", "0..2", "x", "1..70000", ""]:
        with assert_raises(contains="GB"):
            _ = parse_memory_gb(bad)


def test_parse_auto() raises:
    # `auto`: learned (morca.recommend), (0, 0); `auto..MAX`: up to MAX.
    assert_equal(parse_cpus("auto")[0], 0)
    assert_equal(parse_cpus("auto")[1], 0)
    assert_equal(parse_cpus("auto..16")[0], 0)
    assert_equal(parse_cpus("auto..16")[1], 16)
    assert_equal(parse_memory_gb("auto")[1], 0)
    assert_equal(parse_memory_gb("auto..24")[0], 0)
    assert_equal(parse_memory_gb("auto..24")[1], 24 * 1024)
    assert_equal(parse_memory_gb("auto..0.5")[1], 512)
    assert_equal(canonical_cpus("auto"), "auto")
    assert_equal(canonical_cpus("auto..16"), "auto..16")
    assert_equal(canonical_memory_gb("auto"), "auto")
    assert_equal(canonical_memory_gb("auto..24.0"), "auto..24")
    for bad in ["auto..", "auto..0", "auto..x", "auto..2000", "1..auto"]:
        with assert_raises(contains="CPUs"):
            _ = parse_cpus(bad)
    with assert_raises(contains="lower end"):
        _ = parse_cpus("1..auto")
    for bad in [
        "auto..",
        "auto..0",
        "auto..x",
        "2..auto",
        "Auto",
        "auto..auto",
    ]:
        with assert_raises(contains="GB"):
            _ = parse_memory_gb(bad)
    var r = Request.of("auto", "auto..24")
    assert_true(r.any())
    assert_true(r.auto())
    assert_true(r.cpus_auto and r.memory_auto)
    assert_equal(r.cpus_max, 0)
    assert_equal(r.memory_min_mb, 0)
    assert_equal(r.memory_max_mb, 24 * 1024)
    assert_equal(r.cpus_text(), "auto")
    assert_equal(r.memory_gb_text(), "auto..24")
    assert_false(Request.of("2", "1..4").auto())
    assert_equal(Request(0, 0, 3328, 3328).memory_gb_text(), "3.25")
    assert_equal(Request(1, 4, 3328, 24576).cpus_text(), "1..4")
    assert_equal(Request(1, 4, 3328, 24576).memory_gb_text(), "3.25..24")
    assert_equal(Request(0, 0, 0, 0).memory_gb_text(), "")


def test_gb_text() raises:
    assert_equal(gb_text(2048), "2")
    assert_equal(gb_text(1331), "1.3")
    assert_equal(gb_text(0), "0")
    assert_equal(gb_text(-5), "0")


def test_request_of() raises:
    var none = Request.of("", "")
    assert_false(none.any())
    var r = Request.of("1..16", "2..24")
    assert_true(r.any())
    assert_equal(r.cpus_min, 1)
    assert_equal(r.memory_max_mb, 24 * 1024)
    assert_true(Request.of("", "1").any())


def test_admit_grants_up_to_the_maximum_then_what_is_left() raises:
    var room = Room(8, 8192, 9, 9728)
    var got = admit(Request.of("1..6", "2..5"), room, 0, 0, 3)
    assert_equal(len(got[0]), 2)
    assert_equal(got[0][0].cpus, 6)
    assert_equal(got[0][0].memory_mb, 5120)
    # The second gets what the first left: 2 CPUs, 3 GB.
    assert_equal(got[0][1].cpus, 2)
    assert_equal(got[0][1].memory_mb, 3072)
    # The third's 2 GB and 1 CPU don't fit in the nothing left.
    assert_equal(
        got[1],
        (
            "waiting: needs 2 GB, 0 GB free of grants; needs 1 CPU, 0 free of"
            " grants"
        ),
    )


def test_admit_counts_the_running_runs_grants() raises:
    var room = Room(4, 6144, 5, 7680)
    var got = admit(Request.of("", "2..4"), room, 0, 4813, 1)
    assert_equal(len(got[0]), 0)
    assert_equal(got[1], "waiting: needs 2 GB, 1.3 GB free of grants")
    got = admit(Request.of("2", ""), room, 3, 0, 2)
    assert_equal(len(got[0]), 0)
    assert_equal(got[1], "waiting: needs 2 CPUs, 1 free of grants")
    got = admit(Request.of("2", "3"), room, 3, 4096, 1)
    assert_equal(
        got[1],
        (
            "waiting: needs 3 GB, 2 GB free of grants; needs 2 CPUs, 1 free of"
            " grants"
        ),
    )
    # A grant of only the resource asked for.
    got = admit(Request.of("2", ""), room, 0, 0, 1)
    assert_equal(got[0][0].cpus, 2)
    assert_equal(got[0][0].memory_mb, 0)
    assert_equal(got[1], "")


def test_admit_more_than_the_host() raises:
    var room = Room(3, 6656, 4, 8192)
    var got = admit(Request.of("", "30"), room, 0, 0, 1)
    assert_equal(
        got[1],
        "waiting: needs 30 GB, more than the 6.5 GB this host grants at most",
    )
    got = admit(Request.of("1", ""), room, 3, 0, 1, "worker bgent-1")
    assert_equal(
        got[1], "waiting: needs 1 CPU, 0 free of grants on worker bgent-1"
    )


def test_run_env() raises:
    var env: List[Tuple[String, String]] = [
        ("DISTA_WORKER_SLOTS", "auto"),
        ("GHARCHIVE_PARALLEL", "auto"),
        ("X", "1"),
    ]
    var got = run_env(env, Grant(8, 4096), ["MORCA_WORKERS=2"])
    assert_equal(len(got), 6)
    assert_equal(got[0], "DISTA_WORKER_SLOTS=4")
    assert_equal(got[1], "GHARCHIVE_PARALLEL=auto")
    assert_equal(got[2], "X=1")
    assert_equal(got[3], "MORCA_GRANT_CPUS=8")
    assert_equal(got[4], "MORCA_GRANT_MEMORY_MB=4096")
    assert_equal(got[5], "MORCA_WORKERS=2")
    got = run_env(env, Grant(1, 0), ["MORCA_WORKERS=4"])
    assert_equal(got[0], "DISTA_WORKER_SLOTS=1")
    assert_equal(len(got), 5)
    got = run_env(env, Grant.none())
    assert_equal(got[0], "DISTA_WORKER_SLOTS=0")
    assert_equal(len(got), 3)
    var plain = resolve_auto(env)
    assert_equal(plain[0][1], "0")
    assert_equal(plain[1][1], "auto")


def test_auto_count() raises:
    _ = unsetenv("MORCA_TEST_PARALLEL")
    assert_equal(auto_count("MORCA_TEST_PARALLEL", 16), 16)
    _ = setenv("MORCA_TEST_PARALLEL", "3")
    assert_equal(auto_count("MORCA_TEST_PARALLEL", 16), 3)
    _ = setenv("MORCA_TEST_PARALLEL", "auto")
    _ = setenv("MORCA_GRANT_CPUS", "6")
    _ = setenv("MORCA_GRANT_MEMORY_MB", "4000")
    assert_equal(auto_count("MORCA_TEST_PARALLEL", 16), 6)
    assert_equal(auto_count("MORCA_TEST_PARALLEL", 16, memory_mb_each=1500), 2)
    assert_equal(auto_count("MORCA_TEST_PARALLEL", 16, memory_mb_each=9000), 1)
    _ = setenv("MORCA_TEST_PARALLEL", "many")
    with assert_raises(contains="a number or auto"):
        _ = auto_count("MORCA_TEST_PARALLEL", 16)
    _ = unsetenv("MORCA_TEST_PARALLEL")
    _ = unsetenv("MORCA_GRANT_CPUS")
    _ = unsetenv("MORCA_GRANT_MEMORY_MB")


def test_host() raises:
    var cap = host_capacity()
    assert_true(cap[0] >= 1)
    assert_true(cap[1] > 0)
    _ = setenv("MORCA_HOST_CPUS", "4")
    _ = setenv("MORCA_HOST_MEMORY_MB", "8192")
    var room = host_room()
    assert_equal(room.cpus, 3)
    assert_equal(room.memory_mb, 8192 - 1536)
    room = host_room(worker=True)
    assert_equal(room.cpus, 4)
    assert_equal(room.memory_mb, 8192 - 512)
    _ = setenv("MORCA_RESERVE_MEMORY_MB", "10000")
    assert_equal(host_room().memory_mb, 0)
    _ = setenv("MORCA_RESERVE_CPUS", "two")
    with assert_raises(contains="MORCA_RESERVE_CPUS"):
        _ = host_room()
    _ = unsetenv("MORCA_RESERVE_CPUS")
    _ = unsetenv("MORCA_RESERVE_MEMORY_MB")
    _ = unsetenv("MORCA_HOST_CPUS")
    _ = unsetenv("MORCA_HOST_MEMORY_MB")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
