"""Containment (orca.cgroup): the limit, margin and retry arithmetic, and
reading /proc/self/cgroup and memory.events."""

from std.os import setenv, unsetenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from orca.cgroup import (
    MemoryKnobs,
    ceiling_mb,
    event,
    growth_pct_of,
    has_word,
    max_mb,
    oom_text,
    own_path,
    owner_of,
    raised_mb,
    raised_request,
    run_name,
    start_limit_mb,
)


def test_max_mb() raises:
    assert_equal(max_mb(1024, 25), 1280)
    assert_equal(max_mb(100, 25), 125)
    assert_equal(max_mb(101, 25), 127)  # 126.25, up
    assert_equal(max_mb(64, 0), 64)


def test_raised_mb() raises:
    assert_equal(raised_mb(1024, 150, 4096), 1536)
    assert_equal(raised_mb(3072, 150, 4096), 4096)  # at the ceiling
    assert_equal(raised_mb(4096, 150, 4096), 4096)  # nothing more
    assert_equal(raised_mb(101, 150, 0), 152)  # no ceiling, rounded up
    # Never down, though the ceiling is under the limit.
    assert_equal(raised_mb(2048, 150, 1024), 2048)


def test_start_limit_mb() raises:
    # The grant, else the request's maximum, else the default or what an
    # out-of-memory kill raised it to; at most the control cap.
    assert_equal(start_limit_mb(3000, 8192, 6000, 0, 0), 3000)
    assert_equal(start_limit_mb(0, 8192, 6000, 0, 0), 8192)
    assert_equal(start_limit_mb(0, 0, 6000, 0, 0), 6000)
    assert_equal(start_limit_mb(0, 0, 1000, 1500, 0), 1500)
    assert_equal(start_limit_mb(0, 0, 6000, 9000, 0), 9000)
    assert_equal(start_limit_mb(3000, 0, 0, 0, 1024), 1024)
    assert_equal(start_limit_mb(0, 0, 1024, 1536, 1024), 1024)


def test_ceiling_mb() raises:
    assert_equal(ceiling_mb(8192, 6000, 0), 8192)
    assert_equal(ceiling_mb(0, 6000, 0), 6000)
    assert_equal(ceiling_mb(8192, 6000, 1024), 1024)
    assert_equal(ceiling_mb(0, 0, 1024), 1024)


def test_raised_request() raises:
    assert_equal(raised_request(2048, 8192, 0), 2048)
    assert_equal(raised_request(2048, 8192, 1024), 2048)
    assert_equal(raised_request(2048, 8192, 3072), 3072)
    assert_equal(raised_request(2048, 8192, 9000), 8192)


def test_retry_sequence() raises:
    # 64 MB, killed: 96, then 144, then the 200 MB maximum.
    var limit = 64
    var seen = List[Int]()
    for _ in range(4):
        limit = raised_mb(limit, 150, 200)
        seen.append(limit)
    assert_equal(seen[0], 96)
    assert_equal(seen[1], 144)
    assert_equal(seen[2], 200)
    assert_equal(seen[3], 200)


def test_oom_text() raises:
    var t = oom_text(1536, 1920, 2304)
    assert_true(t.startswith("out of memory at 1.5 GB"), t)
    assert_true("memory.max (1.9 GB)" in t, t)
    assert_true("its retry asks for 2.3 GB" in t, t)
    var top = oom_text(64, 80, 64)
    assert_true(top.startswith("out of memory at 64 MB"), top)
    assert_true("the most it may ask for" in top, top)


def test_own_path() raises:
    assert_equal(own_path("0::/\n"), "/")
    assert_equal(
        own_path("0::/system.slice/docker-abc.scope\n"),
        "/system.slice/docker-abc.scope",
    )
    # Hybrid: v1 controllers first, the unified line last.
    assert_equal(
        own_path("12:memory:/x\n11:cpu:/x\n0::/user.slice\n"), "/user.slice"
    )
    assert_equal(own_path("12:memory:/x\n11:cpu,cpuacct:/x\n"), "")
    assert_equal(own_path(""), "")


def test_events() raises:
    var text = "low 0\nhigh 1520\nmax 27\noom 1\noom_kill 4\noom_group_kill 1\n"
    assert_equal(event(text, "oom_kill"), 4)
    assert_equal(event(text, "oom_group_kill"), 1)
    assert_equal(event(text, "high"), 1520)
    assert_equal(event(text, "nope"), 0)
    assert_equal(event("", "oom_kill"), 0)
    assert_equal(event("oom_kill x\n", "oom_kill"), 0)


def test_controllers() raises:
    assert_true(has_word("cpuset cpu io memory pids\n", "memory"))
    assert_false(has_word("cpuset cpu io pids\n", "memory"))
    assert_false(has_word("", "memory"))


def test_names() raises:
    assert_equal(run_name(42, 3, 1234), "orca-run-42-3-1234")
    assert_equal(owner_of("orca-run-42-3-1234"), 1234)
    assert_equal(owner_of("orca"), -1)
    assert_equal(owner_of("orca-run-x"), -1)


def test_knobs() raises:
    _ = unsetenv("ORCA_RUN_MEMORY_MB")
    _ = unsetenv("ORCA_RUN_MEMORY_MARGIN")
    _ = unsetenv("ORCA_RUN_MEMORY_GROWTH")
    var k = MemoryKnobs.from_env()
    assert_equal(k.default_mb, 0)
    assert_equal(k.margin_pct, 25)
    assert_equal(k.growth_pct, 150)
    _ = setenv("ORCA_RUN_MEMORY_GROWTH", "2", True)
    _ = setenv("ORCA_RUN_MEMORY_MARGIN", "10", True)
    _ = setenv("ORCA_RUN_MEMORY_MB", "512", True)
    k = MemoryKnobs.from_env()
    assert_equal(k.growth_pct, 200)
    assert_equal(k.margin_pct, 10)
    assert_equal(k.default_mb, 512)
    assert_equal(growth_pct_of("1.25"), 125)
    for bad in ["0.5", "x", "-1"]:
        with assert_raises(contains="ORCA_RUN_MEMORY_GROWTH"):
            _ = growth_pct_of(bad)
    _ = setenv("ORCA_RUN_MEMORY_MARGIN", "-3", True)
    with assert_raises(contains="ORCA_RUN_MEMORY_MARGIN"):
        _ = MemoryKnobs.from_env()
    _ = unsetenv("ORCA_RUN_MEMORY_MB")
    _ = unsetenv("ORCA_RUN_MEMORY_MARGIN")
    _ = unsetenv("ORCA_RUN_MEMORY_GROWTH")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
