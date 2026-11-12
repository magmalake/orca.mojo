"""Memory pressure (orca.pressure): parsing PSI files, choosing the file,
and admission's hysteresis."""

from std.os import setenv, unsetenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from morca.pressure import (
    Admission,
    Psi,
    Stall,
    admit_limit,
    cgroup_dirs,
    find_source,
    memory_pressure,
    own_cgroup,
    parse_psi,
    pct_text,
    pick_source,
    read_psi,
)

comptime FIXTURES = "tests/fixtures/psi/"


def _psi(avg10: Float64) -> Optional[Psi]:
    return Psi(Stall(avg10, 0.0, 0.0, 0), Stall.zero(), False)


def test_parse_host_file() raises:
    var p = read_psi(String(FIXTURES, "host.txt"))
    assert_true(Bool(p))
    ref v = p.value()
    assert_equal(v.some.avg10, 23.47)
    assert_equal(v.some.avg60, 8.12)
    assert_equal(v.some.avg300, 1.90)
    assert_equal(v.some.total, 48211933)
    assert_true(v.has_full)
    assert_equal(v.full.avg10, 11.02)
    assert_equal(v.full.avg300, 0.71)
    assert_equal(v.full.total, 20114002)


def test_parse_some_only() raises:
    var p = parse_psi("some avg10=0.50 avg60=0.25 avg300=0.10 total=7\n")
    assert_equal(p.some.avg10, 0.5)
    assert_false(p.has_full)
    var q = read_psi(String(FIXTURES, "some-only.txt"))
    assert_true(Bool(q))
    assert_equal(q.value().some.avg10, 0.0)


def test_parse_refuses() raises:
    with assert_raises(contains="not a PSI line"):
        _ = parse_psi("some avg10=x avg60=0 avg300=0 total=0")
    with assert_raises(contains="not a PSI line"):
        _ = parse_psi("some avg10=1.0 total=0")
    with assert_raises(contains="not a PSI line"):
        _ = parse_psi("cpu avg10=1.0 avg60=0 avg300=0 total=0")
    with assert_raises(contains="no `some` line"):
        _ = parse_psi("full avg10=1.0 avg60=0 avg300=0 total=0")
    with assert_raises(contains="no `some` line"):
        _ = parse_psi("")
    # What a reader takes for none: no file, an unreadable one, junk.
    assert_false(Bool(read_psi(String(FIXTURES, "bad.txt"))))
    assert_false(Bool(read_psi(String(FIXTURES, "missing.txt"))))
    assert_false(Bool(read_psi("")))


def test_own_cgroup() raises:
    assert_equal(own_cgroup("0::/\n"), "/")
    assert_equal(
        own_cgroup("12:memory:/docker/abc\n0::/system.slice/morca.service\n"),
        "/system.slice/morca.service",
    )
    assert_equal(own_cgroup("12:memory:/docker/abc\n"), "")


def test_cgroup_dirs() raises:
    var root = cgroup_dirs("/")
    assert_equal(len(root), 1)
    assert_equal(root[0], "/sys/fs/cgroup")
    var deep = cgroup_dirs("/system.slice/morca.service", "/cg")
    assert_equal(len(deep), 3)
    assert_equal(deep[0], "/cg/system.slice/morca.service")
    assert_equal(deep[1], "/cg/system.slice")
    assert_equal(deep[2], "/cg")


def test_pick_source() raises:
    # The override wins over everything.
    var o = pick_source("/x/psi", ["/cg"], [8 << 30], [True], True)
    assert_equal(o.kind, "file")
    assert_equal(o.path, "/x/psi")
    # A container with a memory limit: its own cgroup's.
    var c = pick_source("", ["/sys/fs/cgroup"], [8 << 30], [True], True)
    assert_equal(c.kind, "cgroup")
    assert_equal(c.path, "/sys/fs/cgroup/memory.pressure")
    assert_equal(c.limit_bytes, 8 << 30)
    # A limit on a parent (a systemd slice) is the nearest one's.
    var s = pick_source(
        "",
        ["/cg/a/b", "/cg/a", "/cg"],
        [0, 4 << 30, 0],
        [True, True, True],
        True,
    )
    assert_equal(s.path, "/cg/a/memory.pressure")
    # No limit anywhere: the host's.
    var h = pick_source("", ["/sys/fs/cgroup"], [0], [True], True)
    assert_equal(h.kind, "host")
    assert_equal(h.path, "/proc/pressure/memory")
    # A limit without a pressure file doesn't count.
    var n = pick_source("", ["/cg"], [4 << 30], [False], True)
    assert_equal(n.kind, "host")
    # No host file: the cgroup's though it has no limit.
    var u = pick_source("", ["/cg"], [0], [True], False)
    assert_equal(u.kind, "cgroup-unlimited")
    # Nothing at all (macOS, PSI off).
    var none = pick_source("", List[String](), List[Int](), List[Bool](), False)
    assert_false(Bool(none))
    assert_true("none" in none.describe())


def test_find_source_override() raises:
    _ = setenv("MORCA_MEMORY_PRESSURE_FILE", String(FIXTURES, "host.txt"))
    var s = find_source()
    assert_equal(s.kind, "file")
    var p = memory_pressure()
    assert_true(Bool(p))
    assert_equal(p.value().some.avg10, 23.47)
    _ = unsetenv("MORCA_MEMORY_PRESSURE_FILE")
    assert_false(Bool(memory_pressure()))


def test_pct_text() raises:
    assert_equal(pct_text(23.47), "23")
    assert_equal(pct_text(10.0), "10")
    assert_equal(pct_text(7.46), "7.5")
    assert_equal(pct_text(5.0), "5")
    assert_equal(pct_text(0.0), "0")


def test_admit_limit() raises:
    _ = unsetenv("MORCA_ADMIT_MEMORY_PRESSURE")
    assert_equal(admit_limit(), 10.0)
    _ = setenv("MORCA_ADMIT_MEMORY_PRESSURE", "0")
    assert_equal(admit_limit(), 0.0)
    _ = setenv("MORCA_ADMIT_MEMORY_PRESSURE", "2.5")
    assert_equal(admit_limit(), 2.5)
    _ = setenv("MORCA_ADMIT_MEMORY_PRESSURE", "lots")
    assert_equal(admit_limit(), 0.0)
    _ = unsetenv("MORCA_ADMIT_MEMORY_PRESSURE")


def test_hold_and_resume_below_half() raises:
    var a = Admission(10.0)
    a.observe(_psi(3.0), 0.0, False)
    assert_false(a.held())
    assert_equal(a.allows(False), -1)
    a.observe(_psi(23.0), 1.0, False)
    assert_true(a.held())
    assert_equal(a.hold, "waiting: memory pressure 23% (limit 10%)")
    assert_equal(a.allows(False), 0)
    # Under the limit but over half of it: still holding, easing.
    a.observe(_psi(7.0), 2.0, False)
    assert_true(a.held())
    assert_equal(a.hold, "waiting: memory pressure 7%, easing (limit 10%)")
    # Under half: runs start again.
    a.observe(_psi(4.9), 3.0, False)
    assert_false(a.held())
    assert_true(a.released)
    assert_equal(a.hold, "")
    a.observe(_psi(4.9), 4.0, False)
    assert_false(a.released)


def test_own_limits_hold_nothing() raises:
    """A run held at its own memory limit stalls the host's reading too;
    what it explains holds nothing (morca #266 on the lab: one run at 512 MB
    held every start with 13 GB free)."""
    var a = Admission(10.0)
    a.observe(_psi(81.0), 0.0, False, own=80.0)
    assert_false(a.held())
    # More than the held runs explain: the rest holds.
    a.observe(_psi(81.0), 1.0, False, own=60.0)
    assert_true(a.held())
    assert_equal(a.hold, "waiting: memory pressure 21% (limit 10%)")
    # Never below 0.
    a.observe(_psi(5.0), 2.0, False, own=40.0)
    assert_false(a.held())


def test_resume_after_calm() raises:
    var a = Admission(10.0)
    a.observe(_psi(12.0), 0.0, False)
    assert_true(a.held())
    a.observe(_psi(9.0), 10.0, False)
    a.observe(_psi(9.5), 39.0, False)
    assert_true(a.held())
    # Back over the limit: the calm starts again.
    a.observe(_psi(11.0), 40.0, False)
    a.observe(_psi(9.0), 41.0, False)
    a.observe(_psi(9.0), 70.0, False)
    assert_true(a.held())
    a.observe(_psi(9.0), 71.0, False)
    assert_false(a.held())
    # At the limit is not over it: no new hold.
    a.observe(_psi(10.0), 72.0, False)
    assert_false(a.held())


def test_off_and_no_reading() raises:
    var off = Admission(0.0)
    off.observe(_psi(99.0), 0.0, False)
    assert_false(off.held())
    var a = Admission(10.0)
    a.observe(_psi(50.0), 0.0, False)
    assert_true(a.held())
    # The file went away: nothing is held.
    a.observe(None, 1.0, False)
    assert_false(a.held())
    assert_true(a.released)


def test_never_blocks_forever() raises:
    var a = Admission(10.0, grace=60.0)
    a.observe(_psi(50.0), 0.0, True)
    assert_false(a.escape)
    # Busy with morca's own runs: no escape however long.
    a.observe(_psi(50.0), 100.0, False)
    assert_false(a.escape)
    assert_equal(a.allows(False), 0)
    # Idle past the grace: one may start, said once.
    a.observe(_psi(50.0), 101.0, True)
    assert_true(a.escape)
    assert_true(a.stuck)
    assert_equal(a.allows(True), 1)
    assert_equal(a.allows(False), 0)
    a.observe(_psi(50.0), 102.0, False)
    assert_false(a.stuck)
    assert_true(a.escape)
    assert_equal(a.allows(False), 0)
    a.observe(_psi(50.0), 103.0, True)
    assert_false(a.stuck)
    assert_equal(a.allows(True), 1)
    # A new hold waits the grace again.
    a.observe(_psi(1.0), 104.0, True)
    a.observe(_psi(50.0), 105.0, True)
    assert_false(a.escape)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
