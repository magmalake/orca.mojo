from std.testing import TestSuite, assert_equal, assert_false, assert_raises, assert_true

from orca.upgrade import UpgradeStep, parse_upgrade, select_steps, step_line, version_less, version_parts

comptime FILE = """
[[version]]
version = "1.1.0"

[[version.step]]
id = "1.1.0-egress"
when = "before"
manual = "Allow outbound HTTPS to huggingface.co."

[[version.step]]
id = "1.1.0-embed"
run = "orca backfill mojo_embed 2026-11-10"

[[version]]
version = "1.2.0"

[[version.step]]
id = "1.2.0-retool"
run = "orca backfill --rerun mojo_retool @all"

[[version]]
version = "1.3.0"

[[version.step]]
id = "1.3.0-check"
when = "before"
manual = "Needs orca 0.23."
"""


def test_versions() raises:
    assert_equal(len(version_parts("627ccae")), 0)
    assert_equal(len(version_parts("")), 0)
    assert_equal(version_parts("v1.2")[2], 0)
    assert_true(version_less(version_parts("1.2.0-rc1"), version_parts("1.2.0")))
    assert_true(version_less(version_parts("1.9.0"), version_parts("1.10.0")))
    assert_false(version_less(version_parts("1.2.0"), version_parts("1.2.0")))


def test_range() raises:
    var steps = parse_upgrade(FILE)
    assert_equal(len(steps), 4)
    assert_equal(steps[1].when, "after")  # the default
    var s = select_steps(steps, "1.0.0", "1.2.0")
    assert_equal(len(s), 3)
    assert_equal(s[0].id, "1.1.0-egress")
    assert_equal(s[2].id, "1.2.0-retool")
    # From the version deployed: its own steps were done then.
    assert_equal(len(select_steps(steps, "1.2.0", "1.2.0")), 0)
    assert_equal(len(select_steps(steps, "1.1.0", "1.3.0", "before")), 1)
    assert_equal(len(select_steps(steps, "1.1.0", "1.3.0", "after")), 1)
    # A commit deployed before: only the new version's steps.
    var c = select_steps(steps, "627ccae", "1.1.0")
    assert_equal(len(c), 2)
    assert_equal(step_line(c[1]), "step\t1.1.0-embed\tafter\trun\torca backfill mojo_embed 2026-11-10")


def test_refused() raises:
    with assert_raises(contains="only orca commands"):
        _ = parse_upgrade('[[version]]\nversion = "1.0.0"\n[[version.step]]\nid = "x"\nrun = "rm -rf /"\n')
    with assert_raises(contains="only"):
        _ = parse_upgrade('[[version]]\nversion = "1.0.0"\n[[version.step]]\nid = "x"\nrun = "orca backfill a;b"\n')
    with assert_raises(contains="one of run and manual"):
        _ = parse_upgrade('[[version]]\nversion = "1.0.0"\n[[version.step]]\nid = "x"\n')
    with assert_raises(contains="used twice"):
        _ = parse_upgrade('[[version]]\nversion = "1.0.0"\n[[version.step]]\nid = "x"\nmanual = "a"\n[[version.step]]\nid = "x"\nmanual = "b"\n')
    with assert_raises(contains="isn't a version"):
        _ = parse_upgrade('[[version]]\nversion = "next"\n')
    with assert_raises(contains="before or after"):
        _ = parse_upgrade('[[version]]\nversion = "1.0.0"\n[[version.step]]\nid = "x"\nwhen = "during"\nmanual = "a"\n')
    with assert_raises(contains="--to"):
        _ = select_steps(List[UpgradeStep](), "1.0.0", "abc")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
