"""The local files of orca.logs: `write_location` replaces a file whole, by
a rename, so a reader never sees it half written and a writer killed
partway (`put-log` on an instance that is terminated) leaves the old file,
not a truncated one. tests/logs.sh runs the logs a run leaves.
"""

from std.os import getenv, listdir, makedirs, remove
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_false

from orca.logs import read_location, write_location


def _dir() raises -> String:
    var d = String(getenv("TMPDIR", "/tmp").rstrip("/"), "/orca-test-logs")
    makedirs(d, exist_ok=True)
    return d^


def test_write_location_replaces_whole() raises:
    var path = String(_dir(), "/worker.log")
    write_location(path, String("trips on w1\n").as_bytes())
    # A reader that has the file open keeps the old one: the new is another
    # file renamed over it. Truncated in place, this read would see the new
    # bytes, or none.
    with open(path, "r") as old:
        write_location(path, String("trips on w1\nmore\n").as_bytes())
        assert_equal(old.read(), "trips on w1\n")
    assert_equal(
        String(unsafe_from_utf8=read_location(path)), "trips on w1\nmore\n"
    )
    # Nothing is left beside it.
    for name in listdir(_dir()):
        assert_false(name.endswith(".tmp"), String("left behind: ", name))
    remove(path)


def test_write_location_makes_its_directory() raises:
    var path = String(_dir(), "/a/b/driver.log")
    write_location(path, String("run succeeded\n").as_bytes())
    assert_equal(
        String(unsafe_from_utf8=read_location(path)), "run succeeded\n"
    )
    remove(path)
    assert_false(exists(path))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
