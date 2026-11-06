"""The sampler (`orca.sample`): what it makes of Linux's /proc, and a line
read from the machine the test runs on, Linux or macOS."""

from std.ffi import c_int, external_call
from std.os import getenv, makedirs
from std.testing import TestSuite, assert_equal, assert_true

from json import loads

from orca.sample import (
    Sampler,
    container_ids,
    container_mem,
    container_name,
    cpu_share,
    default_dirs,
    fixed1,
    metrics_location,
    parse_diskstats,
    parse_meminfo,
    parse_net_dev,
    parse_pid_stat,
    parse_proc_stat,
)


def test_proc_stat() raises:
    var before = parse_proc_stat(
        "cpu  100 0 50 800 10 0 0 40 0 0\n"
        "cpu0 50 0 25 400 5 0 0 20 0 0\n"
        "cpu1 50 0 25 400 5 0 0 20 0 0\n"
        "intr 1 2 3\n"
    )
    assert_equal(len(before), 3)
    assert_equal(before[0][3], 800)
    var after = parse_proc_stat(
        "cpu  200 0 70 850 20 0 0 60 0 0\n"
        "cpu0 140 0 30 410 10 0 0 30 0 0\n"
        "cpu1 60 0 40 440 10 0 0 50 0 0\n"
    )
    var s = cpu_share(before, after)
    # 100 user, 20 system, 50 idle, 10 iowait, 20 steal of 200 ticks.
    assert_equal(fixed1(s.user), "50.0")
    assert_equal(fixed1(s.system), "10.0")
    assert_equal(fixed1(s.iowait), "5.0")
    assert_equal(fixed1(s.steal), "10.0")
    # cpu0: 105 of 120 ticks neither idle nor waiting.
    assert_equal(fixed1(s.busiest), "87.5")


def test_meminfo() raises:
    var m = parse_meminfo(
        "MemTotal:        8000 kB\nMemAvailable:    3000 kB\n"
        "Cached:          1000 kB\nHugePages_Total:       0\n"
    )
    assert_equal(m["MemTotal"], 8000 * 1024)
    assert_equal(m["MemAvailable"], 3000 * 1024)
    assert_equal(m["HugePages_Total"], 0)


def test_diskstats_whole_disks() raises:
    var text = String(
        " 259  0 nvme0n1 10 0 200 0 20 0 400 0 0 0 0\n",
        " 259  1 nvme0n1p1 5 0 100 0 10 0 300 0 0 0 0\n",
        "   7  0 loop0 1 0 999 0 1 0 999 0 0 0 0\n",
    )
    var got = parse_diskstats(text, [String("nvme0n1")])
    assert_equal(got[0], 200 * 512)
    assert_equal(got[1], 400 * 512)
    # Without /sys/block, loop devices still don't count.
    got = parse_diskstats(text, List[String]())
    assert_equal(got[0], 300 * 512)


def test_net_dev() raises:
    var n = parse_net_dev(
        "Inter-|   Receive  |  Transmit\n face |bytes packets\n"
        "    lo: 999 1 0 0 0 0 0 0 999 1 0 0 0 0 0 0\n"
        "  eth0: 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0\n"
    )
    assert_equal(len(n), 1)
    assert_equal(n["eth0"][0], 1000)
    assert_equal(n["eth0"][1], 2000)


def test_pid_stat() raises:
    # A command with spaces and parentheses in it.
    var got = parse_pid_stat(
        "42 (a (b) c) S 7 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 5"
        " 1000000 250 18446744073709551615"
    )
    assert_equal(got[0], 7)
    assert_equal(got[1], 250)


def test_location() raises:
    assert_true(len(default_dirs()) >= 1)
    # Without ORCA_LOGS there's nowhere to put them.
    assert_equal(metrics_location("p", 3, "local"), "")


def test_a_line_of_this_machine() raises:
    var s = Sampler(Int(external_call["getpid", c_int]()), "", default_dirs())
    var junk = 0
    for k in range(3_000_000):
        junk += k * k % 7
    var line = s.line()
    assert_true(junk > 0)
    var v = loads(line)
    assert_equal(Int(v.at("/v").int_value()), 1)
    assert_true(Int(v.at("/cores").int_value()) >= 1)
    var total = Int(v.at("/mem/total").int_value())
    assert_true(total > 100_000_000, line)
    var used = Int(v.at("/mem/used").int_value())
    assert_true(used > 0 and used <= total, line)
    # This process is resident.
    assert_true(Int(v.at("/mem/rss").int_value()) > 1_000_000, line)
    var user = v.at("/cpu/user").float_value()
    assert_true(user >= 0.0 and user <= 100.0, line)
    var disk = Int(v.at("/disk/0/total").int_value())
    assert_true(
        disk > 0 and Int(v.at("/disk/0/free").int_value()) <= disk, line
    )
    assert_true(Int(v.at("/net/in").int_value()) >= 0, line)
    assert_true(line.byte_length() < 600, line)


def test_container_mem() raises:
    # docker stats: usage less the page cache that can be dropped.
    assert_equal(
        container_mem("1000000\n", "anon 1\ninactive_file 400000\nfile 9\n"),
        600000,
    )
    assert_equal(container_mem("1000000\n", ""), 1000000)
    assert_equal(container_mem("5\n", "inactive_file 50\n"), 0)
    assert_equal(container_mem("", ""), -1)


def test_container_name() raises:
    assert_equal(
        container_name('{"ID": "x", "Name": "/postgres", "Z": 1}'), "postgres"
    )
    assert_equal(container_name("not json"), "")
    assert_equal(container_name("{}"), "")


def test_container_ids() raises:
    var root = String(
        getenv("TMPDIR", "/tmp"),
        "/orca-cg-",
        Int(external_call["getpid", c_int]()),
    )
    var id = String(
        "a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0a1b2"
    )
    makedirs(String(root, "/system.slice/docker-", id, ".scope"), exist_ok=True)
    makedirs(String(root, "/system.slice/sshd.service"), exist_ok=True)
    makedirs(String(root, "/docker/", id[byte=:63], "x"), exist_ok=True)
    var got = container_ids(root)
    assert_equal(len(got), 2)
    assert_equal(got[0][0], id)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
