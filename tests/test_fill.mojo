"""Fills (`orca.fill`): what the keys take, the window, the range a fill
covers, the price of a run, and which pipelines can't fill."""

from std.testing import TestSuite, assert_equal, assert_false, assert_true

from orca.civil import parse_time
from orca.fill import (
    Fill,
    eta_text,
    fill_refusal,
    last_ended,
    money,
    parse_fill_budget,
    parse_fill_max_runs,
    parse_fill_order,
    parse_fill_window,
    parse_fills,
    range_keys,
    run_hourly,
    unknown_type,
)


def test_values() raises:
    assert_equal(parse_fills("all"), "all")
    assert_equal(parse_fills("since_registration"), "since_registration")
    try:
        _ = parse_fills("everything")
        raise Error("accepted fills = everything")
    except e:
        assert_true("since_registration" in String(e), String(e))
    assert_equal(parse_fill_order("newest"), "newest")
    try:
        _ = parse_fill_order("random")
        raise Error("accepted fill_order = random")
    except e:
        assert_true("oldest" in String(e), String(e))
    assert_equal(parse_fill_max_runs("3"), 3)
    assert_equal(parse_fill_budget("2.5"), 2.5)
    assert_equal(parse_fill_budget("0"), 0.0)
    try:
        _ = parse_fill_budget("-1")
        raise Error("a negative budget")
    except e:
        assert_true("from 0" in String(e), String(e))
    var w = parse_fill_window("22..6")
    assert_equal(w[0], 22)
    assert_equal(w[1], 6)
    for bad in ["6", "6..6", "24..2", "1..25", "a..b", "-1..3"]:
        try:
            _ = parse_fill_window(bad)
            raise Error("accepted the window ", bad)
        except e:
            assert_true("UTC hours" in String(e), String(e))


def test_window() raises:
    var night = Fill.of("all", "", 0, -1.0, "22..6")
    assert_true(night.in_window(22))
    assert_true(night.in_window(0))
    assert_true(night.in_window(5))
    assert_false(night.in_window(6))
    assert_false(night.in_window(12))
    assert_equal(night.window_text(), "22:00–06:00 UTC")
    var day = Fill.of("all", "", 0, -1.0, "9..17")
    assert_true(day.in_window(9))
    assert_false(day.in_window(17))
    var always = Fill.of("all", "", 0, -1.0, "")
    for h in range(24):
        assert_true(always.in_window(h))
    assert_equal(always.window_text(), "all day")
    assert_equal(always.max_runs, 1)
    assert_false(always.newest_first)
    assert_false(Fill.of("", "", 0, -1.0, "").all)


def test_range() raises:
    var t = parse_time("2024-07-15T12:00Z")
    assert_equal(last_ended("monthly", "UTC", t), "2024-06")
    assert_equal(last_ended("daily", "UTC", t), "2024-07-14")
    var months = range_keys("monthly", "2024-01", "UTC", t)
    assert_equal(len(months), 6)
    assert_equal(months[0], "2024-01")
    assert_equal(months[5], "2024-06")
    # On the schedule's wall clock: 02:00Z on the 1st is still the 31st
    # in New York.
    var early = parse_time("2024-03-01T02:00Z")
    assert_equal(last_ended("daily", "America/New_York", early), "2024-02-28")
    assert_equal(last_ended("daily", "UTC", early), "2024-02-29")
    # Nothing has ended yet.
    assert_equal(len(range_keys("monthly", "2024-07", "UTC", t)), 0)
    assert_equal(len(range_keys("daily", "", "UTC", t)), 0)


def test_prices() raises:
    assert_equal(run_hourly("control", "x9g.huge", "", 4), 0.0)
    assert_equal(run_hourly("local", "", "", 4), 0.0)
    # A worker pool's machines run anyway.
    assert_equal(run_hourly("worker:gharchive", "x9g.huge", "", 4), 0.0)
    assert_true(run_hourly("kubernetes", "", "", 0) < 0)
    # A head and its most workers.
    var p = run_hourly("ec2", "t4g.medium", "c7g.xlarge", 4)
    assert_true(p > 0.6133 and p < 0.6137, String(p))
    assert_equal(run_hourly("ec2", "c7g.large", "x9g.huge", 0), 0.0725)
    assert_true(run_hourly("ec2", "c7g.large", "x9g.huge", 2) < 0)
    assert_true(run_hourly("ec2", "x9g.huge", "", 0) < 0)
    assert_equal(unknown_type("ec2", "x9g.huge", "", 0), "x9g.huge")
    assert_equal(unknown_type("ec2", "c7g.large", "x9g.huge", 2), "x9g.huge")
    assert_equal(unknown_type("ec2", "c7g.large", "c7g.xlarge", 2), "")
    assert_equal(money(0.0), "$0.00")
    assert_equal(money(1.006), "$1.01")
    assert_equal(money(12.3), "$12.30")
    assert_equal(eta_text(-1), "unknown")
    assert_equal(eta_text(30), "1m")
    assert_equal(eta_text(7260), "2h 1m")
    assert_equal(eta_text(3 * 86400 + 7200), "3d 2h")


def test_who_fills() raises:
    assert_equal(fill_refusal("daily", "2023-01-01", False, "gharchive"), "")
    assert_equal(fill_refusal("monthly", "2024-01", False, "toy"), "")
    assert_true("keyed" in fill_refusal("keyed", "", False, "labels"))
    assert_true("sensor" in fill_refusal("daily", "2024-01-01", True, "s"))
    assert_true("first partition" in fill_refusal("daily", "", False, "d"))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
