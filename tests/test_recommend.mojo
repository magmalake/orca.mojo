"""Requests learned from the measured peaks (orca.recommend): the
percentiles, the margin and its rounding, too few runs, the floor an
out-of-memory raised, and the clamp to the declared range."""

from std.testing import TestSuite, assert_equal, assert_false, assert_true

from orca.recommend import (
    MIN_RUNS,
    RunPeaks,
    memory_gb_text,
    percentile,
    recommend,
)
from orca.resources import Request

comptime MB = 1048576.0


def _runs(mb: List[Int], cores: Float64 = 2.0) -> List[RunPeaks]:
    var out = List[RunPeaks]()
    for m in mb:
        out.append(RunPeaks(Float64(m) * MB, cores))
    return out^


def _none() -> Request:
    return Request(0, 0, 0, 0)


def test_percentile() raises:
    assert_equal(percentile(List[Float64](), 0.95), -1.0)
    assert_equal(percentile([5.0], 0.95), 5.0)
    # Nearest rank: of 20, p95 is the 19th, p90 the 18th.
    var xs = List[Float64]()
    for i in range(20, 0, -1):
        xs.append(Float64(i))
    assert_equal(percentile(xs.copy(), 0.95), 19.0)
    assert_equal(percentile(xs.copy(), 0.90), 18.0)
    assert_equal(percentile(xs.copy(), 0.5), 10.0)
    # Of 3, p95 is the largest.
    assert_equal(percentile([1.0, 3.0, 2.0], 0.95), 3.0)


def test_memory_gb_text() raises:
    assert_equal(memory_gb_text(3328), "3.25")
    assert_equal(memory_gb_text(3584), "3.5")
    assert_equal(memory_gb_text(4096), "4")
    assert_equal(memory_gb_text(256), "0.25")


def test_too_few_runs() raises:
    var r = recommend(_runs([1000, 1000]), 0, _none())
    assert_false(r.any())
    assert_equal(r.runs, 2)
    assert_equal(r.memory_mb, 0)
    assert_equal(r.cpus, 0)
    assert_true("2 of the 3" in r.text())
    assert_true('"memory_mb": null' in r.json())
    # Runs without a measured peak don't count.
    var p = _runs([1000, 1000])
    p.append(RunPeaks(-1, 2.0))
    assert_equal(recommend(p, 0, _none()).runs, 2)
    assert_equal(MIN_RUNS, 3)


def test_margin_and_rounding() raises:
    # 2000 MB × 1.15 = 2300 MB, up to 2304 (9 quarter GBs).
    var r = recommend(_runs([1000, 2000, 1500]), 0, _none())
    assert_equal(r.runs, 3)
    assert_equal(r.memory_mb, 2304)
    assert_equal(r.cpus, 2)
    assert_equal(r.clamped, "")
    assert_equal(r.text(), "learned from 3 runs: 2.25 GB, 2 CPUs")
    assert_equal(
        r.source,
        "p95 of the runs' peak memory × 1.15; p90 of their peak cores",
    )
    assert_true('"applied": false' in r.json())
    # p95 of 20 skips the one largest run: 19 runs of 1000 MB and one of
    # 9000 give 1000 × 1.15 = 1150, up to 1280.
    var mb = List[Int]()
    for _ in range(19):
        mb.append(1000)
    mb.append(9000)
    assert_equal(recommend(_runs(mb), 0, _none()).memory_mb, 1280)
    # Tiny runs get a quarter GB.
    assert_equal(recommend(_runs([1, 1, 1]), 0, _none()).memory_mb, 256)


def test_cpus() raises:
    # Rounded up, less the sampling's slack, at least 1.
    assert_equal(recommend(_runs([100, 100, 100], 2.05), 0, _none()).cpus, 2)
    assert_equal(recommend(_runs([100, 100, 100], 2.3), 0, _none()).cpus, 3)
    assert_equal(recommend(_runs([100, 100, 100], 0.2), 0, _none()).cpus, 1)
    # p90 of 10: the 9th.
    var p = List[RunPeaks]()
    for i in range(1, 11):
        p.append(RunPeaks(100 * MB, Float64(i)))
    assert_equal(recommend(p, 0, _none()).cpus, 9)
    # Not measured (macOS): memory only.
    var r = recommend(_runs([100, 100, 100], -1), 0, _none())
    assert_equal(r.cpus, 0)
    assert_equal(r.cpu_runs, 0)
    assert_equal(r.text(), "learned from 3 runs: 0.25 GB")


def test_never_below_the_oom_bump() raises:
    var r = recommend(_runs([1000, 1000, 1000]), 3072, _none())
    assert_equal(r.memory_mb, 3072)
    assert_true("out-of-memory" in r.source)
    # A bump below what the peaks give changes nothing.
    assert_equal(
        recommend(_runs([1000, 1000, 1000]), 512, _none()).memory_mb, 1280
    )


def test_clamped_to_the_declared_range() raises:
    var req = Request.of("1..2", "2..3")
    var r = recommend(_runs([4000, 4000, 4000], 6.0), 0, req)
    assert_equal(r.memory_mb, 3072)
    assert_equal(r.cpus, 2)
    assert_equal(r.clamped, "memory_gb's maximum and cpus' maximum")
    assert_equal(
        r.text(),
        (
            "learned from 3 runs: 3 GB, 2 CPUs (clamped to memory_gb's"
            " maximum and cpus' maximum)"
        ),
    )
    var low = recommend(_runs([100, 100, 100], 1.0), 0, Request.of("2..4", "2"))
    assert_equal(low.memory_mb, 2048)
    assert_equal(low.cpus, 2)
    assert_equal(low.clamped, "memory_gb's minimum and cpus' minimum")
    # Within the range, it isn't.
    assert_equal(recommend(_runs([2000, 2000, 2000]), 0, req).clamped, "")
    # The OOM bump is clamped too.
    assert_equal(recommend(_runs([100, 100, 100]), 9999, req).memory_mb, 3072)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
