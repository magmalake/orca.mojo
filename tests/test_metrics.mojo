"""A run's machine metrics on its page (`orca.metrics`): samples read
back, peaks, warnings and the charts."""

from std.testing import TestSuite, assert_equal, assert_true

from json import loads

from orca.metrics import (
    StepSpan,
    axis_of,
    bytes_text,
    metrics_svg,
    parse_samples,
    peaks_json,
)


def _worker() raises -> String:
    with open("tests/fixtures/metrics-worker.jsonl", "r") as f:
        return f.read()


def test_samples_and_peaks() raises:
    var m = parse_samples("worker-i-1", _worker())
    assert_equal(len(m.samples), 40)
    assert_equal(m.skipped, 1)
    var p = m.peaks()
    assert_equal(Int(p.cpu + 0.5), 98 + 3)
    assert_equal(p.mem_total, 8_000_000_000)
    assert_true(p.mem_pct > 94.9 and p.mem_pct < 95.1)
    assert_equal(p.disk_used, 10_000_000_000 + 39 * 500_000_000)
    assert_equal(p.io, 39_000_000 + 2_000_000)


def test_warnings() raises:
    var m = parse_samples("worker-i-1", _worker())
    var ws = m.warnings()
    assert_equal(len(ws), 2)
    assert_equal(ws[0].kind, "memory")
    # Samples 10 to 15: from the start of 10's interval to 15.
    assert_equal(Int(ws[0].end - ws[0].start), 30)
    assert_equal(ws[1].kind, "steal")
    assert_equal(Int(ws[1].peak), 25)


def test_json() raises:
    var m = parse_samples("worker-i-1", _worker())
    var v = loads(peaks_json(m))
    assert_equal(Int(v.at("/samples").int_value()), 40)
    assert_equal(len(v.at("/warnings").array_items()), 2)
    assert_equal(v.at("/warnings/0/kind").string_value(), "memory")


def test_unordered_and_empty() raises:
    var m = parse_samples(
        "x",
        String(
            (
                '{"v": 1, "t": 1793800010, "dt": 5, "cpu": {}, "mem": {},'
                ' "disk": [], "io": null, "net": {}}\n{"v": 2, "t": 1}\n'
            ),
            (
                '{"v": 1, "t": 1793800005, "dt": 5, "cpu": {}, "mem": {},'
                ' "disk": [], "io": null, "net": {}}\n'
            ),
        ),
    )
    assert_equal(len(m.samples), 2)
    assert_true(m.samples[0].t < m.samples[1].t)
    assert_equal(m.samples[0].io, -1)
    assert_equal(m.peaks().io, -1)


def test_svg() raises:
    var m = parse_samples("worker-i-1", _worker())
    var t0 = m.samples[0].t - 5
    var steps: List[StepSpan] = [
        StepSpan("load", "2024-01", "succeeded", t0, t0 + 100, 0),
        StepSpan("compact", "", "failed", t0 + 100, t0 + 200, 0),
    ]
    var ax = axis_of([m.copy()], steps)
    assert_equal(ax[0], t0)
    var svg = metrics_svg([m.copy()], steps, False)
    assert_true(svg.startswith("<svg"))
    assert_true(svg.endswith("</svg>"))
    assert_true("worker-i-1" in svg)
    assert_true('class="mx__cpu" d="M' in svg)
    assert_true('class="mx__io"' in svg)
    assert_equal(svg.count('class="mx__warn"'), 2)
    assert_true("memory over 90%" in svg)
    assert_true(">load</text>" in svg and ">compact</text>" in svg)
    # The broken line stays out: a gap of samples breaks the path.
    assert_true(svg.count("<title>") >= 4)


def test_bytes_text() raises:
    assert_equal(bytes_text(512), "512 B")
    assert_equal(bytes_text(3_200_000), "3.2 MB")
    assert_equal(bytes_text(12_000_000_000), "12 GB")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
