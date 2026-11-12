"""The run page's drawings (`morca.timeline`): a dista timeline read into
lanes and bars, and the workers over a run."""

from std.testing import TestSuite, assert_equal, assert_true

from morca.timeline import read_timeline, timeline_svg, workers_svg


def _fixture() raises -> String:
    with open("tests/fixtures/timeline.json", "r") as f:
        return f.read()


def test_lanes_are_workers() raises:
    var t = read_timeline(_fixture())
    assert_equal(len(t.lanes), 2)
    assert_true(t.lanes[0].startswith("worker 127.0.0.1:"))
    assert_true(len(t.bars) > 0)
    assert_equal(t.left_out, 0)
    var running = 0
    for b in t.bars:
        assert_true(b.start >= 0 and b.start + b.length <= t.end + 1e-9)
        assert_true(b.lane == 0 or b.lane == 1)
        if not b.waiting:
            running += 1
            assert_equal(b.name, "fanout::trips(fanout::Day)")
    assert_true(running > 0)


def test_events_without_names() raises:
    var t = read_timeline(
        '[{"name": "a", "ph": "X", "pid": 7, "tid": 1, "ts": 1000000,'
        ' "dur": 500000}, {"name": "b", "ph": "X", "pid": 3, "tid": 1,'
        ' "ts": 2000000, "dur": 1000000}, {"name": "c", "ph": "i", "pid": 3,'
        ' "tid": 1, "ts": 9000000}]'
    )
    # Lanes by pid; times from the first event.
    assert_equal(len(t.lanes), 2)
    assert_equal(t.lanes[0], "worker 3")
    assert_equal(t.bars[0].lane, 1)
    assert_equal(t.bars[1].start, 1.0)
    assert_equal(t.end, 2.0)


def test_svg_escapes_names() raises:
    var t = read_timeline(
        '[{"name": "process_name", "ph": "M", "pid": 1, "tid": 0, "args":'
        ' {"name": "<b>w</b>"}}, {"name": "x<y", "ph": "X", "pid": 1, "tid":'
        ' 1, "ts": 0, "dur": 10}]'
    )
    var svg = timeline_svg(t)
    assert_true(svg.startswith('<svg class="tl"'))
    assert_true("&lt;b&gt;w&lt;/b&gt;" in svg)
    assert_true("x&lt;y" in svg)
    assert_true("<b>" not in svg)


def test_workers_step_line() raises:
    var changes: List[Tuple[Float64, Int]] = [
        (0.0, 1),
        (5.0, 1),
        (5.0, 1),
        (20.0, -1),
        (30.0, -2),
    ]
    var svg = workers_svg(changes, 40.0)
    assert_true(">3 workers<" in svg)
    # Up to 3, down to 2, down to 0.
    assert_true(" V4" in svg and " V0" not in svg)
    assert_true(svg.endswith("</svg>"))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
