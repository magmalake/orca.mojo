"""Runs per day, drawn as SVG on the server for the home page: one bar a
day, stacked by how the runs ended."""

from morca._internal.html import escape

comptime _LEFT = 34
comptime _WIDTH = 760
comptime _HIGH = 120
comptime _AXIS = 20


@fieldwise_init
struct DayRuns(Copyable, Movable):
    var day: String
    """`YYYY-MM-DD`, UTC."""
    var succeeded: Int
    var failed: Int
    var other: Int
    """Queued, running or cancelled."""

    def total(self) -> Int:
        return self.succeeded + self.failed + self.other


def _round_up(n: Int) -> Int:
    """A scale's top: 1, 2, 5, 10, 20, 50, ... at least `n`."""
    var top = 1
    while True:
        for m in [1, 2, 5]:
            if top * m >= n:
                return top * m
        top *= 10


def runs_chart(days: List[DayRuns]) -> String:
    var most = 1
    for d in days:
        most = max(most, d.total())
    var top = _round_up(most)
    var n = max(len(days), 1)
    var slot = _WIDTH // n
    var bar = max(2, slot - 4)
    var width = _LEFT + _WIDTH + 10
    var out = String(
        '<svg class="tl" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ',
        width,
        " ",
        _HIGH + _AXIS,
        '" width="',
        width,
        '" height="',
        _HIGH + _AXIS,
        '" role="img" aria-label="Runs per day">',
        '<line class="tl__tick" x1="',
        _LEFT,
        '" x2="',
        _LEFT + _WIDTH,
        '" y1="',
        _HIGH,
        '" y2="',
        _HIGH,
        '"/><text class="tl__time" x="0" y="10">',
        top,
        '</text><text class="tl__time" x="0" y="',
        _HIGH,
        '">0</text>',
    )
    for i in range(len(days)):
        ref d = days[i]
        var x = _LEFT + i * slot + 2
        var y = _HIGH
        out.write(
            "<g><title>",
            escape(d.day),
            ": ",
            d.total(),
            " runs, ",
            d.succeeded,
            " succeeded, ",
            d.failed,
            " failed, ",
            d.other,
            " other</title>",
        )
        for part in [
            (d.succeeded, "ok"),
            (d.failed, "bad"),
            (d.other, "other"),
        ]:
            if part[0] == 0:
                continue
            var h = max(1, part[0] * _HIGH // top)
            y -= h
            out.write(
                '<rect class="runs--',
                part[1],
                '" x="',
                x,
                '" y="',
                y,
                '" width="',
                bar,
                '" height="',
                h,
                '"/>',
            )
        # The empty part of the day, so the whole column has the tooltip.
        out.write(
            '<rect class="runs--none" x="',
            x,
            '" y="0" width="',
            bar,
            '" height="',
            y,
            '"/></g>',
        )
        # A label a week, and the last day's; a week's label too close to
        # the last day's would run into it.
        if (i % 7 == 0 and len(days) - 1 - i >= 3) or i == len(days) - 1:
            out.write(
                '<text class="tl__time" x="',
                x,
                '" y="',
                _HIGH + 14,
                '">',
                escape(String(d.day[byte=5:])),
                "</text>",
            )
    out.write("</svg>")
    return out^
