"""A pipeline's DAG drawn as SVG, on the server.

Steps are laid out left to right by depth (a step is one column right of the
step it's `after`), and steps at the same depth are stacked top to bottom in
pipeline order. A mapped step, which runs once per partition, is drawn as a
stack of boxes; a gather step as one. Each box carries a status class, the
aggregate of its partitions in the run being shown, which the stylesheet
colours. Pipelines are chains today, so this is usually one row; it's
written for a DAG, as datasets may branch one later.
"""

from morca.html import escape

comptime _W = 150
comptime _H = 46
comptime _GAP_X = 56
comptime _GAP_Y = 22
comptime _PAD = 12


@fieldwise_init
struct DagStep(Copyable, Movable):
    var name: String
    var kind: String
    """`mapped` or `gather`."""
    var after: String
    """The step it follows, or empty."""
    var status: String
    """Its status in the run shown, or empty on a pipeline's own page."""
    var href: String
    """Its function's code, which the box links to, or empty."""


def aggregate(statuses: List[String]) -> String:
    """One status for a step from its partitions': failed or running if any
    is, succeeded if all are, otherwise the first one's."""
    if len(statuses) == 0:
        return ""
    for want in ["failed", "running"]:
        for s in statuses:
            if s == want:
                return String(want)
    for s in statuses:
        if s != "succeeded":
            return s
    return "succeeded"


def dag_svg(steps: List[DagStep]) -> String:
    """The steps as an `<svg>` element, sized to fit."""
    var n = len(steps)
    var col = List[Int](length=n, fill=0)
    var row = List[Int](length=n, fill=0)
    var rows_at = List[Int]()
    for i in range(n):
        for j in range(i):
            if steps[j].name == steps[i].after:
                col[i] = col[j] + 1
        while len(rows_at) <= col[i]:
            rows_at.append(0)
        row[i] = rows_at[col[i]]
        rows_at[col[i]] += 1
    var cols = len(rows_at)
    var rows = 0
    for r in rows_at:
        rows = max(rows, r)
    var width = _PAD * 2 + cols * _W + max(cols - 1, 0) * _GAP_X + 8
    var height = _PAD * 2 + rows * _H + max(rows - 1, 0) * _GAP_Y + 8

    var out = String(
        '<svg class="dag" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ',
        width,
        " ",
        height,
        '" width="',
        width,
        '" height="',
        height,
        '" role="img" aria-label="The pipeline\'s steps">',
        '<defs><marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4"',
        ' markerWidth="7" markerHeight="7" orient="auto-start-reverse">',
        '<path d="M0 0 8 4 0 8z" class="dag__head"/></marker></defs>',
    )
    for i in range(n):
        for j in range(i):
            if steps[j].name != steps[i].after:
                continue
            var x1 = (
                _PAD
                + col[j] * (_W + _GAP_X)
                + _W
                + (4 if steps[j].kind == "mapped" else 0)
            )
            var y1 = _PAD + row[j] * (_H + _GAP_Y) + _H // 2
            var x2 = _PAD + col[i] * (_W + _GAP_X) - 2
            var y2 = _PAD + row[i] * (_H + _GAP_Y) + _H // 2
            out.write(
                '<path class="dag__edge" d="M',
                x1,
                " ",
                y1,
                " C",
                (x1 + x2) // 2,
                " ",
                y1,
                " ",
                (x1 + x2) // 2,
                " ",
                y2,
                " ",
                x2,
                " ",
                y2,
                '" marker-end="url(#arrow)"/>',
            )
    for i in range(n):
        ref s = steps[i]
        var x = _PAD + col[i] * (_W + _GAP_X)
        var y = _PAD + row[i] * (_H + _GAP_Y)
        var cls = String("dag__box")
        if s.status:
            cls.write(" dag__box--", escape(s.status))
        if s.href:
            out.write(
                '<a href="', escape(s.href), '" target="_blank" rel="noopener">'
            )
        # Its card (`morca.ui.step_cards`) on hover, focus or a tap: the link is
        # the tab stop where there is one, the box where there isn't.
        out.write(
            '<g class="',
            cls,
            '" data-step="',
            escape(s.name),
            '"' if s.href else '" tabindex="0"',
            "><title>",
            escape(s.name),
        )
        if s.status:
            out.write(": ", escape(s.status))
        if s.href:
            out.write(" (view source)")
        out.write("</title>")
        if s.kind == "mapped":
            for k in [8, 4]:
                out.write(
                    '<rect x="',
                    x + k,
                    '" y="',
                    y + k,
                    '" width="',
                    _W,
                    '" height="',
                    _H,
                    '" rx="6"/>',
                )
        out.write(
            '<rect x="',
            x,
            '" y="',
            y,
            '" width="',
            _W,
            '" height="',
            _H,
            '" rx="6"/><text x="',
            x + 12,
            '" y="',
            y + 20,
            '" class="dag__name">',
            escape(s.name),
            '</text><text x="',
            x + 12,
            '" y="',
            y + 36,
            '" class="dag__kind">',
            escape(s.kind),
        )
        if s.status:
            out.write(" · ", escape(s.status))
        if s.href:
            out.write(" ↗")
        out.write("</text></g>")
        if s.href:
            out.write("</a>")
    out.write("</svg>")
    return out^
