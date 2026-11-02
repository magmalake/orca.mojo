"""A directed graph drawn as SVG, on the server: the pipelines and datasets
that feed each other, and a snapshot's lineage.

Nodes are laid out left to right by their longest path from a node with no
edge in, and top to bottom in the order given. Each node is a box with a
label, a second line, a kind (`dataset`, `pipeline` or `snapshot`, which
the stylesheet shapes) and optionally a link and a class, such as `start`
for the snapshot a lineage page is about. Edges that would close a cycle
(a pipeline that reads the dataset it writes) are drawn but don't move a
node right.
"""

from orca.html import escape

comptime _W = 170
comptime _H = 46
comptime _GAP_X = 60
comptime _GAP_Y = 18
comptime _PAD = 12


@fieldwise_init
struct Node(Copyable, Movable):
    var label: String
    var sub: String
    var kind: String
    var href: String
    """Empty for no link."""
    var cls: String
    """An extra class, or empty."""


@fieldwise_init
struct Edge(Copyable, Movable):
    var src: Int
    var dst: Int


def _levels(n: Int, edges: List[Edge]) -> List[Int]:
    """Each node's longest path from a source, relaxing at most `n` times,
    so a cycle stops rather than grows."""
    var level = List[Int](length=n, fill=0)
    for _ in range(n):
        var moved = False
        for e in edges:
            if e.src != e.dst and level[e.dst] < level[e.src] + 1:
                if level[e.src] + 1 < n:
                    level[e.dst] = level[e.src] + 1
                    moved = True
        if not moved:
            break
    return level^


def graph_svg(nodes: List[Node], edges: List[Edge], label: String) -> String:
    """The graph as an `<svg>` element, sized to fit; `label` names it for
    screen readers."""
    var n = len(nodes)
    var col = _levels(n, edges)
    var row = List[Int](length=n, fill=0)
    var rows_at = List[Int]()
    for i in range(n):
        while len(rows_at) <= col[i]:
            rows_at.append(0)
        row[i] = rows_at[col[i]]
        rows_at[col[i]] += 1
    var cols = len(rows_at)
    var rows = 0
    for r in rows_at:
        rows = max(rows, r)
    var width = _PAD * 2 + cols * _W + max(cols - 1, 0) * _GAP_X
    var height = _PAD * 2 + rows * _H + max(rows - 1, 0) * _GAP_Y

    var out = String(
        '<svg class="graph" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ',
        width,
        " ",
        height,
        '" width="',
        width,
        '" height="',
        height,
        '" role="img" aria-label="',
        escape(label),
        '"><defs><marker id="garrow" viewBox="0 0 8 8" refX="7" refY="4"',
        ' markerWidth="7" markerHeight="7" orient="auto-start-reverse">',
        '<path d="M0 0 8 4 0 8z" class="graph__head"/></marker></defs>',
    )
    for e in edges:
        if e.src == e.dst:
            continue
        var x1 = _PAD + col[e.src] * (_W + _GAP_X) + _W
        var y1 = _PAD + row[e.src] * (_H + _GAP_Y) + _H // 2
        var x2 = _PAD + col[e.dst] * (_W + _GAP_X) - 2
        var y2 = _PAD + row[e.dst] * (_H + _GAP_Y) + _H // 2
        var back = x2 <= x1
        if back:
            # Into a node at or left of where it starts: around underneath.
            x2 = _PAD + col[e.dst] * (_W + _GAP_X) + _W // 2
            y2 = _PAD + row[e.dst] * (_H + _GAP_Y) + _H + 2
        out.write(
            '<path class="graph__edge" d="M',
            x1,
            " ",
            y1,
            " C",
            x1 + _GAP_X // 2 if back else (x1 + x2) // 2,
            " ",
            y1 + _H if back else y1,
            " ",
            x2 if back else (x1 + x2) // 2,
            " ",
            y2 + _GAP_Y if back else y2,
            " ",
            x2,
            " ",
            y2,
            '" marker-end="url(#garrow)"/>',
        )
    for i in range(n):
        ref d = nodes[i]
        var x = _PAD + col[i] * (_W + _GAP_X)
        var y = _PAD + row[i] * (_H + _GAP_Y)
        var cls = String("graph__node graph__node--", escape(d.kind))
        if d.cls:
            cls.write(" graph__node--", escape(d.cls))
        if d.href:
            out.write('<a href="', escape(d.href), '">')
        out.write(
            '<g class="',
            cls,
            '"><title>',
            escape(d.label),
            " ",
            escape(d.sub),
            '</title><rect x="',
            x,
            '" y="',
            y,
            '" width="',
            _W,
            '" height="',
            _H,
            '" rx="',
            14 if d.kind == "dataset" else 4,
            '"/><text x="',
            x + 12,
            '" y="',
            y + 20,
            '" class="graph__label">',
            escape(_clip(d.label)),
            '</text><text x="',
            x + 12,
            '" y="',
            y + 36,
            '" class="graph__sub">',
            escape(_clip(d.sub)),
            "</text></g>",
        )
        if d.href:
            out.write("</a>")
    out.write("</svg>")
    return out^


def _clip(s: String) -> String:
    """`s` cut to what fits in a box, with an ellipsis."""
    comptime fits = 22
    if s.count_codepoints() <= fits:
        return s
    var out = String()
    var k = 0
    for cp in s.codepoint_slices():
        if k == fits - 1:
            break
        out.write(cp)
        k += 1
    out.write("…")
    return out^
