"""A directed graph drawn as SVG, on the server: the pipelines and datasets
that feed each other, and a snapshot's lineage.

Nodes are laid out top to bottom, a row of boxes per level: a node's level
is its longest path from a node with no edge in (such a node itself just
above the first node it points to), and arrows point down. A level holds at
most `PER_ROW` boxes side by side and wraps onto more rows, so the drawing
is about as wide as a phone and grows downward with the graph's depth. In a
level, nodes are ordered by where the nodes pointing to them are, then as
given, which keeps most arrows short and uncrossed. Each node is a box with a
label, a second line, a kind (`dataset`, `pipeline` or `snapshot`, which
the stylesheet shapes) and optionally a link and a class, such as `start`
for the snapshot a lineage page is about. Edges that close a cycle
(a pipeline that reads the dataset it writes, `back_edges`) are drawn round
the right-hand side, back up, and don't move a node down.
"""

from morca._internal.html import escape

comptime _W = 170
comptime _H = 46
comptime _GAP_X = 24
comptime _GAP_Y = 40
"""Between levels: room for the arrows."""
comptime _WRAP_Y = 14
"""Between the rows of one level."""
comptime _PAD = 12
comptime PER_ROW = 2
"""Boxes side by side at most: two fit a phone's width."""


struct Node(Copyable, Movable):
    var label: String
    var sub: String
    var kind: String
    var href: String
    """Empty for no link."""
    var cls: String
    """An extra class, or empty."""
    var title: String
    """Its tooltip; empty for the label and the second line."""

    def __init__(
        out self,
        label: String,
        sub: String,
        kind: String,
        href: String,
        cls: String,
        title: String = "",
    ):
        self.label = label
        self.sub = sub
        self.kind = kind
        self.href = href
        self.cls = cls
        self.title = title


@fieldwise_init
struct Edge(Copyable, Movable):
    var src: Int
    var dst: Int


def back_edges(n: Int, edges: List[Edge]) -> List[Bool]:
    """Which edges close a cycle: those a depth-first walk, from each node
    in the order given, finds pointing back to a node it is still inside.
    Without them the graph has no cycle."""
    var back = List[Bool](length=len(edges), fill=False)
    var state = List[Int](length=n, fill=0)  # 0 not seen, 1 inside, 2 done
    for root in range(n):
        if state[root] != 0:
            continue
        # (node, the next edge of it to look at)
        var stack: List[Tuple[Int, Int]] = [(root, 0)]
        state[root] = 1
        while len(stack) > 0:
            var top = stack[len(stack) - 1]
            var node = top[0]
            var k = top[1]
            while k < len(edges) and edges[k].src != node:
                k += 1
            if k == len(edges):
                state[node] = 2
                _ = stack.pop()
                continue
            stack[len(stack) - 1] = (node, k + 1)
            var to = edges[k].dst
            if state[to] == 1:
                back[k] = True
            elif state[to] == 0:
                state[to] = 1
                stack.append((to, 0))
    return back^


def _levels(n: Int, edges: List[Edge]) -> List[Int]:
    """Each node's longest path from a source, over the edges that don't
    close a cycle (`back_edges`)."""
    var back = back_edges(n, edges)
    var level = List[Int](length=n, fill=0)
    for _ in range(n):
        var moved = False
        for k in range(len(edges)):
            ref e = edges[k]
            if (
                not back[k]
                and e.src != e.dst
                and level[e.dst] < level[e.src] + 1
            ):
                level[e.dst] = level[e.src] + 1
                moved = True
        if not moved:
            break
    # A node nothing points to sits just above the first node it points
    # to, not at the top: an input read late in a long chain is drawn
    # beside what read it.
    for i in range(n):
        var into = False
        var first = n
        for k in range(len(edges)):
            ref e = edges[k]
            if back[k]:
                continue
            if e.dst == i and e.src != i:
                into = True
            if e.src == i and e.dst != i:
                first = min(first, level[e.dst])
        if not into and first < n and first > 0:
            level[i] = first - 1
    return level^


def graph_svg(nodes: List[Node], edges: List[Edge], label: String) -> String:
    """The graph as an `<svg>` element, sized to fit; `label` names it for
    screen readers."""
    var n = len(nodes)
    var level = _levels(n, edges)
    var levels = 0
    for l in level:
        levels = max(levels, l + 1)
    # Each level's nodes in order: by the mean place of the nodes pointing
    # to them in the levels above, then as given.
    var place = List[Float64](length=n, fill=0.0)
    var order = List[List[Int]]()
    for l in range(levels):
        var here = List[Int]()
        var key = List[Float64]()
        for i in range(n):
            if level[i] != l:
                continue
            var sum = 0.0
            var count = 0
            for e in edges:
                if e.dst == i and e.src != i and level[e.src] < l:
                    sum += place[e.src]
                    count += 1
            var k = sum / Float64(count) if count > 0 else Float64(len(here))
            var at = len(here)
            while at > 0 and key[at - 1] > k:
                at -= 1
            here.insert(at, i)
            key.insert(at, k)
        for j in range(len(here)):
            place[here[j]] = Float64(j)
        order.append(here^)
    # Where each box goes: its level's rows, PER_ROW to a row, centred.
    var x = List[Int](length=n, fill=0)
    var y = List[Int](length=n, fill=0)
    var across = 1
    for here in order:
        across = max(across, min(len(here), PER_ROW))
    var width = _PAD * 2 + across * _W + (across - 1) * _GAP_X
    # Room at the sides for the arrows that go back up (a cycle) or skip a
    # level, which go round the boxes rather than through them.
    var sides = False
    for e in edges:
        if e.src != e.dst and (
            level[e.dst] <= level[e.src] or level[e.dst] > level[e.src] + 1
        ):
            sides = True
    var margin = _GAP_X if sides else 0
    width += 2 * margin
    var inner = width
    var top = _PAD
    for here in order:
        var rows = (len(here) + PER_ROW - 1) // PER_ROW
        for j in range(len(here)):
            var row = j // PER_ROW
            var in_row = min(PER_ROW, len(here) - row * PER_ROW)
            var row_w = in_row * _W + (in_row - 1) * _GAP_X
            x[here[j]] = (inner - row_w) // 2 + (j % PER_ROW) * (_W + _GAP_X)
            y[here[j]] = top + row * (_H + _WRAP_Y)
        top += rows * _H + max(rows - 1, 0) * _WRAP_Y + _GAP_Y
    var height = max(top - _GAP_Y + _PAD, _PAD * 2)

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
    var lanes = 0
    for e in edges:
        if e.src == e.dst:
            continue
        var d: String
        if level[e.dst] > level[e.src] + 1 or level[e.dst] <= level[e.src]:
            # Round the side its start is on, each in a lane of its own.
            var left = x[e.src] + _W // 2 < width // 2
            var lane = (lanes % 6) * 4 + 6
            lanes += 1
            var cx = lane if left else width - lane
            var x1 = x[e.src] if left else x[e.src] + _W
            var y1 = y[e.src] + _H // 2
            var x2 = x[e.dst] - 2 if left else x[e.dst] + _W + 2
            var y2 = y[e.dst] + _H // 2
            var turn = 16 if y2 > y1 else -16
            d = String(
                "M",
                x1,
                " ",
                y1,
                " C",
                cx,
                " ",
                y1,
                " ",
                cx,
                " ",
                y1,
                " ",
                cx,
                " ",
                y1 + turn,
                " L",
                cx,
                " ",
                y2 - turn,
                " C",
                cx,
                " ",
                y2,
                " ",
                cx,
                " ",
                y2,
                " ",
                x2,
                " ",
                y2,
            )
        elif y[e.dst] > y[e.src]:
            # Down: from the bottom of one box to the top of the other.
            var x1 = x[e.src] + _W // 2
            var y1 = y[e.src] + _H
            var x2 = x[e.dst] + _W // 2
            var y2 = y[e.dst] - 2
            var mid = (y1 + y2) // 2
            d = String(
                "M",
                x1,
                " ",
                y1,
                " C",
                x1,
                " ",
                mid,
                " ",
                x2,
                " ",
                mid,
                " ",
                x2,
                " ",
                y2,
            )
        else:
            # Within a level (rows of one level): round the right-hand side.
            var x1 = x[e.src] + _W
            var y1 = y[e.src] + _H // 2
            var x2 = x[e.dst] + _W + 2
            var y2 = y[e.dst] + _H // 2
            var out_x = min(max(x1, x2) + _GAP_X, width - 2)
            d = String(
                "M",
                x1,
                " ",
                y1,
                " C",
                out_x,
                " ",
                y1,
                " ",
                out_x,
                " ",
                y2,
                " ",
                x2,
                " ",
                y2,
            )
        out.write(
            '<path class="graph__edge" d="', d, '" marker-end="url(#garrow)"/>'
        )
    for i in range(n):
        ref d = nodes[i]
        var bx = x[i]
        var by = y[i]
        var cls = String("graph__node graph__node--", escape(d.kind))
        if d.cls:
            cls.write(" graph__node--", escape(d.cls))
        if d.href:
            out.write('<a href="', escape(d.href), '">')
        out.write(
            '<g class="',
            cls,
            '"><title>',
            escape(d.title) if d.title else escape(String(d.label, " ", d.sub)),
            '</title><rect x="',
            bx,
            '" y="',
            by,
            '" width="',
            _W,
            '" height="',
            _H,
            '" rx="',
            14 if d.kind == "dataset" else 4,
            '"/><text x="',
            bx + 12,
            '" y="',
            by + 20,
            '" class="graph__label">',
            escape(_clip(d.label)),
            '</text><text x="',
            bx + 12,
            '" y="',
            by + 36,
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
