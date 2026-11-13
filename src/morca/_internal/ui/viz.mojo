"""Two views `morca ui` draws in the browser: a run's Dista timeline, and the
pipelines over time on the home page. Both are plain scripts in the page
(nothing is fetched from elsewhere) over JSON the UI serves, and both load
that JSON only when they're looked at, so the pages stay as fast as they
were.

**A run's timeline** (`timeline_section`): the run's `timeline.json`
(Chrome's trace format, as dista writes it) drawn on a canvas, a track per
process (the driver or head first, then each worker) and a lane per slot
its spans overlap on, `X` and `B`/`E` events alike. Spans are coloured by
their function's name; "inputs" (a task waiting for what it reads) are thin
bars under them. A wheel zooms around the pointer, a drag zooms to what it
covers, and Reset zoom goes back; spans narrower than a pixel are drawn as
one pixel per lane, so a few hundred thousand events stay quick. Hovering
says the span's name, process and slot, start, duration and arguments. The
file is fetched when the section scrolls into view; Perfetto and the raw
file are linked beside it.

**The pipelines over time** (`gantt_tabs`, `gantt_panel`, `gantt_json`):
"List" and "Timeline" tabs over the home page's table, the choice kept in
`localStorage`; the list is the default and is all there is without
scripts. The timeline is a row per pipeline in the order of their datasets
(a pipeline that reads what another writes comes after it, indented by its
depth), its runs over the last hour, 6 hours, day or week as bars coloured
by status, fill runs thinner, a line at now, and a thin connector from a
run to each run its commits started (`orca.sensed` joined to
`orca.commits`). Datasets between pipelines are drawn at the left. Bars
narrower than two pixels are merged, and the JSON holds the newest
`MAX_RUNS` runs of the window.

```
GET /api/gantt?window=24h      # 1h, 6h, 24h or 7d
```
"""

from std.os import getenv

import morca._internal.db.home_view as home_db

from morca.html import Html, url_encode
from morca.json import quote
from morca.lineage import flows
from morca.state import State
from morca._internal.views.code import pipeline_srcs

comptime MAX_RUNS = 20_000
"""Runs `/api/gantt` answers at most: the newest of the window."""
comptime MAX_TRIGGERS = 5_000
"""Connectors between runs `/api/gantt` answers at most."""


def window_seconds(window: String) -> Int:
    """The seconds of a window `/api/gantt` takes: `1h`, `6h`, `24h` (the
    default, and what anything else means) or `7d`."""
    if window == "1h":
        return 3600
    if window == "6h":
        return 6 * 3600
    if window == "7d":
        return 7 * 86400
    return 86400


def _window_name(window: String) -> String:
    if window == "1h" or window == "6h" or window == "7d":
        return window
    return "24h"


# ── the pipelines over time ──────────────────────────────────────────────────


struct _Pipe(Copyable, Movable):
    var name: String
    var paused: Bool
    var how: String
    """What starts its runs: sensor, keyed, schedule or manual."""
    var reads: List[String]
    """Datasets it reads and doesn't write itself, its keys' among them."""
    var writes: List[String]
    var ups: List[Int]
    var downs: List[Int]
    var depth: Int

    def __init__(out self, name: String, paused: Bool, how: String):
        self.name = name
        self.paused = paused
        self.how = how
        self.reads = List[String]()
        self.writes = List[String]()
        self.ups = List[Int]()
        self.downs = List[Int]()
        self.depth = 0


def _add(mut xs: List[String], x: String):
    if x not in xs:
        xs.append(x)


def _add_int(mut xs: List[Int], x: Int):
    if x not in xs:
        xs.append(x)


def _json_list(mut out: String, xs: List[String]):
    out.write("[")
    for i in range(len(xs)):
        out.write(", " if i else "", quote(xs[i]))
    out.write("]")


def gantt_json(mut db: State, window: String) raises -> String:
    """`/api/gantt`: the pipelines in dataset order, with their runs in the
    window and the runs each started."""
    var seconds = window_seconds(window)
    var pipes = List[_Pipe]()
    var at = Dict[String, Int]()
    for r in home_db.gantt_pipelines(db.conn):
        var how = String("manual")
        if r.sensor:
            how = "sensor"
        elif r.keys:
            how = "keyed"
        elif r.cron:
            how = "schedule"
        var name = r.name
        at[name] = len(pipes)
        pipes.append(_Pipe(name, r.paused, how))
        if r.keys:
            pipes[len(pipes) - 1].reads.append(r.keys)
    # Declared reads and writes, and a keyed pipeline's keys as a read.
    for f in flows(db):
        var i = at.get(f.pipeline, -1)
        if i < 0:
            continue
        if f.writes:
            _add(pipes[i].writes, f.dataset)
        else:
            _add(pipes[i].reads, f.dataset)
    # The edges: from each writer of a dataset to each of its readers.
    var edges = List[Tuple[Int, Int, String]]()
    for i in range(len(pipes)):
        var reads = List[String]()
        for ds in pipes[i].reads:
            if ds not in pipes[i].writes:
                reads.append(ds)
        pipes[i].reads = reads^
        for ds in pipes[i].reads:
            for j in range(len(pipes)):
                if j != i and ds in pipes[j].writes:
                    _add_int(pipes[i].ups, j)
                    _add_int(pipes[j].downs, i)
                    edges.append((j, i, ds))
    # Depth: the longest path from a pipeline nothing feeds (register
    # refuses cycles; the bound keeps a stray one finite).
    for _ in range(len(pipes)):
        var moved = False
        for i in range(len(pipes)):
            for u in pipes[i].ups:
                if pipes[u].depth + 1 > pipes[i].depth and pipes[u].depth < len(
                    pipes
                ):
                    pipes[i].depth = pipes[u].depth + 1
                    moved = True
        if not moved:
            break
    # Order: each pipeline once everything it reads from is placed, its
    # readers right after it when they can be, by name otherwise.
    var placed = List[Bool](length=len(pipes), fill=False)
    var order = List[Int]()
    while len(order) < len(pipes):
        var start = -1
        for i in range(len(pipes)):
            if placed[i]:
                continue
            var ready = True
            for u in pipes[i].ups:
                if not placed[u]:
                    ready = False
            if ready:
                start = i
                break
        if start < 0:
            for i in range(len(pipes)):
                if not placed[i]:
                    start = i
                    break
        var stack: List[Int] = [start]
        var first = True
        while len(stack) > 0:
            var j = stack.pop()
            if placed[j]:
                continue
            var ready = True
            for u in pipes[j].ups:
                if not placed[u]:
                    ready = False
            if not ready and not first:
                continue
            first = False
            placed[j] = True
            order.append(j)
            # Readers by name, the first popped first.
            var downs = pipes[j].downs.copy()
            sort(downs)
            for k in range(len(downs) - 1, -1, -1):
                if not placed[downs[k]]:
                    stack.append(downs[k])

    var now = home_db.db_now(db.conn)
    var out = String(
        '{"window": ',
        quote(_window_name(window)),
        ', "seconds": ',
        seconds,
        ', "now": ',
        now,
        ', "rows": [',
    )
    var names = List[String]()
    for p in pipes:
        names.append(p.name)
    var srcs = pipeline_srcs(db, names)
    for k in range(len(order)):
        ref p = pipes[order[k]]
        out.write(
            ", " if k else "",
            '{"pipeline": ',
            quote(p.name),
            ', "src": ',
            quote(srcs.get(p.name, String())),
            ', "depth": ',
            p.depth,
            ', "paused": ',
            "true" if p.paused else "false",
            ', "how": ',
            quote(p.how),
            ', "reads": ',
        )
        _json_list(out, p.reads)
        out.write(', "writes": ')
        _json_list(out, p.writes)
        out.write("}")
    out.write('], "edges": [')
    for k in range(len(edges)):
        out.write(
            ", " if k else "",
            '{"from": ',
            quote(pipes[edges[k][0]].name),
            ', "to": ',
            quote(pipes[edges[k][1]].name),
            ', "dataset": ',
            quote(edges[k][2]),
            "}",
        )
    out.write('], "runs": [')
    var runs = home_db.window_runs(db.conn, seconds, MAX_RUNS + 1)
    var n = min(len(runs), MAX_RUNS)
    var skip = len(runs) - n
    var written = 0
    for r in runs:
        # The oldest is the one past the limit.
        if skip > 0:
            skip -= 1
            continue
        var t1 = r.end
        out.write(
            ", " if written else "",
            '{"id": ',
            r.id,
            ', "pipeline": ',
            quote(r.pipeline),
            ', "status": ',
            quote(r.status),
            ', "trigger": ',
            quote(r.trigger),
            ', "starts": ',
            r.starts,
            ', "partitions": ',
            quote(r.partitions),
            ', "start": ',
            r.start,
            ', "end": ',
        )
        if t1 < 0:
            out.write("null")
        else:
            out.write(t1)
        out.write("}")
        written += 1
    out.write(
        '], "truncated": ',
        "true" if len(runs) > MAX_RUNS else "false",
        ', "triggers": [',
    )
    var k = 0
    for r in home_db.window_triggers(db.conn, seconds, MAX_TRIGGERS):
        out.write(
            ", " if k else "",
            '{"from": ',
            r.up,
            ', "to": ',
            r.down,
            ', "dataset": ',
            quote(r.dataset),
            ', "snapshot": ',
            r.snapshot,
            "}",
        )
        k += 1
    out.write("]}")
    return out^


def gantt_tabs(mut h: Html):
    """The tabs over the home page's pipelines, hidden until the script
    shows them, then the list's panel opening."""
    h.raw(
        '<div class="vtabs" hidden><div role="tablist"',
        ' aria-label="How to show the pipelines">',
        '<button type="button" role="tab" class="vtabs__tab" data-view="list"',
        ' aria-selected="true" aria-controls="pipelines-list">List</button>',
        '<button type="button" role="tab" class="vtabs__tab"',
        ' data-view="timeline" aria-selected="false"',
        ' aria-controls="pipelines-gantt">Timeline</button></div>',
        '<div class="vtabs__win" role="group" aria-label="Window" hidden>',
    )
    for w in ["1h", "6h", "24h", "7d"]:
        h.raw(
            '<button type="button" class="vtabs__w" data-window="',
            w,
            '" aria-pressed="false">',
            w.replace("h", " h").replace("d", " d"),
            "</button>",
        )
    h.raw('</div></div><div id="pipelines-list" role="tabpanel">')


def gantt_panel(mut h: Html):
    """Closes the list's panel, then the timeline's, empty until it's
    shown, and its script."""
    h.raw(
        '</div><section class="card gantt" id="pipelines-gantt"',
        ' role="tabpanel" data-src="/api/gantt" hidden>',
        '<h2>Pipelines over time</h2><div class="gantt__wrap">',
        '<p class="gantt__msg muted">Loading…</p></div>',
        '<div class="gantt__legend">',
    )
    for s in [
        ("succeeded", "succeeded"),
        ("running", "running"),
        ("retrying", "retrying"),
        ("failed", "failed"),
        ("queued", "queued"),
        ("cancelled", "cancelled"),
    ]:
        h.raw(
            '<span><i class="gantt__key g--',
            s[0],
            '"></i>',
            s[1],
            "</span>",
        )
    h.raw(
        '<span><i class="gantt__key gantt__key--fill g--succeeded"></i>fill',
        ' run</span><span><i class="gantt__key gantt__key--now"></i>now</span>',
        '<span><i class="gantt__key gantt__key--link"></i>started by</span>',
        "</div></section><script>",
        _GANTT_JS,
        "</script>",
    )


# ── a run's timeline ─────────────────────────────────────────────────────────


def timeline_section(mut h: Html, run: Int):
    """The run's timeline viewer, which fetches `/runs/<run>/timeline.json`
    once it's in view, with Perfetto and the file linked beside it."""
    var src = String("/runs/", run, "/timeline.json")
    h.raw('<section class="card tlv" id="timeline" data-src="')
    h.text(src)
    h.raw(
        '"><h2>Timeline</h2><div class="tlv__bar">',
        '<button type="button" class="act__btn" data-act="reset" disabled>',
        "Reset zoom</button>",
    )
    # Perfetto fetches the file itself, so only from a UI it can reach.
    var base = String(getenv("MORCA_UI_URL").rstrip("/"))
    if base:
        h.link_out(
            String(
                "https://ui.perfetto.dev/#!/?url=",
                url_encode(String(base, src)),
            ),
            "Open in Perfetto ↗",
        )
    else:
        h.raw(
            '<a href="https://ui.perfetto.dev" target="_blank" rel="noopener"',
            ' title="Set MORCA_UI_URL for a link that opens this run; or',
            ' open the file there">Open in Perfetto ↗</a>',
        )
    h.link(src, "timeline.json")
    h.raw(
        '<span class="tlv__info muted"></span></div>',
        '<div class="tlv__wrap" hidden><canvas class="tlv__canvas"',
        ' role="img" aria-label="The run&#39;s spans on its processes over',
        ' time"></canvas><div class="tlv__tip" hidden></div></div>',
        '<p class="tlv__msg muted">The timeline loads as it comes into',
        " view.<noscript> It needs JavaScript; the links above open it",
        " elsewhere.</noscript></p>",
        '<p class="tlv__help muted" hidden>Wheel to zoom, drag to zoom to a',
        " span of time, hover for a span&#39;s details.</p></section><script>",
        _TIMELINE_JS,
        "</script>",
    )


comptime VIZ_STYLE = """
/* The views drawn in the browser (morca.ui.viz). */
.vtabs { display: flex; flex-wrap: wrap; gap: 0.5rem 1rem; align-items: center;
  justify-content: space-between; margin: 1.25rem 0 -0.5rem; }
.vtabs [role=tablist], .vtabs__win { display: inline-flex; border: 1px solid var(--border);
  border-radius: var(--radius-sm); overflow: hidden; background: var(--surface); }
.vtabs__tab, .vtabs__w { font: inherit; font-size: 0.8125rem; color: var(--muted); background: none;
  border: 0; padding: 0.3rem 0.8rem; cursor: pointer; }
.vtabs__tab + .vtabs__tab, .vtabs__w + .vtabs__w { border-left: 1px solid var(--border); }
.vtabs__tab[aria-selected=true], .vtabs__w[aria-pressed=true] { color: var(--text); font-weight: 600;
  background: var(--surface-2); box-shadow: inset 0 -2px 0 var(--ember); }
.vtabs__tab:focus-visible, .vtabs__w:focus-visible { outline: 2px solid var(--lake); outline-offset: -2px; }
.vtabs__w { font-family: var(--mono); font-size: 0.75rem; }
.gantt__wrap, .tlv__wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; position: relative; }
.gantt__msg, .tlv__msg { padding: 0.8rem 1.1rem; margin: 0; }
.gantt svg { display: block; font-family: var(--mono); }
.gantt__row { fill: transparent; }
.gantt__row--alt { fill: color-mix(in srgb, var(--text) 2.5%, transparent); }
.gantt__name { font-size: 12px; fill: var(--text); }
a:hover .gantt__name { fill: var(--lake); text-decoration: underline; }
.gantt__sub { font-size: 10px; fill: var(--faint); }
.gantt__chip { fill: none; stroke: var(--muted); stroke-dasharray: 3 2; }
.gantt__chiptext { font-size: 9.5px; fill: var(--muted); }
.gantt__tick { stroke: var(--border); }
.gantt__time { font-size: 10px; fill: var(--faint); }
.gantt__edge { fill: none; stroke: var(--border-strong); stroke-width: 1.2; }
.gantt__link { fill: none; stroke: var(--muted); stroke-width: 1; opacity: 0.7; }
.gantt__now { stroke: var(--ember-soft); stroke-width: 1.5; }
.gantt__nowtext { font-size: 10px; fill: var(--ember-soft); }
.gantt a:hover rect { opacity: 0.8; }
.g--succeeded { fill: var(--lake); background: var(--lake); }
.g--running { fill: var(--live); background: var(--live); }
.g--retrying { fill: var(--warn); background: var(--warn); }
.g--failed { fill: var(--ember); background: var(--ember); }
.g--queued { fill: color-mix(in srgb, var(--warn) 35%, transparent); stroke: var(--warn);
  stroke-dasharray: 2 2; background: color-mix(in srgb, var(--warn) 35%, transparent); }
.g--cancelled { fill: var(--border-strong); background: var(--border-strong); }
.g--many { stroke: var(--text); stroke-width: 0.6; stroke-opacity: 0.35; }
.gantt__legend { display: flex; flex-wrap: wrap; gap: 0.4rem 1rem; padding: 0.6rem 1.1rem;
  border-top: 1px solid var(--border); font-size: 0.75rem; color: var(--muted); }
.gantt__legend span { display: inline-flex; align-items: center; gap: 0.35rem; }
.gantt__key { display: inline-block; width: 1.1rem; height: 0.6rem; border-radius: 2px; }
.gantt__key--fill { height: 0.25rem; }
.gantt__key--now { width: 2px; height: 0.9rem; background: var(--ember-soft); }
.gantt__key--link { width: 0.8rem; height: 0.55rem; border-left: 1px solid var(--muted);
  border-bottom: 1px solid var(--muted); border-radius: 0; }
.tlv__bar { display: flex; flex-wrap: wrap; align-items: center; gap: 0.4rem 0.9rem;
  padding: 0.6rem 1.1rem; border-bottom: 1px solid var(--border); font-size: 0.8125rem; }
.tlv__info { font-size: 0.75rem; margin-left: auto; }
.tlv__canvas { display: block; cursor: crosshair; touch-action: pan-x pan-y; }
.tlv__help { font-size: 0.75rem; padding: 0.4rem 1.1rem 0.7rem; margin: 0; }
.tlv__tip, .gantt__tip { position: absolute; z-index: 2; pointer-events: none; max-width: 22rem;
  background: var(--surface); border: 1px solid var(--border-strong); border-radius: var(--radius-sm);
  box-shadow: var(--shadow); padding: 0.45rem 0.6rem; font-size: 0.75rem; line-height: 1.4; }
.tlv__tip b { display: block; font-family: var(--mono); font-weight: 600; word-break: break-all; }
.tlv__tip dl { display: grid; grid-template-columns: auto 1fr; gap: 0 0.6rem; margin: 0.25rem 0 0; }
.tlv__tip dt { color: var(--faint); }
.tlv__tip dd { margin: 0; font-family: var(--mono); word-break: break-all; }
"""
"""The views' rules, served after `morca._internal.ui.style.STYLE` in `/static/morca.css`."""


comptime _GANTT_JS = """
(() => {
  const tabs = document.querySelector(".vtabs");
  const list = document.getElementById("pipelines-list");
  const panel = document.getElementById("pipelines-gantt");
  if (!tabs || !list || !panel) return;
  const wrap = panel.querySelector(".gantt__wrap");
  const wins = tabs.querySelector(".vtabs__win");
  const get = (k, d) => { try { return localStorage.getItem(k) || d; } catch (e) { return d; } };
  const put = (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} };
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  let win = get("morca.gantt.window", "24h"), data = null, shown = "list";
  tabs.hidden = false;
  const ORDER = { failed: 5, running: 4, retrying: 3, queued: 2, cancelled: 1, succeeded: 0 };
  const cls = (st) => st === "deferred" ? "retrying" : (st in ORDER ? st : "cancelled");
  const took = (s) => s < 60 ? Math.round(s) + "s" : s < 3600 ? Math.floor(s / 60) + "m " + Math.round(s % 60) + "s" : Math.floor(s / 3600) + "h " + Math.round((s % 3600) / 60) + "m";
  const pad = (n) => String(n).padStart(2, "0");
  const clock = (t, days) => { const d = new Date(t * 1000); const hm = pad(d.getHours()) + ":" + pad(d.getMinutes()); return days ? d.toLocaleDateString(undefined, { weekday: "short", day: "numeric" }) + (d.getHours() || d.getMinutes() ? " " + hm : "") : hm; };
  function render() {
    if (!data) return;
    const d = data, LABEL = 230, ROW = 32, TOP = 22;
    const W = Math.max(wrap.clientWidth, 720), H = TOP + d.rows.length * ROW + 6;
    const t0 = d.now - d.seconds, t1 = d.now + d.seconds * 0.03, k = (W - LABEL - 10) / (t1 - t0);
    const sx = (t) => LABEL + (t - t0) * k;
    const row = new Map(d.rows.map((r, i) => [r.pipeline, i]));
    const ymid = (i) => TOP + i * ROW + ROW / 2;
    const o = [];
    o.push('<svg xmlns="http://www.w3.org/2000/svg" width="' + W + '" height="' + H + '" viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="Pipelines and their runs over time">');
    d.rows.forEach((r, i) => o.push('<rect class="gantt__row' + (i % 2 ? " gantt__row--alt" : "") + '" x="0" y="' + (TOP + i * ROW) + '" width="' + W + '" height="' + ROW + '"/>'));
    // The time axis: ticks a round number of minutes, hours or days apart.
    const steps = [300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400, 172800];
    const step = steps.find((s) => (t1 - t0) / s <= (W - LABEL) / 90) || 172800;
    const off = new Date().getTimezoneOffset() * 60;
    for (let t = Math.ceil((t0 - off) / step) * step + off; t < t1; t += step) {
      const x = sx(t);
      o.push('<line class="gantt__tick" x1="' + x + '" x2="' + x + '" y1="' + (TOP - 4) + '" y2="' + H + '"/><text class="gantt__time" x="' + (x + 3) + '" y="12">' + esc(clock(t, d.seconds > 86400)) + "</text>");
    }
    // Datasets between rows, at the left: from a writer down to a reader.
    d.edges.forEach((e, n) => {
      const a = row.get(e.from), b = row.get(e.to);
      if (a === undefined || b === undefined) return;
      const x = 7 - (n % 3) * 2, dx = 12 + d.rows[b].depth * 14;
      o.push('<path class="gantt__edge" d="M' + (12 + d.rows[a].depth * 14) + " " + (ymid(a) - 4) + " H" + x + " V" + (ymid(b) - 4) + " H" + (dx - 2) + '"><title>' + esc(e.from + " writes " + e.dataset + ", which " + e.to + " reads") + "</title></path>");
    });
    d.rows.forEach((r, i) => {
      const x = 14 + r.depth * 14, y = TOP + i * ROW;
      o.push('<a href="/pipelines/' + encodeURIComponent(r.pipeline) + '"' + (r.src ? ' data-src="' + esc(r.src) + '"' : "") + '><text class="gantt__name" x="' + x + '" y="' + (y + 14) + '">' + esc(r.pipeline) + "</text></a>");
      const sub = (r.reads.length ? "← " + r.reads.join(", ") + " · " : "") + r.how;
      o.push('<text class="gantt__sub" x="' + x + '" y="' + (y + 26) + '"><title>' + esc(sub) + "</title>" + esc(sub.length > 30 - r.depth * 2 ? sub.slice(0, 29 - r.depth * 2) + "…" : sub) + "</text>");
      if (r.paused) o.push('<rect class="gantt__chip" x="' + (LABEL - 48) + '" y="' + (y + 5) + '" width="42" height="13" rx="6"/><text class="gantt__chiptext" x="' + (LABEL - 42) + '" y="' + (y + 15) + '">paused</text>');
    });
    // Runs: a bar each, merged with its neighbours while it's under 2px.
    const at = new Map(), byRow = d.rows.map(() => []);
    for (const r of d.runs) { const i = row.get(r.pipeline); if (i !== undefined) byRow[i].push(r); }
    const bars = [];
    byRow.forEach((runs, i) => {
      runs.sort((a, b) => a.start - b.start);
      let c = null;
      const flush = () => { if (c) bars.push(c); c = null; };
      for (const r of runs) {
        const end = r.end === null ? d.now : r.end;
        if (end < t0) continue;
        const x0 = Math.max(LABEL, sx(r.start)), x1 = Math.max(x0, sx(end)), fill = r.trigger === "fill";
        at.set(r.id, { x0, x1, y: ymid(i) });
        if (x1 - x0 >= 2) { flush(); bars.push({ i, x0, x1, runs: [r], fill }); continue; }
        if (c && x0 - c.x1 < 2) { c.x1 = Math.max(c.x1, x0 + 2); c.runs.push(r); c.fill = c.fill && fill; continue; }
        flush(); c = { i, x0, x1: x0 + 2, runs: [r], fill };
      }
      flush();
    });
    for (const b of bars) {
      const worst = b.runs.reduce((w, r) => ORDER[cls(r.status)] > ORDER[w] ? cls(r.status) : w, "succeeded");
      const h = b.fill ? 6 : 14, y = ymid(b.i) - h / 2 + 2, r0 = b.runs[0], name = d.rows[b.i].pipeline;
      let title, href;
      if (b.runs.length === 1) {
        const end = r0.end === null ? d.now : r0.end;
        title = "run " + r0.id + " · " + cls(r0.status) + " · " + r0.trigger + " · " + r0.partitions + " · " + took(end - r0.start) + (r0.end === null ? " so far" : "");
        href = "/runs/" + r0.id;
      } else {
        const n = {};
        for (const r of b.runs) n[cls(r.status)] = (n[cls(r.status)] || 0) + 1;
        title = b.runs.length + " runs of " + name + ": " + Object.entries(n).map(([s, c]) => c + " " + s).join(", ");
        href = "/runs?pipeline=" + encodeURIComponent(name);
      }
      o.push('<a href="' + esc(href) + '"><rect class="g--' + worst + (b.runs.length > 1 ? " g--many" : "") + '" x="' + b.x0.toFixed(1) + '" y="' + y + '" width="' + Math.max(2, b.x1 - b.x0).toFixed(1) + '" height="' + h + '" rx="2"><title>' + esc(title) + "</title></rect></a>");
    }
    // A run to the runs its commits started: along its row, then down.
    for (const t of d.triggers) {
      const a = at.get(t.from), b = at.get(t.to);
      if (!a || !b) continue;
      const x = Math.max(b.x0, LABEL + 1);
      o.push('<path class="gantt__link" d="M' + a.x1.toFixed(1) + " " + (a.y + 2) + " H" + x.toFixed(1) + " V" + (b.y + 2 + (b.y > a.y ? -7 : 7)) + '"><title>' + esc("run " + t.from + " committed " + t.dataset + "@" + t.snapshot + ", which started run " + t.to) + "</title></path>");
    }
    const nx = sx(d.now);
    o.push('<line class="gantt__now" x1="' + nx + '" x2="' + nx + '" y1="' + (TOP - 6) + '" y2="' + H + '"/><text class="gantt__nowtext" x="' + (nx - 24) + '" y="' + (TOP - 8) + '">now</text>');
    o.push("</svg>");
    let note = "";
    if (d.truncated) note = '<p class="gantt__msg muted">The newest ' + d.runs.length + " runs of the window.</p>";
    if (!d.rows.length) note = '<p class="gantt__msg muted">No pipelines yet.</p>';
    wrap.innerHTML = o.join("") + note;
  }
  async function load() {
    for (const b of wins.querySelectorAll("[data-window]")) b.setAttribute("aria-pressed", String(b.dataset.window === win));
    try {
      const r = await fetch(panel.dataset.src + "?window=" + encodeURIComponent(win), { headers: { Accept: "application/json" } });
      if (!r.ok) throw new Error(r.status + " " + r.statusText);
      data = await r.json();
      render();
    } catch (e) {
      wrap.innerHTML = '<p class="gantt__msg error">Can&#39;t load the timeline: ' + esc(e.message) + "</p>";
    }
  }
  function show(v, save) {
    shown = v === "timeline" ? "timeline" : "list";
    for (const b of tabs.querySelectorAll("[data-view]")) b.setAttribute("aria-selected", String(b.dataset.view === shown));
    list.hidden = shown !== "list";
    panel.hidden = shown !== "timeline";
    wins.hidden = shown !== "timeline";
    if (save) put("morca.home.view", shown);
    if (shown === "timeline") load();
  }
  tabs.addEventListener("click", (e) => {
    const b = e.target.closest("button");
    if (!b) return;
    if (b.dataset.view) show(b.dataset.view, true);
    if (b.dataset.window) { win = b.dataset.window; put("morca.gantt.window", win); load(); }
  });
  let pending = false;
  addEventListener("resize", () => { if (pending || shown !== "timeline") return; pending = true; requestAnimationFrame(() => { pending = false; render(); }); });
  show(get("morca.home.view", "list"), false);
})();
"""
"""The home page's tabs, and the timeline drawn from `/api/gantt` as SVG
each time it's shown or its window changes."""


comptime _TIMELINE_JS = """
(() => {
  const PALETTE = ["--lake", "--ember", "--ok", "--live", "--warn", "--lake-soft", "--ember-soft", "--bad"];
  const LABEL = 170, ROW = 16, THIN = 7, GAP = 6, AXIS = 22, MAX_LANES = 8;
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  const dur = (us) => {
    const a = Math.abs(us);
    if (a < 1000) return (Math.round(us * 10) / 10) + " µs";
    if (a < 1e6) return (Math.round(us / 10) / 100) + " ms";
    if (a < 6e7) return (Math.round(us / 1e4) / 100) + " s";
    return Math.floor(us / 6e7) + "m " + Math.round((us % 6e7) / 1e6) + "s";
  };
  const family = (n) => { const p = n.indexOf("("); return p < 0 ? n : n.slice(0, p); };
  const hash = (s) => { let h = 2166136261; for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619); return h >>> 0; };
  const lower = (a, x) => { let lo = 0, hi = a.length; while (lo < hi) { const m = (lo + hi) >> 1; if (a[m] < x) lo = m + 1; else hi = m; } return lo; };

  function build(j) {
    const ev = Array.isArray(j) ? j : (j && j.traceEvents) || [];
    const procs = new Map();
    const proc = (pid) => { let p = procs.get(pid); if (!p) { p = { pid, name: "process " + pid, sort: null, threads: new Map() }; procs.set(pid, p); } return p; };
    const thread = (p, tid) => { let t = p.threads.get(tid); if (!t) { t = { tid, name: "", spans: [] }; p.threads.set(tid, t); } return t; };
    const open = new Map();
    let t0 = Infinity, t1 = -Infinity, n = 0;
    const add = (pid, tid, s) => { thread(proc(pid), tid).spans.push(s); t0 = Math.min(t0, s.s); t1 = Math.max(t1, s.s + s.d); n++; };
    for (const e of ev) {
      const pid = e.pid || 0, tid = e.tid || 0, ph = e.ph;
      if (ph === "M") {
        const name = e.args && e.args.name;
        if (e.name === "process_name" && name) proc(pid).name = String(name);
        else if (e.name === "thread_name" && name) thread(proc(pid), tid).name = String(name);
        else if (e.name === "process_sort_index" && e.args) proc(pid).sort = +e.args.sort_index;
      } else if (ph === "X") {
        add(pid, tid, { n: String(e.name || ""), s: +e.ts || 0, d: Math.max(0, +e.dur || 0), a: e.args });
      } else if (ph === "B") {
        const key = pid + ":" + tid;
        if (!open.has(key)) open.set(key, []);
        open.get(key).push(e);
      } else if (ph === "E") {
        const b = (open.get(pid + ":" + tid) || []).pop();
        if (b) add(pid, tid, { n: String(b.name || e.name || ""), s: +b.ts || 0, d: Math.max(0, (+e.ts || 0) - (+b.ts || 0)), a: Object.assign({}, b.args, e.args) });
      }
    }
    // A begin without its end ends with the trace.
    for (const [key, st] of open) for (const b of st) { const [pid, tid] = key.split(":").map(Number); add(pid, tid, { n: String(b.name || ""), s: +b.ts || 0, d: 0, a: Object.assign({ unfinished: true }, b.args) }); }
    if (!n) { t0 = 0; t1 = 1; }
    for (const p of procs.values()) for (const t of p.threads.values()) for (const s of t.spans) if (s.a && s.a.unfinished) s.d = t1 - s.s;
    const first = (p) => /^(driver|head)/i.test(p.name) ? 0 : 1;
    const order = [...procs.values()].filter((p) => [...p.threads.values()].some((t) => t.spans.length))
      .sort((a, b) => first(a) - first(b) || (a.sort ?? a.pid) - (b.sort ?? b.pid) || a.pid - b.pid);
    const lanes = [];
    let y = AXIS;
    for (const p of order) {
      let top = true;
      for (const t of [...p.threads.values()].sort((a, b) => a.tid - b.tid)) {
        if (!t.spans.length) continue;
        t.spans.sort((a, b) => a.s - b.s);
        const thin = /waiting/i.test(t.name) || t.spans.every((s) => s.n === "inputs");
        const mine = [];
        if (thin) mine.push([]);
        for (const s of t.spans) {
          if (thin) { mine[0].push(s); continue; }
          let best = -1;
          for (let i = 0; i < mine.length; i++) { const last = mine[i][mine[i].length - 1]; if (last.s + last.d <= s.s) { best = i; break; } }
          if (best < 0 && mine.length < MAX_LANES) { mine.push([]); best = mine.length - 1; }
          if (best < 0) { best = 0; let end = Infinity; for (let i = 0; i < mine.length; i++) { const last = mine[i][mine[i].length - 1]; if (last.s + last.d < end) { end = last.s + last.d; best = i; } } }
          mine[best].push(s);
        }
        mine.forEach((spans, i) => {
          spans.sort((a, b) => a.s - b.s);
          const h = thin ? THIN : ROW;
          const what = t.name && t.name !== "running" && t.name !== p.name ? t.name : "";
          lanes.push({ proc: p.name, top, y, h, thin, spans, starts: Float64Array.from(spans, (s) => s.s), longest: spans.reduce((m, s) => Math.max(m, s.d), 0),
            label: thin ? what || "waiting" : mine.length > 1 ? (what ? what + " · " : "") + "slot " + (i + 1) : what });
          top = false;
          y += h + 2;
        });
      }
      y += GAP;
    }
    return { lanes, t0, t1, n, procs: order.length, height: y + 4 };
  }

  function viewer(box) {
    const wrap = box.querySelector(".tlv__wrap"), cv = box.querySelector("canvas"), tip = box.querySelector(".tlv__tip");
    const msg = box.querySelector(".tlv__msg"), info = box.querySelector(".tlv__info"), help = box.querySelector(".tlv__help");
    const reset = box.querySelector("[data-act=reset]");
    let data = null, view = null, drag = null, colours = [], plot = 0, started = false;
    const k = () => plot / (view[1] - view[0]);
    const theme = () => { const cs = getComputedStyle(box); colours = PALETTE.map((v) => cs.getPropertyValue(v).trim() || "#888"); colours.text = cs.getPropertyValue("--muted").trim(); colours.faint = cs.getPropertyValue("--faint").trim(); colours.line = cs.getPropertyValue("--border").trim(); colours.sel = cs.getPropertyValue("--lake").trim(); colours.on = cs.getPropertyValue("--surface").trim(); };
    const colour = (s) => s.n === "inputs" ? colours.faint : colours[hash(family(s.n)) % colours.length];
    function draw() {
      if (!data) return;
      const W = Math.max(wrap.clientWidth, 640), H = data.height, dpr = window.devicePixelRatio || 1;
      plot = W - LABEL - 10;
      if (cv.width !== Math.round(W * dpr) || cv.height !== Math.round(H * dpr)) { cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr); cv.style.width = W + "px"; cv.style.height = H + "px"; }
      const g = cv.getContext("2d");
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      g.clearRect(0, 0, W, H);
      g.font = "11px ui-monospace, SFMono-Regular, Menlo, monospace";
      g.textBaseline = "middle";
      // The axis, in seconds from the trace's start.
      const span = view[1] - view[0], kk = k();
      const raw = span / Math.max(1, plot / 90), mag = Math.pow(10, Math.floor(Math.log10(raw)));
      const step = [1, 2, 5, 10].map((m) => m * mag).find((s) => s >= raw) || 10 * mag;
      g.fillStyle = colours.faint; g.strokeStyle = colours.line; g.lineWidth = 1;
      for (let t = Math.ceil((view[0] - data.t0) / step) * step + data.t0; t <= view[1]; t += step) {
        const x = Math.round(LABEL + (t - view[0]) * kk) + 0.5;
        g.beginPath(); g.moveTo(x, AXIS - 6); g.lineTo(x, H); g.stroke();
        g.fillText(dur(t - data.t0), x + 3, 9);
      }
      let lastProc = null;
      for (const l of data.lanes) {
        if (l.top) {
          if (lastProc !== null) { g.strokeStyle = colours.line; g.beginPath(); g.moveTo(0, l.y - GAP / 2 - 1); g.lineTo(W, l.y - GAP / 2 - 1); g.stroke(); }
          lastProc = l.proc;
        }
        g.fillStyle = l.top ? colours.text : colours.faint;
        let label = l.top ? l.proc + (l.label ? " · " + l.label : "") : "  " + l.label;
        if (g.measureText(label).width > LABEL - 10) { while (label.length > 2 && g.measureText(label + "…").width > LABEL - 10) label = label.slice(0, -1); label += "…"; }
        g.fillText(label, 4, l.y + l.h / 2);
        g.save(); g.beginPath(); g.rect(LABEL, l.y, plot, l.h); g.clip();
        let i = lower(l.starts, view[0] - l.longest), last = -1;
        for (; i < l.spans.length; i++) {
          const s = l.spans[i];
          if (s.s > view[1]) break;
          if (s.s + s.d < view[0]) continue;
          const x0 = LABEL + (s.s - view[0]) * kk, w = s.d * kk;
          if (w < 1) {
            const px = Math.floor(x0);
            if (px === last) continue;
            last = px;
            g.fillStyle = colour(s); g.fillRect(px, l.y, 1, l.h);
          } else {
            g.fillStyle = colour(s); g.fillRect(x0, l.y, w > 3 ? w - 0.5 : w, l.h);
            last = Math.floor(x0 + w);
            if (w > 40 && !l.thin) { g.fillStyle = colours.on; g.fillText(family(s.n).slice(0, Math.floor(w / 7)), Math.max(x0, LABEL) + 3, l.y + l.h / 2); }
          }
        }
        g.restore();
      }
      if (drag && Math.abs(drag.x1 - drag.x0) > 3) {
        g.fillStyle = colours.sel; g.globalAlpha = 0.18;
        g.fillRect(Math.min(drag.x0, drag.x1), AXIS - 6, Math.abs(drag.x1 - drag.x0), H - AXIS + 6);
        g.globalAlpha = 1;
      }
      reset.disabled = view[0] <= data.t0 && view[1] >= data.t1;
    }
    let queued = false;
    const redraw = () => { if (queued) return; queued = true; requestAnimationFrame(() => { queued = false; draw(); }); };
    const timeAt = (x) => view[0] + (x - LABEL) / k();
    const zoom = (a, b) => {
      const min = 1, full = data.t1 - data.t0 || 1;
      if (b - a < min) { const m = (a + b) / 2; a = m - min / 2; b = m + min / 2; }
      if (b - a > full) { a = data.t0; b = data.t1; }
      if (a < data.t0) { b += data.t0 - a; a = data.t0; }
      if (b > data.t1) { a -= b - data.t1; b = data.t1; }
      view = [Math.max(a, data.t0), Math.min(b, data.t1)];
      redraw();
    };
    const pos = (e) => { const r = cv.getBoundingClientRect(); return [e.clientX - r.left, e.clientY - r.top]; };
    function hover(e) {
      const [x, y] = pos(e);
      const l = data.lanes.find((l) => y >= l.y && y < l.y + l.h + 2);
      if (!l || x < LABEL) { tip.hidden = true; return; }
      const t = timeAt(x), tol = 2 / k();
      let best = null, many = 0;
      for (let i = lower(l.starts, t - l.longest - tol); i < l.spans.length && l.spans[i].s <= t + tol; i++) {
        const s = l.spans[i];
        if (s.s + s.d < t - tol) continue;
        many++;
        if (!best || (s.s <= t && s.s + s.d >= t && s.d > best.d)) best = s;
      }
      if (!best) { tip.hidden = true; return; }
      let rows = "<dt>where</dt><dd>" + esc(l.proc + (l.label ? " · " + l.label : "")) + "</dd><dt>start</dt><dd>" + esc(dur(best.s - data.t0)) + "</dd><dt>took</dt><dd>" + esc(dur(best.d)) + "</dd>";
      if (best.a) for (const [key, v] of Object.entries(best.a).slice(0, 6)) rows += "<dt>" + esc(key) + "</dt><dd>" + esc(typeof v === "object" ? JSON.stringify(v) : v) + "</dd>";
      if (many > 1) rows += "<dt>here</dt><dd>" + many + " spans in this pixel; zoom in</dd>";
      tip.innerHTML = "<b>" + esc(best.n) + "</b><dl>" + rows + "</dl>";
      tip.hidden = false;
      const wr = wrap.getBoundingClientRect(), left = e.clientX - wr.left + wrap.scrollLeft + 14;
      tip.style.left = Math.min(left, wrap.scrollLeft + wrap.clientWidth - tip.offsetWidth - 4) + "px";
      tip.style.top = (y + 16) + "px";
    }
    cv.addEventListener("wheel", (e) => {
      if (!data) return;
      const [x] = pos(e);
      if (x < LABEL) return;
      e.preventDefault();
      const t = timeAt(x), f = Math.exp((e.deltaY || e.deltaX) * 0.002);
      zoom(t - (t - view[0]) * f, t + (view[1] - t) * f);
    }, { passive: false });
    cv.addEventListener("mousedown", (e) => { if (!data || e.button) return; const [x] = pos(e); if (x >= LABEL) drag = { x0: x, x1: x }; });
    addEventListener("mousemove", (e) => { if (drag) { drag.x1 = Math.max(LABEL, Math.min(pos(e)[0], LABEL + plot)); redraw(); } });
    addEventListener("mouseup", () => {
      if (!drag) return;
      const d = drag; drag = null;
      if (Math.abs(d.x1 - d.x0) > 3) { const a = timeAt(Math.min(d.x0, d.x1)), b = timeAt(Math.max(d.x0, d.x1)); zoom(a, b); } else redraw();
    });
    cv.addEventListener("mousemove", (e) => { if (data && !drag) hover(e); else tip.hidden = true; });
    cv.addEventListener("mouseleave", () => { tip.hidden = true; });
    reset.addEventListener("click", () => { if (data) { view = [data.t0, data.t1]; redraw(); } });
    addEventListener("resize", redraw);
    if (window.matchMedia) matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => { theme(); redraw(); });
    async function load() {
      if (started) return;
      started = true;
      msg.textContent = "Loading the timeline…";
      try {
        const r = await fetch(box.dataset.src);
        if (!r.ok) throw new Error(r.status + " " + r.statusText);
        data = build(await r.json());
        view = [data.t0, data.t1];
        theme();
        msg.hidden = true; wrap.hidden = false; help.hidden = false;
        info.textContent = data.n.toLocaleString() + " spans on " + data.procs + " processes over " + dur(data.t1 - data.t0);
        draw();
      } catch (e) {
        msg.textContent = "The timeline can't be drawn: " + e.message;
      }
    }
    if ("IntersectionObserver" in window) {
      const io = new IntersectionObserver((es) => { if (es.some((e) => e.isIntersecting)) { io.disconnect(); load(); } }, { rootMargin: "200px" });
      io.observe(box);
    } else load();
  }
  for (const box of document.querySelectorAll(".tlv[data-src]")) viewer(box);
})();
"""
"""The run page's timeline viewer: fetches its trace once the section is in
view, packs each process's spans into lanes, and draws what's in view on a
canvas, a pixel at most per lane for what's narrower than one."""
