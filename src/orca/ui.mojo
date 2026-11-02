"""`orca ui`: pipelines and runs as web pages, and the same as JSON.

```
ORCA_DATABASE_URL=postgresql:///orca ./build/orca ui     # 127.0.0.1:8270
```

| Path | |
|---|---|
| `/` | pipelines, with their schedules, next fires and last runs |
| `/pipelines/<name>` | a pipeline's steps, partitions, schedule, next fires and runs |
| `/runs` | runs, newest first; `?pipeline=`, `?status=`, `?trigger=` |
| `/runs/<id>` | a run's DAG, steps and partitions, attempts, errors, reads and commits |
| `/runs/<id>/attempts/<attempt>` | an attempt's log, with its error |
| `/runs/<id>/timeline.json` | the run's Dista timeline, for Perfetto |

And the actions, each a `POST` of a form (see `orca.actions`):

| `POST` | |
|---|---|
| `/pipelines/<name>/runs` | start a run over `partitions` |
| `/pipelines/<name>/backfill` | a run per partition in `partitions` that has none |
| `/runs/<id>/resume` | queue a failed, cancelled or stalled run again; `allow_new_code=1` confirms new code |
| `/runs/<id>/rerun` | a new run from `step` on, the steps before it copied |
| `/runs/<id>/cancel` | cancel a run, before its next attempt if it's running |

A page of something still going on (a queued or running run, a running
attempt) refreshes its live part every two seconds from a fragment path,
`/runs/<id>/steps` or `/runs/<id>/attempts/<attempt>/log`, which answers
286 once there's nothing more to show, and the page stops asking. That's
the only JavaScript.

Every path answers HTML, or JSON to `Accept: application/json` or
`?format=json`, built from the same rows, so a script and a person see the
same thing. An action answers `303 See Other` to the page of what it made,
or that page's JSON; one refused answers 409 saying why. The UI holds no
state: its actions are rows the scheduler acts on, it connects to Postgres
per request, and it can run beside the scheduler or anywhere else that
reaches the database. `ORCA_UI_ADDR` (`host:port`) moves it; it binds to
localhost unless told otherwise, as there's no authentication, and a `POST`
whose `Origin` (or `Referer`) isn't the UI's own is refused, so another
site's page can't post to it through a browser.
"""

from std.os import getenv

from flare.http import Handler, HttpServer, Request, Response, ServerConfig
from flare.net import SocketAddr
from postgres import Params

from orca.actions import (
    Outcome,
    backfill_runs,
    cancel_run,
    rerun_from,
    resume_run,
    start_run,
    stalled_sql,
)
from orca.civil import Civil, now
from orca.logs import run_dir
from orca.cron import CronExpr
from orca.dag import DagStep, aggregate, dag_svg
from orca.html import Html, url_decode, url_encode
from orca.json import quote
from orca.scheduler import fill_key
from orca.schedule import Schedule
from orca.state import State
from orca.tz import Zone
from orca.ui_style import STYLE

comptime DEFAULT_ADDR = "127.0.0.1:8270"


def _ts(col: StaticString) -> String:
    """SQL for a timestamptz as `YYYY-MM-DDTHH:MM:SSZ`, or `''`."""
    return String(
        "coalesce(to_char(",
        col,
        " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '')",
    )


def _epoch(t: Int64) -> String:
    """Seconds since the epoch as `YYYY-MM-DDTHH:MM:SSZ`."""
    return String(Civil.of(t), "Z")


# ── what the pages show ──────────────────────────────────────────────────────


@fieldwise_init
struct PipelineRow(Copyable, Movable):
    var name: String
    var registry_hash: String
    var binary: String
    var kind: String
    var start: String
    var cron: String
    var tz: String
    var fills: String
    var max_runs: Int
    var run_retries: Int
    var running: Int
    var queued: Int
    var last_run: Int
    """0 when it has none."""
    var last_status: String
    var last_partitions: String
    var next_fire: String
    var next_partition: String


@fieldwise_init
struct RunRow(Copyable, Movable):
    var id: Int
    var pipeline: String
    var partitions: String
    var trigger: String
    var status: String
    var starts: Int
    var created: String
    var started: String
    var ended: String
    var cancel_requested: Bool
    var stalled: Bool
    """`running` with no live process (`orca.actions.stalled_sql`)."""


@fieldwise_init
struct StepDef(Copyable, Movable):
    var name: String
    var kind: String
    var after: String


@fieldwise_init
struct Fire(Copyable, Movable):
    var at: String
    var local: String
    var partition: String


@fieldwise_init
struct StepRow(Copyable, Movable):
    var step: String
    var partition: String
    var status: String
    var attempts: Int
    var error: String
    var log: String
    var started: String
    var ended: String
    var attempt_id: Int
    """The latest attempt's ID, 0 before the first."""
    var from_run: Int
    """The run a rerun copied this step from, or 0."""


@fieldwise_init
struct Touch(Copyable, Movable):
    """A table a step read or committed to."""

    var step: String
    var partition: String
    var kind: String
    """`read` or `commit`."""
    var table: String
    var snapshot: String


def _int(s: String) -> Int:
    try:
        return Int(s)
    except:
        return 0


def _fires(p: PipelineRow, after: Int64, count: Int) raises -> List[Fire]:
    var out = List[Fire]()
    if not p.cron:
        return out^
    var s = Schedule(CronExpr.parse(p.cron), Zone.load(p.tz))
    var t = after
    for _ in range(count):
        t = s.next_fire(t)
        var local = Civil.of(s.zone.to_local(t))
        var key = fill_key(p.kind, p.fills, local)
        out.append(Fire(_epoch(t), String(local), key))
    return out^


def _pipelines(mut db: State, name: String = "") raises -> List[PipelineRow]:
    """Every registered pipeline, or the one named."""
    var rows = db.conn.query(
        String(
            "SELECT p.name, p.registry_hash, coalesce(p.binary_path, '') AS"
            " binary, coalesce(p.describe->'partitions'->>'kind', '') AS"
            " kind, coalesce(p.describe->'partitions'->>'start', '') AS"
            " start, coalesce(p.describe->'schedule'->>'cron', '') AS cron,"
            " coalesce(p.describe->'schedule'->>'tz', '') AS tz,"
            " coalesce(p.describe->'schedule'->>'fills', '') AS fills,"
            " coalesce(p.describe->>'max_runs', '1') AS max_runs,"
            " coalesce(p.describe->>'run_retries', '0') AS run_retries,"
            " (SELECT count(*) FROM orca.runs r WHERE r.pipeline = p.name AND"
            " r.status = 'running')::text AS running, (SELECT count(*) FROM"
            " orca.runs r WHERE r.pipeline = p.name AND r.status ="
            " 'queued')::text AS queued, coalesce(l.id, 0)::text AS last_run,"
            " coalesce(l.status, '') AS last_status, coalesce(l.partitions,"
            " '') AS last_partitions FROM orca.pipelines p LEFT JOIN LATERAL"
            " (SELECT id, status, partitions FROM orca.runs r WHERE"
            " r.pipeline = p.name ORDER BY id DESC LIMIT 1) l ON true WHERE"
            " ($1 = '' OR p.name = $1) ORDER BY p.name"
        ),
        Params().text(name),
    )
    var out = List[PipelineRow]()
    var t = now()
    for r in rows:
        var p = PipelineRow(
            r.text("name"),
            r.text("registry_hash"),
            r.text("binary"),
            r.text("kind"),
            r.text("start"),
            r.text("cron"),
            r.text("tz"),
            r.text("fills"),
            _int(r.text("max_runs")),
            _int(r.text("run_retries")),
            _int(r.text("running")),
            _int(r.text("queued")),
            _int(r.text("last_run")),
            r.text("last_status"),
            r.text("last_partitions"),
            "",
            "",
        )
        try:
            var f = _fires(p, t, 1)
            if len(f) > 0:
                p.next_fire = f[0].at
                p.next_partition = f[0].partition
        except:
            pass
        out.append(p^)
    return out^


def _runs(
    mut db: State,
    pipeline: String = "",
    status: String = "",
    trigger: String = "",
    limit: Int = 100,
) raises -> List[RunRow]:
    """Runs newest first, those of a pipeline, status or trigger if given."""
    var rows = db.conn.query(
        String(
            "SELECT id::text AS id, pipeline, partitions, trigger, status,",
            " starts::text AS starts, ",
            _ts("created"),
            " AS created, ",
            _ts("started"),
            " AS started, ",
            _ts("ended"),
            " AS ended, cancel_requested, ",
            stalled_sql(),
            " AS stalled FROM orca.runs WHERE ($1 = '' OR pipeline = $1) AND",
            " ($2 = '' OR status = $2) AND ($3 = '' OR trigger = $3) ORDER BY",
            " id DESC LIMIT $4",
        ),
        Params().text(pipeline).text(status).text(trigger).int64(Int64(limit)),
    )
    var out = List[RunRow]()
    for r in rows:
        out.append(
            RunRow(
                _int(r.text("id")),
                r.text("pipeline"),
                r.text("partitions"),
                r.text("trigger"),
                r.text("status"),
                _int(r.text("starts")),
                r.text("created"),
                r.text("started"),
                r.text("ended"),
                r.bool("cancel_requested"),
                r.bool("stalled"),
            )
        )
    return out^


def _step_defs(mut db: State, name: String) raises -> List[StepDef]:
    var rows = db.conn.query(
        (
            "SELECT s->>'name' AS name, s->>'kind' AS kind,"
            " coalesce(s->>'after', '') AS after FROM orca.pipelines p,"
            " jsonb_array_elements(p.describe->'steps') WITH ORDINALITY AS x(s,"
            " n) WHERE p.name = $1 ORDER BY n"
        ),
        Params().text(name),
    )
    var out = List[StepDef]()
    for r in rows:
        out.append(StepDef(r.text("name"), r.text("kind"), r.text("after")))
    return out^


def _steps(mut db: State, run: Int) raises -> List[StepRow]:
    """A run's steps in run order: what `status <run>` shows, with times."""
    var rows = db.conn.query(
        String(
            "SELECT s.step, s.partition, s.status, coalesce(s.error, '') AS",
            " error, (SELECT count(*) FROM orca.attempts a WHERE a.step =",
            " s.id)::text AS attempts, coalesce((SELECT a.log FROM",
            " orca.attempts a WHERE a.step = s.id ORDER BY a.attempt DESC",
            " LIMIT 1), '') AS log, ",
            _ts(
                "(SELECT min(a.started) FROM orca.attempts a WHERE a.step ="
                " s.id)"
            ),
            " AS started, ",
            _ts(
                "(SELECT max(a.ended) FROM orca.attempts a WHERE a.step = s.id)"
            ),
            " AS ended, coalesce((SELECT a.id FROM orca.attempts a WHERE",
            " a.step = s.id ORDER BY a.attempt DESC LIMIT 1), 0)::text AS",
            " attempt_id, coalesce((SELECT c.run FROM orca.steps c WHERE c.id",
            " = s.copied_from), 0)::text AS from_run FROM orca.steps s WHERE",
            " s.run = $1 ORDER BY s.ord,",
            " s.id",
        ),
        Params().int64(Int64(run)),
    )
    var out = List[StepRow]()
    for r in rows:
        out.append(
            StepRow(
                r.text("step"),
                r.text("partition"),
                r.text("status"),
                _int(r.text("attempts")),
                r.text("error"),
                r.text("log"),
                r.text("started"),
                r.text("ended"),
                _int(r.text("attempt_id")),
                _int(r.text("from_run")),
            )
        )
    return out^


def _touches(mut db: State, run: Int) raises -> List[Touch]:
    var rows = db.conn.query(
        (
            "SELECT st.step, st.partition, x.kind, x.table_name,"
            " x.snapshot_id::text AS snapshot FROM (SELECT attempt, 'read' AS"
            " kind, table_name, snapshot_id FROM orca.reads UNION ALL SELECT"
            " attempt, 'commit', table_name, snapshot_id FROM orca.commits) x"
            " JOIN orca.attempts a ON a.id = x.attempt JOIN orca.steps st ON"
            " st.id = a.step WHERE st.run = $1 ORDER BY st.ord, st.id, a.id"
        ),
        Params().int64(Int64(run)),
    )
    var out = List[Touch]()
    for r in rows:
        out.append(
            Touch(
                r.text("step"),
                r.text("partition"),
                r.text("kind"),
                r.text("table_name"),
                r.text("snapshot"),
            )
        )
    return out^


# ── JSON ─────────────────────────────────────────────────────────────────────


def _json_pipeline(mut out: String, p: PipelineRow):
    out.write(
        '{"name": ',
        quote(p.name),
        ', "registry_hash": ',
        quote(p.registry_hash),
        ', "binary": ',
        quote(p.binary),
        ', "partitions": {"kind": ',
        quote(p.kind),
        ', "start": ',
        quote(p.start),
        '}, "schedule": ',
    )
    if p.cron:
        out.write(
            '{"cron": ',
            quote(p.cron),
            ', "tz": ',
            quote(p.tz),
            ', "fills": ',
            quote(p.fills),
            "}",
        )
    else:
        out.write("null")
    out.write(
        ', "max_runs": ',
        p.max_runs,
        ', "run_retries": ',
        p.run_retries,
        ', "running": ',
        p.running,
        ', "queued": ',
        p.queued,
        ', "last_run": ',
    )
    if p.last_run:
        out.write(
            '{"id": ',
            p.last_run,
            ', "status": ',
            quote(p.last_status),
            ', "partitions": ',
            quote(p.last_partitions),
            "}",
        )
    else:
        out.write("null")
    out.write(', "next_fire": ')
    if p.next_fire:
        out.write(
            '{"at": ',
            quote(p.next_fire),
            ', "partition": ',
            quote(p.next_partition),
            "}",
        )
    else:
        out.write("null")
    out.write("}")


def _json_run(mut out: String, r: RunRow):
    out.write(
        '{"id": ',
        r.id,
        ', "pipeline": ',
        quote(r.pipeline),
        ', "partitions": ',
        quote(r.partitions),
        ', "trigger": ',
        quote(r.trigger),
        ', "status": ',
        quote(r.status),
        ', "starts": ',
        r.starts,
        ', "created": ',
        quote(r.created),
        ', "started": ',
        quote(r.started),
        ', "ended": ',
        quote(r.ended),
        ', "cancel_requested": ',
        "true" if r.cancel_requested else "false",
        ', "stalled": ',
        "true" if r.stalled else "false",
        "}",
    )


def _json_runs(mut out: String, runs: List[RunRow]):
    out.write("[")
    for i in range(len(runs)):
        if i:
            out.write(", ")
        _json_run(out, runs[i])
    out.write("]")


# ── HTML ─────────────────────────────────────────────────────────────────────


comptime _POLL = """
for (const el of document.querySelectorAll("[data-poll]")) {
  const tick = async () => {
    let r;
    try { r = await fetch(el.dataset.poll); } catch (e) { setTimeout(tick, 5000); return; }
    if (r.ok) el.innerHTML = await r.text();
    if (r.status !== 286) setTimeout(tick, 2000);
  };
  setTimeout(tick, 2000);
}
"""
"""What makes a page live: each element with `data-poll` is replaced every
two seconds by what its path returns, until that answers 286 (htmx's "stop
polling"), which a fragment does once what it shows has ended."""


def _page(title: String, active: StaticString, body: Html) -> String:
    var h = Html()
    h.raw(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>",
    )
    h.text(title, " · orca")
    h.raw(
        '</title><link rel="stylesheet" href="/static/orca.css"></head><body>',
        '<header class="top"><a class="top__brand" href="/">orca</a><nav>',
    )
    for item in [("/", "Pipelines"), ("/runs", "Runs")]:
        h.raw('<a href="', item[0], '"')
        if item[0] == active:
            h.raw(' aria-current="page"')
        h.raw(">", item[1], "</a>")
    h.raw("</nav></header><main>")
    h.raw(body.out)
    h.raw("</main><script>", _POLL, "</script></body></html>")
    return h^.finish()


def _runs_table(mut h: Html, runs: List[RunRow], show_pipeline: Bool):
    if len(runs) == 0:
        h.raw('<p class="empty">No runs.</p>')
        return
    h.raw('<div class="scroll"><table><thead><tr><th>Run</th>')
    if show_pipeline:
        h.raw("<th>Pipeline</th>")
    h.raw(
        "<th>Partitions</th><th>Trigger</th><th>Status</th><th>Starts</th>",
        "<th>Created</th><th>Ended</th></tr></thead><tbody>",
    )
    for r in runs:
        h.raw("<tr><td>")
        h.link(String("/runs/", r.id), String(r.id))
        h.raw("</td>")
        if show_pipeline:
            h.raw("<td>")
            h.link(String("/pipelines/", url_encode(r.pipeline)), r.pipeline)
            h.raw("</td>")
        h.cell(r.partitions)
        h.cell(r.trigger)
        h.raw("<td>")
        h.status(r.status)
        h.raw("</td>")
        h.cell(r.starts)
        h.cell(r.created)
        h.cell(r.ended)
        h.raw("</tr>")
    h.raw("</tbody></table></div>")


# ── the pages ────────────────────────────────────────────────────────────────


struct Page(Movable):
    var status: Int
    var json: String
    var html: String
    var content_type: String
    """Set for what isn't a page or its JSON (a fragment, a file): `html` is
    sent as this, whatever was asked for."""
    var location: String
    """Set for an action that was done: a browser is sent there with 303,
    and a script gets `json` with 200."""

    def __init__(
        out self,
        status: Int,
        var json: String,
        var html: String,
        content_type: String = "",
        location: String = "",
    ):
        self.status = status
        self.json = json^
        self.html = html^
        self.content_type = content_type
        self.location = location


def home(mut db: State) raises -> Page:
    var ps = _pipelines(db)
    var recent = _runs(db, limit=10)
    var j = String('{"pipelines": [')
    for i in range(len(ps)):
        if i:
            j.write(", ")
        _json_pipeline(j, ps[i])
    j.write('], "runs": ')
    _json_runs(j, recent)
    j.write("}")

    var h = Html()
    h.raw("<h1>Pipelines</h1>")
    if len(ps) == 0:
        h.raw(
            '<p class="empty">No pipelines yet: <code>orca register',
            " &lt;binary&gt;</code> adds one.</p>",
        )
    else:
        h.raw(
            '<div class="scroll"><table><thead><tr><th>Pipeline</th>',
            "<th>Partitions</th><th>Schedule</th><th>Next fire</th>",
            "<th>Running</th><th>Queued</th><th>Last run</th></tr></thead>",
            "<tbody>",
        )
        for p in ps:
            h.raw("<tr><td>")
            h.link(String("/pipelines/", url_encode(p.name)), p.name)
            h.raw("</td>")
            h.cell(
                p.kind, " from ", p.start if p.start else String("the start")
            )
            if p.cron:
                h.raw("<td><code>")
                h.text(p.cron)
                h.raw("</code> ")
                h.text(p.tz)
                h.raw("</td>")
                h.cell(p.next_fire, " → ", p.next_partition)
            else:
                h.raw('<td class="muted">none</td><td></td>')
            h.cell(p.running, " / ", p.max_runs)
            h.cell(p.queued)
            h.raw("<td>")
            if p.last_run:
                h.link(String("/runs/", p.last_run), String(p.last_run))
                h.raw(" ")
                h.status(p.last_status)
                h.raw(" ")
                h.text(p.last_partitions)
            h.raw("</td></tr>")
        h.raw("</tbody></table></div>")
    h.raw("<h2>Recent runs</h2>")
    _runs_table(h, recent, True)
    h.raw('<p class="more"><a href="/runs">All runs</a></p>')
    return Page(200, j^, _page("Pipelines", "/", h))


def pipeline(mut db: State, name: String) raises -> Page:
    var ps = _pipelines(db, name)
    if len(ps) == 0:
        return not_found(String("no pipeline ", name))
    ref p = ps[0]
    var steps = _step_defs(db, name)
    var fires = List[Fire]()
    try:
        fires = _fires(p, now(), 5)
    except:
        pass
    var runs = _runs(db, pipeline=name, limit=20)

    var j = String('{"pipeline": ')
    _json_pipeline(j, p)
    j.write(', "steps": [')
    for i in range(len(steps)):
        if i:
            j.write(", ")
        j.write(
            '{"name": ',
            quote(steps[i].name),
            ', "kind": ',
            quote(steps[i].kind),
            ', "after": ',
            quote(steps[i].after) if steps[i].after else String("null"),
            "}",
        )
    j.write('], "fires": [')
    for i in range(len(fires)):
        if i:
            j.write(", ")
        j.write(
            '{"at": ',
            quote(fires[i].at),
            ', "local": ',
            quote(fires[i].local),
            ', "partition": ',
            quote(fires[i].partition),
            "}",
        )
    j.write('], "runs": ')
    _json_runs(j, runs)
    j.write("}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/">Pipelines</a></p><h1>')
    h.text(p.name)
    h.raw('</h1><dl class="facts"><dt>Partitions</dt><dd>')
    h.text(p.kind, " from ", p.start if p.start else String("the start"))
    h.raw("</dd><dt>Schedule</dt><dd>")
    if p.cron:
        h.raw("<code>")
        h.text(p.cron)
        h.raw("</code> ")
        h.text(p.tz, ", fills the ", p.fills, " partition")
    else:
        h.raw('<span class="muted">none</span>')
    h.raw("</dd><dt>Runs at once</dt><dd>")
    h.text(p.running, " running of ", p.max_runs, ", ", p.queued, " queued")
    h.raw("</dd><dt>Run retries</dt><dd>")
    h.text(p.run_retries)
    h.raw("</dd><dt>Binary</dt><dd><code>")
    h.text(p.binary)
    h.raw("</code></dd><dt>Code</dt><dd><code>")
    h.text(p.registry_hash)
    h.raw("</code></dd></dl>")
    var dag = List[DagStep]()
    for d in steps:
        dag.append(DagStep(d.name, d.kind, d.after, ""))
    h.raw('<div class="scroll">', dag_svg(dag), "</div><h2>Steps</h2>")
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Step</th><th>Kind</th>',
        "<th>After</th></tr></thead><tbody>",
    )
    for s in steps:
        h.raw("<tr>")
        h.cell(s.name)
        h.cell(s.kind)
        h.cell(s.after)
        h.raw("</tr>")
    h.raw("</tbody></table></div>")
    if p.binary:
        var base = String("/pipelines/", url_encode(p.name))
        var example = String("2024-01-01" if p.kind == "daily" else "2024-01")
        h.raw(
            '<h2>Start runs</h2><div class="actions">',
            '<form class="action" method="post" action="',
        )
        h.text(base, "/runs")
        h.raw('"><label>One run over <input name="partitions" required')
        h.raw(' placeholder="')
        h.text(example, "..", example)
        h.raw(
            '"></label><button type="submit">Start</button></form>',
            '<form class="action" method="post" action="',
        )
        h.text(base, "/backfill")
        h.raw('"><label>A run per partition without one <input')
        h.raw(' name="partitions" required placeholder="')
        h.text(example, "..", example)
        h.raw(
            '"></label><button type="submit">Backfill</button></form></div>',
            '<p class="muted more">The scheduler (<code>orca scheduler',
            "</code>) starts queued runs, up to ",
        )
        h.text(p.max_runs)
        h.raw(" at once.</p>")
    if len(fires) > 0:
        h.raw(
            '<h2>Next fires</h2><div class="scroll"><table><thead><tr>',
            "<th>UTC</th><th>Wall clock</th><th>Fills</th></tr></thead><tbody>",
        )
        for f in fires:
            h.raw("<tr>")
            h.cell(f.at)
            h.cell(f.local)
            h.cell(f.partition)
            h.raw("</tr>")
        h.raw("</tbody></table></div>")
    h.raw("<h2>Runs</h2>")
    _runs_table(h, runs, False)
    h.raw('<p class="more"><a href="/runs?pipeline=')
    h.text(url_encode(p.name))
    h.raw('">All its runs</a></p>')
    return Page(200, j^, _page(p.name, "/", h))


def runs(
    mut db: State, pipeline: String, status: String, trigger: String
) raises -> Page:
    var rs = _runs(db, pipeline, status, trigger)
    var j = String('{"runs": ')
    _json_runs(j, rs)
    j.write("}")

    var h = Html()
    h.raw("<h1>Runs</h1>")
    h.raw('<form class="filters" method="get" action="/runs">')
    for f in [("pipeline", pipeline), ("status", status), ("trigger", trigger)]:
        h.raw("<label>", f[0], ' <input name="', f[0], '" value="')
        h.text(f[1])
        h.raw('"></label>')
    h.raw('<button type="submit">Filter</button></form>')
    _runs_table(h, rs, True)
    return Page(200, j^, _page("Runs", "/runs", h))


def run(mut db: State, id: Int) raises -> Page:
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var r = found.take()
    var steps = _steps(db, id)
    var touches = _touches(db, id)
    var current = (
        db.conn.query(
            "SELECT registry_hash FROM orca.pipelines WHERE name = $1",
            Params().text(r.pipeline),
        )
        .row(0)
        .text("registry_hash")
    )
    var hash = (
        db.conn.query(
            "SELECT registry_hash FROM orca.runs WHERE id = $1",
            Params().int64(Int64(id)),
        )
        .row(0)
        .text("registry_hash")
    )

    var j = String('{"run": ')
    _json_run(j, r)
    j.write(
        ', "registry_hash": ',
        quote(hash),
        ', "current_code": ',
        "true" if hash == current else "false",
        ', "steps": [',
    )
    for i in range(len(steps)):
        ref s = steps[i]
        if i:
            j.write(", ")
        j.write(
            '{"step": ',
            quote(s.step),
            ', "partition": ',
            quote(s.partition),
            ', "status": ',
            quote(s.status),
            ', "attempts": ',
            s.attempts,
            ', "error": ',
            quote(s.error),
            ', "log": ',
            quote(s.log),
            ', "started": ',
            quote(s.started),
            ', "ended": ',
            quote(s.ended),
            ', "attempt_id": ',
            s.attempt_id,
            ', "from_run": ',
            s.from_run,
            "}",
        )
    j.write('], "tables": [')
    for i in range(len(touches)):
        ref t = touches[i]
        if i:
            j.write(", ")
        j.write(
            '{"step": ',
            quote(t.step),
            ', "partition": ',
            quote(t.partition),
            ', "kind": ',
            quote(t.kind),
            ', "table": ',
            quote(t.table),
            ', "snapshot": ',
            quote(t.snapshot),
            "}",
        )
    j.write("]}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/runs">Runs</a> · ')
    h.link(String("/pipelines/", url_encode(r.pipeline)), r.pipeline)
    h.raw("</p><h1>Run ")
    h.text(r.id)
    h.raw(" ")
    h.status(r.status)
    h.raw('</h1><dl class="facts"><dt>Partitions</dt><dd>')
    h.text(r.partitions)
    h.raw("</dd><dt>Trigger</dt><dd>")
    h.text(r.trigger)
    h.raw("</dd><dt>Created</dt><dd>")
    h.text(r.created)
    h.raw("</dd><dt>Started</dt><dd>")
    h.text(r.started)
    h.raw("</dd><dt>Ended</dt><dd>")
    h.text(r.ended)
    h.raw("</dd><dt>Starts</dt><dd>")
    h.text(r.starts)
    h.raw("</dd><dt>Code</dt><dd><code>")
    h.text(hash)
    h.raw("</code>")
    if hash != current:
        h.raw(' <span class="muted">(the pipeline has newer code)</span>')
    h.raw("</dd>")
    var timeline = _timeline_path(db, r)
    if timeline:
        h.raw("<dt>Timeline</dt><dd>")
        h.link(String("/runs/", r.id, "/timeline.json"), "timeline.json")
        h.raw(
            ' <span class="muted">(Dista tasks; open it in',
            ' <a href="https://ui.perfetto.dev">Perfetto</a>)</span></dd>',
        )
    if r.stalled:
        h.raw(
            "<dt>Process</dt><dd>stalled: its heartbeat stopped, so it can",
            " be resumed</dd>",
        )
    elif r.cancel_requested and _live(r.status):
        h.raw(
            "<dt>Cancel</dt><dd>asked for; the run stops before its next",
            " attempt</dd>",
        )
    h.raw("</dl>")
    if _live(r.status):
        h.raw('<div data-poll="/runs/')
        h.text(r.id)
        h.raw('/steps">')
    else:
        h.raw("<div>")
    _run_live(h, db, r, steps)
    h.raw("</div>")
    if len(touches) > 0:
        h.raw(
            '<h2>Tables</h2><div class="scroll"><table><thead><tr>',
            "<th>Step</th><th>Partition</th><th></th><th>Table</th>",
            "<th>Snapshot</th></tr></thead><tbody>",
        )
        for t in touches:
            h.raw("<tr>")
            h.cell(t.step)
            h.cell(t.partition)
            h.cell(t.kind)
            h.raw("<td><code>")
            h.text(t.table)
            h.raw("</code></td>")
            h.cell(t.snapshot)
            h.raw("</tr>")
        h.raw("</tbody></table></div>")
    return Page(200, j^, _page(String("Run ", r.id), "/runs", h))


def _action(mut h: Html, run: Int, name: StaticString):
    """The start of a form that posts to one of a run's actions."""
    h.raw('<form class="action" method="post" action="/runs/')
    h.text(run)
    h.raw("/", name, '">')


def _run_actions(mut h: Html, r: RunRow, steps: List[StepDef]):
    """The forms for what can be done to the run as it is: resume, rerun
    from a step, cancel."""
    var resumable = r.status == "failed" or r.status == "cancelled" or r.stalled
    var cancellable = (
        r.status == "queued" or r.status == "failed" or r.status == "running"
    ) and not r.cancel_requested
    var ended = not _live(r.status)
    if not (resumable or cancellable or ended):
        return
    h.raw('<div class="actions">')
    if resumable:
        _action(h, r.id, "resume")
        h.raw(
            '<button type="submit">Resume</button><span class="muted">',
            " what hasn't succeeded</span></form>",
        )
    if ended and len(steps) > 0:
        _action(h, r.id, "rerun")
        h.raw('<label>Rerun from <select name="step">')
        for s in steps:
            h.raw("<option>")
            h.text(s.name)
            h.raw("</option>")
        h.raw('</select></label><button type="submit">Rerun</button></form>')
    if cancellable:
        _action(h, r.id, "cancel")
        h.raw('<button type="submit" class="danger">Cancel</button></form>')
    h.raw("</div>")


def _live(status: String) -> Bool:
    """Whether a run or attempt with this status can still change."""
    return status == "queued" or status == "running"


def _run_live(
    mut h: Html, mut db: State, r: RunRow, steps: List[StepRow]
) raises:
    """What changes while a run goes on: what can be done to it, its DAG in
    its steps' colours, its status, and its steps."""
    var defs = _step_defs(db, r.pipeline)
    _run_actions(h, r, defs)
    var dag = List[DagStep]()
    for d in defs:
        var st = List[String]()
        for s in steps:
            if s.step == d.name:
                st.append(s.status)
        dag.append(DagStep(d.name, d.kind, d.after, aggregate(st)))
    h.raw('<div class="scroll">', dag_svg(dag), "</div><h2>Steps ")
    h.status(r.status)
    h.raw(
        '</h2><div class="scroll"><table><thead><tr><th>Step</th>',
        "<th>Partition</th><th>Status</th><th>Attempts</th><th>Started</th>",
        "<th>Ended</th><th>Log</th><th>Error</th></tr></thead><tbody>",
    )
    for s in steps:
        h.raw("<tr>")
        h.cell(s.step)
        h.cell(s.partition)
        h.raw("<td>")
        h.status(s.status)
        h.raw("</td>")
        h.cell(s.attempts)
        h.cell(s.started)
        h.cell(s.ended)
        h.raw("<td>")
        if s.attempt_id:
            h.link(
                String("/runs/", r.id, "/attempts/", s.attempt_id),
                String("attempt ", s.attempts),
            )
        elif s.from_run:
            h.raw('<span class="muted">from </span>')
            h.link(String("/runs/", s.from_run), String("run ", s.from_run))
        h.raw('</td><td class="error">')
        h.text(s.error)
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")


def run_steps(mut db: State, id: Int) raises -> Page:
    """The live part of a run's page, as a fragment; 286 once it's ended."""
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var r = found.take()
    var h = Html()
    _run_live(h, db, r, _steps(db, id))
    return Page(
        200 if _live(r.status) else 286,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
    )


def _timeline_path(mut db: State, r: RunRow) raises -> String:
    """The run's Dista timeline, if it left one: beside its attempts' logs,
    or in its directory under this process's ORCA_LOGS."""
    var dirs = List[String]()
    var rows = db.conn.query(
        (
            "SELECT a.log FROM orca.attempts a JOIN orca.steps s ON s.id ="
            " a.step WHERE s.run = $1 AND a.log IS NOT NULL AND a.log <> ''"
            " LIMIT 1"
        ),
        Params().int64(Int64(r.id)),
    )
    if len(rows) > 0:
        # <dir>/<step>/<partition>/<attempt>.log
        var log = rows.row(0).text("log")
        var d = log
        for _ in range(3):
            var at = d.rfind("/")
            if at < 0:
                break
            var up = String(d[byte=:at])
            d = up^
        dirs.append(d)
    var own = run_dir(r.pipeline, r.id)
    if own:
        dirs.append(own)
    for d in dirs:
        var path = String(d, "/timeline.json")
        try:
            with open(path, "r") as f:
                _ = f.read_bytes(1)
            return path
        except:
            pass
    return ""


def timeline(mut db: State, id: Int) raises -> Page:
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var path = _timeline_path(db, found.value())
    if not path:
        return not_found(String("run ", id, " has no timeline"))
    var body: String
    with open(path, "r") as f:
        body = f.read()
    return Page(200, String(), body^, "application/json")


comptime _LOG_LIMIT = 1 << 20
"""The most of a log a page shows: its last MiB."""


def _read_log(path: String) -> Tuple[String, Bool]:
    """The log's text, its last MiB if it's longer, and whether it was cut;
    empty if it can't be read."""
    if not path:
        return (String(), False)
    try:
        with open(path, "r") as f:
            var b = f.read_bytes()
            if len(b) <= _LOG_LIMIT:
                return (String(from_utf8_lossy=Span(b)), False)
            var start = len(b) - _LOG_LIMIT
            return (String(from_utf8_lossy=Span(b)[start:]), True)
    except:
        return (String(), False)


@fieldwise_init
struct AttemptRow(Copyable, Movable):
    var id: Int
    var run: Int
    var pipeline: String
    var step: String
    var partition: String
    var attempt: Int
    var status: String
    var host: String
    var started: String
    var ended: String
    var error: String
    var log: String


def _attempts(
    mut db: State, run: Int, cond: String, arg: Int
) raises -> List[AttemptRow]:
    var rows = db.conn.query(
        String(
            "SELECT a.id::text AS id, r.pipeline, s.step, s.partition,",
            " a.attempt::text AS attempt, a.status, a.host, ",
            _ts("a.started"),
            " AS started, ",
            _ts("a.ended"),
            " AS ended, coalesce(a.error, '') AS error, coalesce(a.log, '')",
            " AS log FROM orca.attempts a JOIN orca.steps s ON s.id = a.step",
            " JOIN orca.runs r ON r.id = s.run WHERE s.run = $1 AND ",
            cond,
            " ORDER BY a.attempt",
        ),
        Params().int64(Int64(run)).int64(Int64(arg)),
    )
    var out = List[AttemptRow]()
    for r in rows:
        out.append(
            AttemptRow(
                _int(r.text("id")),
                run,
                r.text("pipeline"),
                r.text("step"),
                r.text("partition"),
                _int(r.text("attempt")),
                r.text("status"),
                r.text("host"),
                r.text("started"),
                r.text("ended"),
                r.text("error"),
                r.text("log"),
            )
        )
    return out^


def _log_html(mut h: Html, a: AttemptRow):
    var got = _read_log(a.log)
    if got[1]:
        h.raw('<p class="muted">Its last MiB:</p>')
    h.raw('<pre class="log">')
    if got[0]:
        h.text(got[0])
    elif a.log:
        h.raw('<span class="muted">Nothing logged yet.</span>')
    else:
        h.raw('<span class="muted">No log file: ORCA_LOGS wasn\'t set.</span>')
    h.raw("</pre>")


def attempt(mut db: State, run: Int, id: Int) raises -> Page:
    var found = _attempts(db, run, "a.id = $2", id)
    if len(found) == 0:
        return not_found(String("no attempt ", id, " in run ", run))
    ref a = found[0]
    var siblings = _attempts(
        db,
        run,
        "a.step = (SELECT step FROM orca.attempts WHERE id = $2)",
        id,
    )
    var log = _read_log(a.log)

    var j = String(
        '{"attempt": {"id": ',
        a.id,
        ', "run": ',
        a.run,
        ', "step": ',
        quote(a.step),
        ', "partition": ',
        quote(a.partition),
        ', "attempt": ',
        a.attempt,
        ', "status": ',
        quote(a.status),
        ', "host": ',
        quote(a.host),
        ', "started": ',
        quote(a.started),
        ', "ended": ',
        quote(a.ended),
        ', "error": ',
        quote(a.error),
        ', "log": ',
        quote(a.log),
        '}, "text": ',
        quote(log[0]),
        ', "truncated": ',
        "true" if log[1] else "false",
        "}",
    )

    var h = Html()
    h.raw('<p class="crumb"><a href="/runs">Runs</a> · ')
    h.link(String("/runs/", a.run), String("run ", a.run))
    h.raw("</p><h1>")
    h.text(a.step)
    if a.partition:
        h.text(" ", a.partition)
    h.text(", attempt ", a.attempt, " ")
    h.status(a.status)
    h.raw("</h1>")
    if a.error:
        h.raw('<pre class="error">')
        h.text(a.error)
        h.raw("</pre>")
    h.raw('<dl class="facts"><dt>Host</dt><dd>')
    h.text(a.host)
    h.raw("</dd><dt>Started</dt><dd>")
    h.text(a.started)
    h.raw("</dd><dt>Ended</dt><dd>")
    h.text(a.ended)
    h.raw("</dd><dt>Log file</dt><dd><code>")
    h.text(a.log)
    h.raw("</code></dd>")
    if len(siblings) > 1:
        h.raw("<dt>Attempts</dt><dd>")
        for i in range(len(siblings)):
            if i:
                h.raw(" · ")
            ref s = siblings[i]
            if s.id == a.id:
                h.text(s.attempt)
            else:
                h.link(
                    String("/runs/", run, "/attempts/", s.id), String(s.attempt)
                )
            h.raw(" ")
            h.status(s.status)
        h.raw("</dd>")
    h.raw("</dl><h2>Log</h2>")
    if _live(a.status):
        h.raw('<div data-poll="/runs/')
        h.text(run, "/attempts/", a.id, "/log")
        h.raw('">')
    else:
        h.raw("<div>")
    _log_html(h, a)
    h.raw("</div>")
    var title = String(a.step, " ", a.partition, " attempt ", a.attempt)
    return Page(200, j^, _page(title, "/runs", h))


def attempt_log(mut db: State, run: Int, id: Int) raises -> Page:
    """An attempt's log, as a fragment; 286 once the attempt has ended."""
    var found = _attempts(db, run, "a.id = $2", id)
    if len(found) == 0:
        return not_found(String("no attempt ", id, " in run ", run))
    var h = Html()
    _log_html(h, found[0])
    return Page(
        200 if _live(found[0].status) else 286,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
    )


def _runs_one(mut db: State, id: Int) raises -> Optional[RunRow]:
    var rows = db.conn.query(
        String(
            "SELECT id::text AS id, pipeline, partitions, trigger, status,",
            " starts::text AS starts, ",
            _ts("created"),
            " AS created, ",
            _ts("started"),
            " AS started, ",
            _ts("ended"),
            " AS ended, cancel_requested, ",
            stalled_sql(),
            " AS stalled FROM orca.runs WHERE id = $1",
        ),
        Params().int64(Int64(id)),
    )
    if len(rows) == 0:
        return None
    var r = rows.row(0)
    return RunRow(
        id,
        r.text("pipeline"),
        r.text("partitions"),
        r.text("trigger"),
        r.text("status"),
        _int(r.text("starts")),
        r.text("created"),
        r.text("started"),
        r.text("ended"),
        r.bool("cancel_requested"),
        r.bool("stalled"),
    )


def not_found(what: String) -> Page:
    var h = Html()
    h.raw("<h1>Not found</h1><p>")
    h.text(what)
    h.raw("</p>")
    return Page(
        404,
        String('{"error": ', quote(what), "}"),
        _page("Not found", "", h),
    )


# ── actions ──────────────────────────────────────────────────────────────────


def _form(body: String, name: StringSlice) -> String:
    """A field of an `application/x-www-form-urlencoded` body, decoded."""
    return _query(String("?", body), name)


def _acted(
    o: Outcome, back: String, var json: String, location: String
) -> Page:
    """The answer to an action: to `location` if it was done, or a 409
    saying why not, with a way back to `back`, and to confirm if it asks."""
    if not o.refused:
        return Page(303, json^, String(), location=location)
    var h = Html()
    h.raw("<h1>Not done</h1><p>")
    h.text(o.refused)
    h.raw("</p>")
    if o.confirm:
        h.raw('<form class="action" method="post" action="')
        h.text(back, "/resume")
        h.raw(
            '"><input type="hidden" name="allow_new_code" value="1">',
            '<button type="submit">Resume with the new code</button></form>',
        )
    h.raw('<p><a href="')
    h.text(back)
    h.raw('">Back</a></p>')
    return Page(
        409,
        String(
            '{"error": ',
            quote(o.refused),
            ', "confirm": ',
            "true" if o.confirm else "false",
            "}",
        ),
        _page("Not done", "", h),
    )


def _act(mut db: State, path: String, body: String) raises -> Optional[Page]:
    """Does the action `path` names, or None if it names none."""
    var parts = path.split("/")
    if len(parts) == 4 and parts[1] == "pipelines":
        var name = url_decode(parts[2])
        var back = String("/pipelines/", url_encode(name))
        var keys = _form(body, "partitions")
        if parts[3] == "runs":
            var o = start_run(db, name, keys)
            var to = String("/runs/", o.run)
            return _acted(o, back, String('{"run": ', o.run, "}"), to)
        if parts[3] == "backfill":
            var o = backfill_runs(db, name, keys)
            var to = String("/runs?pipeline=", url_encode(name))
            return _acted(o, back, String('{"queued": ', o.queued, "}"), to)
    if len(parts) == 4 and parts[1] == "runs":
        var id: Int
        try:
            id = Int(parts[2])
        except:
            return None
        var back = String("/runs/", id)
        var o: Outcome
        if parts[3] == "resume":
            o = resume_run(db, id, _form(body, "allow_new_code") == "1")
        elif parts[3] == "rerun":
            o = rerun_from(db, id, _form(body, "step"))
        elif parts[3] == "cancel":
            o = cancel_run(db, id)
        else:
            return None
        var to = String("/runs/", o.run)
        return _acted(o, back, String('{"run": ', o.run, "}"), to)
    return None


def _same_origin(req: Request) raises -> Bool:
    """Whether a `POST` came from the UI's own pages: its `Origin`, or
    without one its `Referer`, is this server as the request addressed it."""
    var host = req.headers.get("Host")
    if not host:
        return False
    var own = String("http://", host)
    var origin = req.headers.get("Origin")
    if origin:
        return origin == own
    var referer = req.headers.get("Referer")
    return referer == own or referer.startswith(String(own, "/"))


# ── serving ──────────────────────────────────────────────────────────────────


def _reason(status: Int) -> String:
    if status == 200:
        return "OK"
    if status == 286:
        return "Done"
    if status == 303:
        return "See Other"
    if status == 403:
        return "Forbidden"
    if status == 404:
        return "Not Found"
    if status == 409:
        return "Conflict"
    return "Internal Server Error"


def _respond(page: Page, json: Bool) -> Response:
    var raw = page.content_type != ""
    var body = page.json if json and not raw else page.html
    var status = 200 if page.location and json else page.status
    var resp = Response(
        status=status,
        reason=_reason(status),
        body=List[UInt8](body.as_bytes()),
    )
    try:
        if page.location and not json:
            resp.headers.set("Location", page.location)
        resp.headers.set(
            "Content-Type",
            page.content_type if raw else (
                "application/json" if json else "text/html; charset=utf-8"
            ),
        )
        resp.headers.set("Cache-Control", "no-store")
    except:
        pass
    return resp^


def _query(url: String, name: StringSlice) -> String:
    """A query parameter's value, decoded, or empty."""
    var q = url.find("?")
    if q < 0:
        return ""
    for pair in url[byte = q + 1 :].split("&"):
        var eq = pair.find("=")
        var k = pair[byte=:eq] if eq >= 0 else pair
        if k == name:
            return url_decode(pair[byte = eq + 1 :]) if eq >= 0 else ""
    return ""


def _is_action(path: String) -> Bool:
    var parts = path.split("/")
    if len(parts) != 4:
        return False
    if parts[1] == "pipelines":
        return parts[3] == "runs" or parts[3] == "backfill"
    return parts[1] == "runs" and (
        parts[3] == "resume" or parts[3] == "rerun" or parts[3] == "cancel"
    )


def _not_allowed() -> Response:
    var resp = Response(status=405, reason="Method Not Allowed")
    try:
        resp.headers.set("Allow", "GET, HEAD")
    except:
        pass
    return resp^


def _cross_origin() -> Page:
    var why = String(
        "this form wasn't posted from orca ui's own pages (its Origin or",
        " Referer isn't this server), so it's refused",
    )
    var h = Html()
    h.raw("<h1>Refused</h1><p>")
    h.text(why)
    h.raw("</p>")
    return Page(
        403, String('{"error": ', quote(why), "}"), _page("Refused", "", h)
    )


struct Ui(Copyable, Handler):
    """The pages, over the database at `url`."""

    var url: String

    def __init__(out self, url: String):
        self.url = url

    def serve(self, req: Request) raises -> Response:
        var q = req.url.find("?")
        var path = String(req.url[byte=:q]) if q >= 0 else req.url
        var json = (
            "application/json" in req.headers.get("Accept")
            or _query(req.url, "format") == "json"
        )
        if path == "/static/orca.css":
            var resp = Response(
                status=200, reason="OK", body=List[UInt8](STYLE.as_bytes())
            )
            resp.headers.set("Content-Type", "text/css; charset=utf-8")
            return resp^
        var post = req.method == "POST"
        if post and not _is_action(path):
            return _not_allowed()
        if not post and req.method != "GET" and req.method != "HEAD":
            return _not_allowed()
        if post and not _same_origin(req):
            return _respond(_cross_origin(), json)
        try:
            var db = State(self.url)
            if not post:
                return _respond(self._page(db, req.url, path), json)
            var done = _act(db, path, String(from_utf8_lossy=Span(req.body)))
            if not done:
                return _respond(not_found(String("nothing at ", path)), json)
            return _respond(done.take(), json)
        except e:
            var h = Html()
            h.raw("<h1>Error</h1><pre>")
            h.text(e)
            h.raw("</pre>")
            var page = Page(
                500,
                String('{"error": ', quote(String(e)), "}"),
                _page("Error", "", h),
            )
            var resp = _respond(page, json)
            resp.reason = "Internal Server Error"
            return resp^

    def _page(self, mut db: State, url: String, path: String) raises -> Page:
        if path == "/" or path == "":
            return home(db)
        if path == "/runs":
            return runs(
                db,
                _query(url, "pipeline"),
                _query(url, "status"),
                _query(url, "trigger"),
            )
        if path.startswith("/runs/"):
            var parts = path[byte=6:].split("/")
            var id: Int
            try:
                id = Int(parts[0])
            except:
                return not_found(String("no run ", parts[0]))
            if len(parts) == 1:
                return run(db, id)
            if len(parts) == 2 and parts[1] == "steps":
                return run_steps(db, id)
            if len(parts) == 2 and parts[1] == "timeline.json":
                return timeline(db, id)
            if len(parts) >= 3 and len(parts) <= 4 and parts[1] == "attempts":
                var aid: Int
                try:
                    aid = Int(parts[2])
                except:
                    return not_found(String("no attempt ", parts[2]))
                if len(parts) == 3:
                    return attempt(db, id, aid)
                if parts[3] == "log":
                    return attempt_log(db, id, aid)
        if path.startswith("/pipelines/"):
            var name = url_decode(path[byte=11:])
            if name and "/" not in name:
                return pipeline(db, name)
        return not_found(String("nothing at ", path))


def serve_ui(url: String, addr: String) raises:
    """Serves the pages on `addr` until the process is stopped."""
    var cfg = ServerConfig()
    cfg.idle_timeout_ms = 30_000
    cfg.write_timeout_ms = 30_000
    var server = HttpServer.bind(SocketAddr.parse(addr), cfg^)
    print("orca ui on http://", addr, sep="")
    server.serve(Ui(url))
