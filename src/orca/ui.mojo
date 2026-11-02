"""`orca ui`: pipelines and runs as web pages, and the same as JSON.

```
ORCA_DATABASE_URL=postgresql:///orca ./build/orca ui     # 127.0.0.1:8270
```

| Path | |
|---|---|
| `/` | pipelines, with their schedules, next fires and last runs |
| `/pipelines/<name>` | a pipeline's steps, partitions, schedule, next fires and runs |
| `/runs` | runs, newest first; `?pipeline=`, `?status=`, `?trigger=` |
| `/runs/<id>` | a run's steps and partitions, attempts, errors, reads and commits |

Every path answers HTML, or JSON to `Accept: application/json` or
`?format=json`, built from the same rows, so a script and a person see the
same thing. The UI only reads: it holds no state, connects to Postgres per
request, and can run beside the scheduler or anywhere else that reaches the
database. `ORCA_UI_ADDR` (`host:port`) moves it; it binds to localhost
unless told otherwise, as there's no authentication.
"""

from std.os import getenv

from flare.http import Handler, HttpServer, Request, Response, ServerConfig
from flare.net import SocketAddr
from postgres import Params

from orca.civil import Civil, now
from orca.cron import CronExpr
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
            " AS ended FROM orca.runs WHERE ($1 = '' OR pipeline = $1) AND",
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
            " AS ended FROM orca.steps s WHERE s.run = $1 ORDER BY s.ord, s.id",
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
    h.raw("</main></body></html>")
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


@fieldwise_init
struct Page(Movable):
    var status: Int
    var json: String
    var html: String


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
    h.raw("</code></dd></dl><h2>Steps</h2>")
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
    h.raw("</dd></dl><h2>Steps</h2>")
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Step</th><th>Partition</th>',
        "<th>Status</th><th>Attempts</th><th>Started</th><th>Ended</th>",
        "<th>Error</th></tr></thead><tbody>",
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
        h.raw('<td class="error">')
        h.text(s.error)
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")
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
            " AS ended FROM orca.runs WHERE id = $1",
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


# ── serving ──────────────────────────────────────────────────────────────────


def _respond(page: Page, json: Bool) -> Response:
    var body = page.json if json else page.html
    var resp = Response(
        status=page.status,
        reason="OK" if page.status == 200 else "Not Found",
        body=List[UInt8](body.as_bytes()),
    )
    try:
        resp.headers.set(
            "Content-Type",
            "application/json" if json else "text/html; charset=utf-8",
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
        if req.method != "GET" and req.method != "HEAD":
            var resp = Response(status=405, reason="Method Not Allowed")
            resp.headers.set("Allow", "GET, HEAD")
            return resp^
        try:
            var db = State(self.url)
            return _respond(self._page(db, req.url, path), json)
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
            var rest = String(path[byte=6:])
            try:
                return run(db, Int(rest))
            except:
                return not_found(String("no run ", rest))
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
