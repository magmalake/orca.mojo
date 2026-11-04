"""`orca ui`: pipelines and runs as web pages, and the same as JSON.

```
ORCA_DATABASE_URL=postgresql:///orca ./build/orca ui     # 127.0.0.1:8270
```

| Path | |
|---|---|
| `/` | pipelines, with their schedules, next runs and last runs |
| `/pipelines/<name>` | a pipeline's steps, partitions, schedule, next runs and runs; a keyed one's keys, done, live and failed |
| `/pipelines/<name>/keys/<key>` | one key of a keyed pipeline: its steps in every run it was in |
| `/runs` | runs, newest first; `?pipeline=`, `?status=`, `?trigger=` |
| `/runs/<id>` | a run's DAG, steps and partitions, attempts, errors, reads and commits |
| `/runs/<id>/attempts/<attempt>` | an attempt's log, with its error |
| `/runs/<id>/timeline.json` | the run's Dista timeline, for Perfetto |
| `/runs/<id>/driver.log` | the run's driver output, as the scheduler kept it |
| `/datasets` | datasets, with their writers, readers and latest snapshots, and the graph of pipelines and datasets |
| `/datasets/<name>` | a dataset's place in that graph and its snapshots |
| `/datasets/<name>/snapshots/<id>` | a snapshot's lineage, back and forward (`orca.lineage`) |
| `/rebuilds/<id>` | a rebuild's pipelines by level, with their runs (`orca.sensors`) |

And the actions, each a `POST` of a form (see `orca.actions`):

| `POST` | |
|---|---|
| `/pipelines/<name>/runs` | start a run over `partitions` |
| `/pipelines/<name>/backfill` | a run per partition in `partitions` that has none; `rerun=1` for done ones too |
| `/pipelines/<name>/cancel` | cancel its queued, running and retrying runs over `partitions` (`@all` or empty: all) |
| `/pipelines/<name>/pause`, `/unpause` | stop the scheduler starting its runs, or let it again |
| `/runs/<id>/resume` | queue a failed, cancelled or stalled run again; `allow_new_code=1` confirms new code |
| `/runs/<id>/rerun` | a new run from `step` on, the steps before it copied |
| `/runs/<id>/cancel` | cancel a run, before its next attempt if it's running |
| `/datasets/<name>/rebuild` | rebuild every pipeline downstream over `partitions` |
| `/login`, `/logout` | a session for the actions, as a cookie; `name`, `password`, `next` |

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
localhost unless told otherwise, and a `POST` whose `Origin` (or
`Referer`) isn't the UI's own is refused, so another site's page can't post
to it through a browser. Reading needs no login; once login is on
(`ORCA_UI_LOGIN=required`, or any user added with `orca user add`), every
action needs a session from `/login`, and each one is recorded as its
user's (`orca.auth`), which a run's page shows.
"""

from std.os import getenv

from flare.http import Handler, HttpServer, Request, Response, ServerConfig
from flare.net import SocketAddr
from postgres import Params

from json import loads

from orca.auth import (
    SESSION_COOKIE,
    audit,
    close_session,
    login_failed,
    login_required,
    open_session,
    session_user,
    too_many_failures,
)
from orca.actions import (
    Outcome,
    backfill_runs,
    cancel_run,
    cancel_runs,
    pause_pipeline,
    rebuild_downstream,
    rerun_from,
    resume_run,
    start_run,
    stalled_sql,
)
from orca.chart import DayRuns, runs_chart
from orca.civil import Civil, now
from orca.logs import location_exists, read_location, run_dir
from orca.cron import CronExpr
from orca.dag import DagStep, aggregate, dag_svg
from orca.graph import Edge, Node, graph_svg
from orca.html import Html, escape, url_decode, url_encode
from orca.lineage import (
    Made,
    SnapshotRef,
    back,
    commits,
    datasets as dataset_rows,
    flows,
    forward,
)
from orca.json import quote
from orca.live import live_sql
from orca.scheduler import (
    current_keys,
    done_keys,
    keep_keys,
    kept_keys,
    fill_key,
    registered,
)
from orca.sensors import Rebuild, rebuilds
from orca.schedule import Schedule
from orca.source import commit_url, is_hex, web_url
from orca.locate import function_key
from orca.timeline import (
    MAX_BARS,
    read_timeline,
    timeline_svg,
    workers_svg,
)
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
    var paused: Bool
    """`orca pause`: the scheduler starts none of its runs."""
    var sense_error: String
    """Why the sensor's last pass failed, or empty."""
    var sense_error_at: String


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
    var error: String
    """Why the run ended without its steps saying so (`runs.error`)."""
    var waiting_until: String
    """When a deferred run starts again; empty for any other."""


@fieldwise_init
struct VersionRow(Copyable, Movable):
    """A version of a pipeline's code (`orca.source`)."""

    var id: Int
    var registry_hash: String
    var source: String
    var commit: String
    var dirty: Bool
    var image: String
    var first_seen: String
    var last_seen: String
    var runs: Int
    var locations: String
    """Where its functions are (`orca.locate`), as JSON, or empty."""


@fieldwise_init
struct InstanceRow(Copyable, Movable):
    """An instance a run ran on (`orca.instances`)."""

    var id: String
    var start: Int
    var role: String
    var instance_type: String
    var market: String
    var launched: String
    var ended: String
    var why: String
    var from_start: Float64
    """Seconds from the run's start to its launch."""
    var to_end: Float64
    """Seconds from the run's start to its end, or to now."""


@fieldwise_init
struct StepDef(Copyable, Movable):
    var name: String
    var kind: String
    var after: String
    var reads: List[String]
    var writes: List[String]


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
    var dataset: String
    """Empty for a table outside the warehouse."""


def _list(s: String) -> List[String]:
    """A comma-separated list, empty for an empty string."""
    var out = List[String]()
    if s:
        for p in s.split(","):
            out.append(String(p))
    return out^


def _json_strings(mut out: String, names: List[String]):
    out.write("[")
    for i in range(len(names)):
        out.write(", " if i else "", quote(names[i]))
    out.write("]")


def _dataset_links(mut h: Html, names: List[String]):
    for i in range(len(names)):
        if i:
            h.raw(", ")
        h.link(String("/datasets/", url_encode(names[i])), names[i])


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
            (
                "SELECT p.name, p.registry_hash, coalesce(p.binary_path, '') AS"
                " binary, coalesce(p.describe->'partitions'->>'kind', '') AS"
                " kind, coalesce(p.describe->'partitions'->>'start', '') AS"
                " start, coalesce(p.describe->'schedule'->>'cron', '') AS cron,"
                " coalesce(p.describe->'schedule'->>'tz', '') AS tz,"
                " coalesce(p.describe->'schedule'->>'fills', '') AS fills,"
                " coalesce(p.describe->>'max_runs', '1') AS max_runs,"
                " coalesce(p.describe->>'run_retries', '0') AS run_retries,"
                " (SELECT count(*) FROM orca.runs r WHERE r.pipeline = p.name"
                " AND r.status = 'running')::text AS running, (SELECT count(*)"
                " FROM orca.runs r WHERE r.pipeline = p.name AND r.status ="
                " 'queued')::text AS queued, coalesce(l.id, 0)::text AS"
                " last_run, coalesce(l.status, '') AS last_status,"
                " coalesce(l.partitions, '') AS last_partitions, p.paused,"
                " coalesce(p.sense_error, '') AS sense_error, "
            ),
            _ts("p.sense_error_at"),
            (
                " AS sense_error_at FROM orca.pipelines p LEFT JOIN LATERAL"
                " (SELECT id, status, partitions FROM orca.runs r WHERE"
                " r.pipeline = p.name ORDER BY id DESC LIMIT 1) l ON true WHERE"
                " ($1 = '' OR p.name = $1) ORDER BY p.name"
            ),
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
            r.bool("paused"),
            r.text("sense_error"),
            r.text("sense_error_at"),
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


comptime _WAITING = (
    "CASE WHEN status = 'deferred' THEN coalesce(retry_at, 0) ELSE 0 END"
)
"""SQL for when a deferred run starts again (`retry_at`), 0 for others."""


def _waiting(t: Int64) -> String:
    return _epoch(t) if t > 0 else String()


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
            " AS ended, cancel_requested, coalesce(error, '') AS error, ",
            stalled_sql(),
            " AS stalled, ",
            _WAITING,
            " AS waiting FROM orca.runs WHERE ($1 = '' OR pipeline = $1) AND",
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
                r.text("error"),
                _waiting(r.int64("waiting")),
            )
        )
    return out^


def _versions(
    mut db: State, pipeline: String = "", id: Int = 0
) raises -> List[VersionRow]:
    """A pipeline's versions newest first, or the one with `id`."""
    var rows = db.conn.query(
        String(
            "SELECT v.id, v.registry_hash, v.source, v.commit_sha, v.dirty,",
            " v.image, ",
            _ts("v.first_seen"),
            " AS first_seen, ",
            _ts("v.last_seen"),
            " AS last_seen, (SELECT count(*) FROM orca.runs r WHERE r.version",
            " = v.id) AS runs, coalesce(v.locations::text, '') AS locations",
            " FROM orca.pipeline_versions v WHERE ($1 = '' OR",
            " v.pipeline = $1) AND ($2 = 0 OR v.id = $2) ORDER BY v.id DESC",
        ),
        Params().text(pipeline).int64(Int64(id)),
    )
    var out = List[VersionRow]()
    for r in rows:
        out.append(
            VersionRow(
                Int(r.int64("id")),
                r.text("registry_hash"),
                r.text("source"),
                r.text("commit_sha"),
                r.bool("dirty"),
                r.text("image"),
                r.text("first_seen"),
                r.text("last_seen"),
                Int(r.int64("runs")),
                r.text("locations"),
            )
        )
    return out^


def _code_url(v: VersionRow, location: String) -> String:
    """`file#Lline` at the version's commit, or "" if it can't be linked."""
    var web = web_url(v.source)
    if not web or not v.commit or not location or not is_hex(v.commit):
        return ""
    if location.startswith("/") or ".." in location:
        return ""
    return String(web, "/blob/", v.commit, "/", location)


struct CodeLinks(Movable):
    """A version's code on its host: the pipeline's declaration, each
    step's function and each dista task, from `orca.locate`'s map."""

    var pipeline: String
    var steps: Dict[String, String]
    """Step name to its function's line."""
    var tasks: List[Tuple[String, String]]
    """A task's function name and its line."""

    def __init__(out self):
        self.pipeline = ""
        self.steps = Dict[String, String]()
        self.tasks = List[Tuple[String, String]]()

    def step(self, name: String) -> String:
        return self.steps.get(name, "")


def _run_version(mut db: State, run: Int) raises -> Optional[VersionRow]:
    """The version a run ran, if it recorded one."""
    var rows = db.conn.query(
        "SELECT coalesce(version, 0) AS v FROM orca.runs WHERE id = $1",
        Params().int64(Int64(run)),
    )
    if len(rows) == 0 or not rows.row(0).int64("v"):
        return None
    var vs = _versions(db, id=Int(rows.row(0).int64("v")))
    if len(vs) == 0:
        return None
    return vs[0].copy()


def _code_links(
    mut db: State, pipeline: String, v: Optional[VersionRow]
) -> CodeLinks:
    var out = CodeLinks()
    if not v or not v.value().locations:
        return out^
    ref ver = v.value()
    try:
        var fn_step = Dict[String, String]()
        var d = loads(
            db.conn.query(
                (
                    "SELECT describe::text AS d FROM orca.pipelines WHERE name"
                    " = $1"
                ),
                Params().text(pipeline),
            )
            .row(0)
            .text("d")
        )
        for st in d["steps"].array_items():
            fn_step[function_key(st["function"].string_value())] = st[
                "name"
            ].string_value()
        for kv in loads(ver.locations).object_items():
            var url = _code_url(ver, kv[1].string_value())
            if not url:
                continue
            var key = kv[0]
            if key == "pipeline":
                out.pipeline = url
            elif key in fn_step:
                out.steps[fn_step[key]] = url
            else:
                out.tasks.append(
                    (String(key[byte = key.rfind("::") + 2 :]), url)
                )
    except:
        pass
    return out^


def _code_html(mut h: Html, name: String, links: CodeLinks):
    """`Code`: the pipeline, then its dista tasks, each linked to its line."""
    if not links.pipeline and len(links.tasks) == 0:
        return
    h.raw("<dt>Code</dt><dd>")
    if links.pipeline:
        h.raw("pipeline ")
        h.link_out(links.pipeline, name)
    for i in range(len(links.tasks)):
        h.raw(" · " if i or links.pipeline else "", "task ")
        h.link_out(links.tasks[i][1], links.tasks[i][0])
    h.raw("</dd>")


def _step_cell(mut h: Html, name: String, links: CodeLinks):
    """A step's name, linked to its function where the code is known."""
    var url = links.step(name)
    h.raw("<td>")
    if url:
        h.link_out(url, name)
    else:
        h.text(name)
    h.raw("</td>")


def _docs(anchor: StaticString) -> String:
    """A small link to the docs' section, beside a heading."""
    var base = getenv("ORCA_DOCS_URL", "https://mlake.dev/s/orca/docs/")
    return String(
        ' <a class="doc" href="',
        escape(String(base, "#", anchor)),
        '" target="_blank" rel="noopener">docs</a>',
    )


def _json_version(mut out: String, v: VersionRow):
    var url = commit_url(v.source, v.commit)
    out.write(
        '{"id": ',
        v.id,
        ', "registry_hash": ',
        quote(v.registry_hash),
        ', "source": ',
        quote(v.source),
        ', "commit": ',
        quote(v.commit),
        ', "commit_url": ',
        quote(url) if url else String("null"),
        ', "dirty": ',
        "true" if v.dirty else "false",
        ', "image": ',
        quote(v.image),
        ', "first_seen": ',
        quote(v.first_seen),
        ', "last_seen": ',
        quote(v.last_seen),
        ', "runs": ',
        v.runs,
        ', "locations": ',
        v.locations if v.locations else String("null"),
        "}",
    )


def _source_html(mut h: Html, v: VersionRow):
    """The commit, linked to its tree where the host is known."""
    if not v.commit:
        h.raw('<span class="muted">not recorded (built without')
        h.raw(" <code>-D ORCA_COMMIT</code>)</span>")
        return
    var url = commit_url(v.source, v.commit)
    var short = String(v.commit[byte = : min(12, v.commit.byte_length())])
    h.raw("<code>")
    if url:
        h.link_out(url, short)
    else:
        h.text(short)
    h.raw("</code>")
    if v.dirty:
        h.raw(' <span class="muted">plus uncommitted changes</span>')
    if v.source:
        h.raw(' <span class="muted">of ')
        var web = web_url(v.source)
        if web:
            h.link_out(web, v.source)
        else:
            h.text(v.source)
        h.raw("</span>")


def _instances(mut db: State, run: Int) raises -> List[InstanceRow]:
    var rows = db.conn.query(
        String(
            "SELECT i.id, i.start, i.role, i.instance_type, i.market, ",
            _ts("i.launched"),
            " AS launched, ",
            _ts("i.ended"),
            " AS ended, coalesce(i.why, '') AS why,",
            " extract(epoch FROM i.launched - coalesce(r.started,",
            " i.launched))::float8 AS from_start, extract(epoch FROM",
            " coalesce(i.ended, now()) - coalesce(r.started,",
            " i.launched))::float8 AS to_end FROM orca.instances i JOIN",
            " orca.runs r ON r.id = i.run WHERE i.run = $1 ORDER BY",
            " i.launched, i.role, i.id",
        ),
        Params().int64(Int64(run)),
    )
    var out = List[InstanceRow]()
    for r in rows:
        out.append(
            InstanceRow(
                r.text("id"),
                Int(r.int64("start")),
                r.text("role"),
                r.text("instance_type"),
                r.text("market"),
                r.text("launched"),
                r.text("ended"),
                r.text("why"),
                r.float64("from_start"),
                r.float64("to_end"),
            )
        )
    return out^


def _cluster_log(i: InstanceRow) -> String:
    """The instance's log's name under its run's `cluster/`."""
    return String(i.role, "-", i.id, ".log")


def _json_instances(mut out: String, run: Int, xs: List[InstanceRow]):
    out.write("[")
    for k in range(len(xs)):
        ref i = xs[k]
        out.write(
            ", " if k else "",
            '{"id": ',
            quote(i.id),
            ', "start": ',
            i.start,
            ', "role": ',
            quote(i.role),
            ', "type": ',
            quote(i.instance_type),
            ', "market": ',
            quote(i.market),
            ', "launched": ',
            quote(i.launched),
            ', "ended": ',
            quote(i.ended) if i.ended else String("null"),
            ', "why": ',
            quote(i.why) if i.why else String("null"),
            ', "log": ',
            quote(String("/runs/", run, "/cluster/", _cluster_log(i))),
            "}",
        )
    out.write("]")


def _instances_html(mut h: Html, run: Int, xs: List[InstanceRow]):
    var changes = List[Tuple[Float64, Int]]()
    var total = Float64(0)
    for i in xs:
        if i.role == "worker":
            changes.append((i.from_start, 1))
            changes.append((i.to_end, -1))
        total = max(total, i.to_end)
    # In time order, a start before an end at the same moment.
    for a in range(1, len(changes)):
        var b = a
        while b > 0 and (
            changes[b - 1][0] > changes[b][0]
            or (
                changes[b - 1][0] == changes[b][0]
                and changes[b - 1][1] < changes[b][1]
            )
        ):
            var t = changes[b - 1]
            changes[b - 1] = changes[b]
            changes[b] = t
            b -= 1
    h.raw("<h2>Instances", _docs("scheduler"), "</h2>")
    if len(changes) > 0:
        h.raw('<div class="scroll">', workers_svg(changes, total), "</div>")
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Role</th><th>Instance</th>',
        "<th>Type</th><th>Start</th><th>Launched</th><th>Ended</th>",
        "<th>Why</th><th>Log</th></tr></thead><tbody>",
    )
    for i in xs:
        h.raw("<tr>")
        h.cell(i.role)
        h.raw("<td><code>")
        h.text(i.id)
        h.raw("</code></td>")
        h.cell(i.instance_type, " Spot" if i.market == "spot" else String(""))
        h.cell(i.start)
        h.cell(i.launched)
        if i.ended:
            h.cell(i.ended)
        else:
            h.raw(
                '<td><span class="status status--running">running</span></td>'
            )
        h.cell(i.why)
        h.raw("<td>")
        h.link(String("/runs/", run, "/cluster/", _cluster_log(i)), "log")
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")


def _step_defs(mut db: State, name: String) raises -> List[StepDef]:
    var rows = db.conn.query(
        (
            "SELECT s->>'name' AS name, s->>'kind' AS kind,"
            " coalesce(s->>'after', '') AS after, coalesce((SELECT"
            " string_agg(v, ',') FROM jsonb_array_elements_text(coalesce(s->"
            "'reads', '[]')) AS v), '') AS reads, coalesce((SELECT"
            " string_agg(v, ',') FROM jsonb_array_elements_text(coalesce(s->"
            "'writes', '[]')) AS v), '') AS writes FROM orca.pipelines p,"
            " jsonb_array_elements(p.describe->'steps') WITH ORDINALITY AS x(s,"
            " n) WHERE p.name = $1 ORDER BY n"
        ),
        Params().text(name),
    )
    var out = List[StepDef]()
    for r in rows:
        out.append(
            StepDef(
                r.text("name"),
                r.text("kind"),
                r.text("after"),
                _list(r.text("reads")),
                _list(r.text("writes")),
            )
        )
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
            " x.snapshot_id::text AS snapshot, coalesce(x.dataset, '') AS"
            " dataset FROM (SELECT attempt, 'read' AS kind, table_name,"
            " snapshot_id, dataset FROM orca.reads UNION ALL SELECT attempt,"
            " 'commit', table_name, snapshot_id, dataset FROM orca.commits) x"
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
                r.text("dataset"),
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
        ', "paused": ',
        "true" if p.paused else "false",
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
    out.write(', "sense_error": ')
    if p.sense_error:
        out.write(
            '{"at": ',
            quote(p.sense_error_at),
            ', "error": ',
            quote(p.sense_error),
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
        ', "error": ',
        quote(r.error),
        ', "waiting_until": ',
        quote(r.waiting_until) if r.waiting_until else String("null"),
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


comptime _USER_SLOT = "<!--orca-user-->"
"""Where `serve` puts who's logged in, or a link to log in."""

comptime _MARK = (
    '<svg class="top__mark" width="22" height="22" viewBox="0 0 24 24"'
    ' fill="none" aria-hidden="true"><path d="M12 1.9 22 20.2H2L12 1.9Z"'
    ' stroke="var(--lake)" stroke-width="1.6" stroke-linejoin="round"/>'
    '<path d="M12 9.4 17.4 19H6.6L12 9.4Z" fill="var(--ember)"/></svg>'
)
"""magmalake's mark, as mlake.dev draws it: a caldera seen from above, a
cold rim and a molten core."""


def _extra_links(mut h: Html):
    """The deployment's own nav links after Docs, from `ORCA_UI_LINKS`:
    `label=url` entries separated by commas, e.g. `Census ↗=https://...`.
    Entries without an http(s) url are left out."""
    for entry in _list(getenv("ORCA_UI_LINKS")):
        var eq = entry.find("=")
        if eq <= 0:
            continue
        var label = String(entry[byte=:eq].strip())
        var url = String(entry[byte = eq + 1 :].strip())
        if not label or not (
            url.startswith("https://") or url.startswith("http://")
        ):
            continue
        h.raw('<a href="')
        h.text(url)
        h.raw('" target="_blank" rel="noopener">')
        h.text(label)
        h.raw("</a>")


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
        '<header class="top"><a class="top__brand" href="/">',
        _MARK,
        "orca</a><nav>",
    )
    for item in [
        ("/", "Pipelines"),
        ("/runs", "Runs"),
        ("/datasets", "Datasets"),
    ]:
        h.raw('<a href="', item[0], '"')
        if item[0] == active:
            h.raw(' aria-current="page"')
        h.raw(">", item[1], "</a>")
    h.raw('<a href="')
    h.text(getenv("ORCA_DOCS_URL", "https://mlake.dev/s/orca/docs/"))
    h.raw('" target="_blank" rel="noopener">Docs</a>')
    _extra_links(h)
    h.raw("</nav>", _USER_SLOT, "</header><main>")
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
        "<th>Partitions</th><th>Trigger</th><th>Status</th>",
        '<th class="num">Starts</th>',
        "<th>Created</th><th>Ended</th></tr></thead><tbody>",
    )
    for r in runs:
        h.raw("<tr><td>")
        h.chip(String("/runs/", r.id), String(r.id))
        h.raw("</td>")
        if show_pipeline:
            h.raw("<td>")
            h.link(String("/pipelines/", url_encode(r.pipeline)), r.pipeline)
            h.raw("</td>")
        h.mono(r.partitions)
        h.cell(r.trigger)
        h.raw("<td>")
        h.status(r.status)
        if r.waiting_until:
            h.raw(' <span class="muted">until ')
            h.text(r.waiting_until)
            h.raw("</span>")
        h.raw("</td>")
        h.num(r.starts)
        h.mono(r.created)
        h.mono(r.ended)
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


def _runs_per_day(mut db: State, days: Int) raises -> List[DayRuns]:
    """Runs by the UTC day they started (or were made, if they haven't),
    for the last `days` days up to today."""
    var out = List[DayRuns]()
    for r in db.conn.query(
        (
            "SELECT d::date::text AS day, count(r.id) FILTER (WHERE r.status ="
            " 'succeeded') AS ok, count(r.id) FILTER (WHERE r.status ="
            " 'failed') AS bad, count(r.id) FILTER (WHERE r.status NOT IN"
            " ('succeeded', 'failed')) AS other FROM generate_series((now() AT"
            " TIME ZONE 'UTC')::date - ($1::int - 1), (now() AT TIME ZONE"
            " 'UTC')::date, interval '1 day') AS d LEFT JOIN orca.runs r ON"
            " (coalesce(r.started, r.created) AT TIME ZONE 'UTC')::date ="
            " d::date GROUP BY d ORDER BY d"
        ),
        Params().int64(Int64(days)),
    ):
        out.append(
            DayRuns(
                r.text("day"),
                Int(r.int64("ok")),
                Int(r.int64("bad")),
                Int(r.int64("other")),
            )
        )
    return out^


def home(mut db: State) raises -> Page:
    var ps = _pipelines(db)
    var recent = _runs(db, limit=10)
    var per_day = _runs_per_day(db, 30)
    var j = String('{"pipelines": [')
    for i in range(len(ps)):
        if i:
            j.write(", ")
        _json_pipeline(j, ps[i])
    j.write('], "runs": ')
    _json_runs(j, recent)
    j.write(', "runs_per_day": [')
    for i in range(len(per_day)):
        ref d = per_day[i]
        j.write(
            ", " if i else "",
            '{"day": ',
            quote(d.day),
            ', "succeeded": ',
            d.succeeded,
            ', "failed": ',
            d.failed,
            ', "other": ',
            d.other,
            "}",
        )
    j.write("]}")

    var h = Html()
    h.raw("<h1>Pipelines</h1>")
    if len(ps) == 0:
        h.raw(
            '<p class="empty">No pipelines yet: <code>orca register',
            " &lt;binary&gt;</code> adds one.</p>",
        )
    else:
        h.raw(
            '<section class="card"><div class="scroll"><table><thead><tr>',
            "<th>Pipeline</th><th>Partitions</th><th>Schedule</th>",
            "<th>Next run</th><th>Running</th><th>Queued</th><th>Last run</th>",
            "</tr></thead><tbody>",
        )
        for p in ps:
            h.raw("<tr><td>")
            h.link(String("/pipelines/", url_encode(p.name)), p.name)
            if p.paused:
                h.raw(" ")
                h.status("paused")
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
                h.mono(p.next_fire, " → ", p.next_partition)
            else:
                h.raw('<td class="muted">none</td><td></td>')
            # The counts as badges, lit while they're not zero.
            h.raw(
                '<td><span class="badge',
                " badge--running" if p.running > 0 else "",
                '">',
            )
            h.text(p.running, " / ", p.max_runs)
            h.raw(
                '</span></td><td><span class="badge',
                " badge--queued" if p.queued > 0 else "",
                '">',
            )
            h.text(p.queued)
            h.raw("</span></td><td>")
            if p.last_run:
                h.chip(String("/runs/", p.last_run), String(p.last_run))
                h.raw(" ")
                h.status(p.last_status)
                h.raw(' <span class="mono muted">')
                h.text(p.last_partitions)
                h.raw("</span>")
            h.raw("</td></tr>")
        h.raw("</tbody></table></div></section>")
    h.raw(
        '<section class="card"><h2>Runs per day</h2>',
        '<p class="muted">The last 30 days, by the day each run started',
        " (UTC).</p>",
        '<div class="scroll">',
        runs_chart(per_day),
        "</div></section>",
    )
    h.raw('<section class="card"><h2>Recent runs</h2>')
    _runs_table(h, recent, True)
    h.raw('<p class="more"><a href="/runs">All runs</a></p></section>')
    return Page(200, j^, _page("Pipelines", "/", h))


@fieldwise_init
struct KeyCounts(Copyable, Movable):
    """A keyed pipeline's keys and how far it is through them."""

    var dataset: String
    var snapshot: Int64
    var total: Int
    var done: Int
    var live: Int
    var failed: List[String]
    """Keys not done or in a live run, with a failed or skipped step."""
    var error: String
    """Why its keys couldn't be read, or empty."""
    var read_at: String
    """When `<binary> keys` printed them."""
    var note: String
    """Why they're older than they'd be, or empty."""


comptime _UI_KEYS_SECONDS = 10.0
"""How long the page waits for `<binary> keys` before showing the keys
last read, if any: it runs on the server's one thread."""


def _keys_seconds() -> Float64:
    """How old kept keys may be before the page reads them again: the
    scheduler's `ORCA_SENSE_SECONDS` (60)."""
    try:
        return Float64(getenv("ORCA_SENSE_SECONDS", "60"))
    except:
        return 60.0


comptime _FAILED_SQL = (
    "SELECT DISTINCT st.partition AS key FROM orca.steps st JOIN orca.runs r"
    " ON r.id = st.run WHERE r.pipeline = $1 AND st.status IN ('failed',"
    " 'skipped') AND st.partition = ANY (string_to_array($2, ',')) ORDER BY 1"
)


def _key_counts(mut db: State, name: String) raises -> KeyCounts:
    var ps = registered(db, name)
    if len(ps) == 0:
        return KeyCounts(
            "", -1, 0, 0, 0, List[String](), "not registered", "", ""
        )
    ref p = ps[0]
    # The keys last read (by the scheduler, or this page) while they're
    # fresh, so a view doesn't scan the keys column each time.
    var got: Tuple[Int64, List[String]]
    var note = String()
    var kept = kept_keys(db, name)
    if kept and kept.value().age < _keys_seconds():
        got = (kept.value().snapshot, kept.value().keys.copy())
    else:
        try:
            got = current_keys(p, _UI_KEYS_SECONDS)
            keep_keys(db, name, got)
            kept = kept_keys(db, name)
        except e:
            if not kept:
                return KeyCounts(
                    p.keys_dataset,
                    -1,
                    0,
                    0,
                    0,
                    List[String](),
                    String(e),
                    "",
                    "",
                )
            got = (kept.value().snapshot, kept.value().keys.copy())
            note = String("reading them again failed: ", e)
    var read_at = kept.value().read_at if kept else String()
    var done = done_keys(db, name, got[1])
    var live = Dict[String, Bool]()
    for r in db.conn.query(live_sql(), Params().text(name)):
        live[r.text("key")] = True
    var n_live = 0
    var open = String()
    for k in got[1]:
        if k in live:
            n_live += 1
        elif k not in done:
            open.write("," if open else "", k)
    var failed = List[String]()
    if open:
        for r in db.conn.query(_FAILED_SQL, Params().text(name).text(open)):
            failed.append(r.text("key"))
    return KeyCounts(
        p.keys_dataset,
        got[0],
        len(got[1]),
        len(done),
        n_live,
        failed^,
        "",
        read_at,
        note,
    )


def _json_keys(mut out: String, k: KeyCounts):
    out.write(
        '{"dataset": ',
        quote(k.dataset),
        ', "snapshot": ',
        k.snapshot,
        ', "total": ',
        k.total,
        ', "done": ',
        k.done,
        ', "live": ',
        k.live,
        ', "failed": ',
    )
    _json_strings(out, k.failed)
    out.write(
        ', "error": ',
        quote(k.error),
        ', "read_at": ',
        quote(k.read_at),
        ', "note": ',
        quote(k.note),
        "}",
    )


def _keys_html(mut h: Html, name: String, k: KeyCounts):
    h.raw("<h2>Keys", _docs("partitions"), "</h2>")
    if k.error:
        h.raw('<p class="empty">Its keys can\'t be read: ')
        h.text(k.error)
        h.raw("</p>")
        return
    h.raw('<dl class="facts"><dt>From</dt><dd>')
    h.link(_dataset_href(k.dataset), k.dataset)
    h.raw(" @ ")
    h.text(k.snapshot)
    if k.read_at:
        h.raw(' <span class="muted">read ')
        h.text(k.read_at)
        h.raw("</span>")
    if k.note:
        h.raw(' <span class="error">')
        h.text(k.note)
        h.raw("</span>")
    h.raw("</dd><dt>Keys</dt><dd>")
    h.text(k.total)
    h.raw("</dd><dt>Done</dt><dd>")
    h.text(k.done)
    h.raw("</dd><dt>In live runs</dt><dd>")
    h.text(k.live)
    h.raw("</dd><dt>Failed</dt><dd>")
    h.text(len(k.failed))
    h.raw("</dd></dl>")
    if len(k.failed) == 0:
        return
    var base = String("/pipelines/", url_encode(name))
    h.raw('<p class="more">')
    for i in range(min(len(k.failed), 100)):
        if i:
            h.raw(", ")
        h.link(String(base, "/keys/", url_encode(k.failed[i])), k.failed[i])
    if len(k.failed) > 100:
        h.raw(" and ")
        h.text(len(k.failed) - 100)
        h.raw(" more")
    var all = String()
    for i in range(len(k.failed)):
        all.write("," if i else "", k.failed[i])
    h.raw(
        '</p><div class="actions"><form class="action" method="post" action="'
    )
    h.text(base, "/backfill")
    h.raw('"><input type="hidden" name="partitions" value="')
    h.text(all)
    h.raw(
        '"><button type="submit">Backfill the failed keys</button></form></div>'
    )


def _control_html(mut h: Html, p: PipelineRow):
    """The forms that cancel a pipeline's runs in bulk and pause it."""
    var base = String("/pipelines/", url_encode(p.name))
    h.raw(
        "<h2>Stop runs</h2>",
        '<div class="actions"><form class="action" method="post" action="',
    )
    h.text(base, "/cancel")
    h.raw('"><label>Of <input name="partitions" placeholder="')
    h.text("@all, or names" if p.kind == "keyed" else "@all, or a range")
    h.raw(
        '"></label><button type="submit">Cancel queued runs</button></form>',
        '<form class="action" method="post" action="',
    )
    if p.paused:
        h.text(base, "/unpause")
        h.raw('"><button type="submit">Unpause</button></form></div>')
    else:
        h.text(base, "/pause")
        h.raw('"><button type="submit">Pause</button></form></div>')
    h.raw(
        '<p class="muted more">Cancelling stops runs that are queued or',
        " waiting to be retried, and asks running ones to stop before their",
        " next step. ",
    )
    if p.paused:
        h.raw(
            "Paused: the scheduler starts none of its runs; fires, sensors,",
            " retries and backfills queue theirs until it's unpaused.</p>",
        )
    else:
        h.raw(
            "Pausing stops the scheduler starting its runs, which stay",
            " queued; runs already going go on.</p>",
        )


def key_page(mut db: State, name: String, key: String) raises -> Page:
    """One key of a keyed pipeline: its steps in every run it was in."""
    var ps = _pipelines(db, name)
    if len(ps) == 0:
        return not_found(String("no pipeline ", name))
    var rows = db.conn.query(
        (
            "SELECT r.id AS run, r.status AS run_status, r.registry_hash ="
            " p.registry_hash AS current, st.step, st.status,"
            " coalesce(st.error, '') AS error, (SELECT count(*) FROM"
            " orca.attempts a WHERE a.step = st.id) AS attempts FROM orca.steps"
            " st JOIN orca.runs r ON r.id = st.run JOIN orca.pipelines p ON"
            " p.name = r.pipeline WHERE r.pipeline = $1 AND st.partition = $2"
            " ORDER BY r.id, st.ord"
        ),
        Params().text(name).text(key),
    )
    var done = done_keys(db, name, [key])
    var j = String(
        '{"pipeline": ',
        quote(name),
        ', "key": ',
        quote(key),
        ', "done": ',
        "true" if key in done else "false",
        ', "steps": [',
    )
    var h = Html()
    h.raw('<p class="crumb"><a href="/">Pipelines</a> · ')
    h.link(String("/pipelines/", url_encode(name)), name)
    h.raw("</p><h1>")
    h.text(key)
    h.raw(" ")
    h.status("succeeded" if key in done else "not done")
    h.raw("</h1>")
    if len(rows) == 0:
        h.raw('<p class="empty">No run has had this key.</p>')
    else:
        h.raw(
            '<div class="scroll"><table><thead><tr><th>Run</th><th>Step</th>',
            "<th>Status</th><th>Attempts</th><th>Code</th><th>Error</th>",
            "</tr></thead><tbody>",
        )
    var i = 0
    for r in rows:
        j.write(
            ", " if i else "",
            '{"run": ',
            r.int64("run"),
            ', "step": ',
            quote(r.text("step")),
            ', "status": ',
            quote(r.text("status")),
            ', "attempts": ',
            r.int64("attempts"),
            ', "current_code": ',
            "true" if r.bool("current") else "false",
            ', "error": ',
            quote(r.text("error")),
            "}",
        )
        i += 1
        h.raw("<tr><td>")
        h.chip(String("/runs/", r.int64("run")), String(r.int64("run")))
        h.raw("</td>")
        h.cell(r.text("step"))
        h.raw("<td>")
        h.status(r.text("status"))
        h.raw("</td>")
        h.num(r.int64("attempts"))
        h.cell("current" if r.bool("current") else "older")
        h.cell(r.text("error"))
        h.raw("</tr>")
    if len(rows) > 0:
        h.raw("</tbody></table></div>")
    j.write("]}")
    return Page(200, j^, _page(String(name, " · ", key), "/", h))


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
            ', "reads": ',
        )
        _json_strings(j, steps[i].reads)
        j.write(', "writes": ')
        _json_strings(j, steps[i].writes)
        j.write("}")
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
    var versions = _versions(db, pipeline=name)
    j.write('], "versions": [')
    for i in range(len(versions)):
        if i:
            j.write(", ")
        _json_version(j, versions[i])
    j.write('], "runs": ')
    _json_runs(j, runs)
    var keys = Optional[KeyCounts](None)
    if p.kind == "keyed":
        keys = _key_counts(db, name)
        j.write(', "keys": ')
        _json_keys(j, keys.value())
    # Snapshots its sensor saw and couldn't run, latest first.
    var unrun = List[Tuple[SnapshotRef, String, String]]()
    for x in db.conn.query(
        String(
            "SELECT dataset, snapshot_id, error, ",
            _ts("created"),
            " AS at FROM orca.sensed WHERE pipeline = $1 AND error IS NOT",
            " NULL ORDER BY created DESC, dataset LIMIT 10",
        ),
        Params().text(name),
    ):
        unrun.append(
            (
                SnapshotRef(x.text("dataset"), x.int64("snapshot_id")),
                x.text("at"),
                x.text("error"),
            )
        )
    j.write(', "not_run": [')
    for i in range(len(unrun)):
        ref u = unrun[i]
        j.write(
            ", " if i else "",
            '{"dataset": ',
            quote(u[0].dataset),
            ', "snapshot": ',
            u[0].snapshot,
            ', "at": ',
            quote(u[1]),
            ', "error": ',
            quote(u[2]),
            "}",
        )
    j.write("]}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/">Pipelines</a></p><h1>')
    h.text(p.name)
    if p.paused:
        h.raw(" ")
        h.status("paused")
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
    if p.sense_error:
        h.raw('</dd><dt>Sensing</dt><dd class="error">Failing since ')
        h.text(p.sense_error_at, ": ", p.sense_error)
    h.raw("</dd><dt>Binary</dt><dd><code>")
    h.text(p.binary)
    h.raw("</code></dd><dt>Code</dt><dd><code>")
    h.text(p.registry_hash)
    h.raw("</code></dd>")
    var latest = _versions(db, pipeline=name)
    var links = _code_links(
        db,
        name,
        latest[0].copy() if len(latest) > 0 else Optional[VersionRow](None),
    )
    _code_html(h, name, links)
    h.raw("</dl>")
    var dag = List[DagStep]()
    for d in steps:
        dag.append(DagStep(d.name, d.kind, d.after, "", links.step(d.name)))
    h.raw(
        '<div class="scroll">',
        dag_svg(dag),
        "</div><h2>Steps",
        _docs("steps"),
        "</h2>",
    )
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Step</th><th>Kind</th>',
        "<th>After</th><th>Reads</th><th>Writes</th></tr></thead><tbody>",
    )
    for s in steps:
        h.raw("<tr>")
        _step_cell(h, s.name, links)
        h.cell(s.kind)
        h.cell(s.after)
        h.raw("<td>")
        _dataset_links(h, s.reads)
        h.raw("</td><td>")
        _dataset_links(h, s.writes)
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")
    if keys:
        _keys_html(h, p.name, keys.value())
    if len(unrun) > 0:
        h.raw(
            "<h2>Snapshots not run",
            _docs("sensors"),
            '</h2><p class="muted">Seen by its sensor, but holding keys that',
            " aren't its partitions. A backfill runs what they should have.",
            '</p><div class="scroll"><table><thead><tr><th>Snapshot</th>',
            "<th>Seen</th><th>Why</th></tr></thead><tbody>",
        )
        for u in unrun:
            h.raw("<tr><td>")
            h.link(_snapshot_href(u[0]), String(u[0]))
            h.raw("</td>")
            h.cell(u[1])
            h.cell(u[2])
            h.raw("</tr>")
        h.raw("</tbody></table></div>")
    if p.binary:
        var base = String("/pipelines/", url_encode(p.name))
        var example = String("2024-01-01" if p.kind == "daily" else "2024-01")
        h.raw(
            "<h2>Start runs",
            _docs("cli"),
            '</h2><div class="actions">',
            '<form class="action" method="post" action="',
        )
        h.text(base, "/runs")
        h.raw('"><label>One run over <input name="partitions" required')
        h.raw(' placeholder="')
        if p.kind == "keyed":
            h.text("name,name")
        else:
            h.text(example, "..", example)
        h.raw(
            '"></label><button type="submit">Start</button></form>',
            '<form class="action" method="post" action="',
        )
        h.text(base, "/backfill")
        h.raw("\"><label>Runs of what isn't done <input")
        h.raw(' name="partitions" required placeholder="')
        if p.kind == "keyed":
            h.text("@all, or names")
        else:
            h.text(example, "..", example)
        h.raw(
            '"></label><label><input type="checkbox" name="rerun" value="1">',
            " and what is</label>",
            '<button type="submit">Backfill</button></form></div>',
            '<p class="muted more">The scheduler (<code>orca scheduler',
            "</code>) starts queued runs, up to ",
        )
        h.text(p.max_runs)
        h.raw(" at once.</p>")
    _control_html(h, p)
    if len(fires) > 0:
        h.raw(
            "<h2>Next runs",
            _docs("partitions"),
            '</h2><div class="scroll"><table><thead><tr>',
            "<th>UTC</th><th>Wall clock</th><th>Fills</th></tr></thead><tbody>",
        )
        for f in fires:
            h.raw("<tr>")
            h.mono(f.at)
            h.mono(f.local)
            h.mono(f.partition)
            h.raw("</tr>")
        h.raw("</tbody></table></div>")
    if len(versions) > 0:
        h.raw(
            "<h2>Versions",
            _docs("build"),
            '</h2><div class="scroll"><table><thead><tr>',
            "<th>Commit</th><th>Image</th><th>Code</th><th>First seen</th>",
            '<th>Last seen</th><th class="num">Runs</th></tr></thead><tbody>',
        )
        for v in versions:
            h.raw("<tr><td>")
            _source_html(h, v)
            h.raw("</td>")
            h.mono(v.image)
            h.raw("<td><code>")
            h.text(v.registry_hash)
            h.raw("</code></td>")
            h.mono(v.first_seen)
            h.mono(v.last_seen)
            h.num(v.runs)
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
    var row = db.conn.query(
        (
            "SELECT registry_hash, coalesce(rebuild, 0) AS rebuild,"
            " coalesce(version, 0) AS version,"
            " coalesce(cpu_millis, 0) AS cpu, coalesce(extract(epoch FROM"
            " coalesce(ended, now()) - started)::bigint, 0) AS secs FROM"
            " orca.runs WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    ).row(0)
    var hash = row.text("registry_hash")
    var rebuild = Int(row.int64("rebuild"))
    var version = Optional[VersionRow](None)
    if row.int64("version"):
        var vs = _versions(db, id=Int(row.int64("version")))
        if len(vs) > 0:
            version = vs[0].copy()
    # What its pods asked for, on Kubernetes: vCPU × seconds since it started.
    var cpu = Int(row.int64("cpu"))
    var secs = Int(row.int64("secs"))
    # The snapshots that started it, for a sensor's run.
    var sensed = List[SnapshotRef]()
    for x in db.conn.query(
        (
            "SELECT dataset, snapshot_id FROM orca.sensed WHERE run = $1"
            " ORDER BY dataset, snapshot_id"
        ),
        Params().int64(Int64(id)),
    ):
        sensed.append(SnapshotRef(x.text("dataset"), x.int64("snapshot_id")))

    var j = String('{"run": ')
    _json_run(j, r)
    j.write(
        ', "registry_hash": ',
        quote(hash),
        ', "current_code": ',
        "true" if hash == current else "false",
        ', "rebuild": ',
        String(rebuild) if rebuild else String("null"),
        ', "cpu_millis": ',
        String(cpu) if cpu else String("null"),
        ', "cpu_seconds": ',
        String(cpu * secs // 1000) if cpu else String("null"),
        ', "version": ',
    )
    if version:
        _json_version(j, version.value())
    else:
        j.write("null")
    var instances = _instances(db, id)
    j.write(', "instances": ')
    _json_instances(j, id, instances)
    j.write(', "sensed": [')
    for i in range(len(sensed)):
        j.write(", " if i else "", quote(String(sensed[i])))
    j.write('], "steps": [')
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
            ', "dataset": ',
            quote(t.dataset) if t.dataset else String("null"),
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
    if rebuild:
        h.raw(" ")
        h.link(String("/rebuilds/", rebuild), String("rebuild ", rebuild))
    if len(sensed) > 0:
        h.raw(" of ")
        for i in range(len(sensed)):
            if i:
                h.raw(", ")
            h.link(_snapshot_href(sensed[i]), String(sensed[i]))
    h.raw("</dd><dt>Created</dt><dd>")
    h.text(r.created)
    h.raw("</dd><dt>Started</dt><dd>")
    h.text(r.started)
    h.raw("</dd><dt>Ended</dt><dd>")
    h.text(r.ended)
    if r.waiting_until:
        h.raw("</dd><dt>Waiting</dt><dd>until ")
        h.text(r.waiting_until)
        h.raw(", when a step asked to run again")
    h.raw("</dd><dt>Starts</dt><dd>")
    h.text(r.starts)
    if cpu:
        h.raw("</dd><dt>CPU</dt><dd>")
        h.text(
            cpu // 1000,
            ".",
            String(cpu % 1000 + 1000)[byte=1:],
            " vCPU for ",
            secs,
            " s, ",
            cpu * secs // 1000,
            " vCPU-seconds",
        )
    h.raw("</dd><dt>Code</dt><dd><code>")
    h.text(hash)
    h.raw("</code>")
    if hash != current:
        h.raw(' <span class="muted">(the pipeline has newer code)</span>')
    h.raw("</dd>")
    if version:
        h.raw("<dt>Source</dt><dd>")
        _source_html(h, version.value())
        if version.value().image:
            h.raw(' <span class="muted">in image</span> <code>')
            h.text(version.value().image)
            h.raw("</code>")
        h.raw("</dd>")
        _code_html(h, r.pipeline, _code_links(db, r.pipeline, version))
    var dir = run_dir(r.pipeline, r.id)
    if dir and location_exists(String(dir, "/driver.log")):
        h.raw("<dt>Driver</dt><dd>")
        h.link(String("/runs/", r.id, "/driver.log"), "driver.log")
        h.raw("</dd>")
    var timeline = _timeline_path(db, r)
    if timeline:
        h.raw("<dt>Timeline</dt><dd>")
        h.link(String("/runs/", r.id, "/timeline.json"), "timeline.json")
        # Perfetto fetches it itself, so only from a UI it can reach.
        var base = String(getenv("ORCA_UI_URL").rstrip("/"))
        if base:
            h.raw(' <span class="muted">·</span> ')
            h.link_out(
                String(
                    "https://ui.perfetto.dev/#!/?url=",
                    url_encode(String(base, "/runs/", r.id, "/timeline.json")),
                ),
                "open in Perfetto",
            )
            h.raw("</dd>")
        else:
            h.raw(
                ' <span class="muted">(Dista tasks; open it in',
                ' <a href="https://ui.perfetto.dev" target="_blank"',
                ' rel="noopener">Perfetto</a>)</span></dd>',
            )
    if r.error:
        h.raw("<dt>Error</dt><dd>")
        h.text(r.error)
        h.raw("</dd>")
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
    if timeline:
        try:
            var t = read_timeline(
                String(from_utf8_lossy=Span(read_location(timeline)))
            )
            h.raw("<h2>Timeline", _docs("dista"), "</h2>")
            if t.left_out:
                h.raw('<p class="muted">The first ')
                h.text(MAX_BARS, " tasks; ", t.left_out, " more are in")
                h.raw(" timeline.json.</p>")
            h.raw('<div class="scroll">', timeline_svg(t), "</div>")
        except e:
            h.raw('<p class="muted">The timeline can\'t be drawn: ')
            h.text(e)
            h.raw("</p>")
    if len(instances) > 0:
        _instances_html(h, id, instances)
    # Who did what to it from the UI (orca.auth).
    var acts = db.conn.query(
        (
            "SELECT to_char(at AT TIME ZONE 'UTC',"
            ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS at, username, action FROM'
            " orca.audit WHERE run = $1 ORDER BY id"
        ),
        Params().int64(Int64(id)),
    )
    if len(acts) > 0:
        h.raw(
            '<h2>Actions</h2><div class="scroll"><table><thead><tr>',
            "<th>At</th><th>By</th><th>Action</th></tr></thead><tbody>",
        )
        for a in acts:
            h.raw("<tr>")
            h.mono(a.text("at"))
            h.cell(a.text("username"))
            h.cell(a.text("action"))
            h.raw("</tr>")
        h.raw("</tbody></table></div>")
    if len(touches) > 0:
        h.raw(
            "<h2>Tables",
            _docs("tables"),
            '</h2><div class="scroll"><table><thead><tr>',
            "<th>Step</th><th>Partition</th><th></th><th>Table</th>",
            "<th>Snapshot</th></tr></thead><tbody>",
        )
        for t in touches:
            h.raw("<tr>")
            h.cell(t.step)
            h.cell(t.partition)
            h.cell(t.kind)
            h.raw("<td>")
            if t.dataset:
                h.link(String("/datasets/", url_encode(t.dataset)), t.dataset)
            else:
                h.raw("<code>")
                h.text(t.table)
                h.raw("</code>")
            h.raw("</td><td>")
            if t.dataset and t.snapshot != "-1":
                h.link(
                    String(
                        "/datasets/",
                        url_encode(t.dataset),
                        "/snapshots/",
                        t.snapshot,
                    ),
                    t.snapshot,
                )
            else:
                h.text(t.snapshot)
            h.raw("</td></tr>")
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
    var resumable = (
        r.status == "failed"
        or r.status == "cancelled"
        or r.status == "deferred"
        or r.stalled
    )
    var cancellable = (
        r.status == "queued"
        or r.status == "failed"
        or r.status == "running"
        or r.status == "deferred"
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
    var links = _code_links(db, r.pipeline, _run_version(db, r.id))
    var dag = List[DagStep]()
    for d in defs:
        var st = List[String]()
        for s in steps:
            if s.step == d.name:
                st.append(s.status)
        dag.append(
            DagStep(d.name, d.kind, d.after, aggregate(st), links.step(d.name))
        )
    h.raw('<div class="scroll">', dag_svg(dag), "</div><h2>Steps ")
    h.status(r.status)
    h.raw(
        '</h2><div class="scroll"><table><thead><tr><th>Step</th>',
        '<th>Partition</th><th>Status</th><th class="num">Attempts</th>',
        "<th>Started</th><th>Ended</th><th>Log</th><th>Error</th></tr>",
        "</thead><tbody>",
    )
    for s in steps:
        h.raw("<tr>")
        _step_cell(h, s.step, links)
        h.mono(s.partition)
        h.raw("<td>")
        h.status(s.status)
        h.raw("</td>")
        h.num(s.attempts)
        h.mono(s.started)
        h.mono(s.ended)
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
        if location_exists(path):
            return path
    return ""


def timeline(mut db: State, id: Int) raises -> Page:
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var path = _timeline_path(db, found.value())
    if not path:
        return not_found(String("run ", id, " has no timeline"))
    var body = String(from_utf8_lossy=Span(read_location(path)))
    return Page(200, String(), body^, "application/json")


def driver_log(mut db: State, id: Int) raises -> Page:
    """The run's driver log, as the scheduler kept it under ORCA_LOGS."""
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var dir = run_dir(found.value().pipeline, id)
    var got = _read_log(String(dir, "/driver.log")) if dir else (
        String(),
        False,
    )
    if not got[0]:
        return not_found(String("run ", id, " has no driver log"))
    return Page(200, String(), got[0], "text/plain; charset=utf-8")


def cluster_log(mut db: State, id: Int, name: String) raises -> Page:
    """An instance's log, as its script kept it under ORCA_LOGS: only the
    names of the run's own instances are served."""
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var known = False
    for i in _instances(db, id):
        if _cluster_log(i) == name:
            known = True
    var dir = run_dir(found.value().pipeline, id)
    if not known or not dir:
        return not_found(String("run ", id, " has no log ", name))
    var path = String(dir, "/cluster/", name)
    var got = _read_log(path)
    if not got[0] and not location_exists(path):
        return not_found(String("run ", id, " has no log ", name, " yet"))
    return Page(200, String(), got[0], "text/plain; charset=utf-8")


comptime _LOG_LIMIT = 1 << 20
"""The most of a log a page shows: its last MiB."""


def _read_log(path: String) -> Tuple[String, Bool]:
    """The log's text, its last MiB if it's longer, and whether it was cut;
    empty if it can't be read."""
    if not path:
        return (String(), False)
    try:
        var b = read_location(path)
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
            " AS ended, cancel_requested, coalesce(error, '') AS error, ",
            stalled_sql(),
            " AS stalled, ",
            _WAITING,
            " AS waiting FROM orca.runs WHERE id = $1",
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
        r.text("error"),
        _waiting(r.int64("waiting")),
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


# ── datasets and lineage ─────────────────────────────────────────────────────


def _dataset_href(name: String) -> String:
    return String("/datasets/", url_encode(name))


def _snapshot_href(at: SnapshotRef) -> String:
    return String(_dataset_href(at.dataset), "/snapshots/", at.snapshot)


def _flow_graph(mut db: State, mark: String) raises -> String:
    """The graph of every pipeline and the datasets it reads and writes,
    with the dataset `mark` (if any) marked."""
    var fs = flows(db)
    var nodes = List[Node]()
    var keys = List[String]()
    var edges = List[Edge]()
    for f in fs:
        var ends = List[Int]()
        for kind, name in [("pipeline", f.pipeline), ("dataset", f.dataset)]:
            var key = String(kind, ":", name)
            var at = -1
            for i in range(len(keys)):
                if keys[i] == key:
                    at = i
            if at < 0:
                at = len(nodes)
                keys.append(key)
                var is_pipeline = kind == "pipeline"
                nodes.append(
                    Node(
                        name,
                        String(kind),
                        String(kind),
                        String(
                            "/pipelines/", url_encode(name)
                        ) if is_pipeline else _dataset_href(name),
                        "start" if (not is_pipeline and name == mark) else "",
                    )
                )
            ends.append(at)
        if f.writes:
            edges.append(Edge(ends[0], ends[1]))
        else:
            edges.append(Edge(ends[1], ends[0]))
    if len(nodes) == 0:
        return String()
    return graph_svg(
        nodes, edges, "Pipelines and the datasets they read and write"
    )


def datasets_page(mut db: State) raises -> Page:
    var ds = dataset_rows(db)
    var j = String('{"datasets": [')
    for i in range(len(ds)):
        ref d = ds[i]
        j.write(", " if i else "", '{"name": ', quote(d.name), ', "writers": ')
        _json_strings(j, d.writers)
        j.write(', "readers": ')
        _json_strings(j, d.readers)
        j.write(
            ', "snapshots": ',
            d.commits,
            ', "latest": ',
            String(d.latest) if d.latest >= 0 else String("null"),
            ', "latest_run": ',
            d.latest_run,
            "}",
        )
    j.write("]}")

    var h = Html()
    h.raw("<h1>Datasets</h1>")
    if len(ds) == 0:
        h.raw(
            '<p class="empty">No datasets: no registered pipeline declares',
            " <code>reads=</code> or <code>writes=</code>.</p>",
        )
        return Page(200, j^, _page("Datasets", "/datasets", h))
    h.raw('<div class="scroll">', _flow_graph(db, ""), "</div>")
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Dataset</th>',
        '<th>Written by</th><th>Read by</th><th class="num">Snapshots</th>',
        "<th>Latest</th><th>By run</th></tr></thead><tbody>",
    )
    for d in ds:
        h.raw("<tr><td>")
        h.link(_dataset_href(d.name), d.name)
        h.raw("</td><td>")
        _pipeline_links(h, d.writers)
        h.raw("</td><td>")
        _pipeline_links(h, d.readers)
        h.raw("</td>")
        h.num(d.commits)
        h.raw("<td>")
        if d.latest >= 0:
            h.link(
                _snapshot_href(SnapshotRef(d.name, d.latest)), String(d.latest)
            )
        h.raw("</td><td>")
        if d.latest_run:
            h.chip(String("/runs/", d.latest_run), String(d.latest_run))
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")
    return Page(200, j^, _page("Datasets", "/datasets", h))


def _pipeline_links(mut h: Html, names: List[String]):
    for i in range(len(names)):
        if i:
            h.raw(", ")
        h.link(String("/pipelines/", url_encode(names[i])), names[i])


def dataset_page(mut db: State, name: String) raises -> Page:
    var ds = dataset_rows(db, name)
    if len(ds) == 0:
        return not_found(String("no dataset ", name))
    ref d = ds[0]
    var cs = commits(db, name)
    var rbs = rebuilds(db, dataset=name, limit=10)
    var j = String('{"dataset": ', quote(d.name), ', "writers": ')
    _json_strings(j, d.writers)
    j.write(', "readers": ')
    _json_strings(j, d.readers)
    j.write(', "rebuilds": [')
    for i in range(len(rbs)):
        if i:
            j.write(", ")
        _json_rebuild(j, rbs[i])
    j.write('], "snapshots": [')
    for i in range(len(cs)):
        ref c = cs[i]
        j.write(
            ", " if i else "",
            '{"snapshot": ',
            c.snapshot,
            ', "pipeline": ',
            quote(c.pipeline),
            ', "run": ',
            c.run,
            ', "step": ',
            quote(c.step),
            ', "partition": ',
            quote(c.partition),
            ', "ended": ',
            quote(c.ended),
            "}",
        )
    j.write("]}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/datasets">Datasets</a></p><h1>')
    h.text(d.name)
    h.raw('</h1><dl class="facts"><dt>Written by</dt><dd>')
    _pipeline_links(h, d.writers)
    h.raw("</dd><dt>Read by</dt><dd>")
    _pipeline_links(h, d.readers)
    h.raw("</dd></dl>")
    h.raw('<div class="scroll">', _flow_graph(db, d.name), "</div>")
    if len(d.readers) > 0:
        h.raw('<div class="actions"><form class="action" method="post"')
        h.raw(' action="', _dataset_href(d.name), '/rebuild">')
        h.raw(
            '<label>Rebuild downstream over <input name="partitions"',
            ' placeholder="2024-01..2024-03" required></label>',
            '<button type="submit">Rebuild</button><span class="muted">',
            " every pipeline that reads it, and what reads theirs, in",
            " order</span></form></div>",
        )
    if len(rbs) > 0:
        h.raw("<h2>Rebuilds", _docs("sensors"), "</h2>")
        _rebuilds_table(h, rbs)
    h.raw("<h2>Snapshots", _docs("datasets"), "</h2>")
    if len(cs) == 0:
        h.raw('<p class="empty">No orca run has committed to it.</p>')
    else:
        h.raw(
            '<div class="scroll"><table><thead><tr><th>Snapshot</th>',
            "<th>Pipeline</th><th>Run</th><th>Step</th><th>Partition</th>",
            "<th>Committed</th></tr></thead><tbody>",
        )
        for c in cs:
            h.raw("<tr><td>")
            h.link(
                _snapshot_href(SnapshotRef(d.name, c.snapshot)),
                String(c.snapshot),
            )
            h.raw("</td><td>")
            h.link(String("/pipelines/", url_encode(c.pipeline)), c.pipeline)
            h.raw("</td><td>")
            h.chip(String("/runs/", c.run), String(c.run))
            h.raw("</td>")
            h.cell(c.step)
            h.mono(c.partition)
            h.mono(c.ended)
            h.raw("</tr>")
        h.raw("</tbody></table></div>")
    return Page(200, j^, _page(d.name, "/datasets", h))


def _json_rebuild(mut out: String, r: Rebuild):
    out.write(
        '{"id": ',
        r.id,
        ', "dataset": ',
        quote(r.dataset),
        ', "partitions": ',
        quote(r.partitions),
        ', "status": ',
        quote(r.status),
        ', "created": ',
        quote(r.created),
        ', "ended": ',
        quote(r.ended),
        ', "plan": [',
    )
    for i in range(len(r.plan)):
        ref p = r.plan[i]
        out.write(
            ", " if i else "",
            '{"pipeline": ',
            quote(p.pipeline),
            ', "level": ',
            p.level,
            ', "partitions": ',
            quote(p.partitions),
            ', "run": ',
            String(p.run) if p.run else String("null"),
            ', "status": ',
            quote(p.status),
            "}",
        )
    out.write("]}")


def _rebuilds_table(mut h: Html, rbs: List[Rebuild]):
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Rebuild</th>',
        "<th>Partitions</th><th>Status</th><th>Pipelines</th>",
        "<th>Created</th></tr></thead><tbody>",
    )
    for r in rbs:
        h.raw("<tr><td>")
        h.chip(String("/rebuilds/", r.id), String(r.id))
        h.raw("</td>")
        h.mono(r.partitions)
        h.raw("<td>")
        h.status(r.status)
        h.raw("</td><td>")
        for i in range(len(r.plan)):
            if i:
                h.raw(", ")
            h.link(
                String("/pipelines/", url_encode(r.plan[i].pipeline)),
                r.plan[i].pipeline,
            )
        h.raw("</td>")
        h.mono(r.created)
        h.raw("</tr>")
    h.raw("</tbody></table></div>")


def rebuild_page(mut db: State, id: Int) raises -> Page:
    var rbs = rebuilds(db, id=id)
    if len(rbs) == 0:
        return not_found(String("no rebuild ", id))
    ref r = rbs[0]
    var j = String('{"rebuild": ')
    _json_rebuild(j, r)
    j.write("}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/datasets">Datasets</a> · ')
    h.link(_dataset_href(r.dataset), r.dataset)
    h.raw("</p><h1>Rebuild ")
    h.text(r.id)
    h.raw(" ")
    h.status(r.status)
    h.raw('</h1><dl class="facts"><dt>Downstream of</dt><dd>')
    h.link(_dataset_href(r.dataset), r.dataset)
    h.raw("</dd><dt>Partitions</dt><dd>")
    h.text(r.partitions)
    h.raw("</dd><dt>Created</dt><dd>")
    h.text(r.created)
    h.raw("</dd><dt>Ended</dt><dd>")
    h.text(r.ended)
    h.raw(
        '</dd></dl><p class="muted">Each level runs once every run before it',
        " has succeeded. A run that fails stops the rebuild; resuming it",
        " carries on.</p>",
    )
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Level</th>',
        "<th>Pipeline</th><th>Partitions</th><th>Run</th><th>Status</th>",
        "</tr></thead><tbody>",
    )
    for p in r.plan:
        h.raw("<tr>")
        h.cell(p.level)
        h.raw("<td>")
        h.link(String("/pipelines/", url_encode(p.pipeline)), p.pipeline)
        h.raw("</td>")
        h.mono(p.partitions)
        h.raw("<td>")
        if p.run:
            h.chip(String("/runs/", p.run), String(p.run))
        h.raw("</td><td>")
        if p.status:
            h.status(p.status)
        else:
            h.raw('<span class="muted">waiting</span>')
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")
    return Page(200, j^, _page(String("Rebuild ", r.id), "/datasets", h))


def _json_made(mut out: String, ms: List[Made]):
    out.write("[")
    for i in range(len(ms)):
        ref m = ms[i]
        out.write(
            ", " if i else "",
            '{"dataset": ',
            quote(m.at.dataset),
            ', "snapshot": ',
            m.at.snapshot,
            ', "depth": ',
            m.depth,
            ', "pipeline": ',
            quote(m.pipeline),
            ', "run": ',
            m.run,
            ', "step": ',
            quote(m.step),
            ', "partition": ',
            quote(m.partition),
            ', "attempt": ',
            m.attempt,
            ', "code": ',
            quote(m.code),
            ', "ended": ',
            quote(m.ended),
            ', "inputs": [',
        )
        for k in range(len(m.inputs)):
            out.write(
                ", " if k else "",
                '{"dataset": ',
                quote(m.inputs[k].dataset),
                ', "snapshot": ',
                m.inputs[k].snapshot,
                "}",
            )
        out.write("]}")
    out.write("]")


def _made_table(mut h: Html, ms: List[Made]):
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Snapshot</th>',
        "<th>Made by</th><th>Run</th><th>Step</th><th>Partition</th>",
        "<th>Code</th><th>Committed</th><th>From</th></tr></thead><tbody>",
    )
    for m in ms:
        h.raw("<tr><td>")
        h.link(_snapshot_href(m.at), String(m.at))
        h.raw("</td><td>")
        h.link(String("/pipelines/", url_encode(m.pipeline)), m.pipeline)
        h.raw("</td><td>")
        h.link(String("/runs/", m.run), String(m.run))
        h.raw("</td>")
        h.cell(m.step)
        h.cell(m.partition)
        h.raw("<td><code>")
        h.text(m.code)
        h.raw("</code></td>")
        h.cell(m.ended)
        h.raw("<td>")
        for k in range(len(m.inputs)):
            if k:
                h.raw(", ")
            h.link(_snapshot_href(m.inputs[k]), String(m.inputs[k]))
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")


def _index_of(refs: List[SnapshotRef], at: SnapshotRef) -> Int:
    for i in range(len(refs)):
        if refs[i] == at:
            return i
    return -1


def _lineage_graph(start: SnapshotRef, b: List[Made], f: List[Made]) -> String:
    """The snapshots of both walks, each from the inputs it was made of."""
    var nodes = List[Node]()
    var refs = List[SnapshotRef]()
    var edges = List[Edge]()

    var all = List[Made]()
    for m in b:
        all.append(m.copy())
    for m in f:
        all.append(m.copy())
    # Sources first (inputs nobody here made), then the made, back to front.
    for m in all:
        for i in m.inputs:
            var made = False
            for o in all:
                made = made or o.at == i
            if not made and _index_of(refs, i) < 0:
                refs.append(i.copy())
                nodes.append(
                    Node(
                        i.dataset,
                        String(i.snapshot, " · source"),
                        "snapshot",
                        _snapshot_href(i),
                        "start" if i == start else "source",
                    )
                )
    if _index_of(refs, start) < 0:
        var made_here = False
        for m in all:
            made_here = made_here or m.at == start
        if not made_here:
            refs.append(start.copy())
            nodes.append(
                Node(
                    start.dataset,
                    String(start.snapshot, " · source"),
                    "snapshot",
                    _snapshot_href(start),
                    "start",
                )
            )
    for m in all:
        if _index_of(refs, m.at) >= 0:
            continue
        refs.append(m.at.copy())
        var sub = String(m.at.snapshot, " · ", m.pipeline)
        if m.partition:
            sub.write(" ", m.partition)
        nodes.append(
            Node(
                m.at.dataset,
                sub^,
                "snapshot",
                _snapshot_href(m.at),
                "start" if m.at == start else "",
            )
        )
    for m in all:
        var dst = _index_of(refs, m.at)
        for i in m.inputs:
            var src = _index_of(refs, i)
            if src >= 0 and dst >= 0:
                var dup = False
                for e in edges:
                    dup = dup or (e.src == src and e.dst == dst)
                if not dup:
                    edges.append(Edge(src, dst))
    return graph_svg(nodes, edges, String("The lineage of ", start))


def snapshot_page(mut db: State, name: String, id: Int64) raises -> Page:
    var start = SnapshotRef(name, id)
    var known = db.conn.query(
        (
            "SELECT 1 FROM orca.commits WHERE dataset = $1 AND snapshot_id = $2"
            " UNION ALL SELECT 1 FROM orca.reads WHERE dataset = $1 AND"
            " snapshot_id = $2 LIMIT 1"
        ),
        Params().text(name).int64(id),
    )
    if len(known) == 0:
        return not_found(String("no snapshot ", start, " that orca knows of"))
    var b = back(db, start)
    var f = forward(db, start)
    var j = String(
        '{"dataset": ', quote(name), ', "snapshot": ', id, ', "back": '
    )
    _json_made(j, b)
    j.write(', "forward": ')
    _json_made(j, f)
    j.write("}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/datasets">Datasets</a> · ')
    h.link(_dataset_href(name), name)
    h.raw("</p><h1>")
    h.text(name, " @ ", id)
    h.raw("</h1>")
    h.raw('<div class="scroll">', _lineage_graph(start, b, f), "</div>")
    h.raw("<h2>Made from</h2>")
    if len(b) == 0:
        h.raw(
            '<p class="empty">No orca run made this snapshot: it is a'
            " source.</p>"
        )
    else:
        _made_table(h, b)
    h.raw("<h2>Made from it</h2>")
    if len(f) == 0:
        h.raw('<p class="empty">Nothing orca made has read it yet.</p>')
    else:
        _made_table(h, f)
    return Page(200, j^, _page(String(name, " @ ", id), "/datasets", h))


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
        if (parts[3] == "runs" or parts[3] == "backfill") and String(
            keys.strip()
        ).startswith("@file:"):
            return _bad_request(
                "partitions from a file (@file:) are for the command line,"
                " which reads its own files; list the names, or use @all"
            )
        if parts[3] == "runs":
            var o = start_run(db, name, keys)
            var to = String("/runs/", o.run)
            return _acted(o, back, String('{"run": ', o.run, "}"), to)
        if parts[3] == "backfill":
            var o = backfill_runs(
                db, name, keys, rerun=_form(body, "rerun") == "1"
            )
            var to = String("/runs?pipeline=", url_encode(name))
            return _acted(o, back, String('{"queued": ', o.queued, "}"), to)
        if parts[3] == "cancel":
            var o = cancel_runs(db, name, keys if keys.strip() else "@all")
            var to = String("/runs?pipeline=", url_encode(name))
            return _acted(o, back, String('{"cancelled": ', o.queued, "}"), to)
        if parts[3] == "pause" or parts[3] == "unpause":
            var paused = parts[3] == "pause"
            var o = pause_pipeline(db, name, paused)
            return _acted(
                o,
                back,
                String('{"paused": ', "true" if paused else "false", "}"),
                back,
            )
    if len(parts) == 4 and parts[1] == "datasets" and parts[3] == "rebuild":
        var name = url_decode(parts[2])
        var o = rebuild_downstream(db, name, _form(body, "partitions"))
        var to = String("/rebuilds/", o.run)
        return _acted(
            o, _dataset_href(name), String('{"rebuild": ', o.run, "}"), to
        )
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


def _scheme(req: Request) raises -> String:
    """The scheme the browser used: behind a proxy (`ORCA_UI_TRUST_PROXY=1`)
    the first of `X-Forwarded-Proto`, else `https` with
    `ORCA_UI_SECURE_COOKIES=1` (secure cookies imply TLS in front), else
    `http`."""
    if getenv("ORCA_UI_TRUST_PROXY") == "1":
        var fwd = req.headers.get("X-Forwarded-Proto")
        if fwd:
            var first = String(fwd.split(",")[0].strip()).lower()
            if first == "http" or first == "https":
                return first
    if getenv("ORCA_UI_SECURE_COOKIES") == "1":
        return "https"
    return "http"


def _without_default_port(origin: String) -> String:
    """`origin` (`scheme://host[:port]`), lower case, without the scheme's
    default port, which a browser leaves out of `Origin`."""
    var o = origin.lower()
    if o.startswith("https://") and o.endswith(":443"):
        return String(o[byte = : o.byte_length() - 4])
    if o.startswith("http://") and o.endswith(":80"):
        return String(o[byte = : o.byte_length() - 3])
    return o^


def _bad_request(why: String) -> Page:
    """A 400: the form asked for something the UI never does."""
    var h = Html()
    h.raw("<h1>Refused</h1><p>")
    h.text(why)
    h.raw("</p>")
    return Page(
        400, String('{"error": ', quote(why), "}"), _page("Refused", "", h)
    )


def _same_origin(req: Request) raises -> Bool:
    """Whether a `POST` came from the UI's own pages: its `Origin`, or
    without one its `Referer`, is this server as the browser addressed it,
    by `Host` and the scheme `_scheme` gives. Host and scheme compare in
    any case."""
    var host = req.headers.get("Host")
    if not host:
        return False
    var own = _without_default_port(String(_scheme(req), "://", host))
    var origin = req.headers.get("Origin")
    if origin:
        return _without_default_port(origin) == own
    var referer = req.headers.get("Referer")
    if not referer:
        return False
    # The Referer's origin is what comes before the path's first '/'.
    var rest = referer.find("/", referer.find("://") + 3)
    var ref_origin = String(referer[byte=:rest]) if rest >= 0 else referer
    return _without_default_port(ref_origin) == own


# ── serving ──────────────────────────────────────────────────────────────────


def _reason(status: Int) -> String:
    if status == 200:
        return "OK"
    if status == 286:
        return "Done"
    if status == 303:
        return "See Other"
    if status == 400:
        return "Bad Request"
    if status == 403:
        return "Forbidden"
    if status == 401:
        return "Unauthorized"
    if status == 429:
        return "Too Many Requests"
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
        # A run's timeline, which Perfetto fetches from its own page.
        if raw and page.content_type == "application/json":
            resp.headers.set(
                "Access-Control-Allow-Origin", "https://ui.perfetto.dev"
            )
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
    if path == "/login" or path == "/logout":
        return True
    var parts = path.split("/")
    if len(parts) != 4:
        return False
    if parts[1] == "pipelines":
        return (
            parts[3] == "runs"
            or parts[3] == "backfill"
            or parts[3] == "cancel"
            or parts[3] == "pause"
            or parts[3] == "unpause"
        )
    if parts[1] == "datasets":
        return parts[3] == "rebuild"
    return parts[1] == "runs" and (
        parts[3] == "resume" or parts[3] == "rerun" or parts[3] == "cancel"
    )


def _cookie(req: Request, name: StringSlice) -> String:
    """A cookie the request sent, or empty."""
    for part in req.headers.get("Cookie").split(";"):
        var kv = part.strip()
        var eq = kv.find("=")
        if eq > 0 and kv[byte=:eq] == name:
            return String(kv[byte = eq + 1 :])
    return ""


def _cookie_header(token: String, max_age: Int) -> String:
    """The session cookie: only for this site's requests (`SameSite`), out
    of scripts' reach (`HttpOnly`), and with `ORCA_UI_SECURE_COOKIES=1`
    (behind HTTPS) never sent in the clear."""
    return String(
        SESSION_COOKIE,
        "=",
        token,
        "; Path=/; HttpOnly; SameSite=Strict; Max-Age=",
        max_age,
        "; Secure" if getenv("ORCA_UI_SECURE_COOKIES") == "1" else "",
    )


def _safe_next(next: String) -> String:
    """Where to go after logging in: a path on this site, else `/`."""
    if next.startswith("/") and not next.startswith("//") and "\\" not in next:
        return next
    return "/"


def login_page(next: String, why: String, status: Int = 200) -> Page:
    var h = Html()
    h.raw('<div class="auth"><h1>Log in</h1>')
    if why:
        h.raw('<p class="error">')
        h.text(why)
        h.raw("</p>")
    h.raw('<form class="login" method="post" action="/login">')
    h.raw('<input type="hidden" name="next" value="')
    h.text(_safe_next(next))
    h.raw(
        '"><label>Name <input name="name" autocomplete="username"',
        ' required></label><label>Password <input type="password"',
        ' name="password" autocomplete="current-password" required></label>',
        '<button type="submit">Log in</button></form>',
        '<p class="muted">Reading needs no login; starting, resuming,',
        " rerunning, backfilling, cancelling and rebuilding do. Users are",
        " added with <code>orca user add</code>.</p></div>",
    )
    return Page(
        status,
        String('{"error": ', quote(why), "}") if why else String(
            '{"login": "/login"}'
        ),
        _page("Log in", "", h),
    )


def _login(
    mut db: State, body: String, json: Bool, address: String
) raises -> Response:
    var name = _form(body, "name")
    if too_many_failures(db, name, address):
        return _respond(
            login_page(
                _form(body, "next"),
                "Too many failed logins; try again in 15 minutes.",
                429,
            ),
            json,
        )
    var token = open_session(db, name, _form(body, "password"))
    if not token:
        login_failed(db, name, address)
        return _respond(
            login_page(_form(body, "next"), "Wrong name or password.", 401),
            json,
        )
    var hours = Float64(getenv("ORCA_UI_SESSION_HOURS", "12"))
    var out = _respond(
        Page(
            303,
            String('{"user": ', quote(_form(body, "name")), "}"),
            String(),
            location=_safe_next(_form(body, "next")),
        ),
        json,
    )
    out.headers.set("Set-Cookie", _cookie_header(token, Int(hours * 3600.0)))
    return out^


def _client(req: Request) -> String:
    """The address a request came from: the connection's peer, or behind a
    proxy (`ORCA_UI_TRUST_PROXY=1`) the first address of
    `X-Forwarded-For`, which only a proxy in front can be trusted to set."""
    if getenv("ORCA_UI_TRUST_PROXY") == "1":
        var fwd = req.headers.get("X-Forwarded-For")
        if fwd:
            return String(fwd.split(",")[0].strip())
    return String(req.peer.ip)


def _login_needed(req: Request, json: Bool) raises -> Response:
    """An action without a session: 401 for a script, to the login page for
    a browser, which comes back to the page the form was on."""
    if json:
        return _respond(
            Page(
                401,
                String('{"error": "log in first", "login": "/login"}'),
                String(),
            ),
            json,
        )
    var back = String("/")
    var referer = req.headers.get("Referer")
    var host = String("http://", req.headers.get("Host"))
    if referer.startswith(String(host, "/")):
        back = String(referer[byte = host.byte_length() :])
    var out = Response(status=303, reason="See Other")
    out.headers.set("Location", String("/login?next=", url_encode(back)))
    return out^


def _run_of(location: String, path: String) -> Int:
    """The run an action made or acted on, from where it sent the browser or
    its own path; 0 for none."""
    for p in [location, path]:
        if p.startswith("/runs/"):
            var rest = p[byte=6:]
            var end = rest.find("/")
            try:
                return Int(rest[byte=:end] if end >= 0 else rest)
            except:
                pass
    return 0


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
            var token = _cookie(req, SESSION_COOKIE)
            var user = session_user(db, token)
            var required = login_required(db)
            var body = String(from_utf8_lossy=Span(req.body))
            if path == "/login":
                if not post:
                    return self._with_user(
                        login_page(_query(req.url, "next"), ""),
                        json,
                        user,
                        required,
                    )
                return _login(db, body, json, _client(req))
            if path == "/logout":
                if token:
                    close_session(db, token)
                var out = _respond(
                    Page(303, String('{"user": null}'), String(), location="/"),
                    json,
                )
                out.headers.set("Set-Cookie", _cookie_header("", 0))
                return out^
            if not post:
                return self._with_user(
                    self._page(db, req.url, path), json, user, required
                )
            if required and not user:
                return _login_needed(req, json)
            var done = _act(db, path, body)
            if not done:
                return _respond(not_found(String("nothing at ", path)), json)
            var page = done.take()
            if page.status == 303 and user:
                audit(db, user, path, _run_of(page.location, path), body)
            return self._with_user(page^, json, user, required)
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

    def _with_user(
        self, var page: Page, json: Bool, user: String, required: Bool
    ) -> Response:
        """The page, with who's logged in (or a way to log in) in its
        header."""
        var slot: String
        if user:
            var h = Html()
            h.raw('<span class="top__user">')
            h.text(user)
            h.raw(
                ' <form method="post" action="/logout"><button type="submit">',
                "Log out</button></form></span>",
            )
            slot = h^.finish()
        elif required:
            slot = String('<a class="top__user" href="/login">Log in</a>')
        else:
            slot = String()
        page.html = page.html.replace(_USER_SLOT, slot)
        return _respond(page, json)

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
            if len(parts) == 2 and parts[1] == "driver.log":
                return driver_log(db, id)
            if len(parts) == 3 and parts[1] == "cluster":
                return cluster_log(db, id, String(parts[2]))
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
        if path == "/datasets":
            return datasets_page(db)
        if path.startswith("/datasets/"):
            var parts = path[byte=10:].split("/")
            var name = url_decode(parts[0])
            if len(parts) == 1 and name:
                return dataset_page(db, name)
            if len(parts) == 3 and parts[1] == "snapshots":
                var id: Int64
                try:
                    id = Int64(Int(parts[2]))
                except:
                    return not_found(String("no snapshot ", parts[2]))
                return snapshot_page(db, name, id)
        if path.startswith("/rebuilds/"):
            var id: Int
            try:
                id = Int(path[byte=10:])
            except:
                return not_found(String("no rebuild ", path[byte=10:]))
            return rebuild_page(db, id)
        if path.startswith("/pipelines/"):
            var parts = path[byte=11:].split("/")
            var name = url_decode(parts[0])
            if len(parts) == 1 and name:
                return pipeline(db, name)
            if len(parts) == 3 and parts[1] == "keys" and name:
                return key_page(db, name, url_decode(parts[2]))
        return not_found(String("nothing at ", path))


def serve_ui(url: String, addr: String) raises:
    """Serves the pages on `addr` until the process is stopped."""
    var cfg = ServerConfig()
    cfg.idle_timeout_ms = 30_000
    cfg.write_timeout_ms = 30_000
    var server = HttpServer.bind(SocketAddr.parse(addr), cfg^)
    print("orca ui on http://", addr, sep="")
    server.serve(Ui(url))
