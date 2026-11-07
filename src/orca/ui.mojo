"""`orca ui`: pipelines and runs as web pages, and the same as JSON.

```
ORCA_DATABASE_URL=postgresql:///orca ./build/orca ui     # 127.0.0.1:8270
```

| Path | |
|---|---|
| `/` | pipelines, with their schedules, next runs and last runs |
| `/pipelines/<name>` | a pipeline's steps, partitions, schedule, next runs and runs; a keyed one's keys, done, live and failed; its fill (`orca.fill`): done of total, rate, ETA, today's estimated spend against its budget and why it's idle; `?preview=1&partitions=` (`&rerun=1`): what Start runs would queue, queueing nothing |
| `/api/pipelines/<name>` | the same as JSON, whatever is asked for |
| `/pipelines/<name>/calendar` | a daily or monthly pipeline's days (months) of `?month=2023-01` (`2023`) with their state, as JSON: done, failed, live, open, outside; the page shows it as coverage (`?cal=`) and as the Start runs form's date picker |
| `/pipelines/<name>/keys/<key>` | one key of a keyed pipeline: its steps in every run it was in |
| `/runs` | runs, newest first, `?n=` (50, at most 500) to a page; `?pipeline=`, `?status=`, `?trigger=`; `?before=<id>`: older than that run |
| `/runs/<id>` | a run's DAG, steps and partitions, attempts, errors, reads and commits |
| `/runs/<id>/attempts/<attempt>` | an attempt's log, with its error |
| `/runs/<id>/timeline.json` | the run's Dista timeline, for Perfetto |
| `/runs/<id>/driver.log` | the run's driver output, as the scheduler kept it |
| `/runs/<id>/metrics` | the run's machines: charts and peaks from its samples (`orca.metrics`) |
| `/ops` | redirects to `/ops/control`; "Ops" in the nav has Control, Fleet and Workers under it |
| `/ops/control` | Control, the one machine running the scheduler, this UI, Postgres and control-placed runs: its own health: CPU, memory, the disk Postgres is on and network over `?range=24h`, `7d` or `14d`, and each container's memory (`orca.system`), and the watchdog's banner; `/api/system` is the same as JSON; `/system` redirects here (301) |
| `/metrics` | Prometheus gauges: the control instance's CPU, memory, disk, network and containers' memory, runs running and queued, hosts, and errors in the last hour; open like the other pages (reads need no login) |
| `/ops/fleet` | Fleet, the EC2 instances orca launches for runs, the machines it is paying for: hosts running and what they cost an hour, runs and errors per hour, a table of live hosts (`?sort=` a column, `&dir=desc`) and charts over `?range=24h` or `7d` (`orca.fleet`); `/ops/fleet/hosts` is the live part of it, and `/api/fleet` the same as JSON; `/fleet` and `/fleet/hosts` redirect here (301) |
| `/ops/workers` | Workers, the machines running pipelines placed `worker:<pool>` (`orca.workers`): each worker token's pool, label, worker, image, last heartbeat, runs, CPUs and memory; `/api/workers` the same as JSON. A worker's own requests go to `/api/worker/...` with its token |
| `/watchdog`, `/api/watchdog` | the watchdog's rules with their last values, limits and snoozes, and the trips with a pipeline still paused (`orca.watchdog`); also the red banner on `/`, `/ops/fleet` and `/ops/control` |
| `/datasets` | datasets, with their writers, readers and latest snapshots, and the graph of pipelines and datasets |
| `/datasets/<name>` | a dataset's place in that graph, the steps that write and read it (linked to their code), what a rebuild of it runs, and its snapshots; `?preview=1&partitions=`: a rebuild's runs by level, queueing nothing |
| `/datasets/<name>/snapshots/<id>` | a snapshot's lineage, back and forward (`orca.lineage`) |
| `/rebuilds/<id>` | a rebuild's pipelines by level, with their runs (`orca.sensors`) |

And the actions, each a `POST` of a form (see `orca.actions`):

| `POST` | |
|---|---|
| `/pipelines/<name>/runs` | start a run over `partitions` |
| `/pipelines/<name>/backfill` | a run per partition in `partitions` that has none; `rerun=1` for done ones too |
| `/pipelines/<name>/cancel` | cancel its queued, running and retrying runs over `partitions` (`@all` or empty: all) |
| `/pipelines/<name>/pause`, `/unpause` | stop the scheduler starting its runs, or let it again |
| `/watchdog/<rule>/snooze` | silence a watchdog rule for `duration` (`2h`, `30m`, `1d`; `off` ends it) |
| `/runs/<id>/resume` | queue a failed, cancelled or stalled run again; `allow_new_code=1` confirms new code |
| `/runs/<id>/rerun` | a new run from `step` on, the steps before it copied |
| `/runs/<id>/cancel` | cancel a run, before its next attempt if it's running |
| `/datasets/<name>/rebuild` | rebuild every pipeline downstream over `partitions` |
| `/login`, `/logout` | a session for the actions, as a cookie; `name`, `password`, `next` |
| `/api/login` | JSON `name`, `password`, `label`, `days`: an API token for `orca login` (`orca.tokens`) |
| `/api/logout` | revokes the API token it's sent with |

A page of something still going on (a queued or running run, a running
attempt) refreshes its live part every two seconds (the fleet page's, every
ten) from a fragment path,
`/runs/<id>/steps`, `/runs/<id>/metrics` or
`/runs/<id>/attempts/<attempt>/log`, which answers
286 once there's nothing more to show, and the page stops asking. Beyond
that, a few lines that are conveniences only: `/runs` filters as a choice
changes, Start runs updates its preview and command lines as you type, and
Cancel, rerun and rebuild ask before posting. Every form works without
them.

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

The `orca` command acts through the same paths from another machine
(`orca.remote`): its actions send JSON, which is read as the form, with
`Authorization: Bearer <token>` instead of a cookie, and are recorded as
the token's user, from `cli`. A request with an `Authorization` header is
answered 401 unless it's a live token, whatever cookie it has; without an
`Origin` it needn't be the UI's own, as no browser sends one to another
site without a preflight this server never allows.
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
    password_ok,
    session_user,
    too_many_failures,
)
from orca.tokens import (
    clean_label,
    issue_token,
    revoke_token,
    token_days,
    token_user,
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
from orca.calendar import Calendar, calendar, calendar_json
from orca.chart import DayRuns, runs_chart
from orca.civil import Civil, now
from orca.logs import location_exists, read_location, run_dir
from orca.cron import CronExpr
from orca.dag import DagStep, aggregate, dag_svg
from orca.graph import Edge, Node, graph_svg
from orca.system import (
    alert_html,
    now_json,
    prometheus,
    range_hours,
    system_charts,
    system_html,
    system_json,
    system_now,
    warnings,
    warnings_json,
)
from orca.fleet import (
    charts_html,
    charts_json,
    fleet_charts,
    fleet_hosts,
    fleet_summary,
    hosts_fragment,
    hosts_json,
    sort_hosts,
    summary_html,
    summary_json,
)
from orca.html import Html, escape, url_decode, url_encode
from orca.lineage import (
    Made,
    SnapshotRef,
    back,
    commits,
    datasets as dataset_rows,
    flows,
    forward,
    latest as latest_snapshot,
)
from orca.json import quote
from orca.live import live_sql
from orca.metrics import (
    InstanceMetrics,
    StepSpan,
    metrics_html,
    peaks_json,
    read_metrics,
)
from orca.scheduler import (
    current_keys,
    done_keys,
    keep_keys,
    kept_keys,
    fill_key,
    fill_pipeline,
    preview_backfill,
    registered,
)
from orca.fill import FillStatus, eta_text, fill_status, money, status_json
from orca.sensors import Link, Rebuild, downstream, plan_rebuild, rebuilds
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
from orca.watchdog import (
    active_trips,
    banner_html,
    config as watchdog_config,
    prometheus as watchdog_prometheus,
    rule_rows,
    snooze as snooze_rule,
    trips_json,
    watchdog_html,
    watchdog_json,
)
from orca.ui_style import STYLE
from orca.workers import (
    ran_on,
    worker_api,
    worker_rows,
    workers_html,
    workers_json,
)

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
    var placement: String
    """`orca register --placement`: `control`, `ec2`, or empty for the
    scheduler's runner."""


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
    var seconds: Int
    """How long it ran (so far, if it's running); -1 before it started."""
    var placement: String
    """Where its latest start ran: `control` (the scheduler's host), `ec2`,
    `kubernetes` or `local`; empty before its first."""
    var held: String
    """Why the scheduler keeps it queued (`orca.control`), or empty."""


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
    var procs: Int
    """The dista worker processes it ran (`--worker-procs`)."""


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
                " coalesce(p.max_runs::text, p.describe->>'max_runs', '1') AS"
                " max_runs, coalesce(p.describe->>'run_retries', '0') AS"
                " run_retries, (SELECT count(*) FROM orca.runs r WHERE"
                " r.pipeline = p.name AND r.status = 'running')::text AS"
                " running, (SELECT count(*) FROM orca.runs r WHERE r.pipeline ="
                " p.name AND r.status = 'queued')::text AS queued,"
                " coalesce(l.id, 0)::text AS last_run, coalesce(l.status, '')"
                " AS last_status, coalesce(l.partitions, '') AS"
                " last_partitions, p.paused, coalesce(p.sense_error, '') AS"
                " sense_error, coalesce(p.placement, '') AS placement, "
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
            r.text("placement"),
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


comptime _SECONDS = (
    "coalesce(extract(epoch FROM coalesce(ended, now()) - started)::bigint, -1)"
)
"""SQL for how long a run ran, up to now if it hasn't ended; -1 before it
started."""


def _took(seconds: Int) -> String:
    """A duration as `42s`, `3m 07s` or `2h 05m`; empty for a negative one."""
    if seconds < 0:
        return String()
    if seconds < 60:
        return String(seconds, "s")
    if seconds < 3600:
        return String(seconds // 60, "m ", _two(seconds % 60), "s")
    return String(seconds // 3600, "h ", _two(seconds % 3600 // 60), "m")


def _two(n: Int) -> String:
    return String("0", n) if n < 10 else String(n)


def _when_cell(mut h: Html, utc: String):
    """A `YYYY-MM-DDTHH:MM:SSZ` time as a table cell that the page's script
    shows as how long ago it was, the full local time on hover."""
    h.raw('<td class="mono"><time class="rel" datetime="')
    h.text(utc)
    h.raw('">')
    h.text(utc)
    h.raw("</time></td>")


comptime _SHOWN_PARTITIONS = 2
"""How many of a run's partitions its row in a table of runs names before
saying how many more there are."""


def _partitions_cell(mut h: Html, partitions: String):
    """A run's partitions as a table cell: the first few, then `+N more`, with
    the whole list in the tooltip."""
    var parts = partitions.split(",")
    h.raw('<td class="mono parts" title="')
    h.text(partitions)
    h.raw('">')
    if len(parts) <= _SHOWN_PARTITIONS + 1:
        h.text(partitions)
    else:
        for i in range(_SHOWN_PARTITIONS):
            if i > 0:
                h.raw(", ")
            h.text(parts[i])
        h.raw(
            ' <span class="muted">+',
            len(parts) - _SHOWN_PARTITIONS,
            " more</span>",
        )
    h.raw("</td>")


def _runs(
    mut db: State,
    pipeline: String = "",
    status: String = "",
    trigger: String = "",
    limit: Int = 100,
    before: Int = 0,
) raises -> List[RunRow]:
    """Runs newest first, those of a pipeline, status or trigger if given,
    and with ids under `before` if it's over 0."""
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
            " AS waiting, ",
            _SECONDS,
            " AS seconds, coalesce(placement, '') AS placement,",
            " coalesce(held, '') AS held FROM orca.runs WHERE ($1 = '' OR",
            " pipeline = $1) AND",
            " ($2 = '' OR status = $2) AND ($3 = '' OR trigger = $3) AND",
            " ($5 = 0 OR id < $5) ORDER BY orca.runs.id DESC LIMIT $4",
        ),
        Params()
        .text(pipeline)
        .text(status)
        .text(trigger)
        .int64(Int64(limit))
        .int64(Int64(before)),
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
                Int(r.int64("seconds")),
                r.text("placement"),
                r.text("held"),
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


@fieldwise_init
struct CodeRef(Copyable, Defaultable, Movable):
    """A line of a version's code on its host: its URL, where it is
    (`file.mojo:26`), and the commit."""

    var url: String
    var at: String
    var commit: String

    def __init__(out self):
        self.url = ""
        self.at = ""
        self.commit = ""

    def __bool__(self) -> Bool:
        return self.url.byte_length() > 0


def _code_ref(v: VersionRow, location: String) -> CodeRef:
    """`file#Lline` at the version's commit, or an empty one."""
    var url = _code_url(v, location)
    if not url:
        return CodeRef()
    return CodeRef(url, location.replace("#L", ":"), v.commit)


struct CodeLinks(Copyable, Movable):
    """A version's code on its host: the pipeline's declaration, each
    step's function and each dista task, from `orca.locate`'s map."""

    var pipeline: CodeRef
    var steps: Dict[String, CodeRef]
    """Step name to its function's line."""
    var tasks: List[Tuple[String, CodeRef]]
    """A task's function name and its line."""

    def __init__(out self):
        self.pipeline = CodeRef()
        self.steps = Dict[String, CodeRef]()
        self.tasks = List[Tuple[String, CodeRef]]()

    def step(self, name: String) -> CodeRef:
        return self.steps.get(name, CodeRef())


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
            var at = _code_ref(ver, kv[1].string_value())
            if not at:
                continue
            var key = kv[0]
            if key == "pipeline":
                out.pipeline = at^
            elif key in fn_step:
                out.steps[fn_step[key]] = at^
            else:
                out.tasks.append(
                    (String(key[byte = key.rfind("::") + 2 :]), at^)
                )
    except:
        pass
    return out^


def _code_of(
    mut db: State,
    mut cache: Dict[String, CodeLinks],
    pipeline: String,
    run: Int = 0,
) -> CodeLinks:
    """The code of the version `run` ran or, if it recorded none or there's
    no run, of the version `pipeline` is registered with; each version's
    worked out once per `cache`."""
    try:
        var rows = db.conn.query(
            (
                "SELECT r.pipeline, coalesce(r.version, p.version, 0) AS v"
                " FROM orca.runs r LEFT JOIN orca.pipelines p ON p.name ="
                " r.pipeline WHERE r.id = $1"
            ),
            Params().int64(Int64(run)),
        ) if run else db.conn.query(
            (
                "SELECT name AS pipeline, coalesce(version, 0) AS v FROM"
                " orca.pipelines WHERE name = $1"
            ),
            Params().text(pipeline),
        )
        if len(rows) == 0 or not rows.row(0).int64("v"):
            return CodeLinks()
        var name = rows.row(0).text("pipeline")
        var id = Int(rows.row(0).int64("v"))
        var key = String(name, "\0", id)
        if key not in cache:
            var vs = _versions(db, id=id)
            cache[key] = _code_links(
                db,
                name,
                vs[0].copy() if len(vs) > 0 else Optional[VersionRow](None),
            )
        return cache[key].copy()
    except:
        return CodeLinks()


def _code(mut h: Html, at: CodeRef):
    """A link to a line of code: every one orca shows is drawn by this,
    so they all look alike and unlike orca's own links."""
    h.code(at.url, at.at, at.commit)


def _code_html(mut h: Html, name: String, links: CodeLinks):
    """`Code`: the pipeline, then its dista tasks, each linked to its line."""
    if not links.pipeline and len(links.tasks) == 0:
        return
    h.raw("<dt>Code</dt><dd>")
    if links.pipeline:
        h.text("pipeline ", name, " ")
        _code(h, links.pipeline)
    for i in range(len(links.tasks)):
        h.raw(" · " if i or links.pipeline else "")
        h.text("task ", links.tasks[i][0], " ")
        _code(h, links.tasks[i][1])
    h.raw("</dd>")


def _step_cell(mut h: Html, name: String, links: CodeLinks):
    """A step's name, then a link to its function where the code is known."""
    var at = links.step(name)
    h.raw("<td>")
    h.text(name)
    if at:
        h.raw(" ")
        _code(h, at)
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
    if url:
        h.code(url, short, v.commit)
    else:
        h.raw("<code>")
        h.text(short)
        h.raw("</code>")
    if v.dirty:
        h.raw(' <span class="muted">plus uncommitted changes</span>')
    if v.source:
        h.raw(' <span class="muted">of ')
        var web = web_url(v.source)
        if web:
            h.code(web, v.source, "")
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
            " coalesce(i.procs, 1) AS procs,",
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
                Int(r.int64("procs")),
            )
        )
    return out^


def _cluster_log(i: InstanceRow) -> String:
    """The instance's log's name under its run's `cluster/`."""
    return String(i.role, "-", i.id, ".log")


def _json_instances(
    mut out: String,
    run: Int,
    xs: List[InstanceRow],
    metrics: List[InstanceMetrics],
):
    """The run's instances, each with its samples' peaks (`orca.metrics`),
    then those with samples and no instance row: a local run's machine and
    a Kubernetes run's pods."""
    out.write("[")
    var matched = List[String]()
    for k in range(len(xs)):
        ref i = xs[k]
        var name = String(i.role, "-", i.id)
        var peaks = String("null")
        for m in metrics:
            if m.name == name:
                peaks = peaks_json(m)
                matched.append(name)
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
            ', "procs": ',
            i.procs,
            ', "log": ',
            quote(String("/runs/", run, "/cluster/", _cluster_log(i))),
            ', "peaks": ',
            peaks,
            "}",
        )
    var n = len(xs)
    for m in metrics:
        if m.name in matched:
            continue
        out.write(
            ", " if n else "",
            '{"id": ',
            quote(m.name),
            ', "start": null, "role": ',
            quote(_role_of(m.name)),
            ', "type": null, "market": null, "launched": null, "ended":',
            ' null, "why": null, "log": null, "peaks": ',
            peaks_json(m),
            "}",
        )
        n += 1
    out.write("]")


def _role_of(name: String) -> String:
    """A sampled instance's role, from its file's name: `local`, an ec2
    `<role>-<id>`, or a pod's `orca-<run>-<start>-<role>[-<i>]`."""
    if name == "local":
        return "local"
    if name.startswith("head-") or name.endswith("-head"):
        return "head"
    if name.startswith("worker-") or "-worker-" in name:
        return "worker"
    return "driver"


def _step_spans(mut db: State, run: Int) raises -> List[StepSpan]:
    """The run's attempts on the wall clock, for the machines' steps lane."""
    var out = List[StepSpan]()
    for r in db.conn.query(
        (
            "SELECT s.step, s.partition, a.status, extract(epoch FROM"
            " a.started)::float8 AS t0, extract(epoch FROM coalesce(a.ended,"
            " now()))::float8 AS t1 FROM orca.attempts a JOIN orca.steps s ON"
            " s.id = a.step WHERE s.run = $1 ORDER BY s.ord, a.started"
        ),
        Params().int64(Int64(run)),
    ):
        out.append(
            StepSpan(
                r.text("step"),
                r.text("partition"),
                r.text("status"),
                r.float64("t0"),
                r.float64("t1"),
            )
        )
    return out^


def _machines_html(
    mut h: Html, mut db: State, r: RunRow, metrics: List[InstanceMetrics]
) raises:
    """The run's machines (`orca.metrics`): nothing for a run without
    samples."""
    if len(metrics) == 0:
        return
    h.raw("<h2>Machines", _docs("metrics"), "</h2>")
    metrics_html(h, metrics, _step_spans(db, r.id), _live(r.status))


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
        h.cell(
            i.instance_type,
            " Spot" if i.market == "spot" else String(""),
            String(" x", i.procs, " workers") if i.procs > 1 else String(""),
        )
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
        ', "placement": ',
        quote(p.placement if p.placement else String("default")),
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
        ', "placement": ',
        quote(r.placement) if r.placement else String("null"),
        ', "held": ',
        quote(r.held) if r.held and r.status == "queued" else String("null"),
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
const ago = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
const full = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium", timeZoneName: "short" });
function relative(t) {
  const s = (t - Date.now()) / 1000, a = Math.abs(s);
  if (a < 45) return ago.format(Math.round(s), "second");
  if (a < 2700) return ago.format(Math.round(s / 60), "minute");
  if (a < 79200) return ago.format(Math.round(s / 3600), "hour");
  if (a < 2592000) return ago.format(Math.round(s / 86400), "day");
  if (a < 31536000) return ago.format(Math.round(s / 2592000), "month");
  return ago.format(Math.round(s / 31536000), "year");
}
function times(root) {
  for (const el of root.querySelectorAll("time.rel")) {
    const t = Date.parse(el.dateTime);
    if (isNaN(t)) continue;
    el.textContent = relative(t);
    el.title = full.format(t) + " (" + el.dateTime + ")";
  }
}
times(document);
setInterval(() => times(document), 30000);
document.addEventListener("submit", (e) => {
  const m = e.target.dataset && e.target.dataset.confirm;
  const get = e.submitter && e.submitter.formMethod === "get";
  if (m && !get && !confirm(m)) e.preventDefault();
});
for (const el of document.querySelectorAll("[data-poll]")) {
  const every = (parseFloat(el.dataset.pollEvery) || 2) * 1000;
  const tick = async () => {
    let r;
    try { r = await fetch(el.dataset.poll); } catch (e) { setTimeout(tick, 5000); return; }
    if (r.ok) { el.innerHTML = await r.text(); times(el); }
    if (r.status !== 286) setTimeout(tick, every);
  };
  setTimeout(tick, every);
}
"""
"""What makes a page live: each element with `data-poll` is replaced every
two seconds (`data-poll-every` seconds) by what its path returns, until that answers 286 (htmx's "stop
polling"), which a fragment does once what it shows has ended. Each
`<time class="rel">` reads as how long ago it was, in the viewer's locale,
with the full local time in its tooltip; without scripts it stays UTC."""


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


comptime _ICON = (
    '<link rel="icon" href="data:image/svg+xml,%3Csvg'
    " xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Crect"
    " width='16' height='16' rx='3' fill='%231d4ed8'/%3E%3Cpath d='M4.5 5L8"
    " 11.5M11.5 5L8 11.5' stroke='%23fff' stroke-width='1.3'/%3E%3Ccircle"
    " cx='4.5' cy='4.5' r='2' fill='%23fff'/%3E%3Ccircle cx='11.5' cy='4.5'"
    " r='2' fill='%23fff'/%3E%3Ccircle cx='8' cy='11.5' r='2'"
    " fill='%23fff'/%3E%3C/svg%3E\">"
)
"""A blue DAG of three nodes as the tab icon, so orca's tab stands apart
from the census's amber bar chart."""


def _page(title: String, active: StaticString, body: Html) -> String:
    var h = Html()
    h.raw(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        _ICON,
        "<title>",
    )
    h.text(title, " · orca")
    h.raw(
        '</title><link rel="stylesheet" href="/static/orca.css"></head>',
        '<body class="',
        _AUTH_SLOT,
        '">',
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
    # Ops: a link to Control, with Control and Fleet under it. The menu is
    # plain links, shown by CSS (hover, keyboard focus) and, on a phone,
    # always, so it needs no JavaScript.
    h.raw('<div class="menu"><a href="/ops"')
    if active.startswith("/ops/"):
        h.raw(' aria-current="page"')
    h.raw(' aria-haspopup="true">Ops</a><div class="menu__sub">')
    for item in [
        ("/ops/control", "Control"),
        ("/ops/fleet", "Fleet"),
        ("/ops/workers", "Workers"),
    ]:
        h.raw('<a href="', item[0], '"')
        if item[0] == active:
            h.raw(' aria-current="page"')
        h.raw(">", item[1], "</a>")
    h.raw("</div></div>")
    h.raw('<a href="')
    h.text(getenv("ORCA_DOCS_URL", "https://mlake.dev/s/orca/docs/"))
    h.raw('" target="_blank" rel="noopener">Docs</a>')
    _extra_links(h)
    h.raw("</nav>", _USER_SLOT, "</header><main>")
    h.raw(body.out)
    h.raw("</main><script>", _POLL, "</script></body></html>")
    return h^.finish()


comptime _AUTH_SLOT = "orca-auth"
"""The `<body>`'s class until the page is answered: `anon` when login is on
and no one is logged in, which hides the inline action buttons and shows
their "Log in to ..." links instead."""

comptime _HERE_SLOT = "__orca_here__"
"""Where a "Log in to ..." link comes back to: the page's own path and
query, encoded, filled in when the page is answered."""


def _act_button(
    mut h: Html,
    action: String,
    label: StaticString,
    verb: StaticString,
    cli: String,
    danger: Bool = False,
    step: String = "",
):
    """A small form posting to `action`, beside the status it changes: a
    button labelled `label` whose title is the command line that does the
    same, with "Log in to `verb`" in its place while login is needed. A
    `danger` one asks first, where there's JavaScript."""
    h.raw('<form class="act" method="post" action="')
    h.text(action)
    h.raw('"')
    if danger:
        h.raw(' data-confirm="')
        h.text(label, "?")
        h.raw('"')
    h.raw(">")
    if step:
        h.raw('<input type="hidden" name="step" value="')
        h.text(step)
        h.raw('">')
    h.raw('<button type="submit" class="act__btn', " danger" if danger else "")
    h.raw('" title="')
    h.text(cli)
    h.raw(
        '">', label, '</button></form><a class="act__login" href="/login?next='
    )
    h.raw(_HERE_SLOT, '" title="')
    h.text(cli)
    h.raw('">Log in to ', verb, "</a>")


def _pause_button(mut h: Html, name: String, paused: Bool):
    """Resume a paused pipeline, or pause an active one."""
    var base = String("/pipelines/", url_encode(name))
    if paused:
        _act_button(
            h,
            String(base, "/unpause"),
            "Resume",
            "resume",
            String("orca unpause ", _sh(name)),
        )
    else:
        _act_button(
            h,
            String(base, "/pause"),
            "Pause",
            "pause",
            String("orca pause ", _sh(name)),
        )


def _cancel_cli(r: RunRow) -> String:
    return String(
        "orca cancel ",
        _sh(r.pipeline),
        " ",
        _sh(r.partitions),
        "  (its queued, running and retrying runs over these partitions)",
    )


def _retry_cli(r: RunRow) -> String:
    return String(
        "<",
        r.pipeline,
        " binary> resume ",
        r.id,
        "  (in that shell's process; orca has no command that queues it)",
    )


def _row_actions(mut h: Html, r: RunRow):
    """A runs table row's Cancel (queued or running) or Retry (failed)."""
    if (r.status == "queued" or r.status == "running") and not (
        r.cancel_requested
    ):
        _act_button(
            h,
            String("/runs/", r.id, "/cancel"),
            "Cancel",
            "cancel",
            _cancel_cli(r),
            danger=True,
        )
    elif r.status == "failed" or r.stalled:
        _act_button(
            h,
            String("/runs/", r.id, "/resume"),
            "Retry",
            "retry",
            _retry_cli(r),
        )


comptime _TRIES = (
    "Times the run's process was started: 1 is normal; more means retries,"
    " a lost instance or a resume."
)
"""What a runs table's Tries column counts (`orca.runs.starts`)."""


def _runs_table(mut h: Html, runs: List[RunRow], show_pipeline: Bool):
    if len(runs) == 0:
        h.raw('<p class="empty">No runs.</p>')
        return
    h.raw('<div class="scroll"><table><thead><tr><th>Run</th>')
    if show_pipeline:
        h.raw("<th>Pipeline</th>")
    h.raw(
        '<th>Created</th><th class="num">Took</th>',
        "<th>Partitions</th><th>Trigger</th><th>Status</th>",
        '<th class="num"><abbr title="',
        _TRIES,
        '" aria-label="Tries: ',
        _TRIES,
        '">Tries</abbr></th></tr></thead><tbody>',
    )
    for r in runs:
        h.raw("<tr><td>")
        h.chip(String("/runs/", r.id), String(r.id))
        h.raw("</td>")
        if show_pipeline:
            h.raw("<td>")
            h.link(String("/pipelines/", url_encode(r.pipeline)), r.pipeline)
            h.raw("</td>")
        _when_cell(h, r.created)
        h.num(_took(r.seconds))
        _partitions_cell(h, r.partitions)
        h.cell(r.trigger)
        h.raw("<td>")
        h.status(r.status)
        if r.waiting_until:
            h.raw(' <span class="muted">until ')
            h.text(r.waiting_until)
            h.raw("</span>")
        _row_actions(h, r)
        h.raw("</td>")
        # Empty for 0 and 1, so a run that took more draws the eye.
        if r.starts > 1:
            h.num(r.starts)
        else:
            h.raw('<td class="num"></td>')
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
    var fleet = fleet_summary(db)
    var alerts = warnings(system_now(db))
    var trips = active_trips(db)
    var j = String(
        '{"warnings": ',
        warnings_json(alerts),
        ', "watchdog": ',
        trips_json(trips),
        ', "pipelines": [',
    )
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
    j.write('], "fleet": ', summary_json(fleet), "}")

    var h = Html()
    h.raw("<h1>Pipelines</h1>")
    banner_html(h, trips)
    alert_html(h, alerts)
    summary_html(h, fleet, True)
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
            _pause_button(h, p.name, p.paused)
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


def _fleet_sort(sort: String) -> String:
    """`sort` if it names a column of the hosts table, else the default."""
    for c in [
        "role",
        "type",
        "run",
        "uptime",
        "cpu",
        "busiest",
        "mem",
        "net",
        "disk",
        "state",
    ]:
        if sort == c:
            return sort
    return String()


def fleet_page(
    mut db: State, sort: String, dir: String, range: String
) raises -> Page:
    """The machines orca is paying for, and what they have been doing."""
    var col = _fleet_sort(sort)
    var desc = dir == "desc"
    var span = String("7d") if range == "7d" else String("24h")
    var summary = fleet_summary(db)
    var hosts = fleet_hosts(db)
    sort_hosts(hosts, col, desc)
    var charts = fleet_charts(db, 168 if span == "7d" else 24)
    var trips = active_trips(db)
    var j = String(
        '{"watchdog": ',
        trips_json(trips),
        ', "summary": ',
        summary_json(summary),
        ', "hosts": ',
        hosts_json(hosts),
        ', "charts": ',
        charts_json(charts),
        "}",
    )
    var h = Html()
    h.raw("<h1>Fleet</h1>")
    h.raw(
        '<p class="muted">The EC2 instances orca launches for runs: head and',
        " worker hosts, their uptime, load and network. The one machine that",
        ' runs the scheduler, this UI and Postgres is <a href="/ops/control">',
        "Control</a>.</p>",
    )
    banner_html(h, trips)
    if getenv("ORCA_RUNNER") == "local":
        h.raw(
            '<p class="alert">This scheduler\'s runner is',
            " <code>local</code>: it runs every run on its own machine and",
            " launches no instances, so there are none to list and no",
            ' per-instance cost. The machine is <a href="/ops/control">',
            "Control</a>; its price is the server's fixed price.</p>",
        )
    h.raw(
        '<div data-poll="/ops/fleet/hosts?sort=',
        col,
        "&amp;dir=",
        "desc" if desc else "asc",
        "&amp;range=",
        span,
        '" data-poll-every="10">',
        hosts_fragment(db, col, desc, span),
        "</div>",
    )
    charts_html(h, charts, span)
    return Page(200, j^, _page("Fleet", "/ops/fleet", h))


def fleet_hosts_page(
    mut db: State, sort: String, dir: String, range: String
) raises -> Page:
    """The live part of the fleet page, as a fragment; it never ends."""
    var span = String("7d") if range == "7d" else String("24h")
    return Page(
        200,
        String(),
        hosts_fragment(db, _fleet_sort(sort), dir == "desc", span),
        "text/html; charset=utf-8",
    )


def fleet_api(mut db: State, range: String) raises -> Page:
    """`/api/fleet`: the fleet page's numbers as JSON, whatever is asked
    for."""
    var page = fleet_page(db, "", "", range)
    return Page(200, String(), page.json.copy(), "application/json")


def _system_range(range: String) -> String:
    return range if range == "7d" or range == "14d" else String("24h")


def system_page(mut db: State, range: String) raises -> Page:
    """The control instance's own health: CPU, memory, the disk Postgres is
    on and the network over `range`, and each container's memory."""
    var span = _system_range(range)
    var now = system_now(db)
    var charts = system_charts(db, range_hours(span))
    var h = Html()
    h.raw("<h1>Control</h1>")
    h.raw(
        (
            '<p class="muted">The one machine running the scheduler, this UI,'
            " Postgres and control-placed runs: its CPU, memory, disk and"
            " network. The EC2 instances orca launches for runs are on"
            ' <a href="/ops/fleet">Fleet</a>; <a href="/watchdog">Watchdog</a>'
            " lists the rules that pause pipelines.</p>"
        ),
    )
    banner_html(h, active_trips(db))
    system_html(h, now, charts, span)
    return Page(
        200, system_json(now, charts), _page("Control", "/ops/control", h)
    )


def metrics_page(mut db: State) raises -> Page:
    """`/metrics`: Prometheus' gauges for the control instance and the
    fleet's counts."""
    return Page(
        200,
        String(),
        String(
            prometheus(system_now(db), fleet_summary(db)),
            watchdog_prometheus(db, watchdog_config()),
        ),
        "text/plain; version=0.0.4; charset=utf-8",
    )


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


@fieldwise_init
struct StartPreview(Copyable, Movable):
    """What the pipeline page's Start runs form would do over a range: the
    backfill's runs and the keys it would leave, and the one run's size, or
    why the range is refused."""

    var refused: String
    var keys: List[String]
    """The keys the backfill would queue, sorted."""
    var runs: Int
    """The runs they'd make, `keys_per_run` to a run."""
    var keys_per_run: Int
    var done: List[String]
    var live: List[String]
    var live_runs: List[Int]


def _start_preview(
    mut db: State,
    name: String,
    keys: String,
    rerun: Bool,
    allow_append: Bool = False,
) raises -> Optional[StartPreview]:
    """The Start runs preview over `keys`: `preview_backfill`, the same
    logic as the backfill, in a transaction it rolls back."""
    var ps = registered(db, name)
    if len(ps) == 0:
        return None
    try:
        if String(keys.strip()).startswith("@file:"):
            raise Error(
                "partitions from a file (@file:) are for the command line,"
                " which reads its own files; list the names, or use @all"
            )
        var q = preview_backfill(db, ps[0], keys, rerun, allow_append)
        return StartPreview(
            "",
            q.keys.copy(),
            q.batches,
            ps[0].keys_per_run,
            q.done.copy(),
            q.live.copy(),
            q.live_runs.copy(),
        )
    except e:
        return StartPreview(
            String(e),
            List[String](),
            0,
            ps[0].keys_per_run,
            List[String](),
            List[String](),
            List[Int](),
        )


def _unit(kind: String, n: Int) -> String:
    """A partition of `kind`, as a word: day, month or key, plural unless
    `n` is 1."""
    var one = String("day") if kind == "daily" else (
        String("month") if kind == "monthly" else String("key")
    )
    return one if n == 1 else String(one, "s")


def _sh(s: StringSlice) -> String:
    """`s` as one shell word: as it is if it's only letters, digits and
    `-_.,:/@=+%`, else in single quotes."""
    var plain = s.byte_length() > 0
    for b in s.as_bytes():
        var c = Int(b)
        if not (
            (c >= 48 and c <= 57)
            or (c >= 65 and c <= 90)
            or (c >= 97 and c <= 122)
            or c == 45
            or c == 95
            or c == 46
            or c == 44
            or c == 58
            or c == 47
            or c == 64
            or c == 61
            or c == 43
            or c == 37
        ):
            plain = False
            break
    if plain:
        return String(s)
    return String("'", String(s).replace("'", "'\\''"), "'")


def _backfill_cmd(name: String, keys: String, rerun: Bool) -> String:
    """`orca backfill [--rerun] <pipeline> <keys>`, as `orca` reads it."""
    return String(
        "orca backfill ",
        "--rerun " if rerun else "",
        _sh(name),
        " ",
        _sh(keys) if keys else String("<range>"),
    )


def _run_cmd(binary: String, keys: String) -> String:
    """The binary's `run --partitions <keys>`: one run, in that process."""
    return String(
        _sh(binary),
        " run --partitions ",
        _sh(keys) if keys else String("<range>"),
    )


def _preview_line(p: PipelineRow, v: StartPreview) -> String:
    """ "This queues 30 runs: 2025-01-02 … 2025-01-31. 1 already done is
    skipped." and the like."""
    var n = len(v.keys)
    var out = String()
    if v.runs == 0:
        out.write("This queues no runs.")
    else:
        out.write("This queues ", v.runs, " run" if v.runs == 1 else " runs")
        if v.keys_per_run > 1:
            out.write(" over ", n, " ", _unit(p.kind, n))
        out.write(": ", v.keys[0])
        if n > 1:
            out.write(" … ", v.keys[n - 1])
        out.write(".")
    if len(v.done) > 0:
        out.write(
            " ",
            len(v.done),
            " already done ",
            "is" if len(v.done) == 1 else "are",
            " skipped.",
        )
    if len(v.live) > 0:
        out.write(
            " ",
            len(v.live),
            " in a queued or running run ",
            "is" if len(v.live) == 1 else "are",
            " left to it (",
            v.live[0],
            ": run ",
            v.live_runs[0],
            ").",
        )
    return out^


def _one_run_line(p: PipelineRow, v: StartPreview) -> String:
    """What "Start one run" would do over the same range."""
    var n = len(v.keys) + len(v.done) + len(v.live)
    if len(v.live) > 0:
        return String(
            "One run over the whole range is refused: ",
            v.live[0],
            " is in run ",
            v.live_runs[0],
            ", which is queued or running.",
        )
    return String(
        "One run over the whole range runs all ",
        n,
        " ",
        _unit(p.kind, n),
        ", done or not.",
    )


def _json_start_preview(
    mut out: String,
    p: PipelineRow,
    v: StartPreview,
    keys: String,
    rerun: Bool,
):
    out.write(
        '{"partitions": ',
        quote(keys),
        ', "rerun": ',
        "true" if rerun else "false",
        ', "refused": ',
        quote(v.refused) if v.refused else String("null"),
        ', "runs": ',
        v.runs,
        ', "keys": ',
    )
    _json_strings(out, v.keys)
    out.write(', "done": ')
    _json_strings(out, v.done)
    out.write(', "live": ')
    _json_strings(out, v.live)
    out.write(
        ', "one_run": ',
        len(v.keys) + len(v.done) + len(v.live),
        ', "summary": ',
        quote(_preview_line(p, v)) if not v.refused else String("null"),
        ', "commands": {"backfill": ',
        quote(_backfill_cmd(p.name, keys, rerun)),
        ', "run": ',
        quote(_run_cmd(p.binary, keys)),
        "}}",
    )


def calendar_page(mut db: State, name: String, month: String) raises -> Page:
    """`/pipelines/<name>/calendar?month=2023-01`: each day (or, for a
    monthly pipeline, each month of the year) with its state, as JSON alone."""
    var ps = _pipelines(db, name)
    if len(ps) == 0:
        return not_found(String("no pipeline ", name))
    try:
        var c = calendar(db, name, ps[0].kind, ps[0].start, month)
        var j = calendar_json(c)
        return Page(200, j.copy(), j^, "application/json")
    except e:
        return _bad_request(String(e))


def _month_name(month: Int) -> String:
    var names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    return String(names[month - 1])


def _state_words(state: String) -> String:
    if state == "live":
        return "running or queued"
    if state == "open":
        return "not run"
    if state == "outside":
        return "outside the pipeline's range"
    return state


def _cal_title(c: Calendar) raises -> String:
    if c.kind == "monthly":
        return c.period
    var y = c.period[byte=:4]
    var m = Int(c.period[byte=5:7])
    return String(_month_name(m), " ", y)


def _coverage_html(
    mut h: Html, mut db: State, p: PipelineRow, cal: String
) raises:
    """The pipeline page's coverage: its calendar with each day's state,
    read-only, a month (or year) at a time with links to the next, so it
    needs no scripts."""
    var daily = p.kind == "daily"
    var base = String("/pipelines/", url_encode(p.name))
    h.raw('<h2 id="coverage">Coverage</h2><div class="cal cal--static"')
    h.raw(' role="group" aria-label="')
    h.text("Days" if daily else "Months", " of ", p.name)
    h.raw('">')
    var c: Calendar
    try:
        c = calendar(db, p.name, p.kind, p.start, cal)
    except e:
        h.raw('<p class="refused">')
        h.text(String(e))
        h.raw("</p></div>")
        return
    h.raw('<div class="cal__head">')
    if c.prev:
        h.raw('<a rel="prev" href="')
        h.text(base, "?cal=", c.prev, "#coverage")
        h.raw(
            '" aria-label="Previous ', "month" if daily else "year", '">‹</a>'
        )
    else:
        h.raw('<span aria-hidden="true">‹</span>')
    h.raw('<strong aria-live="polite">')
    h.text(_cal_title(c))
    h.raw("</strong>")
    if c.next:
        h.raw('<a rel="next" href="')
        h.text(base, "?cal=", c.next, "#coverage")
        h.raw('" aria-label="Next ', "month" if daily else "year", '">›</a>')
    else:
        h.raw('<span aria-hidden="true">›</span>')
    h.raw('</div><ul class="cal__grid')
    h.raw(" cal__grid--days" if daily else " cal__grid--months", '">')
    if daily:
        for w in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]:
            h.raw('<li class="cal__dow" aria-hidden="true">', w, "</li>")
        for _ in range(c.weekday):
            h.raw('<li class="cal__pad" aria-hidden="true"></li>')
    for i in range(len(c.cells)):
        ref cell = c.cells[i]
        h.raw('<li class="cal__day cal--', cell.state, '" data-key="')
        h.text(cell.key)
        h.raw('" title="')
        h.text(cell.key, ": ", _state_words(cell.state))
        h.raw('">')
        if daily:
            h.text(i + 1)
        else:
            h.text(_month_name(i + 1)[byte=:3])
        h.raw('<span class="sr">')
        h.text(", ", cell.key, " ", _state_words(cell.state))
        h.raw("</span></li>")
    h.raw('</ul><p class="muted cal__legend">')
    var first = True
    for s in ["done", "failed", "live", "open", "outside"]:
        var n = c.count(String(s))
        h.raw("" if first else " · ")
        first = False
        h.raw('<span class="cal__key cal--', s, '"></span> ')
        h.text(n, " ", _state_words(String(s)))
    h.raw("</p></div>")


comptime _CAL_SCRIPT = """
(() => {
  const root = document.querySelector("[data-calendar]");
  const f = document.getElementById("start");
  if (!root || !f) return;
  const field = f.elements.partitions;
  const words = { done: "done", failed: "failed", live: "running or queued", open: "not run", outside: "outside the pipeline's range" };
  const names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
  const two = (n) => String(n).padStart(2, "0");
  const cache = {};
  let data = null, focus = "", anchor = "", base = "", mine = false;
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  };
  const ranges = () => field.value.split(",").map((s) => s.trim()).filter(Boolean).map((s) => {
    const p = s.split("..");
    return [p[0], p[1] || p[0]];
  });
  const marks = () => {
    const rs = ranges();
    for (const b of root.querySelectorAll("button.cal__day")) {
      const k = b.dataset.key;
      const on = rs.some((r) => k >= r[0] && k <= r[1]);
      b.classList.toggle("cal--sel", on);
      b.setAttribute("aria-pressed", on ? "true" : "false");
      b.classList.toggle("cal--anchor", k === anchor);
    }
    const hint = root.querySelector(".cal__hint");
    if (hint) hint.textContent = anchor ? "Now pick the last " + data.unit + " (the same one again for just that one)." : "Pick the first " + data.unit + ", then the last. Hold ctrl or cmd to add another range.";
  };
  const render = () => {
    root.replaceChildren();
    const daily = data.unit === "day";
    const title = daily ? names[+data.period.slice(5, 7) - 1] + " " + data.period.slice(0, 4) : data.period;
    const head = el("div", "cal__head");
    const nav = (to, label, glyph) => {
      const b = el("button", "", glyph);
      b.type = "button";
      b.setAttribute("aria-label", label);
      b.disabled = !to;
      b.addEventListener("click", () => load(to));
      return b;
    };
    const t = el("strong", "", title);
    t.setAttribute("aria-live", "polite");
    head.append(nav(data.prev, "Previous " + data.unit + (daily ? " month" : " year"), "‹"), t, nav(data.next, "Next " + (daily ? "month" : "year"), "›"));
    const grid = el("div", "cal__grid " + (daily ? "cal__grid--days" : "cal__grid--months"));
    grid.setAttribute("role", "group");
    grid.setAttribute("aria-label", title);
    if (daily) {
      for (const w of ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]) grid.append(el("span", "cal__dow", w));
      for (let i = 0; i < data.weekday; i++) grid.append(el("span", "cal__pad"));
    }
    const keys = data.cells.map((c) => c.key);
    if (!keys.includes(focus)) {
      const sel = ranges().map((r) => r[0]).find((k) => keys.includes(k));
      focus = sel || (keys.includes(data.today) ? data.today : (data.cells.find((c) => c.state !== "outside") || data.cells[0]).key);
    }
    data.cells.forEach((c, i) => {
      const b = el("button", "cal__day cal--" + c.state, daily ? String(i + 1) : names[i].slice(0, 3));
      b.type = "button";
      b.dataset.key = c.key;
      b.setAttribute("aria-label", c.key + ": " + words[c.state]);
      b.tabIndex = c.key === focus ? 0 : -1;
      if (c.state === "outside") b.setAttribute("aria-disabled", "true");
      grid.append(b);
    });
    const legend = el("p", "muted cal__legend");
    for (const s of ["done", "failed", "live", "open", "outside"]) {
      legend.append(el("span", "cal__key cal--" + s), " " + data.counts[s] + " " + words[s] + " ");
    }
    const hint = el("p", "muted cal__hint");
    hint.setAttribute("aria-live", "polite");
    root.append(head, grid, legend, hint);
    marks();
  };
  const load = async (period, then) => {
    try {
      if (!cache[period || ""]) {
        const r = await fetch(root.dataset.calendar + (period ? "?month=" + encodeURIComponent(period) : ""));
        if (!r.ok) throw new Error(r.status);
        cache[period || ""] = await r.json();
      }
      data = cache[period || ""];
      if (then) focus = then;
      render();
      if (then) root.querySelector('button[data-key="' + then + '"]').focus();
    } catch (e) {
      if (period) return load("");
      root.replaceChildren(el("p", "muted", "The calendar couldn't be loaded; type the range instead."));
    }
  };
  const pick = (b, e) => {
    if (b.getAttribute("aria-disabled")) return;
    const k = b.dataset.key;
    focus = k;
    if (!anchor) {
      base = (e.ctrlKey || e.metaKey) && field.value.trim() ? field.value.trim() + "," : "";
      anchor = k;
      field.value = base + k;
    } else {
      const lo = anchor < k ? anchor : k, hi = anchor < k ? k : anchor;
      field.value = base + (lo === hi ? lo : lo + ".." + hi);
      anchor = "";
    }
    mine = true;
    field.dispatchEvent(new Event("input", { bubbles: true }));
    mine = false;
    marks();
  };
  const step = (k, dx, dy) => {
    if (data.unit === "day") {
      const d = new Date(Date.UTC(+k.slice(0, 4), +k.slice(5, 7) - 1, +k.slice(8, 10) + dx + 7 * dy));
      return d.getUTCFullYear() + "-" + two(d.getUTCMonth() + 1) + "-" + two(d.getUTCDate());
    }
    const d = new Date(Date.UTC(+k.slice(0, 4), +k.slice(5, 7) - 1 + dx + 4 * dy, 1));
    return d.getUTCFullYear() + "-" + two(d.getUTCMonth() + 1);
  };
  root.addEventListener("click", (e) => {
    const b = e.target.closest("button.cal__day");
    if (b) pick(b, e);
  });
  root.addEventListener("keydown", (e) => {
    const b = e.target.closest("button.cal__day");
    if (!b) return;
    const move = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] }[e.key];
    if (!move) {
      if (e.key === "PageUp" || e.key === "PageDown") {
        e.preventDefault();
        const to = e.key === "PageUp" ? data.prev : data.next;
        if (to) load(to);
      }
      return;
    }
    e.preventDefault();
    const to = step(b.dataset.key, move[0], move[1]);
    const at = root.querySelector('button[data-key="' + to + '"]');
    if (at) {
      focus = to;
      for (const x of root.querySelectorAll("button.cal__day")) x.tabIndex = x === at ? 0 : -1;
      at.focus();
    } else {
      const p = data.unit === "day" ? to.slice(0, 7) : to.slice(0, 4);
      if ((to < b.dataset.key ? data.prev : data.next) === p) load(p, to);
    }
  });
  field.addEventListener("input", () => {
    if (!mine) anchor = "";
    if (data) marks();
  });
  const first = ranges()[0];
  root.hidden = false;
  root.textContent = "Loading calendar…";
  load(first ? first[0].slice(0, data_len(first[0])) : "");
  function data_len(k) { return k.length === 4 ? 4 : 7; }
})();
"""
"""The Start runs form's calendar: a month of days (a year of months for a
monthly pipeline) from `<pipeline>/calendar`, each coloured by its state.
Two clicks fill the range field, which the preview and command lines already
follow; the arrows move between days, Page Up and Down between months."""


comptime _START_SCRIPT = """
(() => {
  const f = document.getElementById("start");
  if (!f) return;
  const sh = (s) => /^[A-Za-z0-9_.,:\\/@=+%-]+$/.test(s) ? s : "'" + s.replaceAll("'", "'\\\\''") + "'";
  let timer;
  const update = () => {
    const keys = f.elements.partitions.value.trim();
    const rerun = f.elements.rerun.checked;
    const k = keys ? sh(keys) : "<range>";
    f.querySelector("[data-cmd=backfill]").textContent =
      "orca backfill " + (rerun ? "--rerun " : "") + f.dataset.pipeline + " " + k;
    f.querySelector("[data-cmd=run]").textContent = f.dataset.binary + " run --partitions " + k;
    clearTimeout(timer);
    if (!keys) return;
    timer = setTimeout(async () => {
      const q = new URLSearchParams({ preview: "1", partitions: keys });
      if (rerun) q.set("rerun", "1");
      try {
        const r = await fetch(f.dataset.page + "?" + q);
        const doc = new DOMParser().parseFromString(await r.text(), "text/html");
        const got = doc.getElementById("start-preview");
        const at = document.getElementById("start-preview");
        if (got && at) at.replaceWith(got);
      } catch (e) {}
    }, 400);
  };
  f.addEventListener("input", update);
  f.addEventListener("change", update);
})();
"""
"""Updates the Start runs form's command lines as you type, and its
preview a moment after; without it, the Preview button does the same."""


def _start_html(
    mut h: Html,
    p: PipelineRow,
    keys: String,
    rerun: Bool,
    preview: Optional[StartPreview],
    keys_per_run: Int,
):
    """The pipeline page's Start runs section: one range, a backfill (the
    primary) or one run over it, a preview of what they'd queue, and the
    same from the command line."""
    var base = String("/pipelines/", url_encode(p.name))
    var unit = _unit(p.kind, 1)
    var units = _unit(p.kind, 2)
    var example = String("2024-01-01" if p.kind == "daily" else "2024-01")
    h.raw("<h2>Start runs", _docs("cli"), '</h2><form class="start" id="start"')
    h.raw(' method="post" action="')
    h.text(base, "/backfill")
    h.raw('" data-page="')
    h.text(base)
    h.raw('" data-pipeline="')
    h.text(_sh(p.name))
    h.raw('" data-binary="')
    h.text(_sh(p.binary))
    h.raw('"><label class="start__range">')
    h.text("Keys" if p.kind == "keyed" else String("Range of ", units))
    h.raw(' <input name="partitions" required placeholder="')
    if p.kind == "keyed":
        h.text("@all, or name,name")
    else:
        h.text(example, "..", example)
    h.raw('" value="')
    h.text(keys)
    h.raw('"></label>')
    if p.kind == "daily" or p.kind == "monthly":
        # Filled in by _CAL_SCRIPT; without scripts it stays hidden and the
        # field above is the form.
        h.raw('<div class="cal" hidden data-calendar="')
        h.text(base, "/calendar")
        h.raw('"></div>')
    h.raw('<label class="start__check"><input type="checkbox"')
    h.raw(' name="rerun" value="1"', " checked" if rerun else "", ">")
    h.text(" Include ", units, " that already succeeded (rerun them)")
    h.raw('</label><div class="start__choices"><div class="start__choice">')
    h.raw('<button type="submit" class="primary">')
    if keys_per_run > 1:
        h.text("Backfill: runs of up to ", keys_per_run, " ", units)
    else:
        h.text("Backfill: one run per ", unit)
    h.raw('</button><p class="muted">')
    h.text(
        "Queues a run for each ",
        unit if keys_per_run <= 1 else String(keys_per_run, " ", units),
        " in the range that hasn't succeeded and isn't in a queued or",
        " running run.",
    )
    h.raw('</p></div><div class="start__choice"><button type="submit"')
    h.raw(' formaction="')
    h.text(base, "/runs")
    h.raw('">Start one run over the whole range</button><p class="muted">')
    h.text(
        "Queues a single run over every ",
        unit,
        " in the range, done or not; refused while a queued or running run",
        " has one of them.",
    )
    h.raw("</p></div></div>")
    h.raw('<button type="submit" formmethod="get" formaction="')
    h.text(base, "#start")
    h.raw('" formnovalidate name="preview" value="1">Preview</button>')
    h.raw('<div id="start-preview" class="start__preview">')
    if preview:
        ref v = preview.value()
        if v.refused:
            h.raw('<p class="refused">')
            h.text(v.refused)
            h.raw("</p>")
        else:
            h.raw('<p data-preview="backfill">')
            h.text(_preview_line(p, v))
            h.raw('</p><p class="muted" data-preview="run">')
            h.text(_one_run_line(p, v))
            h.raw("</p>")
    h.raw('</div><p class="muted start__cli">Same from the command line (on')
    h.raw(" the control instance today, remotely once #111 lands):</p>")
    h.raw('<pre class="cmd"><code data-cmd="backfill">')
    h.text(_backfill_cmd(p.name, keys, rerun))
    h.raw('</code>\n<code data-cmd="run">')
    h.text(_run_cmd(p.binary, keys))
    h.raw('</code></pre><p class="muted more">The second runs in that')
    h.raw(" shell's process rather than queuing it; orca has no command that")
    h.raw(" queues one run. The scheduler (<code>orca scheduler</code>) starts")
    h.raw(" queued runs, up to ")
    h.text(p.max_runs)
    h.raw(" at once.</p></form><script>", _START_SCRIPT)
    if p.kind == "daily" or p.kind == "monthly":
        h.raw(_CAL_SCRIPT)
    h.raw("</script>")


def _fill_of(mut db: State, name: String) raises -> Optional[FillStatus]:
    """`name`'s fill (`orca.fill`), if it fills its range or has fill runs
    from when it did."""
    var regs = registered(db, name)
    if len(regs) == 0:
        return None
    if not regs[0].fill.all:
        var any = db.conn.query(
            (
                "SELECT 1 FROM orca.runs WHERE pipeline = $1 AND trigger ="
                " 'fill' LIMIT 1"
            ),
            Params().text(name),
        )
        if len(any) == 0:
            return None
    var tx = db.conn.begin()
    var s = fill_status(tx, fill_pipeline(regs[0]), now(), regs[0].run_retries)
    tx.rollback()
    return s^


def _fill_html(mut h: Html, s: FillStatus):
    """The pipeline page's fill line: done of total, what's going, the
    rate and ETA, today's spend against the budget, and why it's idle."""
    h.raw("</dd><dt>Fill</dt><dd>")
    var pct = (s.done * 100 // s.total) if s.total > 0 else 100
    h.text(s.done, " / ", s.total, " done (", pct, "%)")
    if s.running > 0:
        h.text(", ", s.running, " fill runs going")
    if s.live > 0:
        h.text(", ", s.live, " in live runs")
    if s.retrying > 0:
        h.text(", ", s.retrying, " waiting for a retry")
    if s.failed > 0:
        h.text(", ", s.failed, " failed")
    if s.rate > 0:
        h.text(
            "; ",
            Float64(Int(s.rate * 10.0 + 0.5)) / 10.0,
            " an hour, ETA ",
            eta_text(s.eta_seconds),
        )
    h.text("; today ")
    if s.spent < 0:
        h.text("unknown")
    else:
        h.text(money(s.spent))
    if s.budget >= 0:
        h.text(" of ", money(s.budget))
    else:
        h.text(", no budget")
    h.raw(' <span class="muted">(estimated)</span>')
    if s.idle:
        h.raw('<br><span class="muted">Idle: ')
        h.text(s.idle)
        h.raw("</span>")


def pipeline(
    mut db: State,
    name: String,
    keys_in: String = "",
    rerun: Bool = False,
    preview: Bool = False,
    cal: String = "",
    allow_append: Bool = False,
) raises -> Page:
    """A pipeline's page; with `preview` and `keys_in`, what its Start runs
    form would queue over them (`?preview=1&partitions=...&rerun=1`, and
    `&allow_append=1` for a rerun that appends again); `cal` is the month
    (or year) its coverage calendar shows."""
    var ps = _pipelines(db, name)
    if len(ps) == 0:
        return not_found(String("no pipeline ", name))
    ref p = ps[0]
    var start = _start_preview(db, name, keys_in, rerun, allow_append) if (
        preview and keys_in.strip()
    ) else Optional[StartPreview](None)
    var steps = _step_defs(db, name)
    var fires = List[Fire]()
    try:
        fires = _fires(p, now(), 5)
    except:
        pass
    var runs = _runs(db, pipeline=name, limit=20)
    var fill = _fill_of(db, name)

    var j = String('{"pipeline": ')
    _json_pipeline(j, p)
    j.write(', "fill": ', status_json(fill.value()) if fill else "null")
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
    j.write("]")
    if start:
        j.write(', "preview": ')
        _json_start_preview(j, p, start.value(), keys_in, rerun)
    j.write("}")

    var h = Html()
    h.raw('<p class="crumb"><a href="/">Pipelines</a></p><h1>')
    h.text(p.name)
    if p.paused:
        h.raw(" ")
        h.status("paused")
    _pause_button(h, p.name, p.paused)
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
    if fill:
        _fill_html(h, fill.value())
    h.raw("</dd><dt>Run retries</dt><dd>")
    h.text(p.run_retries)
    h.raw("</dd><dt>Placement</dt><dd>")
    if p.placement == "control":
        h.text("control")
        h.raw(' <span class="muted">(on the scheduler\'s host)</span>')
    elif p.placement.startswith("worker:"):
        h.text(p.placement)
        h.raw(' <span class="muted">(its runs wait for a worker of pool ')
        h.text(p.placement.removeprefix("worker:"))
        h.raw(', <a href="/ops/workers">Workers</a>)</span>')
    elif p.placement:
        h.text(p.placement)
    else:
        h.raw('<span class="muted">default (the scheduler\'s runner)</span>')
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
        dag.append(DagStep(d.name, d.kind, d.after, "", links.step(d.name).url))
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
    if p.kind == "daily" or p.kind == "monthly":
        _coverage_html(h, db, p, cal)
    if p.binary:
        var regs = registered(db, name)
        _start_html(
            h,
            p,
            keys_in,
            rerun,
            start,
            regs[0].keys_per_run if len(regs) > 0 else 1,
        )
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


comptime _RUN_STATUSES: List[String] = [
    "queued",
    "running",
    "deferred",
    "succeeded",
    "failed",
    "cancelled",
]
"""The statuses a run takes (`orca.runs.status`), in the order a run goes
through them; a deferred run waits for its `retry_at`."""

comptime _RUN_TRIGGERS: List[String] = [
    "schedule",
    "sensor",
    "backfill",
    "manual",
    "ui",
    "rerun",
    "rebuild",
    "fill",
]
"""The triggers orca writes (`orca.runs.trigger`): the scheduler's fires and
sensors, `backfill`, a binary's `run`, the UI's start button, a rerun, a
rebuild and a fill (`orca.fill`). The filter adds any others the table has."""

comptime _RUNS_PAGE = 50
"""Runs to a page of `/runs`, unless `?n=` says (at most `_RUNS_PAGE_MAX`)."""
comptime _RUNS_PAGE_MAX = 500

comptime _SUBMIT_ON_CHANGE = (
    '<script>document.querySelector("form.filters").addEventListener('
    '"change", (e) => e.currentTarget.submit());</script>'
)
"""Filtering as soon as a choice changes; the Filter button does the same
without JavaScript."""


def _grouped(n: Int) -> String:
    """`n` with commas between thousands: 1,234."""
    var digits = String(n if n >= 0 else -n)
    var out = String("-" if n < 0 else "")
    var first = digits.byte_length() % 3
    if first == 0:
        first = 3
    out.write(digits[byte=:first])
    var i = first
    while i < digits.byte_length():
        out.write(",", digits[byte = i : i + 3])
        i += 3
    return out^


def _runs_href(
    pipeline: String, status: String, trigger: String, n: Int, before: Int
) -> String:
    """`/runs` with these filters, page size (unless the default) and
    `before` (unless 0), each value encoded."""
    var q = String()
    for f in [("pipeline", pipeline), ("status", status), ("trigger", trigger)]:
        if f[1]:
            q.write("&" if q else "?", f[0], "=", url_encode(f[1]))
    if n != _RUNS_PAGE:
        q.write("&" if q else "?", "n=", n)
    if before > 0:
        q.write("&" if q else "?", "before=", before)
    return String("/runs", q)


def _select(
    mut h: Html, name: StaticString, options: List[String], current: String
):
    """A labelled `<select>` of `options` after "any", `current` selected;
    `current` is added if it isn't one of them, so the page says what it
    filters by."""
    h.raw("<label>", name, ' <select name="', name, '">')
    h.raw(
        '<option value=""', " selected" if not current else "", ">any</option>"
    )
    var found = False
    for o in options:
        h.raw('<option value="')
        h.text(o)
        h.raw('"')
        if o == current:
            h.raw(" selected")
            found = True
        h.raw(">")
        h.text(o)
        h.raw("</option>")
    if current and not found:
        h.raw('<option value="')
        h.text(current)
        h.raw('" selected>')
        h.text(current)
        h.raw("</option>")
    h.raw("</select></label>")


def _with_seen(
    known: List[String], mut db: State, sql: String
) raises -> List[String]:
    """`known`, then the values `sql` returns (as `v`) that aren't in it."""
    var out = known.copy()
    for r in db.conn.query(sql):
        var v = r.text("v")
        if v and v not in out:
            out.append(v)
    return out^


def runs(
    mut db: State,
    pipeline: String,
    status: String,
    trigger: String,
    before: Int = 0,
    n: Int = 0,
) raises -> Page:
    """`/runs`: a page of runs newest first, by id under `before` if it's
    over 0, filtered; with how many match and links to the newer and older
    pages."""
    var size = _RUNS_PAGE if n <= 0 else min(n, _RUNS_PAGE_MAX)
    var at = before if before > 0 else 0
    var rs = _runs(db, pipeline, status, trigger, limit=size + 1, before=at)
    var more = len(rs) > size
    if more:
        _ = rs.pop()
    var next_before = rs[len(rs) - 1].id if more else 0

    # How many match, and how many of them are newer than this page; then
    # where the newer page starts: `size` runs above this one, or the first.
    comptime matching = (
        " FROM orca.runs WHERE ($1 = '' OR pipeline = $1) AND ($2 = '' OR"
        " status = $2) AND ($3 = '' OR trigger = $3)"
    )
    var counts = db.conn.query(
        String(
            "SELECT count(*) AS total, count(*) FILTER (WHERE $4 > 0 AND id",
            " >= $4) AS newer",
            matching,
        ),
        Params().text(pipeline).text(status).text(trigger).int64(Int64(at)),
    )
    var total = Int(counts.row(0).int64("total"))
    var newer = Int(counts.row(0).int64("newer"))
    var newer_before = 0
    if newer > size:
        var top = db.conn.query(
            String(
                "SELECT id",
                matching,
                " AND id >= $4 ORDER BY id OFFSET $5 LIMIT 1",
            ),
            Params()
            .text(pipeline)
            .text(status)
            .text(trigger)
            .int64(Int64(at))
            .int64(Int64(size - 1)),
        )
        newer_before = Int(top.row(0).int64("id")) + 1

    var j = String('{"runs": ')
    _json_runs(j, rs)
    j.write(
        ', "filters": {"pipeline": ',
        quote(pipeline),
        ', "status": ',
        quote(status),
        ', "trigger": ',
        quote(trigger),
        '}, "n": ',
        size,
        ', "before": ',
        String(at) if at > 0 else String("null"),
        ', "total": ',
        total,
        ', "newer": ',
        newer,
        ', "next_before": ',
        String(next_before) if more else String("null"),
        "}",
    )

    var pipelines = List[String]()
    for r in db.conn.query("SELECT name FROM orca.pipelines ORDER BY name"):
        pipelines.append(r.text("name"))
    var triggers = _with_seen(
        materialize[_RUN_TRIGGERS](),
        db,
        "SELECT DISTINCT trigger AS v FROM orca.runs ORDER BY v",
    )

    var h = Html()
    h.raw("<h1>Runs</h1>")
    h.raw('<form class="filters" method="get" action="/runs">')
    _select(h, "pipeline", pipelines, pipeline)
    _select(h, "status", materialize[_RUN_STATUSES](), status)
    _select(h, "trigger", triggers, trigger)
    if size != _RUNS_PAGE:
        h.raw('<input type="hidden" name="n" value="', size, '">')
    h.raw('<button type="submit">Filter</button></form>', _SUBMIT_ON_CHANGE)
    _runs_table(h, rs, True)
    if len(rs) > 0:
        h.raw('<nav class="pager" aria-label="Pages of runs">')
        if newer > 0:
            h.link(
                _runs_href(pipeline, status, trigger, size, newer_before),
                "← Newer",
            )
        h.raw('<span class="pager__at">')
        h.text(
            _grouped(newer + 1),
            "–",
            _grouped(newer + len(rs)),
            " of ",
            _grouped(total),
        )
        h.raw("</span>")
        if more:
            h.link(
                _runs_href(pipeline, status, trigger, size, next_before),
                "Older →",
            )
        h.raw("</nav>")
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
    var metrics = read_metrics(r.pipeline, id)
    j.write(', "instances": ')
    _json_instances(j, id, instances, metrics)
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
    if r.status == "succeeded":
        var defs = _step_defs(db, r.pipeline)
        if len(defs) > 0:
            _act_button(
                h,
                String("/runs/", r.id, "/rerun"),
                "Rerun",
                "rerun",
                "no orca command for this yet: a new run of every step",
                danger=True,
                step=defs[0].name,
            )
    else:
        _row_actions(h, r)
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
    if r.held and r.status == "queued":
        h.raw('</dd><dt>Held</dt><dd class="error">')
        h.text(r.held.removeprefix("held: "))
    if r.placement:
        h.raw("</dd><dt>Placed</dt><dd>")
        if r.placement == "control":
            h.raw("on the control host")
        else:
            h.text(r.placement)
        var on = ran_on(db, id)
        if on:
            h.raw("</dd><dt>Ran on</dt><dd>")
            h.text(on)
    h.raw('</dd><dt><abbr title="', _TRIES, '">Tries</abbr></dt><dd>')
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
    # The machines' charts, on the steps' time axis, live while it runs.
    if _live(r.status):
        h.raw('<div data-poll="/runs/')
        h.text(r.id)
        h.raw('/metrics">')
    else:
        h.raw("<div>")
    _machines_html(h, db, r, metrics)
    h.raw("</div>")
    if _live(r.status):
        var waiting = db.conn.query(
            (
                "SELECT to_char(waiting_since AT TIME ZONE 'UTC',"
                ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS since FROM orca.runs'
                " WHERE id = $1 AND waiting_since IS NOT NULL"
            ),
            Params().int64(Int64(id)),
        )
        if len(waiting) > 0:
            h.raw('<p class="muted">Waiting for workers since ')
            h.text(waiting.row(0).text("since"))
            h.raw(
                ": its head has tasks and no worker; the attempt fails when"
                " that has lasted <code>ORCA_NO_WORKERS_SECONDS</code>.</p>"
            )
    if len(instances) > 0:
        _instances_html(h, id, instances)
    # Who did what to it from the UI (orca.auth).
    var acts = db.conn.query(
        (
            "SELECT to_char(at AT TIME ZONE 'UTC',"
            ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS at, username, action,'
            " coalesce(source, '') AS source FROM orca.audit WHERE run = $1"
            " ORDER BY id"
        ),
        Params().int64(Int64(id)),
    )
    if len(acts) > 0:
        h.raw(
            '<h2>Actions</h2><div class="scroll"><table><thead><tr>',
            "<th>At</th><th>By</th><th>Action</th><th>From</th></tr>",
            "</thead><tbody>",
        )
        for a in acts:
            h.raw("<tr>")
            h.mono(a.text("at"))
            h.cell(a.text("username"))
            h.cell(a.text("action"))
            h.cell(a.text("source"))
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
            DagStep(
                d.name,
                d.kind,
                d.after,
                aggregate(st),
                links.step(d.name).url,
            )
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


def run_metrics(mut db: State, id: Int) raises -> Page:
    """The run's machines, as a fragment; 286 once it's ended."""
    var found = _runs_one(db, id)
    if not found:
        return not_found(String("no run ", id))
    var r = found.take()
    var h = Html()
    _machines_html(h, db, r, read_metrics(r.pipeline, id))
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
    # A new run from this step on, once the run has ended, as the run
    # page's "Rerun from" does.
    var ended = db.conn.query(
        (
            "SELECT status NOT IN ('queued', 'running') AS ended FROM orca.runs"
            " WHERE id = $1"
        ),
        Params().int64(Int64(run)),
    )
    if len(ended) > 0 and ended.row(0).bool("ended"):
        _act_button(
            h,
            String("/runs/", run, "/rerun"),
            "Rerun from here",
            "rerun from here",
            String(
                "no orca command for this yet: a new run from ",
                a.step,
                " on, the steps before it copied",
            ),
            danger=True,
            step=a.step,
        )
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
    var cache = Dict[String, CodeLinks]()
    var at = _code_of(db, cache, "", run).step(a.step)
    if at:
        h.raw("<dt>Code</dt><dd>")
        _code(h, at)
        h.raw("</dd>")
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
            " AS waiting, ",
            _SECONDS,
            " AS seconds, coalesce(placement, '') AS placement,",
            " coalesce(held, '') AS held FROM orca.runs WHERE id = $1",
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
        Int(r.int64("seconds")),
        r.text("placement"),
        r.text("held"),
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


@fieldwise_init
struct StepUse(Copyable, Movable):
    """A step that writes (or reads) a dataset: declared in its `writes=`
    (`reads=`), or seen committing to (reading) it; and the newest run that
    did, or 0."""

    var pipeline: String
    var step: String
    var run: Int


def _dataset_steps(
    mut db: State, dataset: String, writes: Bool
) raises -> List[StepUse]:
    var kind = "writes" if writes else "reads"
    var table = "orca.commits" if writes else "orca.reads"
    var rows = db.conn.query(
        String(
            "WITH d AS (SELECT p.name AS pipeline, st->>'name' AS step FROM",
            " orca.pipelines p, jsonb_array_elements(p.describe->'steps') AS",
            " x(st) WHERE EXISTS (SELECT 1 FROM",
            " jsonb_array_elements_text(coalesce(st->'",
            kind,
            "', '[]')) AS v WHERE v = $1)), seen AS (SELECT DISTINCT ON",
            " (r.pipeline, s.step) r.pipeline, s.step, r.id AS run FROM ",
            table,
            " c JOIN orca.attempts a ON a.id = c.attempt JOIN orca.steps s ON",
            " s.id = a.step JOIN orca.runs r ON r.id = s.run WHERE c.dataset",
            " = $1 ORDER BY r.pipeline, s.step, c.attempt DESC) SELECT",
            " coalesce(d.pipeline, seen.pipeline) AS pipeline,",
            " coalesce(d.step, seen.step) AS step, coalesce(seen.run, 0) AS",
            " run FROM d FULL JOIN seen ON seen.pipeline = d.pipeline AND",
            " seen.step = d.step ORDER BY 1, 2",
        ),
        Params().text(dataset),
    )
    var out = List[StepUse]()
    for r in rows:
        out.append(
            StepUse(r.text("pipeline"), r.text("step"), Int(r.int64("run")))
        )
    return out^


def _steps_using(
    mut h: Html,
    mut db: State,
    mut cache: Dict[String, CodeLinks],
    uses: List[StepUse],
):
    """Each pipeline, then its steps, each linked to its line at the commit
    of the newest run that used the dataset."""
    if len(uses) == 0:
        h.raw('<span class="muted">none</span>')
        return
    for i in range(len(uses)):
        ref u = uses[i]
        if i == 0 or uses[i - 1].pipeline != u.pipeline:
            if i:
                h.raw("; ")
            h.link(String("/pipelines/", url_encode(u.pipeline)), u.pipeline)
            h.raw(' <span class="steps-of">step ')
        else:
            h.raw(", ")
        h.text(u.step)
        var at = _code_of(db, cache, u.pipeline, u.run).step(u.step)
        if at:
            h.raw(" ")
            _code(h, at)
        if i + 1 == len(uses) or uses[i + 1].pipeline != u.pipeline:
            h.raw("</span>")


@fieldwise_init
struct RebuildPreview(Copyable, Movable):
    """What a rebuild over a range would queue, level by level, or why it's
    refused: `plan_rebuild`, which writes nothing."""

    var refused: String
    var partitions: List[String]
    var levels: List[List[Tuple[String, String]]]
    """Each level's runs: the pipeline and its partitions."""


def _rebuild_preview(
    mut db: State, dataset: String, keys: String
) raises -> RebuildPreview:
    var levels = List[List[Tuple[String, String]]]()
    try:
        var got = plan_rebuild(db, dataset, keys)
        for p in got[1]:
            while len(levels) <= p.level:
                levels.append(List[Tuple[String, String]]())
            levels[p.level].append((p.pipeline, p.partitions))
        return RebuildPreview("", got[0].copy(), levels^)
    except e:
        var why = String(e)
        if why.startswith("orca: "):
            var rest = String(why[byte=6:])
            why = rest^
        return RebuildPreview(why^, List[String](), levels^)


def _rebuild_cmd(dataset: String, keys: String) -> String:
    """`orca rebuild <dataset> <keys>`, as `orca` reads it."""
    return String(
        "orca rebuild ",
        _sh(dataset),
        " ",
        _sh(keys) if keys else String("<range>"),
    )


def _rebuild_html(
    mut h: Html,
    dataset: String,
    chain: List[Link],
    keys: String,
    preview: Optional[RebuildPreview],
):
    """The dataset page's Rebuild section: what it does, the chain of
    pipelines it runs, a preview over a range, the button and the same from
    the command line."""
    h.raw("<h2>Rebuild", _docs("sensors"), "</h2>")
    h.raw(
        "<p>Rebuild reruns everything downstream of this dataset for the",
        " partitions you give, in dependency order. Use it after this",
        " dataset's data was fixed or reloaded, so the tables built from it",
        " catch up.</p>",
    )
    if len(chain) == 0:
        h.raw('<p class="empty">Nothing reads this dataset.</p>')
        return
    h.raw('<ol class="chain" aria-label="What a rebuild runs, in order">')
    var at = -1
    for i in range(len(chain)):
        ref c = chain[i]
        if c.level != at:
            if at >= 0:
                h.raw("</li>")
            at = c.level
            h.raw('<li class="chain__level">')
        else:
            h.raw('<span class="chain__and"> and </span>')
        h.raw('<span class="chain__step">')
        h.link(String("/pipelines/", url_encode(c.pipeline)), c.pipeline)
        h.raw(' <span class="muted">(reads ')
        for k in range(len(c.reads)):
            if k:
                h.raw(", ")
            h.raw("<code>")
            h.text(c.reads[k])
            h.raw("</code>")
        h.raw(")</span></span>")
    h.raw("</li></ol>")
    var href = _dataset_href(dataset)
    h.raw('<form class="start" id="rebuild" method="post" action="', href)
    h.raw('/rebuild" data-confirm="Rebuild?"><label class="start__range">')
    h.raw('Partitions <input name="partitions" required')
    h.raw(' placeholder="2024-01..2024-03" value="')
    h.text(keys)
    h.raw('"></label><div class="start__choices"><div class="start__choice">')
    h.raw('<button type="submit" class="primary act__need" title="')
    h.text(_rebuild_cmd(dataset, keys))
    h.raw('">Rebuild downstream</button><a class="act__login" href="/login')
    h.raw("?next=", _HERE_SLOT, '">Log in to rebuild</a><p class="muted">')
    h.raw("Plans a run of each")
    h.raw(" pipeline above over those partitions; each level is queued once")
    h.raw(" the one before it has succeeded.</p></div></div>")
    h.raw('<button type="submit" formmethod="get" formaction="', href)
    h.raw('#rebuild" formnovalidate name="preview" value="1">Preview</button>')
    h.raw('<div class="start__preview">')
    if preview:
        ref v = preview.value()
        if v.refused:
            h.raw('<p class="refused">')
            h.text(v.refused)
            h.raw("</p>")
        else:
            var runs = 0
            for l in v.levels:
                runs += len(l)
            h.raw('<p data-preview="rebuild">')
            h.text(
                "This queues ",
                runs,
                " run" if runs == 1 else " runs",
                " in ",
                len(v.levels),
                " level" if len(v.levels) == 1 else " levels",
                ", over ",
                len(v.partitions),
                " partition" if len(v.partitions) == 1 else " partitions",
                ":",
            )
            h.raw("</p><ol>")
            for l in range(len(v.levels)):
                h.raw("<li>")
                h.text(
                    len(v.levels[l]),
                    " run: " if len(v.levels[l]) == 1 else " runs: ",
                )
                for k in range(len(v.levels[l])):
                    if k:
                        h.raw(", ")
                    h.text(v.levels[l][k][0], " over ", v.levels[l][k][1])
                h.raw("</li>")
            h.raw("</ol>")
    h.raw('</div><p class="muted start__cli">Same from the command line (on')
    h.raw(" the control instance today, remotely once #111 lands):</p>")
    h.raw('<pre class="cmd"><code data-cmd="rebuild">')
    h.text(_rebuild_cmd(dataset, keys))
    h.raw("</code></pre></form>")


def dataset_page(
    mut db: State, name: String, keys: String = "", preview: Bool = False
) raises -> Page:
    """A dataset's page; with `preview` and `keys`, what a rebuild over them
    would queue (`?preview=1&partitions=...`)."""
    var ds = dataset_rows(db, name)
    if len(ds) == 0:
        return not_found(String("no dataset ", name))
    ref d = ds[0]
    var cs = commits(db, name)
    var rbs = rebuilds(db, dataset=name, limit=10)
    var chain = downstream(db, name)
    var rp = Optional[RebuildPreview](None)
    if preview and keys.strip() and len(chain) > 0:
        rp = _rebuild_preview(db, name, keys)
    var j = String('{"dataset": ', quote(d.name), ', "latest": ')
    var newest = latest_snapshot(db, name)
    j.write(String(newest) if newest >= 0 else String("null"), ', "writers": ')
    _json_strings(j, d.writers)
    j.write(', "readers": ')
    _json_strings(j, d.readers)
    j.write(', "downstream": [')
    for i in range(len(chain)):
        j.write(
            ", " if i else "",
            '{"pipeline": ',
            quote(chain[i].pipeline),
            ', "level": ',
            chain[i].level,
            ', "reads": ',
        )
        _json_strings(j, chain[i].reads)
        j.write("}")
    j.write("]")
    if rp:
        ref v = rp.value()
        j.write(
            ', "preview": {"partitions": ',
            quote(keys),
            ', "refused": ',
            quote(v.refused) if v.refused else String("null"),
            ', "levels": [',
        )
        for l in range(len(v.levels)):
            j.write(", " if l else "", "[")
            for k in range(len(v.levels[l])):
                j.write(
                    ", " if k else "",
                    '{"pipeline": ',
                    quote(v.levels[l][k][0]),
                    ', "partitions": ',
                    quote(v.levels[l][k][1]),
                    "}",
                )
            j.write("]")
        j.write('], "command": ', quote(_rebuild_cmd(name, keys)), "}")
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
    var cache = Dict[String, CodeLinks]()
    h.raw('</h1><dl class="facts"><dt>Written by</dt><dd>')
    _steps_using(h, db, cache, _dataset_steps(db, name, writes=True))
    h.raw("</dd><dt>Read by</dt><dd>")
    _steps_using(h, db, cache, _dataset_steps(db, name, writes=False))
    h.raw("</dd></dl>")
    h.raw('<div class="scroll">', _flow_graph(db, d.name), "</div>")
    _rebuild_html(h, d.name, chain, keys, rp)
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
            h.raw("</td><td>")
            h.text(c.step)
            var at = _code_of(db, cache, c.pipeline, c.run).step(c.step)
            if at:
                h.raw(" ")
                _code(h, at)
            h.raw("</td>")
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
    o: Outcome,
    back: String,
    var json: String,
    location: String,
    again: String = "",
    again_fields: List[Tuple[String, String]] = [],
    again_label: String = "",
) -> Page:
    """The answer to an action: to `location` if it was done, or a 409
    saying why not, with a way back to `back`, and to confirm if it asks:
    posting `again_fields` to `again` with `again_label` on the button, or,
    without `again`, resuming the run at `back` onto new code."""
    if not o.refused:
        return Page(303, json^, String(), location=location)
    var h = Html()
    h.raw("<h1>Not done</h1><p>")
    h.text(o.refused)
    h.raw("</p>")
    if o.confirm and again:
        h.raw('<form class="action" method="post" action="')
        h.text(again)
        h.raw('">')
        for f in again_fields:
            h.raw('<input type="hidden" name="')
            h.text(f[0])
            h.raw('" value="')
            h.text(f[1])
            h.raw('">')
        h.raw('<button type="submit">')
        h.text(again_label)
        h.raw("</button></form>")
    elif o.confirm:
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
            var rerun = _form(body, "rerun") == "1"
            var o = backfill_runs(
                db,
                name,
                keys,
                rerun=rerun,
                allow_append=_form(body, "allow_append") == "1",
            )
            var to = String("/runs?pipeline=", url_encode(name))
            var j = String('{"queued": ', o.queued, ', "skipped": ')
            _json_strings(j, o.skipped)
            j.write("}")
            return _acted(
                o,
                back,
                j^,
                to,
                String(back, "/backfill"),
                [
                    (String("partitions"), keys),
                    (String("rerun"), String("1")),
                    (String("allow_append"), String("1")),
                ],
                "Rerun them, appending again",
            )
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
    if len(parts) == 4 and parts[1] == "watchdog" and parts[3] == "snooze":
        var duration = _form(body, "duration")
        try:
            var secs = snooze_rule(db, url_decode(parts[2]), duration, "ui")
            return _acted(
                Outcome.done(0),
                String("/watchdog"),
                String('{"snoozed_seconds": ', secs, "}"),
                String("/watchdog"),
            )
        except e:
            return _acted(
                Outcome.no(String(e)),
                String("/watchdog"),
                String(),
                String("/watchdog"),
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
            var step = _form(body, "step")
            o = rerun_from(db, id, step, _form(body, "allow_append") == "1")
            if o.refused and o.confirm:
                return _acted(
                    o,
                    back,
                    String('{"run": 0}'),
                    back,
                    String(back, "/rerun"),
                    [
                        (String("step"), step),
                        (String("allow_append"), String("1")),
                    ],
                    "Rerun it, appending again",
                )
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
    if status == 301:
        return "Moved Permanently"
    if status == 302:
        return "Found"
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
    var status = (
        200 if page.location and json and page.status == 303 else (page.status)
    )
    var resp = Response(
        status=status,
        reason=_reason(status),
        body=List[UInt8](body.as_bytes()),
    )
    try:
        if page.location and (not json or page.status != 303):
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
    if path == "/api/login" or path == "/api/logout":
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
    if parts[1] == "watchdog":
        return parts[3] == "snooze"
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


# ── API tokens (orca.tokens) ─────────────────────────────────────────────────


def _bearer(authorization: String) -> String:
    """The token of `Authorization: Bearer <token>`, or empty for any other
    header."""
    var a = String(authorization.strip())
    if not a.lower().startswith("bearer "):
        return ""
    return String(a[byte=7:].strip())


def _content_type(req: Request) -> String:
    """The request's media type, lower case, without its parameters, which
    a cross-site page may set however it likes."""
    var t = req.headers.get("Content-Type")
    var semi = t.find(";")
    var essence = String(t[byte=:semi]) if semi >= 0 else t
    return String(essence.strip()).lower()


def _json_form(body: String) raises -> String:
    """A JSON object of strings, numbers and booleans (`orca.remote`'s
    actions) as the form the UI's own pages post, so both go through `_act`
    and are audited alike: true is `1`, false and null are left form."""
    var v = loads(body if body.strip() else "{}")
    if not v.is_object():
        raise Error("an action's JSON is an object of its fields")
    var form = String()
    for kv in v.object_items():
        var val: String
        if kv[1].is_string():
            val = kv[1].string_value()
        elif kv[1].is_bool():
            if not kv[1].bool_value():
                continue
            val = "1"
        elif kv[1].is_int():
            val = String(kv[1].int_value())
        elif kv[1].is_null():
            continue
        else:
            raise Error("an action's fields are strings, numbers or booleans")
        form.write("&" if form else "", url_encode(kv[0]), "=", url_encode(val))
    return form^


def _json_error(status: Int, why: String) -> Response:
    return _respond(
        Page(status, String('{"error": ', quote(why), "}"), String()), True
    )


def _token_refused() -> Response:
    """A request with a token that's no use: unknown, expired or revoked,
    or an `Authorization` that isn't a bearer token. Cookies are never
    looked at instead."""
    var resp = _json_error(
        401,
        (
            "this token is unknown, expired or revoked (orca login again), or"
            " Authorization isn't Bearer <token>"
        ),
    )
    try:
        resp.headers.set("WWW-Authenticate", 'Bearer realm="orca"')
    except:
        pass
    return resp^


def _api_login(mut db: State, body: String, address: String) raises -> Response:
    """`POST /api/login`: a new API token for a JSON `name` and `password`,
    labelled `label`, lasting `days` (at most `ORCA_API_TOKEN_DAYS`).
    Failures count against the same limit as `/login`'s, and the token is
    in this answer only."""
    var name: String
    var password: String
    var label = String()
    var days = 0
    try:
        var v = loads(body)
        name = v["name"].string_value()
        password = v["password"].string_value()
        if String("label") in v and v["label"].is_string():
            label = v["label"].string_value()
        if String("days") in v and v["days"].is_int():
            days = Int(v["days"].int_value())
    except:
        return _json_error(
            400, "a login is a JSON object with a name and a password"
        )
    if too_many_failures(db, name, address):
        return _json_error(
            429, "Too many failed logins; try again in 15 minutes."
        )
    if not password_ok(db, name, password):
        login_failed(db, name, address, "cli")
        return _json_error(401, "Wrong name or password.")
    var got = issue_token(db, name, label, token_days(days))
    audit(
        db,
        name,
        "/api/login",
        0,
        String(clean_label(label), " from ", address),
        "cli",
    )
    return _respond(
        Page(
            200,
            String(
                '{"user": ',
                quote(name),
                ', "token": ',
                quote(got.token),
                ', "expires": ',
                quote(got.expires),
                "}",
            ),
            String(),
        ),
        True,
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
        if post and path.startswith("/api/worker/"):
            # A remote worker (orca.workers): its token, its JSON, its calls.
            var got = worker_api(
                self.url,
                path,
                _bearer(req.headers.get("Authorization")),
                String(from_utf8_lossy=Span(req.body)),
            )
            var status = got.status
            var resp = _respond(Page(status, got.json.copy(), String()), True)
            if status == 401:
                resp.headers.set("WWW-Authenticate", 'Bearer realm="orca"')
            return resp^
        if post and not _is_action(path):
            return _not_allowed()
        if not post and req.method != "GET" and req.method != "HEAD":
            return _not_allowed()
        # A script's request (orca.remote): one with an `Authorization`
        # header, or a JSON login for a token. A browser sends neither to
        # another site without a CORS preflight, which the UI never
        # answers, so with no `Origin` it needn't be the UI's own; the
        # token, not a cookie, is what it acts with.
        var authorization = req.headers.get("Authorization")
        var api_login = path == "/api/login"
        var scripted = (
            (
                authorization != ""
                or (api_login and _content_type(req) == "application/json")
            )
            and path != "/login"
            and path != "/logout"
        )
        if scripted or path.startswith("/api/"):
            json = True
        if (
            post
            and not (scripted and not req.headers.get("Origin"))
            and not _same_origin(req)
        ):
            return _respond(_cross_origin(), json)
        if api_login and not scripted:
            return _json_error(400, "a login for a token is JSON")
        try:
            var db = State(self.url)
            var body = String(from_utf8_lossy=Span(req.body))
            if api_login:
                return _api_login(db, body, _client(req))
            var token = String()
            var user: String
            var source = String("ui")
            if authorization:
                var bearer = _bearer(authorization)
                user = token_user(db, bearer)
                if not user:
                    return _token_refused()
                source = "cli"
                if path == "/api/logout":
                    _ = revoke_token(db, bearer)
                    audit(db, user, path, 0, "", source)
                    return _respond(
                        Page(200, String('{"revoked": true}'), String()), True
                    )
            else:
                token = _cookie(req, SESSION_COOKIE)
                user = session_user(db, token)
                if path == "/api/logout":
                    return _token_refused()
            var required = login_required(db)
            if post and _content_type(req) == "application/json":
                try:
                    body = _json_form(body)
                except e:
                    return _respond(_bad_request(String(e)), json)
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
                    self._page(db, req.url, path),
                    json,
                    user,
                    required,
                    req.url,
                )
            if required and not user:
                return _login_needed(req, json)
            var done = _act(db, path, body)
            if not done:
                return _respond(not_found(String("nothing at ", path)), json)
            var page = done.take()
            if page.status == 303 and user:
                audit(
                    db, user, path, _run_of(page.location, path), body, source
                )
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
        self,
        var page: Page,
        json: Bool,
        user: String,
        required: Bool,
        here: String = "/",
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
        page.html = page.html.replace(
            String('<body class="', _AUTH_SLOT, '">'),
            '<body class="anon">' if required and not user else "<body>",
        )
        page.html = page.html.replace(_HERE_SLOT, url_encode(here))
        return _respond(page, json)

    def _page(self, mut db: State, url: String, path: String) raises -> Page:
        if path == "/" or path == "":
            return home(db)
        if path.startswith("/api/pipelines/"):
            # A pipeline's page as JSON, the fill among it.
            return self._page(db, url, String(path[byte=4:]))
        if path == "/runs":
            return runs(
                db,
                _query(url, "pipeline"),
                _query(url, "status"),
                _query(url, "trigger"),
                before=_int(_query(url, "before")),
                n=_int(_query(url, "n")),
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
            if len(parts) == 2 and parts[1] == "metrics":
                return run_metrics(db, id)
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
        if path == "/ops":
            return Page(302, String(), String(), location="/ops/control")
        if path == "/system" or path == "/fleet" or path == "/fleet/hosts":
            # The pages' old homes: moved for good, query kept.
            var q = url.find("?")
            var to = String("/ops/control") if path == "/system" else String(
                "/ops", path
            )
            return Page(
                301,
                String(),
                String(),
                location=String(
                    to, String(url[byte=q:]) if q >= 0 else String("")
                ),
            )
        if path == "/ops/fleet":
            return fleet_page(
                db,
                _query(url, "sort"),
                _query(url, "dir"),
                _query(url, "range"),
            )
        if path == "/ops/fleet/hosts":
            return fleet_hosts_page(
                db,
                _query(url, "sort"),
                _query(url, "dir"),
                _query(url, "range"),
            )
        if path == "/api/fleet":
            return fleet_api(db, _query(url, "range"))
        if path == "/ops/workers" or path == "/api/workers":
            var rows = worker_rows(db)
            var h = Html()
            workers_html(h, rows)
            return Page(
                200, workers_json(rows), _page("Workers", "/ops/workers", h)
            )
        if path == "/ops/control":
            return system_page(db, _query(url, "range"))
        if path == "/api/system":
            var page = system_page(db, _query(url, "range"))
            return Page(200, String(), page.json.copy(), "application/json")
        if path == "/metrics":
            return metrics_page(db)
        if path == "/watchdog":
            var h = Html()
            watchdog_html(h, rule_rows(db, watchdog_config()), active_trips(db))
            return Page(
                200,
                watchdog_json(db, watchdog_config()),
                _page("Watchdog", "", h),
            )
        if path == "/api/watchdog":
            var j = watchdog_json(db, watchdog_config())
            return Page(200, j.copy(), j^, "application/json")
        if path == "/datasets":
            return datasets_page(db)
        if path.startswith("/datasets/"):
            var parts = path[byte=10:].split("/")
            var name = url_decode(parts[0])
            if len(parts) == 1 and name:
                return dataset_page(
                    db,
                    name,
                    _query(url, "partitions"),
                    _query(url, "preview") == "1",
                )
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
                return pipeline(
                    db,
                    name,
                    _query(url, "partitions"),
                    _query(url, "rerun") == "1",
                    _query(url, "preview") == "1",
                    _query(url, "cal"),
                    _query(url, "allow_append") == "1",
                )
            if len(parts) == 2 and parts[1] == "calendar" and name:
                return calendar_page(db, name, _query(url, "month"))
            if len(parts) == 3 and parts[1] == "keys" and name:
                return key_page(db, name, url_decode(parts[2]))
        return not_found(String("nothing at ", path))


def read_page(mut db: State, url: String) raises -> Page:
    """What `GET url` answers, for `orca` commands that print a page's JSON
    over the database (`orca.remote`)."""
    var q = url.find("?")
    var path = String(url[byte=:q]) if q >= 0 else url
    return Ui("")._page(db, url, path)


def act(mut db: State, path: String, form: String) raises -> Optional[Page]:
    """The action `path` with the fields of `form`, as the UI's button does
    it, or None if `path` names none (`orca.remote`). Not audited: the
    command acts as whoever can reach the database."""
    return _act(db, path, form)


def serve_ui(url: String, addr: String) raises:
    """Serves the pages on `addr` until the process is stopped."""
    var cfg = ServerConfig()
    cfg.idle_timeout_ms = 30_000
    cfg.write_timeout_ms = 30_000
    var server = HttpServer.bind(SocketAddr.parse(addr), cfg^)
    print("orca ui on http://", addr, sep="")
    server.serve(Ui(url))
