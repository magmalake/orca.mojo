"""Runs as the pages and their JSON read them: `RunRow` with its grant
and what it used, its steps (`StepRow`), the tables they read and wrote
(`Touch`), its machines (`InstanceRow`) and code (`VersionRow`), a step's
attempts (`AttemptRow`), and a runs list's page (`RunsQuery`, `RunsPage`:
keyset by run id, so pages stay put as runs arrive).
"""

from postgres import Params, Row

from orca.actions import stalled_sql
from orca.html import query_param, url_encode
from orca.json import quote
from orca.lineage import SnapshotRef
from orca.logs import location_exists, run_dir
from orca.metrics import InstanceMetrics, StepSpan, read_metrics
from orca.resources import Grant
from orca.state import State
from orca.store import Usage
from orca.views.pipelines import utc_text


def utc_sql(col: StaticString) -> String:
    """SQL for a timestamptz as `YYYY-MM-DDTHH:MM:SSZ`, or `''`."""
    return String(
        "coalesce(to_char(",
        col,
        " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '')",
    )


@fieldwise_init
struct RunResources(Copyable, Movable):
    """What a run's latest start asked for and was granted
    (`orca.resources`); empty and 0 for none."""

    var cpus: String
    var memory_gb: String
    var grant: Grant
    var limit_mb: Int
    """Its latest start's cgroup limit, `memory.high` (`orca.cgroup`); 0
    for none."""
    var max_mb: Int
    """Its `memory.max`, past which the kernel kills its processes."""
    var memory_bytes: Int
    """Its cgroup's memory a few seconds ago while it runs; -1 else."""

    def any(self) -> Bool:
        return self.cpus != "" or self.memory_gb != ""

    def limit_json(self) -> String:
        """`"memory_limit": {...}|null` for a run's JSON."""
        if self.limit_mb <= 0:
            return String('"memory_limit": null')
        return String(
            '"memory_limit": {"limit_mb": ',
            self.limit_mb,
            ', "max_mb": ',
            self.max_mb,
            ', "current_bytes": ',
            String(self.memory_bytes) if self.memory_bytes >= 0 else String(
                "null"
            ),
            "}",
        )

    def json(self) -> String:
        """`"request": {...}|null, "grant": {...}|null` for a run's JSON."""
        if not self.any():
            return String('"request": null, "grant": null')
        return String(
            '"request": {"cpus": ',
            quote(self.cpus) if self.cpus else String("null"),
            ', "memory_gb": ',
            quote(self.memory_gb) if self.memory_gb else String("null"),
            '}, "grant": {"cpus": ',
            String(self.grant.cpus) if self.grant.cpus else String("null"),
            ', "memory_mb": ',
            String(self.grant.memory_mb) if self.grant.memory_mb else String(
                "null"
            ),
            "}",
        )


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
    var usage: Usage
    """What its attempts used (`orca.usage`): the largest peak, the CPU
    seconds summed and the most cores."""
    var resources: RunResources
    """What its latest start asked for and was granted (`orca.resources`)."""


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
    var usage: Usage
    """What the latest attempt used."""
    var limit_mb: Int
    """The latest attempt's memory limit (`orca.cgroup`); 0 for none."""
    var grant_mb: Int
    """Its start's memory grant (`orca.resources`); 0 for none."""


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


def int_or_zero(s: String) -> Int:
    try:
        return Int(s)
    except:
        return 0


comptime _WAITING = (
    "CASE WHEN status = 'deferred' THEN coalesce(retry_at, 0) ELSE 0 END"
)
"""SQL for when a deferred run starts again (`retry_at`), 0 for others."""


def _waiting(t: Int64) -> String:
    return utc_text(t) if t > 0 else String()


comptime _RUN_USAGE = (
    " LEFT JOIN LATERAL (SELECT max(a.peak_rss_bytes) AS peak,"
    " sum(a.cpu_seconds) AS cpu, max(a.peak_cpus) AS cpus FROM orca.attempts"
    " a JOIN orca.steps s ON s.id = a.step WHERE s.run = orca.runs.id) u ON"
    " true"
)
"""SQL joining a run (`orca.runs`) to what its attempts used, as `u`."""


comptime _USAGE_COLUMNS = (
    " coalesce(u.peak, -1) AS peak, coalesce(u.cpu, -1) AS cpu,"
    " coalesce(u.cpus, -1) AS cpus"
)


comptime _GRANT_COLUMNS = (
    " coalesce(request_cpus, '') AS request_cpus, coalesce(request_memory_gb,"
    " '') AS request_memory_gb, coalesce(grant_cpus, 0)::bigint AS"
    " grant_cpus, coalesce(grant_memory_mb, 0)::bigint AS grant_memory_mb,"
    " coalesce(memory_limit_mb, 0)::bigint AS memory_limit_mb,"
    " coalesce(memory_max_mb, 0)::bigint AS memory_max_mb, CASE WHEN status"
    " = 'running' THEN coalesce(memory_bytes, -1) ELSE -1 END::bigint AS"
    " memory_bytes,"
)
"""SQL for a run's request and grant (`orca.resources`)."""


def _resources_of(r: Row) raises -> RunResources:
    """The `_GRANT_COLUMNS`' request and grant."""
    return RunResources(
        r.text("request_cpus"),
        r.text("request_memory_gb"),
        Grant(Int(r.int64("grant_cpus")), Int(r.int64("grant_memory_mb"))),
        Int(r.int64("memory_limit_mb")),
        Int(r.int64("memory_max_mb")),
        Int(r.int64("memory_bytes")),
    )


def usage_columns(r: Row) raises -> Usage:
    """The `peak`, `cpu` and `cpus` columns' usage."""
    return Usage(Int(r.int64("peak")), r.float64("cpu"), r.float64("cpus"))


comptime _SECONDS = (
    "coalesce(extract(epoch FROM coalesce(ended, now()) - started)::bigint, -1)"
)
"""SQL for how long a run ran, up to now if it hasn't ended; -1 before it
started."""


def run_rows(
    mut db: State,
    pipeline: String = "",
    status: String = "",
    trigger: String = "",
    limit: Int = 100,
    before: Int = 0,
    after: Int = 0,
) raises -> List[RunRow]:
    """Runs newest first, those of a pipeline, status or trigger if given,
    and with ids under `before` if it's over 0; with `after` over 0, the
    `limit` runs just above it, oldest first."""
    var rows = db.conn.query(
        String(
            "SELECT id::text AS id, pipeline, partitions, trigger, status,",
            " starts::text AS starts, ",
            utc_sql("created"),
            " AS created, ",
            utc_sql("started"),
            " AS started, ",
            utc_sql("ended"),
            " AS ended, cancel_requested, coalesce(error, '') AS error, ",
            stalled_sql(),
            " AS stalled, ",
            _WAITING,
            " AS waiting, ",
            _SECONDS,
            " AS seconds, coalesce(placement, '') AS placement,",
            " coalesce(held, '') AS held,",
            _GRANT_COLUMNS,
            _USAGE_COLUMNS,
            " FROM orca.runs",
            _RUN_USAGE,
            " WHERE ($1 = '' OR pipeline = $1) AND",
            " ($2 = '' OR status = $2) AND ($3 = '' OR trigger = $3) AND",
            " ($5 = 0 OR id < $5) AND ($6 = 0 OR id > $6) ORDER BY",
            " orca.runs.id ",
            "ASC" if after > 0 else "DESC",
            " LIMIT $4",
        ),
        Params()
        .text(pipeline)
        .text(status)
        .text(trigger)
        .int64(Int64(limit))
        .int64(Int64(before))
        .int64(Int64(after)),
    )
    var out = List[RunRow]()
    for r in rows:
        out.append(
            RunRow(
                int_or_zero(r.text("id")),
                r.text("pipeline"),
                r.text("partitions"),
                r.text("trigger"),
                r.text("status"),
                int_or_zero(r.text("starts")),
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
                usage_columns(r),
                _resources_of(r),
            )
        )
    return out^


def version_rows(
    mut db: State, pipeline: String = "", id: Int = 0
) raises -> List[VersionRow]:
    """A pipeline's versions newest first, or the one with `id`."""
    var rows = db.conn.query(
        String(
            "SELECT v.id, v.registry_hash, v.source, v.commit_sha, v.dirty,",
            " v.image, ",
            utc_sql("v.first_seen"),
            " AS first_seen, ",
            utc_sql("v.last_seen"),
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


def run_version(mut db: State, run: Int) raises -> Optional[VersionRow]:
    """The version a run ran, if it recorded one."""
    var rows = db.conn.query(
        "SELECT coalesce(version, 0) AS v FROM orca.runs WHERE id = $1",
        Params().int64(Int64(run)),
    )
    if len(rows) == 0 or not rows.row(0).int64("v"):
        return None
    var vs = version_rows(db, id=Int(rows.row(0).int64("v")))
    if len(vs) == 0:
        return None
    return vs[0].copy()


def instance_rows(mut db: State, run: Int) raises -> List[InstanceRow]:
    var rows = db.conn.query(
        String(
            "SELECT i.id, i.start, i.role, i.instance_type, i.market, ",
            utc_sql("i.launched"),
            " AS launched, ",
            utc_sql("i.ended"),
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


def cluster_log_name(i: InstanceRow) -> String:
    """The instance's log's name under its run's `cluster/`."""
    return String(i.role, "-", i.id, ".log")


def step_spans(mut db: State, run: Int) raises -> List[StepSpan]:
    """The run's attempts on the wall clock, for the machines' steps lane."""
    var out = List[StepSpan]()
    for r in db.conn.query(
        (
            "SELECT s.step, s.partition, a.status, a.id::text AS id,"
            " extract(epoch FROM a.started)::float8 AS t0, extract(epoch FROM"
            " coalesce(a.ended, now()))::float8 AS t1 FROM orca.attempts a JOIN"
            " orca.steps s ON s.id = a.step WHERE s.run = $1 ORDER BY s.ord,"
            " a.started"
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
                int_or_zero(r.text("id")),
            )
        )
    return out^


def step_rows(mut db: State, run: Int) raises -> List[StepRow]:
    """A run's steps in run order: what `status <run>` shows, with times."""
    var rows = db.conn.query(
        String(
            "SELECT s.step, s.partition, s.status, coalesce(s.error, '') AS",
            " error, (SELECT count(*) FROM orca.attempts a WHERE a.step =",
            " s.id)::text AS attempts, coalesce((SELECT a.log FROM",
            " orca.attempts a WHERE a.step = s.id ORDER BY a.attempt DESC",
            " LIMIT 1), '') AS log, ",
            utc_sql(
                "(SELECT min(a.started) FROM orca.attempts a WHERE a.step ="
                " s.id)"
            ),
            " AS started, ",
            utc_sql(
                "(SELECT max(a.ended) FROM orca.attempts a WHERE a.step = s.id)"
            ),
            " AS ended, coalesce((SELECT a.id FROM orca.attempts a WHERE",
            " a.step = s.id ORDER BY a.attempt DESC LIMIT 1), 0)::text AS",
            " attempt_id, coalesce((SELECT c.run FROM orca.steps c WHERE c.id",
            " = s.copied_from), 0)::text AS from_run, coalesce(u.peak, -1) AS",
            " peak, coalesce(u.cpu, -1) AS cpu, coalesce(u.cpus, -1) AS cpus,",
            " coalesce(u.limit_mb, 0) AS limit_mb, coalesce(u.grant_mb, 0) AS",
            " grant_mb FROM orca.steps s LEFT JOIN LATERAL (SELECT",
            " a.peak_rss_bytes AS peak, a.cpu_seconds AS cpu, a.peak_cpus AS",
            " cpus, a.memory_limit_mb AS limit_mb, a.grant_memory_mb AS",
            " grant_mb FROM orca.attempts a WHERE a.step = s.id ORDER BY",
            " a.attempt DESC",
            " LIMIT 1) u ON true WHERE s.run = $1 ORDER BY s.ord, s.id",
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
                int_or_zero(r.text("attempts")),
                r.text("error"),
                r.text("log"),
                r.text("started"),
                r.text("ended"),
                int_or_zero(r.text("attempt_id")),
                int_or_zero(r.text("from_run")),
                usage_columns(r),
                Int(r.int64("limit_mb")),
                Int(r.int64("grant_mb")),
            )
        )
    return out^


def touch_rows(mut db: State, run: Int) raises -> List[Touch]:
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


comptime RUNS_PAGE = 50
"""Runs to a page of `/runs`, unless `?limit=` (or the older `?n=`) says, at
most `_RUNS_PAGE_MAX`."""


comptime _RUNS_PAGE_MAX = 500


comptime _PIPELINE_RUNS_PAGE = 20
"""Runs to a page of a pipeline's Runs tab."""


comptime HOME_RUNS = 10
"""Recent runs on the home page."""


@fieldwise_init
struct RunsQuery(Copyable, Movable):
    """Which runs a list shows: its filters, how many to a page, and where
    the page is (keyset, by run id: under `before`, or the page just above
    `after`; neither is the newest page)."""

    var view: String
    """`list` (`/runs`), `pipeline` (a pipeline's Runs tab) or `home`."""
    var pipeline: String
    var status: String
    var trigger: String
    var size: Int
    var before: Int
    var after: Int

    def first(self) -> Bool:
        """Whether this is the newest page, which new runs join."""
        return self.before <= 0 and self.after <= 0

    def default_size(self) -> Int:
        if self.view == "pipeline":
            return _PIPELINE_RUNS_PAGE
        if self.view == "home":
            return HOME_RUNS
        return RUNS_PAGE

    def _filters(self, mut q: String):
        if self.view != "pipeline":
            for f in [
                ("pipeline", self.pipeline),
                ("status", self.status),
                ("trigger", self.trigger),
            ]:
                if f[1]:
                    q.write("&" if q else "?", f[0], "=", url_encode(f[1]))

    def href(self, before: Int = 0, after: Int = 0) -> String:
        """This list's page under `before`, above `after`, or (neither) the
        newest, with its filters and page size (unless the default)."""
        var q = String("?tab=runs") if self.view == "pipeline" else String()
        self._filters(q)
        if self.size != self.default_size():
            q.write("&" if q else "?", "limit=", self.size)
        if before > 0:
            q.write("&" if q else "?", "before=", before)
        elif after > 0:
            q.write("&" if q else "?", "after=", after)
        if self.view == "pipeline":
            return String("/pipelines/", url_encode(self.pipeline), q)
        return String("/runs", q)

    def here(self) -> String:
        """The page this list is on: where its "Log in to ..." links come
        back to."""
        if self.view == "home":
            return String("/")
        return self.href(self.before, self.after)

    def fragment(self) -> String:
        """`/runs/table`: this page of the list, as the fragment it
        refreshes from."""
        var q = String("?view=", self.view)
        if self.view == "pipeline":
            q.write("&pipeline=", url_encode(self.pipeline))
        else:
            self._filters(q)
        q.write("&limit=", self.size)
        if self.before > 0:
            q.write("&before=", self.before)
        elif self.after > 0:
            q.write("&after=", self.after)
        return String("/runs/table", q)


def runs_query(url: String, view: String, pipeline: String = "") -> RunsQuery:
    """The list `url` asks for: `view`'s, filtered by `?pipeline=` (or, for a
    pipeline's tab, `pipeline`), `?status=` and `?trigger=`, `?limit=` (or
    `?n=`) to a page, at `?before=` or `?after=`."""
    var v = view if (view == "pipeline" or view == "home") else String("list")
    var q = RunsQuery(
        v,
        pipeline if v == "pipeline" else query_param(url, "pipeline"),
        query_param(url, "status") if v == "list" else String(),
        query_param(url, "trigger") if v == "list" else String(),
        0,
        0,
        0,
    )
    var n = int_or_zero(query_param(url, "limit"))
    if n <= 0:
        n = int_or_zero(query_param(url, "n"))
    q.size = q.default_size() if n <= 0 else min(n, _RUNS_PAGE_MAX)
    if v != "home":
        q.before = max(0, int_or_zero(query_param(url, "before")))
        q.after = 0 if q.before > 0 else max(
            0, int_or_zero(query_param(url, "after"))
        )
    return q^


struct RunsPage(Movable):
    """A page of a runs list, newest first, with how many runs that match
    are newer and older than it."""

    var q: RunsQuery
    """As asked, but with no `after` when the page above it is the newest."""
    var rows: List[RunRow]
    var total: Int
    var newer: Int
    var older: Int

    def __init__(
        out self,
        var q: RunsQuery,
        var rows: List[RunRow],
        total: Int,
        newer: Int,
        older: Int,
    ):
        self.q = q^
        self.rows = rows^
        self.total = total
        self.newer = newer
        self.older = older

    def live(self) -> Bool:
        """Whether a run on it is queued or running."""
        for r in self.rows:
            if live_status(r.status):
                return True
        return False

    def newer_href(self) -> String:
        """The page above this one (the newest, if that's within a page),
        or empty if this is the newest."""
        if self.newer <= 0:
            return String()
        if self.newer <= self.q.size or len(self.rows) == 0:
            return self.q.href()
        return self.q.href(after=self.rows[0].id)

    def older_href(self) -> String:
        """The page under this one, or empty if this is the last."""
        if self.older <= 0 or len(self.rows) == 0:
            return String()
        return self.q.href(before=self.rows[len(self.rows) - 1].id)


def runs_page(mut db: State, var q: RunsQuery) raises -> RunsPage:
    """The page of runs `q` asks for, and the counts around it. Paged by id
    (keyset), so a page stays put while new runs arrive and each query reads
    one range of an index (`orca.runs` by id, or `runs_pipeline_id`)."""
    var rs: List[RunRow]
    if q.after > 0:
        # The `size` runs just above `after`, read upwards; if there aren't
        # more than that, the page is the newest one.
        rs = run_rows(
            db, q.pipeline, q.status, q.trigger, limit=q.size + 1, after=q.after
        )
        if len(rs) > q.size:
            _ = rs.pop()
            rs.reverse()
        else:
            q.after = 0
            rs = run_rows(db, q.pipeline, q.status, q.trigger, limit=q.size)
    else:
        rs = run_rows(
            db, q.pipeline, q.status, q.trigger, limit=q.size, before=q.before
        )
    if q.view == "home":
        return RunsPage(q^, rs^, len(rs), 0, 0)
    # How many match, and how many of them are above this page.
    var top = rs[0].id + 1 if len(rs) > 0 else q.before
    var counts = db.conn.query(
        String(
            "SELECT count(*) AS total, count(*) FILTER (WHERE $4 > 0 AND id",
            " >= $4) AS newer FROM orca.runs WHERE ($1 = '' OR pipeline = $1)",
            " AND ($2 = '' OR status = $2) AND ($3 = '' OR trigger = $3)",
        ),
        Params()
        .text(q.pipeline)
        .text(q.status)
        .text(q.trigger)
        .int64(Int64(top)),
    )
    var total = Int(counts.row(0).int64("total"))
    var newer = Int(counts.row(0).int64("newer"))
    var older = max(0, total - newer - len(rs))
    return RunsPage(q^, rs^, total, newer, older)


def live_status(status: String) -> Bool:
    """Whether a run or attempt with this status can still change."""
    return status == "queued" or status == "running"


def timeline_path(mut db: State, r: RunRow) raises -> String:
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
    var usage: Usage
    var limit_mb: Int
    """Its memory limit (`orca.cgroup`); 0 for none."""
    var grant_mb: Int
    """Its start's memory grant (`orca.resources`); 0 for none."""


def attempt_rows(
    mut db: State, run: Int, cond: String, arg: Int
) raises -> List[AttemptRow]:
    var rows = db.conn.query(
        String(
            "SELECT a.id::text AS id, r.pipeline, s.step, s.partition,",
            " a.attempt::text AS attempt, a.status, a.host, ",
            utc_sql("a.started"),
            " AS started, ",
            utc_sql("a.ended"),
            " AS ended, coalesce(a.error, '') AS error, coalesce(a.log, '')",
            " AS log, coalesce(a.peak_rss_bytes, -1) AS peak,",
            " coalesce(a.cpu_seconds, -1) AS cpu, coalesce(a.peak_cpus, -1)",
            " AS cpus, coalesce(a.memory_limit_mb, 0) AS limit_mb,",
            " coalesce(a.grant_memory_mb, 0) AS grant_mb FROM orca.attempts a",
            " JOIN orca.steps s ON s.id = a.step",
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
                int_or_zero(r.text("id")),
                run,
                r.text("pipeline"),
                r.text("step"),
                r.text("partition"),
                int_or_zero(r.text("attempt")),
                r.text("status"),
                r.text("host"),
                r.text("started"),
                r.text("ended"),
                r.text("error"),
                r.text("log"),
                usage_columns(r),
                Int(r.int64("limit_mb")),
                Int(r.int64("grant_mb")),
            )
        )
    return out^


def runs_one(mut db: State, id: Int) raises -> Optional[RunRow]:
    var rows = db.conn.query(
        String(
            "SELECT id::text AS id, pipeline, partitions, trigger, status,",
            " starts::text AS starts, ",
            utc_sql("created"),
            " AS created, ",
            utc_sql("started"),
            " AS started, ",
            utc_sql("ended"),
            " AS ended, cancel_requested, coalesce(error, '') AS error, ",
            stalled_sql(),
            " AS stalled, ",
            _WAITING,
            " AS waiting, ",
            _SECONDS,
            " AS seconds, coalesce(placement, '') AS placement,",
            " coalesce(held, '') AS held,",
            _GRANT_COLUMNS,
            _USAGE_COLUMNS,
            " FROM orca.runs",
            _RUN_USAGE,
            " WHERE id = $1",
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
        int_or_zero(r.text("starts")),
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
        usage_columns(r),
        _resources_of(r),
    )


@fieldwise_init
struct AuditRow(Copyable, Movable):
    """Who did something to a run, from the UI or the `orca` command
    (`orca.auth`)."""

    var at: String
    var user: String
    var action: String
    var source: String


struct RunView(Movable):
    """A run as its page and its JSON show it, read once: `run_json`
    (`orca.api.runs`) and `run_html` (`orca.ui.run`) both draw this."""

    var run: RunRow
    var registry_hash: String
    """The code it ran (its pipeline's registration then)."""
    var current_code: Bool
    """Whether its pipeline is still registered with that code."""
    var rebuild: Int
    """The rebuild it's part of, or 0."""
    var version: Optional[VersionRow]
    var cpu_millis: Int
    """What its pods asked for, on Kubernetes (0 elsewhere)."""
    var seconds: Int
    """How long it ran, or has run so far."""
    var sensed: List[SnapshotRef]
    """The snapshots that started it, for a sensor's run."""
    var steps: List[StepRow]
    var touches: List[Touch]
    var instances: List[InstanceRow]
    var metrics: List[InstanceMetrics]
    var prev: Int64
    """The pipeline's run before it, or 0."""
    var next: Int64
    """The pipeline's run after it, or 0."""
    var actions: List[AuditRow]

    def __init__(out self, var run: RunRow):
        self.run = run^
        self.registry_hash = String()
        self.current_code = False
        self.rebuild = 0
        self.version = None
        self.cpu_millis = 0
        self.seconds = 0
        self.sensed = List[SnapshotRef]()
        self.steps = List[StepRow]()
        self.touches = List[Touch]()
        self.instances = List[InstanceRow]()
        self.metrics = List[InstanceMetrics]()
        self.prev = 0
        self.next = 0
        self.actions = List[AuditRow]()


def run_view(mut db: State, id: Int) raises -> Optional[RunView]:
    """Run `id` as its page shows it, or None if there's no such run."""
    var found = runs_one(db, id)
    if not found:
        return None
    var v = RunView(found.take())
    ref r = v.run
    v.steps = step_rows(db, id)
    v.touches = touch_rows(db, id)
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
    v.registry_hash = row.text("registry_hash")
    v.current_code = v.registry_hash == current
    v.rebuild = Int(row.int64("rebuild"))
    if row.int64("version"):
        var vs = version_rows(db, id=Int(row.int64("version")))
        if len(vs) > 0:
            v.version = vs[0].copy()
    v.cpu_millis = Int(row.int64("cpu"))
    v.seconds = Int(row.int64("secs"))
    for x in db.conn.query(
        (
            "SELECT dataset, snapshot_id FROM orca.sensed WHERE run = $1"
            " ORDER BY dataset, snapshot_id"
        ),
        Params().int64(Int64(id)),
    ):
        v.sensed.append(SnapshotRef(x.text("dataset"), x.int64("snapshot_id")))
    v.instances = instance_rows(db, id)
    v.metrics = read_metrics(r.pipeline, id)
    # The pipeline's runs in order (all triggers, fills too).
    var nb = db.conn.query(
        (
            "SELECT coalesce((SELECT max(id) FROM orca.runs WHERE pipeline = $1"
            " AND id < $2), 0) AS prev, coalesce((SELECT min(id) FROM"
            " orca.runs WHERE pipeline = $1 AND id > $2), 0) AS next"
        ),
        Params().text(r.pipeline).int64(Int64(r.id)),
    ).row(0)
    v.prev = nb.int64("prev")
    v.next = nb.int64("next")
    for a in db.conn.query(
        (
            "SELECT to_char(at AT TIME ZONE 'UTC',"
            ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS at, username, action,'
            " coalesce(source, '') AS source FROM orca.audit WHERE run = $1"
            " ORDER BY id"
        ),
        Params().int64(Int64(id)),
    ):
        v.actions.append(
            AuditRow(
                a.text("at"),
                a.text("username"),
                a.text("action"),
                a.text("source"),
            )
        )
    return v^
