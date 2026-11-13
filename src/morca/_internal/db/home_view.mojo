"""The home page's queries (`morca.views.recent`, `morca.ui.home`,
`morca.ui.viz`): its recent runs a row per batch, its runs per day, and
the pipelines' timeline (`/api/gantt`).
"""

from postgres import Connection, Params

from morca._internal.db.runs_view import utc_sql


comptime FAILED_LATELY = "1 day"
"""How recently a failure ended for its row to come first."""


comptime _CANDIDATES = 1000
"""The newest runs whose batches the home page looks at, beside every live
run's and every recent failure's."""


@fieldwise_init
struct RecentRecord(Copyable, Movable):
    """A row of the home page's recent runs (`morca.views.recent.RecentRow`):
    a batch, or one run."""

    var batch: Int
    var first: Int
    var last: Int
    var runs: Int
    var pipeline: String
    var trigger: String
    var running: Int
    var queued: Int
    var succeeded: Int
    var failed: Int
    var cancelled: Int
    var created: String
    var seconds: Int
    var first_partitions: String
    var last_partitions: String


def recent_records(
    mut conn: Connection, limit: Int
) raises -> List[RecentRecord]:
    """The `limit` rows the home page's Recent runs shows: batches with a
    run running or failed in the last `FAILED_LATELY` first, then those
    with runs waiting, newest first; then the rest, the latest to end or
    start first. A batch counts all its runs; a run with no batch is a row
    of its own."""
    var out = List[RecentRecord]()
    for r in conn.query(
        String(
            "WITH c AS (SELECT coalesce(batch, -id) AS g FROM orca.runs WHERE",
            " status IN ('queued', 'deferred', 'running') UNION SELECT",
            " coalesce(batch, -id) FROM orca.runs WHERE status = 'failed'",
            " AND ended > now() - interval '",
            FAILED_LATELY,
            "' UNION SELECT g FROM (SELECT coalesce(batch, -id) AS g FROM",
            " orca.runs ORDER BY id DESC LIMIT $2) n), g AS (SELECT",
            " coalesce(batch, -id) AS g, coalesce(max(batch), 0) AS batch,",
            " min(id) AS first, max(id) AS last, count(*) AS n, min(pipeline)",
            " AS pipeline, min(trigger) AS trigger, count(*) FILTER (WHERE",
            " status = 'running') AS running, count(*) FILTER (WHERE status",
            " IN ('queued', 'deferred')) AS queued, count(*) FILTER (WHERE",
            " status = 'succeeded') AS succeeded, count(*) FILTER (WHERE",
            " status = 'failed') AS failed, count(*) FILTER (WHERE status =",
            " 'cancelled') AS cancelled, coalesce(bool_or(status = 'failed'",
            " AND ended > now() - interval '",
            FAILED_LATELY,
            "'), false) AS failed_lately, ",
            utc_sql("min(created)"),
            " AS created, coalesce(extract(epoch FROM max(coalesce(ended,",
            " now())) - min(started))::bigint, -1) AS seconds,",
            " max(coalesce(ended, started, created)) AS latest,",
            " (array_agg(partitions ORDER BY id))[1] AS first_parts,",
            " (array_agg(partitions ORDER BY id DESC))[1] AS last_parts FROM",
            " orca.runs WHERE batch IN (SELECT g FROM c WHERE g > 0) OR (batch",
            " IS NULL AND id IN (SELECT -g FROM c WHERE g < 0)) GROUP BY 1),",
            " t AS (SELECT g.*, CASE WHEN running > 0 OR failed_lately THEN 0",
            " WHEN queued > 0 THEN 1 ELSE 2 END AS tier FROM g) SELECT batch,",
            " first, last, n, pipeline, trigger, running, queued, succeeded,",
            " failed, cancelled, created, seconds, first_parts, last_parts",
            " FROM t ORDER BY tier, CASE WHEN tier = 2 THEN latest END DESC,",
            " last DESC LIMIT $1",
        ),
        Params().int64(Int64(limit)).int64(Int64(_CANDIDATES)),
    ):
        out.append(
            RecentRecord(
                Int(r.int64("batch")),
                Int(r.int64("first")),
                Int(r.int64("last")),
                Int(r.int64("n")),
                r.text("pipeline"),
                r.text("trigger"),
                Int(r.int64("running")),
                Int(r.int64("queued")),
                Int(r.int64("succeeded")),
                Int(r.int64("failed")),
                Int(r.int64("cancelled")),
                r.text("created"),
                Int(r.int64("seconds")),
                r.text("first_parts"),
                r.text("last_parts"),
            )
        )
    return out^


@fieldwise_init
struct DayCount(Copyable, Movable):
    """A UTC day's runs (`morca.chart.DayRuns`)."""

    var day: String
    var succeeded: Int
    var failed: Int
    var other: Int


def runs_per_day(mut conn: Connection, days: Int) raises -> List[DayCount]:
    """Runs by the UTC day they started (or were made, if they haven't),
    for the last `days` days up to today."""
    var out = List[DayCount]()
    for r in conn.query(
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
            DayCount(
                r.text("day"),
                Int(r.int64("ok")),
                Int(r.int64("bad")),
                Int(r.int64("other")),
            )
        )
    return out^


# ---------------------------------------------------------------------------
# The timeline (`/api/gantt`)
# ---------------------------------------------------------------------------


@fieldwise_init
struct GanttPipeline(Copyable, Movable):
    """A registered pipeline as the timeline groups it."""

    var name: String
    var paused: Bool
    var sensor: Bool
    var keys: String
    """A keyed pipeline's keys' dataset, or empty."""
    var cron: String


def gantt_pipelines(mut conn: Connection) raises -> List[GanttPipeline]:
    """Every registered pipeline, by name."""
    var out = List[GanttPipeline]()
    for r in conn.query(
        (
            "SELECT name, paused, coalesce((describe->>'sensor')::boolean,"
            " false) AS sensor, coalesce(describe->'partitions'->>'dataset',"
            " '') AS keys, coalesce(describe->'schedule'->>'cron', '') AS cron"
            " FROM orca.pipelines ORDER BY name"
        ),
        Params(),
    ):
        out.append(
            GanttPipeline(
                r.text("name"),
                r.bool("paused"),
                r.bool("sensor"),
                r.text("keys"),
                r.text("cron"),
            )
        )
    return out^


def db_now(mut conn: Connection) raises -> Float64:
    """The database's clock, in seconds since the epoch."""
    return (
        conn.query("SELECT extract(epoch FROM now())::float8 AS now", Params())
        .row(0)
        .float64("now")
    )


comptime _WINDOW_RUNS = (
    "(r.ended >= now() - make_interval(secs => $1) OR (r.ended IS NULL AND"
    " r.status IN ('queued', 'running', 'deferred')))"
)
"""SQL: a run `r` the window shows: one that ended in it, or still going."""


@fieldwise_init
struct GanttRun(Copyable, Movable):
    var id: Int64
    var pipeline: String
    var status: String
    var trigger: String
    var starts: Int64
    var partitions: String
    var start: Float64
    """When it started, or was made if it hasn't."""
    var end: Float64
    """When it ended; -1 while it hasn't."""


def window_runs(
    mut conn: Connection, seconds: Int, limit: Int
) raises -> List[GanttRun]:
    """The newest `limit` runs that ended in the last `seconds` or are
    still going, oldest first."""
    var out = List[GanttRun]()
    for r in conn.query(
        String(
            "SELECT * FROM (SELECT r.id, r.pipeline, r.status, r.trigger,",
            " r.starts, r.partitions, extract(epoch FROM coalesce(r.started,",
            " r.created))::float8 AS t0, coalesce(extract(epoch FROM",
            " r.ended)::float8, -1) AS t1 FROM orca.runs r WHERE ",
            _WINDOW_RUNS,
            " ORDER BY r.id DESC LIMIT $2) x ORDER BY id",
        ),
        Params().int64(Int64(seconds)).int64(Int64(limit)),
    ):
        out.append(
            GanttRun(
                r.int64("id"),
                r.text("pipeline"),
                r.text("status"),
                r.text("trigger"),
                r.int64("starts"),
                r.text("partitions"),
                r.float64("t0"),
                r.float64("t1"),
            )
        )
    return out^


@fieldwise_init
struct GanttTrigger(Copyable, Movable):
    """A run a sensor started over a snapshot another run committed."""

    var up: Int64
    var down: Int64
    var dataset: String
    var snapshot: Int64


def window_triggers(
    mut conn: Connection, seconds: Int, limit: Int
) raises -> List[GanttTrigger]:
    """The runs in the window (`window_runs`') that sensed a snapshot
    another run committed, and that run: at most `limit`."""
    var out = List[GanttTrigger]()
    for r in conn.query(
        String(
            "SELECT DISTINCT s.run AS up, se.run AS down, se.dataset,",
            " se.snapshot_id FROM orca.sensed se JOIN orca.runs r ON r.id =",
            " se.run JOIN orca.commits c ON c.dataset = se.dataset AND",
            " c.snapshot_id = se.snapshot_id JOIN orca.attempts a ON a.id =",
            " c.attempt JOIN orca.steps s ON s.id = a.step WHERE ",
            _WINDOW_RUNS,
            " AND s.run <> se.run ORDER BY se.run, s.run LIMIT $2",
        ),
        Params().int64(Int64(seconds)).int64(Int64(limit)),
    ):
        out.append(
            GanttTrigger(
                r.int64("up"),
                r.int64("down"),
                r.text("dataset"),
                r.int64("snapshot_id"),
            )
        )
    return out^
