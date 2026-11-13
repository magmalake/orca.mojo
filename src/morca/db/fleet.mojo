"""The fleet's queries (`morca.fleet`): `orca.fleet_samples` and
`orca.fleet_hourly`, which the scheduler fills, and what the fleet page
reads of them and of `orca.instances` and `orca.runs`."""

from postgres import Connection, Params

from morca.db.live import dead_sql


comptime _INSERT = (
    "WITH ins AS (INSERT INTO orca.fleet_samples (host, t, cores, cpu,"
    " busiest, mem, disk, net_in, net_out) SELECT $1, to_timestamp($2),"
    " $3::int, $4, $5, $6, $7, $8, $9 WHERE to_timestamp($2) > now() -"
    " make_interval(mins => $12::int) ON CONFLICT DO NOTHING RETURNING t)"
    " INSERT INTO orca.fleet_hourly (hour, role, seconds, core_seconds,"
    " busy_seconds, net_in_bytes, net_out_bytes) SELECT date_trunc('hour', t),"
    " $10, $11::float8, $3::float8 * $11::float8, $3::float8 * $11::float8 *"
    " $4 / 100, $8 * $11::float8, $9 * $11::float8 FROM ins ON CONFLICT"
    " (hour, role) DO UPDATE SET seconds = orca.fleet_hourly.seconds +"
    " EXCLUDED.seconds, core_seconds = orca.fleet_hourly.core_seconds +"
    " EXCLUDED.core_seconds, busy_seconds = orca.fleet_hourly.busy_seconds +"
    " EXCLUDED.busy_seconds, net_in_bytes = orca.fleet_hourly.net_in_bytes +"
    " EXCLUDED.net_in_bytes, net_out_bytes = orca.fleet_hourly.net_out_bytes"
    " + EXCLUDED.net_out_bytes"
)


@fieldwise_init
struct SampledInstance(Copyable, Movable):
    """An instance whose samples `ingest` reads: live, or just ended."""

    var id: String
    var run: Int
    var role: String
    var pipeline: String


@fieldwise_init
struct SparkPoint(Copyable, Movable):
    """A host's sample in the spark window."""

    var host: String
    var ago: Float64
    var cpu: Float64


@fieldwise_init
struct LastSample(Copyable, Movable):
    """A host's newest sample."""

    var cpu: Float64
    var busiest: Float64
    var mem: Float64
    var disk: Float64
    var net_in: Float64
    var net_out: Float64
    var ago: Float64
    """Seconds since it."""


@fieldwise_init
struct LiveInstance(Copyable, Movable):
    """A live instance with its newest sample, if it has one."""

    var id: String
    var role: String
    var instance_type: String
    var run: Int
    var up: Float64
    var draining: Bool
    var idle: Bool
    var idle_for: Float64
    var sample: Optional[LastSample]


@fieldwise_init
struct RoleTypeCount(Copyable, Movable):
    var role: String
    var instance_type: String
    var n: Int


@fieldwise_init
struct HourCounts(Copyable, Movable):
    """Runs started, failed attempts and failed runs in the last hour."""

    var runs: Int
    var attempts: Int
    var failed: Int


@fieldwise_init
struct ChartHour(Copyable, Movable):
    """One hour of the fleet's charts."""

    var label: String
    """`MM-DD HH:00` (UTC)."""
    var heads: Float64
    var workers: Float64
    var control: Float64
    var self_hosted: Float64
    var started: Float64
    var finished: Float64
    var failed: Float64
    """Runs that failed (`attempts` holds the failed attempts)."""
    var attempts: Float64
    var busy: Float64
    var capacity: Float64
    var nin: Float64
    """Bytes a second."""
    var nout: Float64


def sample_count(mut conn: Connection, host: String) raises -> Int:
    """The samples kept of `host`."""
    return Int(
        conn.query(
            "SELECT count(*) AS n FROM orca.fleet_samples WHERE host = $1",
            Params().text(host),
        )
        .row(0)
        .int64("n")
    )


def keep_sample(
    mut conn: Connection,
    host: String,
    role: String,
    t: Float64,
    dt: Float64,
    cores: Float64,
    cpu: Float64,
    busiest: Float64,
    mem: Float64,
    disk: Float64,
    net_in: Float64,
    net_out: Float64,
    keep_minutes: Int,
) raises:
    """Stores a sample of `host` unless it has it or it is older than
    `keep_minutes`, and adds it to its hour's totals for `role`."""
    _ = conn.execute(
        _INSERT,
        Params()
        .text(host)
        .float64(t)
        .float64(cores)
        .float64(cpu)
        .float64(busiest)
        .float64(mem)
        .float64(disk)
        .float64(net_in)
        .float64(net_out)
        .text(role)
        .float64(dt)
        .int32(Int32(keep_minutes)),
    )


def sampled_instances(mut conn: Connection) raises -> List[SampledInstance]:
    """The instances up, or ended in the last 2 minutes (which uploaded
    their last lines as they did)."""
    var out = List[SampledInstance]()
    for row in conn.query(
        (
            "SELECT i.id, i.run, i.start, i.role, r.pipeline FROM"
            " orca.instances i JOIN orca.runs r ON r.id = i.run WHERE"
            " i.ended IS NULL OR i.ended > now() - interval '2 minutes'"
        ),
        Params(),
    ):
        out.append(
            SampledInstance(
                row.text("id"),
                Int(row.int64("run")),
                row.text("role"),
                row.text("pipeline"),
            )
        )
    return out^


def prune(mut conn: Connection, sample_minutes: Int, hourly_days: Int) raises:
    """Drops samples older than `sample_minutes` and hours older than
    `hourly_days`."""
    _ = conn.execute(
        (
            "DELETE FROM orca.fleet_samples WHERE t < now() -"
            " make_interval(mins => $1::int)"
        ),
        Params().int32(Int32(sample_minutes)),
    )
    _ = conn.execute(
        (
            "DELETE FROM orca.fleet_hourly WHERE hour < now() -"
            " make_interval(days => $1::int)"
        ),
        Params().int32(Int32(hourly_days)),
    )


def spark_points(mut conn: Connection, seconds: Int) raises -> List[SparkPoint]:
    """Every host's samples of the last `seconds`, by host, oldest first."""
    var out = List[SparkPoint]()
    for r in conn.query(
        (
            "SELECT host, extract(epoch FROM now() - t)::float8 AS ago, cpu"
            " FROM orca.fleet_samples WHERE t > now() - make_interval(secs =>"
            " $1::int) ORDER BY host, t"
        ),
        Params().int32(Int32(seconds)),
    ):
        out.append(
            SparkPoint(r.text("host"), r.float64("ago"), r.float64("cpu"))
        )
    return out^


def live_instances(mut conn: Connection) raises -> List[LiveInstance]:
    """The instances up, in launch order, each with its newest sample."""
    var out = List[LiveInstance]()
    var sql = (
        "SELECT i.id, i.role, i.instance_type, i.run, extract(epoch FROM now()"
        " - i.launched)::float8 AS up, i.drained IS NOT NULL AS draining,"
        " i.idle IS NOT NULL AS idle, coalesce(extract(epoch FROM now() -"
        " i.idle), 0)::float8 AS idle_for,"
        " s.cpu, s.busiest, s.mem, s.disk, s.net_in, s.net_out, s.ago FROM"
        " orca.instances i LEFT JOIN LATERAL (SELECT cpu, busiest, mem, disk,"
        " net_in, net_out, extract(epoch FROM now() - t)::float8 AS ago FROM"
        " orca.fleet_samples WHERE host = i.id ORDER BY t DESC LIMIT 1) s ON"
        " true WHERE i.ended IS NULL ORDER BY i.launched, i.id"
    )
    for r in conn.query(sql, Params()):
        var sample = Optional[LastSample]()
        if not r.is_null("cpu"):
            sample = LastSample(
                r.float64("cpu"),
                r.float64("busiest"),
                r.float64("mem"),
                r.float64("disk"),
                r.float64("net_in"),
                r.float64("net_out"),
                r.float64("ago"),
            )
        out.append(
            LiveInstance(
                r.text("id"),
                r.text("role"),
                r.text("instance_type"),
                Int(r.int64("run")),
                r.float64("up"),
                r.bool("draining"),
                r.bool("idle"),
                r.float64("idle_for"),
                sample^,
            )
        )
    return out^


def control_runs(
    mut conn: Connection, stale: Float64, grace: Float64
) raises -> List[Int]:
    """The runs placed on the control instance that are live: running, with
    a process that hasn't gone stale (`dead_sql`)."""
    var runs = List[Int]()
    for r in conn.query(
        String(
            "SELECT id FROM orca.runs WHERE status = 'running' AND placement",
            " = 'control' AND NOT ",
            dead_sql(String(stale), grace),
            " ORDER BY id",
        ),
        Params(),
    ):
        runs.append(Int(r.int64("id")))
    return runs^


def last_sample(
    mut conn: Connection, host: String
) raises -> Optional[LastSample]:
    """`host`'s newest sample, if it has one."""
    var c = conn.query(
        (
            "SELECT cpu, busiest, mem, disk, net_in, net_out,"
            " extract(epoch FROM now() - t)::float8 AS ago FROM"
            " orca.fleet_samples WHERE host = $1 ORDER BY t DESC LIMIT 1"
        ),
        Params().text(host),
    )
    if len(c) == 0:
        return None
    var r = c.row(0)
    return LastSample(
        r.float64("cpu"),
        r.float64("busiest"),
        r.float64("mem"),
        r.float64("disk"),
        r.float64("net_in"),
        r.float64("net_out"),
        r.float64("ago"),
    )


def live_type_counts(mut conn: Connection) raises -> List[RoleTypeCount]:
    """The instances up, counted by role and type, by type."""
    var out = List[RoleTypeCount]()
    for r in conn.query(
        (
            "SELECT role, instance_type, count(*) AS n FROM orca.instances"
            " WHERE ended IS NULL GROUP BY role, instance_type ORDER BY"
            " instance_type"
        ),
        Params(),
    ):
        out.append(
            RoleTypeCount(
                r.text("role"), r.text("instance_type"), Int(r.int64("n"))
            )
        )
    return out^


def sampled_lately(mut conn: Connection, host: String) raises -> Bool:
    """Whether `host` has a sample from the last 5 minutes."""
    return (
        len(
            conn.query(
                (
                    "SELECT 1 AS one FROM orca.fleet_samples WHERE host = $1"
                    " AND t > now() - interval '5 minutes' LIMIT 1"
                ),
                Params().text(host),
            )
        )
        > 0
    )


def running_and_queued(mut conn: Connection) raises -> Tuple[Int, Int]:
    """How many runs are running, and how many queued."""
    var counts = conn.query(
        (
            "SELECT count(*) FILTER (WHERE status = 'running') AS running,"
            " count(*) FILTER (WHERE status = 'queued') AS queued FROM"
            " orca.runs WHERE status IN ('running', 'queued')"
        ),
        Params(),
    ).row(0)
    return (Int(counts.int64("running")), Int(counts.int64("queued")))


def last_hour(mut conn: Connection) raises -> HourCounts:
    var hour = conn.query(
        (
            "SELECT (SELECT count(*) FROM orca.runs WHERE started > now() -"
            " interval '1 hour') AS runs, (SELECT count(*) FROM"
            " orca.attempts WHERE status = 'failed' AND ended > now() -"
            " interval '1 hour') AS attempts, (SELECT count(*) FROM orca.runs"
            " WHERE status = 'failed' AND ended > now() - interval '1 hour')"
            " AS failed"
        ),
        Params(),
    ).row(0)
    return HourCounts(
        Int(hour.int64("runs")),
        Int(hour.int64("attempts")),
        Int(hour.int64("failed")),
    )


def _hours_sql(hours: Int) -> String:
    return String(
        "generate_series(date_trunc('hour', now()) - interval '1 hour' * ",
        hours - 1,
        ", date_trunc('hour', now()), interval '1 hour')",
    )


def hourly_sparks(
    mut conn: Connection, hours: Int
) raises -> List[Tuple[Float64, Float64]]:
    """Per hour over the last `hours`, the current one last: the runs
    started, and the failed attempts plus failed runs."""
    var out = List[Tuple[Float64, Float64]]()
    for r in conn.query(
        String(
            (
                "SELECT (SELECT count(*) FROM orca.runs WHERE started >= h AND"
                " started < h + interval '1 hour') AS runs, (SELECT count(*)"
                " FROM orca.attempts WHERE status = 'failed' AND ended >= h AND"
                " ended < h + interval '1 hour') + (SELECT count(*) FROM"
                " orca.runs WHERE status = 'failed' AND ended >= h AND ended <"
                " h + interval '1 hour') AS errors FROM "
            ),
            _hours_sql(hours),
            " AS h ORDER BY h",
        ),
        Params(),
    ):
        out.append((Float64(r.int64("runs")), Float64(r.int64("errors"))))
    return out^


def chart_hours(mut conn: Connection, hours: Int) raises -> List[ChartHour]:
    """The fleet charts' hours, the last `hours` of them, oldest first."""
    # An hour's totals over the time it has had: less than 3600 s for this one.
    var span = String(
        "least(3600, greatest(extract(epoch FROM now() - hrs.h), 1))"
    )
    var sql = String(
        "WITH hrs AS (SELECT ",
        "h FROM ",
        _hours_sql(hours),
        (
            " AS h), st AS (SELECT date_trunc('hour', started) AS h, count(*)"
            " AS n FROM orca.runs WHERE started >= now() - interval '1 hour' * "
        ),
        hours,
        (
            " GROUP BY 1), en AS (SELECT date_trunc('hour', ended) AS h,"
            " count(*) AS n, count(*) FILTER (WHERE status = 'failed') AS bad"
            " FROM orca.runs WHERE ended >= now() - interval '1 hour' * "
        ),
        hours,
        (
            " GROUP BY 1), fa AS (SELECT date_trunc('hour', ended) AS h,"
            " count(*) AS n FROM orca.attempts WHERE status = 'failed' AND"
            " ended >= now() - interval '1 hour' * "
        ),
        hours,
        (
            " GROUP BY 1), fl AS (SELECT hour AS h, sum(busy_seconds) AS busy,"
            " sum(core_seconds) AS cap, sum(net_in_bytes) AS nin,"
            " sum(net_out_bytes) AS nout, bool_or(role = 'control') AS ctl FROM"
            " orca.fleet_hourly GROUP BY hour) SELECT to_char(hrs.h AT TIME"
            " ZONE 'UTC', 'MM-DD HH24\":00\"') AS label, (SELECT count(*) FROM"
            " orca.instances i WHERE i.role = 'head' AND i.launched < hrs.h +"
            " interval '1 hour' AND coalesce(i.ended, now()) > hrs.h)::float8"
            " AS heads, (SELECT count(*) FROM orca.instances i WHERE i.role ="
            " 'worker' AND i.launched < hrs.h + interval '1 hour' AND"
            " coalesce(i.ended, now()) > hrs.h)::float8 AS workers, CASE WHEN"
            " coalesce(fl.ctl, false) THEN 1.0 ELSE 0.0 END AS control,"
            # A self-hosted worker (`morca worker`, a token per machine) that
            # ran a run that hour, or was seen in it.
            " (SELECT count(*) FROM orca.workers w WHERE (w.last_seen >="
            " hrs.h AND w.last_seen < hrs.h + interval '1 hour') OR EXISTS"
            " (SELECT 1 FROM orca.runs r WHERE r.worker = w.id AND r.started"
            " < hrs.h + interval '1 hour' AND coalesce(r.ended, now()) >"
            " hrs.h))::float8 AS self_hosted,"
            " coalesce(st.n, 0)::float8 AS started, coalesce(en.n, 0)::float8"
            " AS finished, coalesce(en.bad, 0)::float8 AS failed,"
            " coalesce(fa.n, 0)::float8 AS attempts, coalesce(fl.busy,"
            " 0)::float8 /"
        ),
        span,
        " AS busy, coalesce(fl.cap, 0)::float8 / ",
        span,
        " AS capacity, coalesce(fl.nin, 0)::float8 / ",
        span,
        " AS nin, coalesce(fl.nout, 0)::float8 / ",
        span,
        (
            " AS nout FROM hrs LEFT JOIN st USING (h) LEFT JOIN en USING (h)"
            " LEFT JOIN fa USING (h) LEFT JOIN fl USING (h) ORDER BY hrs.h"
        ),
    )
    var out = List[ChartHour]()
    for r in conn.query(sql, Params()):
        out.append(
            ChartHour(
                r.text("label"),
                r.float64("heads"),
                r.float64("workers"),
                r.float64("control"),
                r.float64("self_hosted"),
                r.float64("started"),
                r.float64("finished"),
                r.float64("failed"),
                r.float64("attempts"),
                r.float64("busy"),
                r.float64("capacity"),
                r.float64("nin"),
                r.float64("nout"),
            )
        )
    return out^
