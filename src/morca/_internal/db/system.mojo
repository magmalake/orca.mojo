"""The control instance's samples (`morca.system`): `orca.system_minutes`,
a row per minute, and `orca.system_containers`, each container's peak in
it."""

from postgres import Connection, Params


@fieldwise_init
struct Container(Copyable, Movable):
    var name: String
    var mem: Int
    """Bytes, as `docker stats` counts them."""


@fieldwise_init
struct Minute(Copyable, Movable):
    """What the samples of one minute add up to."""

    var minute: Int
    """Unix time of its start."""
    var n: Int
    var last_t: Float64
    var cpu_sum: Float64
    var cpu_max: Float64
    var mem_sum: Float64
    var mem_max: Float64
    var mem_used: Int
    var mem_total: Int
    var disk_max: Float64
    var disk_used: Int
    var disk_total: Int
    var disk_path: String
    var net_in_sum: Float64
    var net_out_sum: Float64
    var names: List[String]
    var peaks: List[Int]
    """Each container's highest memory in the minute."""


comptime _UPSERT = (
    "INSERT INTO orca.system_minutes AS m (minute, n, last_t, cpu_sum,"
    " cpu_max, mem_sum, mem_max, mem_used, mem_total, disk_max, disk_used,"
    " disk_total, disk_path, net_in_sum, net_out_sum) VALUES"
    " (to_timestamp($1), $2::int, to_timestamp($3), $4, $5, $6, $7, $8::int8,"
    " $9::int8, $10, $11::int8, $12::int8, $13, $14, $15) ON CONFLICT"
    " (minute) DO UPDATE SET n = m.n + EXCLUDED.n, last_t = GREATEST(m.last_t,"
    " EXCLUDED.last_t), cpu_sum = m.cpu_sum + EXCLUDED.cpu_sum, cpu_max ="
    " GREATEST(m.cpu_max, EXCLUDED.cpu_max), mem_sum = m.mem_sum +"
    " EXCLUDED.mem_sum, mem_max = GREATEST(m.mem_max, EXCLUDED.mem_max),"
    " mem_used = GREATEST(m.mem_used, EXCLUDED.mem_used), mem_total ="
    " EXCLUDED.mem_total, disk_max = GREATEST(m.disk_max, EXCLUDED.disk_max),"
    " disk_used = EXCLUDED.disk_used, disk_total = EXCLUDED.disk_total,"
    " disk_path = EXCLUDED.disk_path, net_in_sum = m.net_in_sum +"
    " EXCLUDED.net_in_sum, net_out_sum = m.net_out_sum + EXCLUDED.net_out_sum"
)

comptime _UPSERT_CONTAINER = (
    "INSERT INTO orca.system_containers AS c (minute, name, mem) VALUES"
    " (to_timestamp($1), $2, $3::int8) ON CONFLICT (minute, name) DO UPDATE"
    " SET mem = GREATEST(c.mem, EXCLUDED.mem)"
)


def last_kept(mut conn: Connection) raises -> Float64:
    """The Unix time of the newest sample kept, or 0."""
    return (
        conn.query(
            (
                "SELECT coalesce(extract(epoch FROM max(last_t)), 0)::float8 AS"
                " t FROM orca.system_minutes"
            ),
            Params(),
        )
        .row(0)
        .float64("t")
    )


def keep_minute(mut conn: Connection, m: Minute) raises:
    """Adds `m` to its minute's row, and each container's peak to theirs."""
    _ = conn.execute(
        _UPSERT,
        Params()
        .float64(Float64(m.minute))
        .int32(Int32(m.n))
        .float64(m.last_t)
        .float64(m.cpu_sum)
        .float64(m.cpu_max)
        .float64(m.mem_sum)
        .float64(m.mem_max)
        .int64(Int64(m.mem_used))
        .int64(Int64(m.mem_total))
        .float64(m.disk_max)
        .int64(Int64(m.disk_used))
        .int64(Int64(m.disk_total))
        .text(m.disk_path)
        .float64(m.net_in_sum)
        .float64(m.net_out_sum),
    )
    for k in range(len(m.names)):
        _ = conn.execute(
            _UPSERT_CONTAINER,
            Params()
            .float64(Float64(m.minute))
            .text(m.names[k])
            .int64(Int64(m.peaks[k])),
        )


def trim(mut conn: Connection, days: Int) raises:
    """Drops the minutes older than `days`."""
    for t in ["system_minutes", "system_containers"]:
        _ = conn.execute(
            String(
                "DELETE FROM orca.",
                t,
                " WHERE minute < now() - make_interval(days => $1::int)",
            ),
            Params().int32(Int32(days)),
        )


@fieldwise_init
struct NewestMinute(Copyable, Movable):
    var age: Float64
    var cpu: Float64
    var cpu_max: Float64
    var mem: Float64
    var mem_used: Int
    var mem_total: Int
    var disk_max: Float64
    var disk_used: Int
    var disk_total: Int
    var disk_path: String
    var nin: Float64
    var nout: Float64
    var containers: List[Container]
    """By memory, most first."""


def newest_minute(mut conn: Connection) raises -> Optional[NewestMinute]:
    """The newest minute, with its containers; None if there is none."""
    var rows = conn.query(
        (
            "SELECT minute, extract(epoch FROM now() - last_t)::float8 AS age,"
            " cpu_sum / n AS cpu, cpu_max, mem_sum / n AS mem, mem_used,"
            " mem_total, disk_max, disk_used, disk_total, disk_path, net_in_sum"
            " / n AS nin, net_out_sum / n AS nout, extract(epoch FROM"
            " minute)::float8 AS e FROM orca.system_minutes ORDER BY minute"
            " DESC LIMIT 1"
        ),
        Params(),
    )
    if len(rows) == 0:
        return None
    var r = rows.row(0)
    var containers = List[Container]()
    for c in conn.query(
        (
            "SELECT name, mem FROM orca.system_containers WHERE minute ="
            " to_timestamp($1) ORDER BY mem DESC, name"
        ),
        Params().float64(r.float64("e")),
    ):
        containers.append(Container(c.text("name"), Int(c.int64("mem"))))
    return NewestMinute(
        r.float64("age"),
        r.float64("cpu"),
        r.float64("cpu_max"),
        r.float64("mem"),
        Int(r.int64("mem_used")),
        Int(r.int64("mem_total")),
        r.float64("disk_max"),
        Int(r.int64("disk_used")),
        Int(r.int64("disk_total")),
        r.text("disk_path"),
        r.float64("nin"),
        r.float64("nout"),
        containers^,
    )


def now_epoch(mut conn: Connection) raises -> Float64:
    """The database's now, in Unix time."""
    return (
        conn.query("SELECT extract(epoch FROM now())::float8 AS e", Params())
        .row(0)
        .float64("e")
    )


@fieldwise_init
struct Point(Copyable, Movable):
    """The minutes of one bucket of the charts."""

    var i: Int
    """The bucket, counted from the first."""
    var cpu: Float64
    var cpu_max: Float64
    var mem: Float64
    var mem_max: Float64
    var disk: Float64
    var nin: Float64
    var nout: Float64


def _bucket_sql(bucket: Int, first: Int) -> String:
    return String(
        "floor(extract(epoch FROM minute) / ", bucket, ")::int8 - ", first
    )


def points(mut conn: Connection, bucket: Int, first: Int) raises -> List[Point]:
    """The minutes from bucket `first` on (of `bucket` seconds each), a
    point per bucket that has any."""
    var out = List[Point]()
    for r in conn.query(
        String(
            "SELECT ",
            _bucket_sql(bucket, first),
            (
                " AS i, sum(cpu_sum) / sum(n) AS cpu, max(cpu_max) AS cpu_max,"
                " sum(mem_sum) / sum(n) AS mem, max(mem_max) AS mem_max,"
                " max(disk_max) AS disk, sum(net_in_sum) / sum(n) AS nin,"
                " sum(net_out_sum) / sum(n) AS nout FROM orca.system_minutes"
                " WHERE minute >= to_timestamp("
            ),
            first * bucket,
            ") GROUP BY 1",
        ),
        Params(),
    ):
        out.append(
            Point(
                Int(r.int64("i")),
                r.float64("cpu"),
                r.float64("cpu_max"),
                r.float64("mem"),
                r.float64("mem_max"),
                r.float64("disk"),
                r.float64("nin"),
                r.float64("nout"),
            )
        )
    return out^


def container_points(
    mut conn: Connection, bucket: Int, first: Int
) raises -> List[Tuple[String, Int, Int]]:
    """Each container's peak memory per bucket from `first` on, by name and
    bucket: its name, the bucket and the bytes."""
    var out = List[Tuple[String, Int, Int]]()
    for r in conn.query(
        String(
            "SELECT name, ",
            _bucket_sql(bucket, first),
            (
                " AS i, max(mem) AS mem FROM orca.system_containers WHERE"
                " minute >= to_timestamp("
            ),
            first * bucket,
            ") GROUP BY name, 2 ORDER BY name, 2",
        ),
        Params(),
    ):
        out.append((r.text("name"), Int(r.int64("i")), Int(r.int64("mem"))))
    return out^
