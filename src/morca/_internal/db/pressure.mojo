"""Memory pressure's queries (`morca.admission`): `orca.pressure`, each
host's newest reading."""

from postgres import Connection, Params


@fieldwise_init
struct Reading(Copyable, Movable):
    """A host's memory pressure, as `orca.pressure` keeps it."""

    var kind: String
    var path: String
    var has: Bool
    """Whether it has the averages; without, they're kept null."""
    var some10: Float64
    var some60: Float64
    var some300: Float64
    var full10: Float64
    var full60: Float64
    var full300: Float64
    var limit: Float64
    var held: String
    """Why it holds its runs back, or empty."""
    var held_seconds: Float64


def record(mut conn: Connection, host: String, r: Reading) raises:
    """Keeps `host`'s newest reading."""
    _ = conn.execute(
        (
            "INSERT INTO orca.pressure (host, kind, path, some_avg10,"
            " some_avg60, some_avg300, full_avg10, full_avg60, full_avg300,"
            " limit_pct, held, held_since, updated) VALUES ($1, $2, $3,"
            " CASE WHEN $4 THEN $5 END, CASE WHEN $4 THEN $6 END, CASE WHEN"
            " $4 THEN $7 END, CASE WHEN $4 THEN $8 END, CASE WHEN $4 THEN $9"
            " END, CASE WHEN $4 THEN $10 END, $11, nullif($12, ''), CASE WHEN"
            " $12 <> '' THEN now() - make_interval(secs => $13) END, now()) ON"
            " CONFLICT (host) DO UPDATE SET kind = EXCLUDED.kind, path ="
            " EXCLUDED.path, some_avg10 = EXCLUDED.some_avg10, some_avg60 ="
            " EXCLUDED.some_avg60, some_avg300 = EXCLUDED.some_avg300,"
            " full_avg10 = EXCLUDED.full_avg10, full_avg60 ="
            " EXCLUDED.full_avg60, full_avg300 = EXCLUDED.full_avg300,"
            " limit_pct = EXCLUDED.limit_pct, held = EXCLUDED.held,"
            " held_since = EXCLUDED.held_since, updated = now()"
        ),
        Params()
        .text(host)
        .text(r.kind)
        .text(r.path)
        .bool(r.has)
        .float64(r.some10)
        .float64(r.some60)
        .float64(r.some300)
        .float64(r.full10)
        .float64(r.full60)
        .float64(r.full300)
        .float64(r.limit)
        .text(r.held)
        .float64(r.held_seconds),
    )


@fieldwise_init
struct HostReading(Copyable, Movable):
    var host: String
    var reading: Reading
    var age: Int
    """Seconds since it was recorded."""


def readings(mut conn: Connection, host: String) raises -> List[HostReading]:
    """Every host's newest reading, or `host`'s alone (empty for every
    host), the scheduler's first."""
    var out = List[HostReading]()
    for r in conn.query(
        (
            "SELECT host, kind, path, some_avg10 IS NOT NULL AS has,"
            " coalesce(some_avg10, 0) AS s10, coalesce(some_avg60, 0) AS s60,"
            " coalesce(some_avg300, 0) AS s300, coalesce(full_avg10, 0) AS"
            " f10, coalesce(full_avg60, 0) AS f60, coalesce(full_avg300, 0) AS"
            " f300, limit_pct, coalesce(held, '') AS held,"
            " coalesce(extract(epoch FROM now() - held_since), 0)::float8 AS"
            " held_s, extract(epoch FROM now() - updated)::bigint AS age FROM"
            " orca.pressure WHERE $1 = '' OR host = $1 ORDER BY host <>"
            " 'scheduler', host"
        ),
        Params().text(host),
    ):
        out.append(
            HostReading(
                r.text("host"),
                Reading(
                    r.text("kind"),
                    r.text("path"),
                    r.bool("has"),
                    r.float64("s10"),
                    r.float64("s60"),
                    r.float64("s300"),
                    r.float64("f10"),
                    r.float64("f60"),
                    r.float64("f300"),
                    r.float64("limit_pct"),
                    r.text("held"),
                    r.float64("held_s"),
                ),
                Int(r.int64("age")),
            )
        )
    return out^
