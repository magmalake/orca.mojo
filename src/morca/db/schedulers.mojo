"""Scheduler instances (docs/overlap.md, `morca.instances`):
`orca.schedulers`, a row per `morca scheduler` process with MORCA_OVERLAP
on, and the lead its advisory lock is."""

from postgres import Connection, Params, Transaction

from morca.db.live import stale_limit


def live_older(
    mut tx: Transaction, floor: Int
) raises -> List[Tuple[Int, String, Int]]:
    """The scheduler instances (docs/overlap.md) still going, seen within
    the stale limit and not ended, that know fewer than `floor` migrations:
    each one's id, host and how many it knows. None where `orca.schedulers`
    doesn't exist yet."""
    var out = List[Tuple[Int, String, Int]]()
    var t = tx.query(
        "SELECT to_regclass('orca.schedulers') IS NOT NULL AS ok"
    ).row(0)
    if not t.bool("ok"):
        return out^
    for r in tx.query(
        (
            "SELECT id, host, known FROM orca.schedulers WHERE ended IS NULL"
            " AND last_seen > now() - make_interval(secs => $1) AND known < $2"
            " ORDER BY id"
        ),
        Params().float64(stale_limit()).int64(Int64(floor)),
    ):
        out.append((Int(r.int64("id")), r.text("host"), Int(r.int64("known"))))
    return out^


def live_cond(stale: Float64) -> String:
    """SQL true for a row of `orca.schedulers` that is live: not ended, and
    seen within `stale` seconds."""
    return String(
        "(ended IS NULL AND last_seen > now() - make_interval(secs => ",
        stale,
        "))",
    )


def join(
    mut conn: Connection, host: String, pid: Int, image: String, known: Int
) raises -> Int:
    """Adds a waiting instance's row; returns its id."""
    return Int(
        conn.query(
            (
                "INSERT INTO orca.schedulers (host, pid, image, known) VALUES"
                " ($1, $2, $3, $4) RETURNING id"
            ),
            Params()
            .text(host)
            .int64(Int64(pid))
            .text(image)
            .int64(Int64(known)),
        )
        .row(0)
        .int64("id")
    )


def beat(mut conn: Connection, id: Int, running: String) raises -> Bool:
    """Marks instance `id` seen, with `running` (run ids, comma-separated)
    its processes' runs; returns whether it was asked to drain."""
    var rows = conn.query(
        (
            "UPDATE orca.schedulers SET last_seen = now(), running ="
            " nullif($2, '') WHERE id = $1 RETURNING"
            " drain_asked IS NOT NULL AS asked"
        ),
        Params().int64(Int64(id)).text(running),
    )
    return len(rows) == 1 and rows.row(0).bool("asked")


def newer(mut conn: Connection, id: Int, stale: Float64) raises -> Int:
    """The newest live instance after `id` that isn't draining, or 0."""
    var rows = conn.query(
        String(
            "SELECT coalesce(max(id), 0) AS n FROM orca.schedulers WHERE id >",
            " $1 AND draining_since IS NULL AND drain_asked IS NULL AND ",
            live_cond(stale),
        ),
        Params().int64(Int64(id)),
    )
    return Int(rows.row(0).int64("n"))


def try_lead(mut conn: Connection, id: Int, lock: Int64) raises -> Bool:
    """Takes the lead, session advisory lock `lock`, for instance `id` if
    no one holds it; records it."""
    var got = conn.query(
        "SELECT pg_try_advisory_lock($1) AS ok",
        Params().int64(lock),
    )
    if not got.row(0).bool("ok"):
        return False
    _ = conn.execute(
        (
            "UPDATE orca.schedulers SET role = 'leading', lead_since = now()"
            " WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    )
    return True


def step_down(mut conn: Connection, id: Int, leading: Bool, lock: Int64) raises:
    """Instance `id` drains: gives up the lead if it held it, and records
    that it drains."""
    if leading:
        _ = conn.query(
            "SELECT pg_advisory_unlock($1) AS ok", Params().int64(lock)
        )
    _ = conn.execute(
        (
            "UPDATE orca.schedulers SET role = 'draining', draining_since ="
            " now() WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    )


def end(mut conn: Connection, id: Int) raises:
    """Instance `id` has ended: gone at once, for the leader's requeue."""
    _ = conn.execute(
        (
            "UPDATE orca.schedulers SET role = 'ended', ended = now(), running"
            " = NULL WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    )


def ask_drain(mut conn: Connection, id: Int, stale: Float64) raises -> Bool:
    """Asks live instance `id` to drain; False if there's no such one."""
    return (
        conn.execute(
            String(
                (
                    "UPDATE orca.schedulers SET drain_asked ="
                    " coalesce(drain_asked,"
                ),
                " now()) WHERE id = $1 AND ",
                live_cond(stale),
            ),
            Params().int64(Int64(id)),
        )
        == 1
    )


@fieldwise_init
struct InstanceRow(Copyable, Movable):
    var id: Int64
    var host: String
    var pid: Int64
    var role: String
    """`gone` for one not ended but not seen within the stale limit."""
    var image: String
    var known: Int64
    var runs: Int64
    var ago: Int64
    """Seconds since it was last seen."""


def instance_rows(
    mut conn: Connection, stale: Float64
) raises -> List[InstanceRow]:
    """The instances not long gone (ended or unseen within the last hour),
    oldest first."""
    var out = List[InstanceRow]()
    for r in conn.query(
        String(
            "SELECT id, host, pid, CASE WHEN ",
            live_cond(stale),
            " THEN role WHEN ended IS NULL THEN 'gone' ELSE role END AS role,",
            " image, known, coalesce(cardinality(string_to_array(running,",
            " ',')), 0) AS n, extract(epoch FROM now() - last_seen)::bigint AS",
            (
                " ago FROM orca.schedulers WHERE coalesce(ended, last_seen) >"
                " now()"
            ),
            " - interval '1 hour' ORDER BY id",
        ),
        Params(),
    ):
        out.append(
            InstanceRow(
                r.int64("id"),
                r.text("host"),
                r.int64("pid"),
                r.text("role"),
                r.text("image"),
                r.int64("known"),
                r.int64("n"),
                r.int64("ago"),
            )
        )
    return out^


def overlap_state(
    mut conn: Connection, floor: Int, stale: Float64
) raises -> Optional[Tuple[List[Tuple[Int, String, Int]], Int]]:
    """The live instances that know fewer than `floor` migrations
    (`live_older`) and how many live ones lead, read in one transaction
    and rolled back; None where `orca.schedulers` doesn't exist yet."""
    var tx = conn.begin()
    var t = tx.query(
        "SELECT to_regclass('orca.schedulers') IS NOT NULL AS ok"
    ).row(0)
    if not t.bool("ok"):
        tx.rollback()
        return None
    var old = live_older(tx, floor)
    var leads = tx.query(
        String(
            "SELECT count(*) AS n FROM orca.schedulers WHERE role = 'leading'",
            " AND ",
            live_cond(stale),
        ),
        Params(),
    ).row(0)
    tx.rollback()
    return (old^, Int(leads.int64("n")))


def newest_leads(mut conn: Connection, stale: Float64) raises -> Bool:
    """Whether the newest live instance leads; True with none live."""
    var r = conn.query(
        String(
            "SELECT coalesce((SELECT role = 'leading' FROM",
            " orca.schedulers WHERE ",
            live_cond(stale),
            " ORDER BY id DESC LIMIT 1), true) AS ok",
        ),
        Params(),
    )
    return r.row(0).bool("ok")
