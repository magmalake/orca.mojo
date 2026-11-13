"""Scheduler instances: two `morca scheduler`s of different images overlapping
on one Postgres during a deploy (docs/overlap.md).

With `MORCA_OVERLAP=on`, each `morca scheduler` has a row in
`orca.schedulers`, kept fresh on every tick with the runs its processes
are. One instance leads: it holds the session advisory lock `LEAD_LOCK`,
and only it decides (fires, sensors, retries, starts, alerts, sweeps). The
newest live instance takes the lead: an older one that sees a newer one
live, or is asked to (`morca scheduler drain`), gives the lock up and
drains: it starts nothing, keeps supervising the processes it started
until they end, then ends its row and exits. A run an instance started
records it (`runs.instance`), and `morca.live.held_sql` keeps it live while
the instance lists it, so the leader counts it and leaves it alone.

An instance is live while it has been seen within the stale limit and
hasn't ended; "gone" is the opposite, and only then are its runs, once
their heartbeats are stale too, queued again (`Scheduler._requeue_orphans`).
"""

from std.os import getenv
from std.time import sleep

from postgres import Params

from morca.live import stale_limit
from morca.migrations import known_migrations, migration_floor
from morca.state import State, live_older
from morca.store import process_owner


comptime LEAD_LOCK = 0x6F726365  # "orce"
"""The session advisory lock the leading instance holds."""


def overlap_on() raises -> Bool:
    """`MORCA_OVERLAP`: `off` (the default: one scheduler at a time, as
    before) or `on` (an instance row, a lead, and a drain)."""
    var v = getenv("MORCA_OVERLAP", "off")
    if v != "on" and v != "off":
        raise Error("morca: MORCA_OVERLAP is on or off, not ", v)
    return v == "on"


def drain_limit() raises -> Float64:
    """`MORCA_DRAIN_SECONDS`, required of an instance (#390): how long it may
    keep draining its runs once a newer one leads before it kills what is
    left. Raises when it's unset, not a number, or not above 0."""
    var v = getenv("MORCA_DRAIN_SECONDS")
    var how = String(
        ": with MORCA_OVERLAP=on it is how long, in seconds, a scheduler may",
        " keep draining its runs once a newer one leads before it kills them",
        " (docs/overlap.md#the-drain). Set it (in the Hetzner stack,",
        " DRAIN_SECONDS in hetzner.env), or MORCA_OVERLAP=off",
    )
    if not v:
        raise Error("morca: MORCA_DRAIN_SECONDS isn't set", how)
    var n: Float64
    try:
        n = Float64(v)
    except:
        raise Error(
            "morca: MORCA_DRAIN_SECONDS isn't a number of seconds (",
            v,
            ")",
            how,
        )
    if not n > 0:
        raise Error("morca: MORCA_DRAIN_SECONDS isn't above 0 (", v, ")", how)
    return n


def live_cond(stale: Float64) -> String:
    """SQL true for a row of `orca.schedulers` that is live: not ended, and
    seen within `stale` seconds."""
    return String(
        "(ended IS NULL AND last_seen > now() - make_interval(secs => ",
        stale,
        "))",
    )


def join(mut db: State) raises -> Int:
    """Adds this process's row, waiting, with the migrations it knows and
    its image (`MORCA_IMAGE`); returns its id."""
    var me = process_owner()
    var at = me.rfind(":")
    return Int(
        db.conn.query(
            (
                "INSERT INTO orca.schedulers (host, pid, image, known) VALUES"
                " ($1, $2, $3, $4) RETURNING id"
            ),
            Params()
            .text(String(me[byte=:at]))
            .int64(Int64(Int(String(me[byte = at + 1 :]))))
            .text(getenv("MORCA_IMAGE"))
            .int64(Int64(known_migrations())),
        )
        .row(0)
        .int64("id")
    )


def beat(mut db: State, id: Int, running: String) raises -> Bool:
    """Marks instance `id` seen, with `running` (run ids, comma-separated)
    its processes' runs; returns whether it was asked to drain."""
    var rows = db.conn.query(
        (
            "UPDATE orca.schedulers SET last_seen = now(), running ="
            " nullif($2, '') WHERE id = $1 RETURNING"
            " drain_asked IS NOT NULL AS asked"
        ),
        Params().int64(Int64(id)).text(running),
    )
    return len(rows) == 1 and rows.row(0).bool("asked")


def newer(mut db: State, id: Int) raises -> Int:
    """The newest live instance after `id` that isn't draining, or 0."""
    var rows = db.conn.query(
        String(
            "SELECT coalesce(max(id), 0) AS n FROM orca.schedulers WHERE id >",
            " $1 AND draining_since IS NULL AND drain_asked IS NULL AND ",
            live_cond(stale_limit()),
        ),
        Params().int64(Int64(id)),
    )
    return Int(rows.row(0).int64("n"))


def try_lead(mut db: State, id: Int) raises -> Bool:
    """Takes the lead for instance `id` if no one holds it; records it."""
    var got = db.conn.query(
        "SELECT pg_try_advisory_lock($1) AS ok",
        Params().int64(LEAD_LOCK),
    )
    if not got.row(0).bool("ok"):
        return False
    _ = db.conn.execute(
        (
            "UPDATE orca.schedulers SET role = 'leading', lead_since = now()"
            " WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    )
    return True


def step_down(mut db: State, id: Int, leading: Bool) raises:
    """Instance `id` drains: gives up the lead if it held it, and records
    that it drains."""
    if leading:
        _ = db.conn.query(
            "SELECT pg_advisory_unlock($1) AS ok", Params().int64(LEAD_LOCK)
        )
    _ = db.conn.execute(
        (
            "UPDATE orca.schedulers SET role = 'draining', draining_since ="
            " now() WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    )


def end(mut db: State, id: Int) raises:
    """Instance `id` has ended: gone at once, for the leader's requeue."""
    _ = db.conn.execute(
        (
            "UPDATE orca.schedulers SET role = 'ended', ended = now(), running"
            " = NULL WHERE id = $1"
        ),
        Params().int64(Int64(id)),
    )


def ask_drain(mut db: State, id: Int) raises -> Bool:
    """Asks live instance `id` to drain; False if there's no such one."""
    return (
        db.conn.execute(
            String(
                (
                    "UPDATE orca.schedulers SET drain_asked ="
                    " coalesce(drain_asked,"
                ),
                " now()) WHERE id = $1 AND ",
                live_cond(stale_limit()),
            ),
            Params().int64(Int64(id)),
        )
        == 1
    )


def instances(mut db: State) raises -> List[String]:
    """A line for each instance not long gone (ended or unseen within the
    last hour), oldest first: id, host and pid, role, image, migrations
    known, runs going, and when it was last seen."""
    var out = List[String]()
    for r in db.conn.query(
        String(
            "SELECT id, host, pid, CASE WHEN ",
            live_cond(stale_limit()),
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
            String(
                r.int64("id"),
                " ",
                r.text("host"),
                ":",
                r.int64("pid"),
                " ",
                r.text("role"),
                " image=",
                r.text("image") if r.text("image") else String("-"),
                " migrations=",
                r.int64("known"),
                " runs=",
                r.int64("n"),
                " seen=",
                r.int64("ago"),
                "s ago",
            )
        )
    return out^


def overlap_refusal(mut db: State) raises -> String:
    """Why this binary can't start beside the scheduler going now, or
    empty if it can (docs/overlap.md#deploying): a live instance leads, and
    no live instance knows fewer migrations than this binary's floor
    (`morca.state.migration_floor`), which its migrations would raise past
    it. Doesn't migrate."""
    var tx = db.conn.begin()
    var t = tx.query(
        "SELECT to_regclass('orca.schedulers') IS NOT NULL AS ok"
    ).row(0)
    if not t.bool("ok"):
        tx.rollback()
        return String(
            "no scheduler instance has ever run on this database (one before",
            " MORCA_OVERLAP, or with it off, keeps none)",
        )
    var floor = migration_floor(known_migrations())
    var old = live_older(tx, floor)
    var leads = tx.query(
        String(
            "SELECT count(*) AS n FROM orca.schedulers WHERE role = 'leading'",
            " AND ",
            live_cond(stale_limit()),
        ),
        Params(),
    ).row(0)
    tx.rollback()
    if len(old) > 0:
        return String(
            "this binary's migrations need a binary that knows at least ",
            floor,
            ", and scheduler instance ",
            old[0][0],
            " on ",
            old[0][1],
            " knows ",
            old[0][2],
        )
    if leads.int64("n") == 0:
        return String(
            "no live scheduler instance leads (none is going, or it has",
            " MORCA_OVERLAP off)",
        )
    return String()


def wait_for_lead(mut db: State, timeout: Float64) raises -> Bool:
    """Waits, up to `timeout` seconds, for the newest live instance to lead
    (a handover over); True at once with no instance live. A deploy
    registers the new image's pipelines only after this, so the instance
    going away never starts their new code."""
    var waited = 0.0
    while True:
        var r = db.conn.query(
            String(
                "SELECT coalesce((SELECT role = 'leading' FROM",
                " orca.schedulers WHERE ",
                live_cond(stale_limit()),
                " ORDER BY id DESC LIMIT 1), true) AS ok",
            ),
            Params(),
        )
        if r.row(0).bool("ok"):
            return True
        if waited >= timeout:
            return False
        sleep(0.5)
        waited += 0.5
