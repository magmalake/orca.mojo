"""The Postgres run store's queries on runs (`morca.state`): a pipeline
binary's registration and versions, its runs' rows in `orca.runs`, their
claims, heartbeats and ends, and the watchdog's look at a running run.

The SQLite file's are `morca.db.sqlite_runs`, a module of their own: the two
dialects share no statement text (module doc there). Steps and attempts are
`morca.db.steps`."""

from postgres import Connection, Params, Transaction

from morca.db.run_rows import Fence, Owned, RunInfo
from morca.migrations import data_version_sql


# --- Pipelines, as a pipeline binary registers itself ---


def register(
    mut conn: Connection,
    pipeline: String,
    registry_hash: String,
    describe: String,
) raises:
    """Records a pipeline, or updates it to this binary's DAG; its version
    is `morca.db.pipelines.set_version`'s."""
    _ = conn.execute(
        (
            "INSERT INTO orca.pipelines (name, registry_hash, describe)"
            " VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE SET"
            " registry_hash = EXCLUDED.registry_hash, describe ="
            " EXCLUDED.describe, last_seen = now()"
        ),
        Params().text(pipeline).text(registry_hash).jsonb(describe),
    )


def scheduled_hash(mut conn: Connection, pipeline: String) raises -> String:
    """The registry hash `morca register` gave `pipeline`, or empty if it
    wasn't registered with a binary."""
    var res = conn.query(
        (
            "SELECT registry_hash FROM orca.pipelines WHERE name = $1 AND"
            " binary_path IS NOT NULL"
        ),
        Params().text(pipeline),
    )
    return res.row(0).text("registry_hash") if len(res) > 0 else String()


def version(
    mut conn: Connection,
    pipeline: String,
    registry_hash: String,
    describe: String,
    image: String,
) raises -> Int:
    """The `orca.pipeline_versions` row of `pipeline`'s code
    `registry_hash`, from the source `describe` names, in `image`;
    recorded the first time it's seen."""
    return Int(
        conn.query(
            (
                String(
                    "INSERT INTO orca.pipeline_versions (pipeline,",
                    " registry_hash, source, commit_sha, dirty, image,",
                    " data_version) SELECT $1, $2,",
                    " coalesce(d->'source'->>'repo', ''),",
                    " coalesce(d->'source'->>'commit', ''),",
                    " coalesce((d->'source'->>'dirty')::boolean, false),",
                    " $4, ",
                    data_version_sql("d"),
                    " FROM (SELECT $3::jsonb AS d) AS x ON CONFLICT",
                    " (pipeline, registry_hash, source, commit_sha, dirty,",
                    " image) DO UPDATE SET last_seen = now(), data_version",
                    " = excluded.data_version RETURNING id",
                )
            ),
            Params()
            .text(pipeline)
            .text(registry_hash)
            .text(describe)
            .text(image),
        )
        .row(0)
        .int64("id")
    )


# --- Runs ---


def ran_version(mut conn: Connection, run: Int, version: Int) raises:
    """Records that `run` is running `version`'s code."""
    _ = conn.execute(
        "UPDATE orca.runs SET version = $2 WHERE id = $1",
        Params().int64(Int64(run)).int64(Int64(version)),
    )


def create(
    mut conn: Connection,
    pipeline: String,
    registry_hash: String,
    partitions: String,
    trigger: String,
    owner: String,
) raises -> Int:
    """A new run, already claimed by `owner`; its id."""
    return Int(
        conn.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash,"
                " partitions, trigger, status, owner, heartbeat, started)"
                " VALUES ($1, $2, $3, $4, 'running', $5, now(), now())"
                " RETURNING id"
            ),
            Params()
            .text(pipeline)
            .text(registry_hash)
            .text(partitions)
            .text(trigger)
            .text(owner),
        )
        .row(0)
        .int64("id")
    )


def info(mut conn: Connection, run: Int) raises -> Optional[RunInfo]:
    """Run `run`'s row, or none if there's no such run."""
    var res = conn.query(
        (
            "SELECT pipeline, registry_hash, partitions, status,"
            " coalesce(owner, '') AS owner, coalesce(error, '') AS error"
            " FROM orca.runs WHERE id = $1"
        ),
        Params().int64(Int64(run)),
    )
    if len(res) == 0:
        return None
    var r = res.row(0)
    return RunInfo(
        run,
        r.text("pipeline"),
        r.text("registry_hash"),
        r.text("partitions"),
        r.text("status"),
        r.text("owner"),
        r.text("error"),
    )


def claim(
    mut conn: Connection, run: Int, owner: String, stale_seconds: Float64
) raises -> Bool:
    """Makes `owner` the run's writer if it's `queued`, `failed`,
    `deferred`, or `running` with a heartbeat older than `stale_seconds`
    or cleared, in one guarded `UPDATE`; whether it did."""
    var res = conn.query(
        (
            "UPDATE orca.runs SET status = 'running', owner = $2, heartbeat"
            " = now(), ended = NULL, error = NULL WHERE id = $1 AND status"
            " IN ('queued', 'running', 'failed', 'deferred') AND (heartbeat"
            " IS NULL OR heartbeat < now() - make_interval(secs => $3))"
            " RETURNING id"
        ),
        Params().int64(Int64(run)).text(owner).float64(stale_seconds),
    )
    return len(res) == 1


def finish(
    mut conn: Connection, run: Int, owner: String, status: String
) raises:
    """Ends the run as `status` and clears its heartbeat, if `owner` holds
    it."""
    _ = conn.execute(
        (
            "UPDATE orca.runs SET status = $2, ended = now(), heartbeat ="
            " NULL WHERE id = $1 AND owner = $3"
        ),
        Params().int64(Int64(run)).text(status).text(owner),
    )


def defer_run(
    mut conn: Connection, run: Int, owner: String, until: Int64
) raises:
    """Ends the run `deferred`, to start again at `until`, and counts the
    deferral, if `owner` holds it."""
    _ = conn.execute(
        (
            "UPDATE orca.runs SET status = 'deferred', ended = now(),"
            " heartbeat = NULL, retry_at = $3, deferrals = deferrals + 1"
            " WHERE id = $1 AND owner = $2"
        ),
        Params().int64(Int64(run)).text(owner).int64(until),
    )


def cancel_requested(mut conn: Connection, run: Int) raises -> Bool:
    """Whether the run was asked to cancel."""
    return (
        conn.query(
            "SELECT cancel_requested FROM orca.runs WHERE id = $1",
            Params().int64(Int64(run)),
        )
        .row(0)
        .bool("cancel_requested")
    )


def latest(
    mut conn: Connection, limit: Int, before: Int, statuses: String
) raises -> List[RunInfo]:
    """The latest `limit` runs, newest first; those under `before` if it
    isn't 0, and with a status of `statuses` (comma-separated) unless it's
    empty."""
    var out = List[RunInfo]()
    var rows = conn.query(
        (
            "SELECT id, pipeline, registry_hash, partitions, status,"
            " coalesce(owner, '') AS owner, coalesce(error, '') AS error"
            " FROM orca.runs WHERE ($2 = 0 OR id < $2) AND ($3 = '' OR"
            " status = ANY(string_to_array($3, ','))) ORDER BY id DESC"
            " LIMIT $1"
        ),
        Params().int64(Int64(limit)).int64(Int64(before)).text(statuses),
    )
    for r in rows:
        out.append(
            RunInfo(
                Int(r.int64("id")),
                r.text("pipeline"),
                r.text("registry_hash"),
                r.text("partitions"),
                r.text("status"),
                r.text("owner"),
                r.text("error"),
            )
        )
    return out^


def token(mut conn: Connection, run: Int) raises -> String:
    """The run's token (`orca.uuidv7()`), empty for none or no run."""
    var rows = conn.query(
        (
            "SELECT coalesce(token::text, '') AS token FROM"
            " orca.runs WHERE id = $1"
        ),
        Params().int64(Int64(run)),
    )
    if len(rows) == 0:
        return String()
    return rows.row(0).text("token")


def fence(
    mut conn: Connection, run: Int, fresh_seconds: Float64
) raises -> Fence:
    """Who holds the run, its status, and whether its heartbeat is younger
    than `fresh_seconds`. Raises if there's no such run."""
    var row = conn.query(
        (
            "SELECT coalesce(owner, '') AS owner, status,"
            " coalesce(heartbeat > now() - make_interval(secs =>"
            " $2), false) AS fresh FROM orca.runs WHERE id = $1"
        ),
        Params().int64(Int64(run)).float64(fresh_seconds),
    ).row(0)
    return Fence(row.text("owner"), row.text("status"), row.bool("fresh"))


def still_owner(mut tx: Transaction, run: Int, owner: String) raises -> Owned:
    """Refreshes the run's heartbeat if `owner` still holds it running, in
    `tx`; whether it did, and whether the run was asked to cancel."""
    var res = tx.query(
        (
            "UPDATE orca.runs SET heartbeat = now() WHERE id = $1 AND owner"
            " = $2 AND status = 'running' RETURNING cancel_requested"
        ),
        Params().int64(Int64(run)).text(owner),
    )
    if len(res) != 1:
        return Owned(False, False)
    return Owned(True, res.row(0).bool("cancel_requested"))


def beat(mut conn: Connection, run: Int, owner: String) raises:
    """One beat of the run's heartbeat, if `owner` holds it running."""
    _ = conn.execute(
        (
            "UPDATE orca.runs SET heartbeat = now() WHERE id = $1 AND"
            " owner = $2 AND status = 'running'"
        ),
        Params().int64(Int64(run)).text(owner),
    )


# --- The watchdog's look at a running run (`morca.state.beat_run`) ---


@fieldwise_init
struct Stop(Copyable, Movable):
    """A reason to stop a run's process now: `why` is "cancel", a cancel
    whose time has come, or "timeout", for `attempt` of `step` and
    `partition` past its step's `secs`."""

    var why: String
    var attempt: Int64
    var step: String
    var partition: String
    var secs: Float64


comptime _WATCH_SQL = (
    "SELECT 'cancel' AS why, 0::bigint AS attempt, '' AS step, '' AS"
    " partition, 0::float8 AS secs FROM orca.runs WHERE id = $1 AND owner ="
    " $2 AND status = 'running' AND cancel_requested AND cancel_at <= now()"
    " UNION ALL SELECT 'timeout', a.id, s.step, s.partition, t.secs FROM"
    " orca.attempts a JOIN orca.steps s ON s.id = a.step JOIN orca.runs r ON"
    " r.id = s.run JOIN (SELECT split_part(x, '=', 1) AS step,"
    " split_part(x, '=', 2)::float8 AS secs FROM unnest(string_to_array($3,"
    " ',')) AS x) AS t ON t.step = s.step WHERE s.run = $1 AND r.owner = $2"
    " AND r.status = 'running' AND a.status = 'running' AND a.started <= now()"
    " - make_interval(secs => t.secs) LIMIT 1"
)


def stop_reason(
    mut conn: Connection, run: Int, owner: String, timeouts: String
) raises -> Optional[Stop]:
    """A reason to stop the run's process now, if there is one: a cancel
    whose time has come, or an attempt past its step's timeout (`timeouts`:
    `step=seconds`, comma-separated)."""
    var rows = conn.query(
        _WATCH_SQL, Params().int64(Int64(run)).text(owner).text(timeouts)
    )
    if len(rows) == 0:
        return None
    var r = rows.row(0)
    return Stop(
        r.text("why"),
        r.int64("attempt"),
        r.text("step"),
        r.text("partition"),
        r.float64("secs"),
    )


def fail(mut tx: Transaction, run: Int, owner: String, why: String) raises:
    """Ends the run `failed` with `why`, in `tx`, if `owner` holds it."""
    _ = tx.execute(
        (
            "UPDATE orca.runs SET status = 'failed', ended = now(),"
            " heartbeat = NULL, error = $3 WHERE id = $1 AND owner = $2"
        ),
        Params().int64(Int64(run)).text(owner).text(why),
    )


def cancel(mut tx: Transaction, run: Int, owner: String) raises:
    """Ends the run `cancelled`, in `tx`, if `owner` holds it."""
    _ = tx.execute(
        (
            "UPDATE orca.runs SET status = 'cancelled', ended = now(),"
            " heartbeat = NULL WHERE id = $1 AND owner = $2"
        ),
        Params().int64(Int64(run)).text(owner),
    )


def audit_stop(mut tx: Transaction, run: Int, detail: String) raises:
    """The `orca.audit` row of the watchdog stopping the run, in `tx`."""
    _ = tx.execute(
        (
            "INSERT INTO orca.audit (username, action, run, detail) VALUES"
            " ('orca', 'stop', $1, $2)"
        ),
        Params().int64(Int64(run)).text(detail),
    )
