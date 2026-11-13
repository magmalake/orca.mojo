"""The Postgres run store's queries on a run's steps and attempts
(`morca.state`): `orca.steps`, `orca.attempts`, and what each attempt read
and committed (`orca.reads`, `orca.commits`).

An attempt's start and end, a skip, and an interrupted run's clean-up are
each one transaction, the caller's (`mut tx: Transaction`), which checks
first that its process still owns the run (`morca.db.runs.still_owner`).
The SQLite file's queries are `morca.db.sqlite_runs`."""

from postgres import Connection, Params, Transaction

from morca.db.run_rows import StepInfo, Succeeded, Usage


def fail_running_attempts(
    mut tx: Transaction, run: Int, why: String
) raises -> Int:
    """Fails the run's attempts still `running` with `why`, in `tx`; how
    many there were."""
    return tx.execute(
        (
            "UPDATE orca.attempts a SET status = 'failed', ended = now(),"
            " error = $2 FROM orca.steps s WHERE a.step = s.id AND s.run ="
            " $1 AND a.status = 'running'"
        ),
        Params().int64(Int64(run)).text(why),
    )


def fail_running_steps(mut tx: Transaction, run: Int, why: String) raises:
    """Fails the run's steps still `running` with `why`, in `tx`."""
    _ = tx.execute(
        (
            "UPDATE orca.steps SET status = 'failed', error = $2"
            " WHERE run = $1 AND status = 'running'"
        ),
        Params().int64(Int64(run)).text(why),
    )


def fail_attempt(
    mut tx: Transaction, attempt: Int64, why: String
) raises -> Bool:
    """Fails attempt `attempt` (its `orca.attempts` id) with `why`, and its
    step, in `tx`, if it's still `running`; whether it was."""
    var n = tx.execute(
        (
            "UPDATE orca.attempts SET status = 'failed', ended = now(),"
            " error = $2 WHERE id = $1 AND status = 'running'"
        ),
        Params().int64(attempt).text(why),
    )
    if n != 1:
        return False
    _ = tx.execute(
        (
            "UPDATE orca.steps SET status = 'failed', error = $2 WHERE id ="
            " (SELECT step FROM orca.attempts WHERE id = $1)"
        ),
        Params().int64(attempt).text(why),
    )
    return True


def succeeded(mut conn: Connection, run: Int) raises -> List[Succeeded]:
    """The run's succeeded steps, with their outputs, for resuming it."""
    var out = List[Succeeded]()
    var rows = conn.query(
        (
            "SELECT step, partition, output FROM orca.steps WHERE run = $1"
            " AND status = 'succeeded'"
        ),
        Params().int64(Int64(run)),
    )
    for r in rows:
        out.append(
            Succeeded(r.text("step"), r.text("partition"), r.bytea("output"))
        )
    return out^


def of_run(mut conn: Connection, run: Int) raises -> List[StepInfo]:
    """A run's steps in run order, with their attempt counts and their
    latest attempts' logs and usage."""
    var out = List[StepInfo]()
    var rows = conn.query(
        (
            "SELECT s.step, s.partition, s.status, coalesce(s.error, '') AS"
            " error, (SELECT count(*) FROM orca.attempts a WHERE a.step ="
            " s.id) AS attempts, coalesce(l.log, '') AS log,"
            " coalesce(l.peak_rss_bytes, -1) AS peak,"
            " coalesce(l.cpu_seconds, -1) AS cpu, coalesce(l.peak_cpus,"
            " -1) AS cpus FROM orca.steps s LEFT JOIN LATERAL (SELECT"
            " a.log, a.peak_rss_bytes, a.cpu_seconds, a.peak_cpus FROM"
            " orca.attempts a WHERE a.step = s.id ORDER BY a.attempt DESC"
            " LIMIT 1) l ON true WHERE s.run = $1 ORDER BY s.ord, s.id"
        ),
        Params().int64(Int64(run)),
    )
    for r in rows:
        out.append(
            StepInfo(
                r.text("step"),
                r.text("partition"),
                r.text("status"),
                Int(r.int64("attempts")),
                r.text("error"),
                r.text("log"),
                Usage(
                    Int(r.int64("peak")),
                    r.float64("cpu"),
                    r.float64("cpus"),
                ),
            )
        )
    return out^


def start(
    mut tx: Transaction,
    run: Int,
    step: String,
    partition: String,
    ord: Int,
    input: Span[UInt8, _],
) raises -> Int64:
    """Marks the step `running` with `input`, adding its row if it has
    none, in `tx`; its `orca.steps` id."""
    return (
        tx.query(
            (
                "INSERT INTO orca.steps (run, step, partition, ord, status,"
                " input) VALUES ($1, $2, $3, $4, 'running', $5) ON CONFLICT"
                " (run, step, partition) DO UPDATE SET status = 'running',"
                " input = EXCLUDED.input, error = NULL RETURNING id"
            ),
            Params()
            .int64(Int64(run))
            .text(step)
            .text(partition)
            .int32(Int32(ord))
            .bytea(input),
        )
        .row(0)
        .int64("id")
    )


def next_attempt(mut tx: Transaction, step: Int64) raises -> Int64:
    """The number of step `step`'s next attempt, in `tx`."""
    return (
        tx.query(
            (
                "SELECT coalesce(max(attempt), 0) + 1 AS n FROM"
                " orca.attempts WHERE step = $1"
            ),
            Params().int64(step),
        )
        .row(0)
        .int64("n")
    )


def add_attempt(
    mut tx: Transaction,
    step: Int64,
    attempt: Int64,
    host: String,
    registry_hash: String,
) raises -> Int64:
    """Adds attempt `attempt` of step `step`, `running` on `host`, in `tx`;
    its `orca.attempts` id."""
    return (
        tx.query(
            (
                "INSERT INTO orca.attempts (step, attempt, status, host,"
                " registry_hash) VALUES ($1, $2, 'running', $3, $4)"
                " RETURNING id"
            ),
            Params().int64(step).int64(attempt).text(host).text(registry_hash),
        )
        .row(0)
        .int64("id")
    )


def step_id(
    mut tx: Transaction, run: Int, step: String, partition: String
) raises -> Int64:
    """The `orca.steps` id of the run's `step` for `partition`, in `tx`."""
    return (
        tx.query(
            (
                "SELECT id FROM orca.steps WHERE run = $1 AND step = $2 AND"
                " partition = $3"
            ),
            Params().int64(Int64(run)).text(step).text(partition),
        )
        .row(0)
        .int64("id")
    )


def finish_attempt(
    mut tx: Transaction,
    step: Int64,
    attempt: Int,
    status: String,
    error: String,
) raises:
    """Ends attempt `attempt` of step `step` as `status`, with `error` (null
    when empty), in `tx`."""
    var err = Params().int64(step).int64(Int64(attempt)).text(status)
    err = err^.null() if not error else err^.text(error)
    _ = tx.execute(
        (
            "UPDATE orca.attempts SET status = $3, ended = now(), error ="
            " $4 WHERE step = $1 AND attempt = $2"
        ),
        err,
    )


def finish(
    mut tx: Transaction,
    step: Int64,
    status: String,
    output: Span[UInt8, _],
    keep_output: Bool,
    error: String,
) raises:
    """Settles step `step` as `status`, with `output` if `keep_output` (it
    succeeded) and `error` (null when empty), in `tx`."""
    var p = Params().int64(step).text(status)
    p = p^.bytea(output) if keep_output else p^.null()
    p = p^.null() if not error else p^.text(error)
    _ = tx.execute(
        (
            "UPDATE orca.steps SET status = $2, output = $3, error = $4"
            " WHERE id = $1"
        ),
        p,
    )


def skip(
    mut tx: Transaction,
    run: Int,
    step: String,
    partition: String,
    ord: Int,
    reason: String,
) raises:
    """Marks the step `skipped` for `reason`, adding its row if it has
    none, in `tx`."""
    _ = tx.execute(
        (
            "INSERT INTO orca.steps (run, step, partition, ord, status,"
            " error) VALUES ($1, $2, $3, $4, 'skipped', $5) ON CONFLICT"
            " (run, step, partition) DO UPDATE SET status = 'skipped',"
            " error = EXCLUDED.error, output = NULL"
        ),
        Params()
        .int64(Int64(run))
        .text(step)
        .text(partition)
        .int32(Int32(ord))
        .text(reason),
    )


def set_log(mut conn: Connection, attempt: Int64, path: String) raises:
    """Records attempt `attempt`'s (its `orca.attempts` id) log file."""
    _ = conn.execute(
        "UPDATE orca.attempts SET log = $2 WHERE id = $1",
        Params().int64(attempt).text(path),
    )


def pinned_reads(
    mut conn: Connection, run: Int, step: String, partition: String
) raises -> Dict[String, Int64]:
    """The snapshot each table was read at by the run's `step` for
    `partition`, in its first attempt that read it."""
    var out = Dict[String, Int64]()
    var rows = conn.query(
        (
            "SELECT r.table_name, r.snapshot_id FROM orca.reads r JOIN"
            " orca.attempts a ON a.id = r.attempt JOIN orca.steps s ON s.id"
            " = a.step WHERE s.run = $1 AND s.step = $2 AND s.partition ="
            " $3 ORDER BY a.attempt DESC"
        ),
        Params().int64(Int64(run)).text(step).text(partition),
    )
    # Newest first, so the first attempt's snapshot is the one left.
    for r in rows:
        out[r.text("table_name")] = r.int64("snapshot_id")
    return out^


def record(
    mut conn: Connection,
    commit: Bool,
    attempt: Int64,
    table: String,
    snapshot_id: Int64,
    dataset: String,
) raises:
    """Records that attempt `attempt` committed (or read) `table` at
    `snapshot_id`, of `dataset` (none when empty)."""
    _ = conn.execute(
        String(
            "INSERT INTO orca.",
            "commits" if commit else "reads",
            " (attempt, table_name, snapshot_id, dataset) VALUES",
            " ($1, $2, $3, nullif($4, ''))",
        ),
        Params().int64(attempt).text(table).int64(snapshot_id).text(dataset),
    )
