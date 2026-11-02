"""What `orca ui` does to runs: each action is rows in Postgres that the
scheduler, or the run's own process, acts on.

The UI never starts a pipeline's binary. Starting, resuming, rerunning and
backfilling leave a `queued` run, which the scheduler starts on its next
tick as it starts any other, within `max_runs`; cancelling sets
`cancel_requested`, which a running run sees before its next attempt (an
attempt already going finishes). So the UI works the same wherever the
scheduler runs, and retries and limits stay in one place.

Each action returns an `Outcome`: the run it made or acted on, or why it
refused, which is something the person can fix (a bad range, a run that's
still going) rather than an error.
"""

from std.os import getenv

from postgres import Params

from orca.scheduler import Registered, backfill, partition_keys, registered
from orca.state import State


@fieldwise_init
struct Outcome(Movable):
    var run: Int
    """The run made or acted on; 0 when refused, or for a backfill."""
    var queued: Int
    """How many runs a backfill queued."""
    var refused: String
    """Why the action wasn't done, or empty."""
    var confirm: Bool
    """Refused only until confirmed: a resume onto new code."""

    @staticmethod
    def done(run: Int, queued: Int = 0) -> Self:
        return Self(run, queued, "", False)

    @staticmethod
    def no(var why: String, confirm: Bool = False) -> Self:
        return Self(0, 0, why^, confirm)


def stale_seconds() -> Float64:
    """`ORCA_STALE_SECONDS` (default 30): how old a running run's heartbeat
    is when its process is taken to have died."""
    try:
        return Float64(getenv("ORCA_STALE_SECONDS", "30"))
    except:
        return 30.0


def stalled_sql() -> String:
    """True for a run that's `running` with no live process: its heartbeat,
    or its start when it has none yet, is older than the stale limit."""
    return String(
        "(status = 'running' AND coalesce(heartbeat, started, created) <",
        " now() - make_interval(secs => ",
        stale_seconds(),
        "))",
    )


def _pipeline(mut db: State, name: String) raises -> Optional[Registered]:
    var ps = registered(db, name)
    if len(ps) == 0:
        return None
    return ps[0].copy()


def start_run(mut db: State, pipeline: String, keys: String) raises -> Outcome:
    """A queued run over the partitions in `keys`, trigger `ui`."""
    var p = _pipeline(db, pipeline)
    if not p:
        return Outcome.no(_unregistered(pipeline))
    var texts: List[String]
    try:
        texts = partition_keys(p.value(), keys)
    except e:
        return Outcome.no(String(e))
    if len(texts) == 0:
        return Outcome.no("no partitions to run")
    var joined = String()
    for i in range(len(texts)):
        joined.write("," if i else "", texts[i])
    var id = (
        db.conn.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash, partitions,"
                " trigger, status) VALUES ($1, $2, $3, 'ui', 'queued')"
                " RETURNING id"
            ),
            Params().text(pipeline).text(p.value().registry_hash).text(joined),
        )
        .row(0)
        .int64("id")
    )
    return Outcome.done(Int(id))


def backfill_runs(
    mut db: State, pipeline: String, keys: String
) raises -> Outcome:
    """What `orca backfill` does: a queued run for each partition in `keys`
    that has none queued, running or succeeded."""
    var p = _pipeline(db, pipeline)
    if not p:
        return Outcome.no(_unregistered(pipeline))
    try:
        return Outcome.done(0, backfill(db, p.value(), keys))
    except e:
        return Outcome.no(String(e))


def resume_run(mut db: State, run: Int, allow_new_code: Bool) raises -> Outcome:
    """Queues a failed, cancelled or stalled run again, so the scheduler
    resumes it at what's unfinished. A run whose pipeline's code has changed
    since is refused until `allow_new_code`, which the scheduler passes on
    as `--allow-new-code`."""
    var rows = db.conn.query(
        String(
            "SELECT r.pipeline, r.status, r.registry_hash, p.registry_hash AS",
            " current,",
            " p.binary_path IS NOT NULL AS has_binary, ",
            stalled_sql(),
            " AS stalled FROM orca.runs r JOIN orca.pipelines p ON p.name =",
            " r.pipeline WHERE r.id = $1",
        ),
        Params().int64(Int64(run)),
    )
    if len(rows) == 0:
        return Outcome.no(String("no run ", run))
    var r = rows.row(0)
    var status = r.text("status")
    if status != "failed" and status != "cancelled" and not r.bool("stalled"):
        return Outcome.no(
            String(
                "run ",
                run,
                " is ",
                status,
                "; only a failed, cancelled or stalled run can be resumed",
            )
        )
    if not r.bool("has_binary"):
        return Outcome.no(_unregistered(r.text("pipeline")))
    if r.text("registry_hash") != r.text("current") and not allow_new_code:
        return Outcome.no(
            String(
                "run ",
                run,
                " ran code ",
                r.text("registry_hash"),
                ", and the pipeline's binary is now ",
                r.text("current"),
                ". Resuming runs the new code on the steps that haven't",
                " succeeded, over the outputs of the old code's that have.",
            ),
            confirm=True,
        )
    var n = db.conn.execute(
        String(
            "UPDATE orca.runs SET status = 'queued', owner = NULL, heartbeat =",
            " NULL, ended = NULL, retry_at = NULL, cancel_requested = false,",
            " allow_new_code = $2 WHERE id = $1 AND (status IN ('failed',",
            " 'cancelled') OR ",
            stalled_sql(),
            ")",
        ),
        Params().int64(Int64(run)).bool(allow_new_code),
    )
    if n != 1:
        return Outcome.no(String("run ", run, " changed; look again"))
    return Outcome.done(run)


def rerun_from(mut db: State, run: Int, step: String) raises -> Outcome:
    """A new queued run over the same partitions, trigger `rerun`, with the
    steps before `step` that succeeded copied from `run` (their outputs, and
    `copied_from` naming the step they came from, whose attempts, reads and
    commits stay with it). Resuming it runs `step` and what follows. `run`
    is left as it was."""
    var rows = db.conn.query(
        (
            "SELECT r.pipeline, r.partitions, r.status, r.registry_hash,"
            " p.registry_hash AS current, (SELECT x.n - 1 FROM"
            " jsonb_array_elements(p.describe->'steps') WITH ORDINALITY AS"
            " x(s, n) WHERE x.s->>'name' = $2) AS ord FROM orca.runs r JOIN"
            " orca.pipelines p ON p.name = r.pipeline WHERE r.id = $1"
        ),
        Params().int64(Int64(run)).text(step),
    )
    if len(rows) == 0:
        return Outcome.no(String("no run ", run))
    var r = rows.row(0)
    var pipeline = r.text("pipeline")
    var status = r.text("status")
    if status == "queued" or status == "running":
        return Outcome.no(
            String("run ", run, " is ", status, "; rerun it once it has ended")
        )
    if r.is_null("ord"):
        return Outcome.no(String("pipeline ", pipeline, " has no step ", step))
    var ord = r.int64("ord")
    if not _pipeline(db, pipeline):
        return Outcome.no(_unregistered(pipeline))
    if ord > 0 and r.text("registry_hash") != r.text("current"):
        return Outcome.no(
            String(
                "run ",
                run,
                " ran code ",
                r.text("registry_hash"),
                ", and the pipeline's binary is now ",
                r.text("current"),
                "; its outputs may not be what the new code reads, so rerun",
                " it from the first step",
            )
        )
    var tx = db.conn.begin()
    var id = (
        tx.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash, partitions,"
                " trigger, status) VALUES ($1, $2, $3, 'rerun', 'queued')"
                " RETURNING id"
            ),
            Params()
            .text(pipeline)
            .text(r.text("current"))
            .text(r.text("partitions")),
        )
        .row(0)
        .int64("id")
    )
    _ = tx.execute(
        (
            "INSERT INTO orca.steps (run, step, partition, ord, status, input,"
            " output, copied_from) SELECT $2, step, partition, ord,"
            " 'succeeded', input, output, coalesce(copied_from, id) FROM"
            " orca.steps WHERE run = $1 AND ord < $3 AND status = 'succeeded'"
        ),
        Params().int64(Int64(run)).int64(id).int64(ord),
    )
    tx.commit()
    return Outcome.done(Int(id))


def cancel_run(mut db: State, run: Int) raises -> Outcome:
    """Cancels a run: a queued or failed one at once (a failed one isn't
    retried), a running one before its next attempt."""
    var rows = db.conn.query(
        (
            "UPDATE orca.runs SET cancel_requested = true, status = CASE WHEN"
            " status = 'running' THEN status ELSE 'cancelled' END, ended ="
            " CASE WHEN status = 'running' THEN ended ELSE now() END WHERE id"
            " = $1 AND status IN ('queued', 'running', 'failed') RETURNING id"
        ),
        Params().int64(Int64(run)),
    )
    if len(rows) == 1:
        return Outcome.done(run)
    var st = db.conn.query(
        "SELECT status FROM orca.runs WHERE id = $1",
        Params().int64(Int64(run)),
    )
    if len(st) == 0:
        return Outcome.no(String("no run ", run))
    return Outcome.no(
        String("run ", run, " is ", st.row(0).text("status"), " already")
    )


def _unregistered(pipeline: String) -> String:
    return String(
        "pipeline ",
        pipeline,
        " isn't registered with a binary (orca register), so the scheduler",
        " can't start its runs",
    )
