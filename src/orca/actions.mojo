"""What `orca ui` does to runs: each action is rows in Postgres that the
scheduler, or the run's own process, acts on.

The UI never starts a pipeline's binary. Starting, resuming, rerunning and
backfilling leave a `queued` run, which the scheduler starts on its next
tick as it starts any other, within `max_runs`; cancelling sets
`cancel_requested`, which a running run sees before its next attempt (an
attempt already going finishes). Pausing a pipeline sets
`pipelines.paused`, and the scheduler starts none of its runs until it's
unpaused. So the UI works the same wherever the
scheduler runs, and retries and limits stay in one place.

Each action returns an `Outcome`: the run it made or acted on, or why it
refused, which is something the person can fix (a bad range, a run that's
still going) rather than an error.
"""

from std.os import getenv

from postgres import Params

from orca.day import Day
from orca.key import parse_keys
from orca.month import Month
from orca.name import parse_names
from orca.scheduler import (
    RETRIED_SQL,
    Registered,
    backfill,
    partition_keys,
    registered,
)
from orca.sensors import rebuild
from orca.state import State


@fieldwise_init
struct Outcome(Movable):
    var run: Int
    """The run made or acted on; 0 when refused, or for a backfill."""
    var queued: Int
    """How many runs a backfill queued, or a bulk cancel cancelled."""
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
    mut db: State, pipeline: String, keys: String, rerun: Bool = False
) raises -> Outcome:
    """What `orca backfill` does: queued runs, `keys_per_run` keys each, of
    the keys in `keys` that aren't done and aren't in a live run; for time
    keys, by default a run for each partition in `keys`
    that has none queued, running or succeeded. With `rerun`, done keys are
    queued too."""
    var p = _pipeline(db, pipeline)
    if not p:
        return Outcome.no(_unregistered(pipeline))
    try:
        return Outcome.done(0, backfill(db, p.value(), keys, rerun))
    except e:
        return Outcome.no(String(e))


def resume_run(mut db: State, run: Int, allow_new_code: Bool) raises -> Outcome:
    """Queues a failed, cancelled, deferred or stalled run again, so the
    scheduler resumes it at what's unfinished (a deferred one now rather
    than when it asked). A run whose pipeline's code has changed
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
    if (
        status != "failed"
        and status != "cancelled"
        and status != "deferred"
        and not r.bool("stalled")
    ):
        return Outcome.no(
            String(
                "run ",
                run,
                " is ",
                status,
                (
                    "; only a failed, cancelled, deferred or stalled run can be"
                    " resumed"
                ),
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
            (
                " error = NULL, allow_new_code = $2 WHERE id = $1 AND (status"
                " IN ('failed',"
            ),
            " 'cancelled', 'deferred') OR ",
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
    """Cancels a run: a queued, deferred or failed one at once (it isn't
    started again), a running one before its next attempt."""
    var rows = db.conn.query(
        (
            "UPDATE orca.runs SET cancel_requested = true, status = CASE WHEN"
            " status = 'running' THEN status ELSE 'cancelled' END, ended ="
            " CASE WHEN status = 'running' THEN ended ELSE now() END WHERE id"
            " = $1 AND status IN ('queued', 'running', 'failed', 'deferred')"
            " RETURNING id"
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


def cancel_runs(
    mut db: State, pipeline: String, keys: String
) raises -> Outcome:
    """What `orca cancel` does: cancels, as `cancel_run` does each, the
    pipeline's runs that are queued, deferred, running, or failed and due
    to be retried, over any of `keys` (a run of several keys is cancelled whole),
    or all of them for `@all`. The outcome's `queued` is how many."""
    var rows = db.conn.query(
        (
            "SELECT coalesce(describe->'partitions'->>'kind', 'monthly') AS"
            " kind, coalesce(describe->'partitions'->>'start', '') AS start,"
            " coalesce((describe->>'run_retries')::bigint, 3) AS run_retries"
            " FROM orca.pipelines WHERE name = $1"
        ),
        Params().text(pipeline),
    )
    if len(rows) == 0:
        return Outcome.no(String("no pipeline ", pipeline))
    var kind = rows.row(0).text("kind")
    var start = rows.row(0).text("start")
    var retries = rows.row(0).int64("run_retries")
    var all = keys.strip() == "@all"
    var joined = String()
    if not all:
        try:
            if kind == "keyed":
                for n in parse_names(keys):
                    joined.write("," if joined else "", n)
            elif kind == "daily":
                for k in parse_keys[Day](keys, start):
                    joined.write("," if joined else "", k)
            else:
                for k in parse_keys[Month](keys, start):
                    joined.write("," if joined else "", k)
        except e:
            return Outcome.no(String(e))
    var done = db.conn.query(
        String(
            "UPDATE orca.runs SET cancel_requested = true, status = CASE WHEN",
            " status = 'running' THEN status ELSE 'cancelled' END, ended =",
            " CASE WHEN status = 'running' THEN ended ELSE now() END WHERE",
            " pipeline = $1 AND ($2 OR string_to_array(partitions, ',') &&",
            " string_to_array($3, ',')) AND (status IN ('queued', 'deferred')",
            " OR (status = 'running' AND NOT cancel_requested) OR (status =",
            " 'failed' AND ",
            RETRIED_SQL,
            " AND starts - deferrals <= $4)) RETURNING id",
        ),
        Params().text(pipeline).bool(all).text(joined).int64(retries),
    )
    return Outcome.done(0, len(done))


def pause_pipeline(
    mut db: State, pipeline: String, paused: Bool
) raises -> Outcome:
    """What `orca pause` (or, with `paused` false, `orca unpause`) does:
    while a pipeline is paused the scheduler starts none of its runs. Fires,
    sensors, retries and backfills still queue theirs, which start once
    it's unpaused; runs already going go on."""
    var rows = db.conn.query(
        (
            "UPDATE orca.pipelines SET paused = $2 WHERE name = $1 AND paused"
            " <> $2 RETURNING name"
        ),
        Params().text(pipeline).bool(paused),
    )
    if len(rows) == 1:
        return Outcome.done(0)
    var have = db.conn.query(
        "SELECT 1 FROM orca.pipelines WHERE name = $1", Params().text(pipeline)
    )
    if len(have) == 0:
        return Outcome.no(String("no pipeline ", pipeline))
    return Outcome.no(
        String(
            "pipeline ",
            pipeline,
            " is ",
            "paused" if paused else "not paused",
            " already",
        )
    )


def rebuild_downstream(
    mut db: State, dataset: String, keys: String
) raises -> Outcome:
    """What `orca rebuild` does: plans runs of every pipeline downstream of
    `dataset` over `keys`, which the scheduler queues a level at a time.
    The outcome's `run` is the rebuild's ID."""
    try:
        return Outcome.done(rebuild(db, dataset, keys))
    except e:
        return Outcome.no(String(e))


def _unregistered(pipeline: String) -> String:
    return String(
        "pipeline ",
        pipeline,
        " isn't registered with a binary (orca register), so the scheduler",
        " can't start its runs",
    )
