"""What `orca ui` does to runs: each action is rows in Postgres that the
scheduler, or the run's own process, acts on.

The UI never starts a pipeline's binary. Starting, resuming, rerunning and
backfilling leave a `queued` run, which the scheduler starts on its next
tick as it starts any other, within `max_runs`; cancelling sets
`cancel_requested`, which a running run sees before its next attempt; an
attempt still going `ORCA_CANCEL_KILL_SECONDS` (60) later is stopped, its
process exiting (`orca.heartbeat`). Pausing a pipeline sets
`pipelines.paused`, and the scheduler starts none of its runs until it's
unpaused. So the UI works the same wherever the
scheduler runs, and retries and limits stay in one place.

Each action returns an `Outcome`: the run it made or acted on, or why it
refused, which is something the person can fix (a bad range, a run that's
still going) rather than an error.
"""

from std.os import getenv

from postgres import Params

from orca.live import live_runs
from orca.name import parse_names
from orca.pause import set_paused
from orca.queueing import (
    ALLOW_APPEND_HOW,
    append_again,
    appending_writes,
    backfill,
    partition_keys,
    rerun_refusal,
)
from orca.registered import (
    Registered,
    registered,
)
from orca.scheduler import RETRIED_SQL
from orca.sensors import rebuild
from orca.state import State
from orca.timekeys import parse_time_keys


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
    var skipped: List[String]
    """Why a backfill left keys, a line per reason (`Queued.skipped`)."""

    @staticmethod
    def done(run: Int, queued: Int = 0) -> Self:
        return Self(run, queued, "", False, List[String]())

    @staticmethod
    def no(var why: String, confirm: Bool = False) -> Self:
        return Self(0, 0, why^, confirm, List[String]())


def stale_seconds() -> Float64:
    """`ORCA_STALE_SECONDS` (default 30): how old a running run's heartbeat
    is when its process is taken to have died."""
    try:
        return Float64(getenv("ORCA_STALE_SECONDS", "30"))
    except:
        return 30.0


def cancel_kill_seconds() -> Float64:
    """`ORCA_CANCEL_KILL_SECONDS` (default 60): how long a running run
    asked to cancel has to finish its step before it's stopped."""
    try:
        return Float64(getenv("ORCA_CANCEL_KILL_SECONDS", "60"))
    except:
        return 60.0


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
    """A queued run over the partitions in `keys`, trigger `ui`. Refused
    while a live run has one of them, which a second run would run beside;
    a partition that's done is run again."""
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
    var tx = db.conn.begin()
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(pipeline),
    )
    var live = live_runs(tx, pipeline)
    for k in texts:
        var run = live.get(k)
        if run:
            tx.commit()
            return Outcome.no(
                String(
                    k,
                    " is in run ",
                    run.value(),
                    ", which is queued or running; wait for it to end, or",
                    " cancel it",
                )
            )
    var id = (
        tx.query(
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
    tx.commit()
    return Outcome.done(Int(id))


def backfill_runs(
    mut db: State,
    pipeline: String,
    keys: String,
    rerun: Bool = False,
    allow_append: Bool = False,
) raises -> Outcome:
    """What `orca backfill` does: queued runs, `keys_per_run` keys each, of
    the keys in `keys` that aren't done and aren't in a live run; for time
    keys, by default a run for each partition in `keys`
    that has none queued, running or succeeded. With `rerun`, done keys are
    queued too: refused until `allow_append` confirms it if the pipeline's
    steps append to what they write (`rerun_refusal`)."""
    var p = _pipeline(db, pipeline)
    if not p:
        return Outcome.no(_unregistered(pipeline))
    try:
        if rerun and not allow_append:
            var why = rerun_refusal(db, p.value(), keys)
            if why:
                return Outcome.no(why^, confirm=True)
        var q = backfill(db, p.value(), keys, rerun, True)
        var o = Outcome.done(0, len(q.runs))
        o.skipped = q.skipped()
        return o^
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


def rerun_from(
    mut db: State, run: Int, step: String, allow_append: Bool = False
) raises -> Outcome:
    """A new queued run over the same partitions, trigger `rerun`, with the
    steps before `step` that succeeded copied from `run` (their outputs, and
    `copied_from` naming the step they came from, whose attempts, reads and
    commits stay with it). Resuming it runs `step` and what follows. `run`
    is left as it was.

    Refused until `allow_append` confirms it if `step` or one after it
    appends to a dataset (`appending_writes`): the new run would append
    again beside what `run` committed."""
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
    if not allow_append:
        var writes = appending_writes(db, pipeline, Int(ord))
        if len(writes) > 0:
            return Outcome.no(
                append_again(
                    pipeline,
                    String("run ", run, " from step ", step),
                    writes,
                    ALLOW_APPEND_HOW,
                ),
                confirm=True,
            )
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


def _cancel_set(kill_seconds: StaticString) -> String:
    """`UPDATE orca.runs SET ...` for cancelling the runs its `WHERE` names:
    one not running is `cancelled` at once, a running one gets
    `cancel_requested` and, at `kill_seconds` (a parameter) from now, its
    `cancel_at`."""
    return String(
        "UPDATE orca.runs SET cancel_requested = true, status = CASE WHEN",
        " status = 'running' THEN status ELSE 'cancelled' END, ended = CASE",
        " WHEN status = 'running' THEN ended ELSE now() END, cancel_at = CASE",
        " WHEN status = 'running' THEN coalesce(cancel_at, now() +",
        " make_interval(secs => ",
        kill_seconds,
        ")) END",
    )


def cancel_run(mut db: State, run: Int) raises -> Outcome:
    """Cancels a run: a queued, deferred or failed one at once (it isn't
    started again), a running one before its next attempt, or, if its step
    is still going `ORCA_CANCEL_KILL_SECONDS` later, by stopping its process
    (`orca.heartbeat`)."""
    var rows = db.conn.query(
        String(
            _cancel_set("$2"),
            " WHERE id = $1 AND status IN ('queued', 'running', 'failed',",
            " 'deferred') RETURNING id",
        ),
        Params().int64(Int64(run)).float64(cancel_kill_seconds()),
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
            else:
                for k in parse_time_keys(kind, keys, start):
                    joined.write("," if joined else "", k)
        except e:
            return Outcome.no(String(e))
    var done = db.conn.query(
        String(
            _cancel_set("$5"),
            " WHERE",
            " pipeline = $1 AND ($2 OR string_to_array(partitions, ',') &&",
            " string_to_array($3, ',')) AND (status IN ('queued', 'deferred')",
            " OR (status = 'running' AND NOT cancel_requested) OR (status =",
            " 'failed' AND ",
            RETRIED_SQL,
            " AND starts - deferrals <= $4)) RETURNING id",
        ),
        Params()
        .text(pipeline)
        .bool(all)
        .text(joined)
        .int64(retries)
        .float64(cancel_kill_seconds()),
    )
    return Outcome.done(0, len(done))


def pause_pipeline(
    mut db: State, pipeline: String, paused: Bool
) raises -> Outcome:
    """What `orca pause` (or, with `paused` false, `orca unpause`) does:
    while a pipeline is paused the scheduler starts none of its runs. Fires,
    sensors, retries and backfills still queue theirs, which start once
    it's unpaused; runs already going go on."""
    var why = set_paused(db, pipeline, paused)
    if why:
        return Outcome.no(why^)
    return Outcome.done(0)


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


# ── in bulk, from a runs list ───────────────────────────────────────────────


@fieldwise_init
struct Bulk(Movable):
    """What an action on runs picked from a list did (`orca.api.actions.bulk`).
    """

    var done: List[Int]
    """The runs it acted on: cancelled (or asked to stop), queued again, or
    whose partitions were backfilled."""
    var skipped: List[Int]
    """The runs it left as they were: ended already, or not failed."""
    var refused: List[Tuple[Int, String]]
    """Runs (or, as 0, a pipeline's) it couldn't act on, and why."""
    var queued: Int
    """The runs a backfill queued."""
    var confirm: String
    """Why a backfill waits to be confirmed (its steps append), or empty;
    nothing was done then."""
    var pipelines: List[Tuple[String, String]]
    """Each pipeline a backfill queued runs of, and its partitions."""

    @staticmethod
    def none() -> Self:
        return Self(
            List[Int](),
            List[Int](),
            List[Tuple[Int, String]](),
            0,
            String(),
            List[Tuple[String, String]](),
        )


def _id_list(runs: List[Int]) -> String:
    """`runs` as `1,2,3`, for `string_to_array($n, ',')::bigint[]`."""
    var ids = String()
    for r in runs:
        ids.write("," if ids else "", r)
    return ids^


def cancel_runs_picked(mut db: State, runs: List[Int]) raises -> Bulk:
    """Cancels, as `cancel_run` does each, those of `runs` that are queued,
    deferred or running (and not already stopping); the others are left as
    they are, and skipped."""
    var b = Bulk.none()
    if len(runs) == 0:
        return b^
    for r in db.conn.query(
        String(
            _cancel_set("$2"),
            " WHERE id = ANY(string_to_array($1, ',')::bigint[]) AND (status",
            " IN ('queued', 'deferred') OR (status = 'running' AND NOT",
            " cancel_requested)) RETURNING id::bigint AS id",
        ),
        Params().text(_id_list(runs)).float64(cancel_kill_seconds()),
    ):
        b.done.append(Int(r.int64("id")))
    for id in runs:
        if id not in b.done and id not in b.skipped:
            b.skipped.append(id)
    return b^


def resume_failed(mut db: State, runs: List[Int]) raises -> Bulk:
    """Queues again, as `resume_run` does each (Retry on a runs list), those
    of `runs` that failed or stalled. One whose pipeline's code has changed
    since is refused, to be resumed from its page; the others are skipped."""
    var b = Bulk.none()
    if len(runs) == 0:
        return b^
    var failed = List[Int]()
    for r in db.conn.query(
        String(
            "SELECT id::bigint AS id FROM orca.runs WHERE id =",
            " ANY(string_to_array($1, ',')::bigint[]) AND (status = 'failed'",
            " OR ",
            stalled_sql(),
            ")",
        ),
        Params().text(_id_list(runs)),
    ):
        failed.append(Int(r.int64("id")))
    for id in runs:
        if id not in failed:
            if id not in b.skipped:
                b.skipped.append(id)
            continue
        var o = resume_run(db, id, False)
        if o.refused:
            b.refused.append((id, o.refused))
        else:
            b.done.append(id)
    return b^


def backfill_picked(
    mut db: State, runs: List[Int], allow_append: Bool
) raises -> Bulk:
    """What `orca backfill --rerun` does, over the partitions of `runs`, a
    backfill per pipeline: a queued run for each of them that isn't in a
    live run. If a pipeline's steps append to what they write, nothing is
    done until `allow_append` confirms it (`confirm` says why)."""
    var b = Bulk.none()
    if len(runs) == 0:
        return b^
    # Each pipeline's partitions, in the order of its runs, each once.
    var names = List[String]()
    var keys = List[List[String]]()
    var of = List[List[Int]]()
    for r in db.conn.query(
        (
            "SELECT id::bigint AS id, pipeline, partitions FROM orca.runs"
            " WHERE id = ANY(string_to_array($1, ',')::bigint[]) ORDER BY id"
        ),
        Params().text(_id_list(runs)),
    ):
        var name = r.text("pipeline")
        var i = -1
        for j in range(len(names)):
            if names[j] == name:
                i = j
        if i < 0:
            names.append(name)
            keys.append(List[String]())
            of.append(List[Int]())
            i = len(names) - 1
        of[i].append(Int(r.int64("id")))
        for k in r.text("partitions").split(","):
            var key = String(k.strip())
            if key and key not in keys[i]:
                keys[i].append(key^)
    for id in runs:
        var seen = False
        for ids in of:
            if id in ids:
                seen = True
        if not seen and id not in b.skipped:
            b.skipped.append(id)
    # Every pipeline checked before any is backfilled, so a confirmation
    # asked for does all of them or none.
    var joined = List[String]()
    for i in range(len(names)):
        joined.append(String(",").join(keys[i]))
        var p = _pipeline(db, names[i])
        if not p:
            continue
        if not allow_append:
            try:
                var why = rerun_refusal(db, p.value(), joined[i])
                if why:
                    b.confirm = why^
                    return b^
            except e:
                pass
    for i in range(len(names)):
        if len(keys[i]) == 0:
            for id in of[i]:
                b.skipped.append(id)
            continue
        var o = backfill_runs(
            db, names[i], joined[i], rerun=True, allow_append=True
        )
        if o.refused:
            b.refused.append((0, String(names[i], ": ", o.refused)))
            continue
        b.queued += o.queued
        b.pipelines.append((names[i], joined[i]))
        for id in of[i]:
            b.done.append(id)
    return b^
