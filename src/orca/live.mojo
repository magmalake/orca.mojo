"""Which runs are live: queued, deferred, or running with their process
alive.

A running run's process is taken to have died when its heartbeat is older
than the stale limit (`ORCA_STALE_SECONDS`), or, before its first beat, when
the scheduler started it longer ago than the start grace
(`ORCA_START_SECONDS`). A live run holds its partition keys: the
scheduler, sensors, rebuilds and the UI's start queue no second run over a
key a live run has.

**Held by its runner.** A local or control-placed run that `orca runner`
claimed (its `worker` is a row of pool `local`, docs/runner.md) isn't dead
while that runner has been seen within half the stale limit and lists it
among its runs, whatever its heartbeat: the runner kills a process whose
heartbeat went stale and reports it, and only then is the run dead. So a
scheduler that restarts leaves the runner's runs alone, and counts them.
"""

from std.os import getenv

from postgres import Params, Transaction


def stale_limit() raises -> Float64:
    """`ORCA_STALE_SECONDS` (default 30): how old a running run's heartbeat
    is when its process is taken to have died."""
    return Float64(getenv("ORCA_STALE_SECONDS", "30"))


def start_grace(stale: Float64) raises -> Float64:
    """`ORCA_START_SECONDS`: how long a run the scheduler started may go
    without a heartbeat. By default the stale limit for local runs, and 300
    s on Kubernetes or EC2, where a pod or an instance can take minutes to
    start."""
    var runner = getenv("ORCA_RUNNER", "local")
    return Float64(
        getenv(
            "ORCA_START_SECONDS",
            "300" if runner == "kubernetes"
            or runner == "ec2" else String(stale),
        )
    )


comptime RUNNER_POOL = "local"
"""The pool `orca runner` is a worker of (docs/runner.md): reserved, no
remote worker's."""


def held_sql(stale: String, t: String = "orca.runs") -> String:
    """SQL true for a run of table reference `t` (`orca.runs`, or its alias
    in the query) that a live `orca runner` holds: a local or control-placed
    run claimed by a worker of pool `local` that has been seen within half
    of `stale` seconds and lists the run among those it's running."""
    return String(
        "(",
        t,
        ".worker IS NOT NULL AND ",
        t,
        ".placement IN ('local', 'control') AND EXISTS (SELECT 1 FROM",
        " orca.workers hw WHERE hw.id = ",
        t,
        ".worker AND hw.pool = '",
        RUNNER_POOL,
        "' AND hw.last_seen > now() - make_interval(secs => ",
        stale,
        " / 2) AND ",
        t,
        ".id::text = ANY (string_to_array(coalesce(hw.running, ''), ','))))",
    )


def dead_sql(stale: String, grace: Float64, t: String = "orca.runs") -> String:
    """SQL true for a running run whose process died: its heartbeat is
    older than `stale` (seconds, or a parameter such as `$2`), or it has
    none and was started longer ago than `grace` seconds, and no live
    runner holds it (`held_sql`). `t` is how the query names `orca.runs`:
    the table itself, or its alias."""
    return String(
        "(((heartbeat IS NOT NULL AND heartbeat <= now() -",
        " make_interval(secs => ",
        stale,
        ")) OR (heartbeat IS NULL AND (started IS NULL OR started <= now()",
        " - make_interval(secs => ",
        grace,
        ")))) AND NOT ",
        held_sql(stale, t),
        ")",
    )


def live_sql() raises -> String:
    """SQL for the keys of pipeline `$1` in a live run, each with the first
    such run (`key`, `id`): one that's queued, deferred (a step said "not
    yet", and the scheduler starts it again at its `retry_at`), or running
    with its process alive. A run whose process died doesn't hold its keys, whatever its
    trigger, so a backfill or a sensor may queue them again."""
    var stale = stale_limit()
    return String(
        "SELECT DISTINCT ON (k.key) k.key, r.id FROM orca.runs r,",
        " unnest(string_to_array(r.partitions, ',')) AS k(key) WHERE",
        " r.pipeline = $1 AND (r.status IN ('queued', 'deferred') OR",
        " (r.status = 'running'",
        " AND NOT ",
        dead_sql(String(stale), start_grace(stale), "r"),
        ")) ORDER BY k.key, r.id",
    )


def live_runs(
    mut tx: Transaction, pipeline: String
) raises -> Dict[String, Int]:
    """The keys of `pipeline` in a live run, each with the first such run.
    Callers that then queue a run hold the pipeline's row `FOR UPDATE`, as
    `orca.scheduler` does, so two can't both see a key free."""
    var out = Dict[String, Int]()
    for r in tx.query(live_sql(), Params().text(pipeline)):
        out[r.text("key")] = Int(r.int64("id"))
    return out^
