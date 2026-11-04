"""Which runs are live: queued, deferred, or running with their process
alive.

A running run's process is taken to have died when its heartbeat is older
than the stale limit (`ORCA_STALE_SECONDS`), or, before its first beat, when
the scheduler started it longer ago than the start grace
(`ORCA_START_SECONDS`). A live run holds its partition keys: the
scheduler, sensors, rebuilds and the UI's start queue no second run over a
key a live run has.
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


def dead_sql(stale: String, grace: Float64) -> String:
    """SQL true for a running run whose process died: its heartbeat is
    older than `stale` (seconds, or a parameter such as `$2`), or it has
    none and was started longer ago than `grace` seconds."""
    return String(
        "((heartbeat IS NOT NULL AND heartbeat <= now() -",
        " make_interval(secs => ",
        stale,
        ")) OR (heartbeat IS NULL AND (started IS NULL OR started <= now()",
        " - make_interval(secs => ",
        grace,
        "))))",
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
        dead_sql(String(stale), start_grace(stale)),
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
