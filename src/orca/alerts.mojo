"""What the scheduler raises alerts about (`orca.notify`, #184), each decided
where the data is and sent through `orca.notify.alert`:

| Kind | Subject | Priority | When |
|---|---|---|---|
| `watchdog` | the rule | high | a rule tripped (sent from `orca.watchdog.trip`); resolved, low, when no unacknowledged trip of it has a pipeline still paused |
| `run-failed` | run and start | default | a run failed with no retry left (`failed_runs`), once per run |
| `fill-stalled` | the pipeline | high | `fill`: no fill run finished in `ORCA_NOTIFY_FILL_STALL_HOURS` (2) while the pipeline is unpaused, inside its window and not done; resolved when one finishes |
| `disk` | the mount | high | `disk`: the control host's fullest disk is over `ORCA_NOTIFY_DISK_PERCENT` (85); resolved when it isn't |
| `worker-lost` | worker and last seen | default | `lost_workers`: a worker that last said it was running runs, quiet for `ORCA_NOTIFY_WORKER_SECONDS` (300), once per loss |
| `backup-failed` | `nightly` | high | `orca notify --kind backup-failed ...`, from orca-stack's backup |

The text is plain and short: names, numbers and the first line of an error,
never a secret.
"""

from std.os import getenv

from postgres import Params

from orca.fill import FillStatus
from orca.notify import (
    alert,
    configured,
    firing_subjects,
    resolve,
    ui_link,
)
from orca.state import State
from orca.system import system_now
from orca.watchdog import active_trips


def _first_line(text: String, limit: Int = 200) -> String:
    var line = String(text.strip())
    var nl = line.find("\n")
    if nl >= 0:
        var first = String(line[byte=:nl])
        line = first^
    if line.byte_length() > limit:
        var cut = String(line[byte=:limit], "...")
        line = cut^
    return line^


def _hours(name: String, default: Float64) -> Float64:
    var v = getenv(name)
    if not v:
        return default
    try:
        return Float64(v)
    except:
        return default


def _duration(seconds: Int) -> String:
    if seconds < 90:
        return String(seconds, " s")
    if seconds < 5400:
        return String(seconds // 60, " min")
    if seconds < 172800:
        return String(seconds // 3600, " h")
    return String(seconds // 86400, " days")


# ---------------------------------------------------------------------------
# The watchdog
# ---------------------------------------------------------------------------


def watchdog_recovered(mut db: State) raises:
    """Resolves the watchdog alerts whose trips were acknowledged or whose
    pipelines were unpaused."""
    var firing = firing_subjects(db, "watchdog")
    if len(firing) == 0:
        return
    var active = active_trips(db)
    for rule in firing:
        var live = False
        for t in active:
            if t.rule == rule:
                live = True
        if not live:
            resolve(
                db,
                "watchdog",
                rule,
                String("orca watchdog: ", rule, " resolved"),
                "Acknowledged, or its pipelines are unpaused.",
                ui_link("/ops/control"),
            )


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


def failed_runs(
    mut db: State, pipeline: String, run_retries: Int, retried_sql: String
) raises -> Int:
    """Alerts for `pipeline`'s runs that failed in the last hour with no retry
    left: past `run_retries` starts, or of a trigger the scheduler doesn't
    retry (`retried_sql`), or not started for their code. Once per run and
    start. Returns how many were raised."""
    if not configured():
        return 0
    var n = 0
    for r in db.conn.query(
        String(
            "SELECT r.id, r.starts, r.partitions, coalesce(r.error, (SELECT",
            " a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step",
            " WHERE s.run = r.id AND a.error IS NOT NULL ORDER BY a.id DESC",
            " LIMIT 1), '') AS error FROM orca.runs r WHERE r.pipeline = $1",
            " AND r.status = 'failed' AND r.retry_at IS NULL AND r.ended >",
            " now() - interval '1 hour' AND (NOT (",
            retried_sql,
            ") OR r.starts - r.deferrals > $2 OR r.error LIKE 'not started:",
            " its code%') ORDER BY r.id LIMIT 20",
        ),
        Params().text(pipeline).int64(Int64(run_retries)),
    ):
        var id = r.int64("id")
        var subject = String(id, "/", r.int64("starts"))
        if _known(db, "run-failed", subject):
            continue
        var keys = r.text("partitions")
        if keys.byte_length() > 80:
            var cut = String(keys[byte=:80], "...")
            keys = cut^
        var why = _first_line(r.text("error"))
        if alert(
            db,
            "run-failed",
            subject,
            "default",
            "x",
            String("orca: ", pipeline, " run ", id, " failed"),
            String(
                pipeline,
                " ",
                keys,
                "\n",
                why if why else String("failed; see the run"),
            ),
            ui_link(String("/runs/", id)),
        ):
            n += 1
    return n


def _known(mut db: State, kind: String, subject: String) raises -> Bool:
    return (
        len(
            db.conn.query(
                (
                    "SELECT 1 FROM orca.notifications WHERE kind = $1 AND"
                    " subject = $2"
                ),
                Params().text(kind).text(subject),
            )
        )
        > 0
    )


# ---------------------------------------------------------------------------
# Fills
# ---------------------------------------------------------------------------


def fill(
    mut db: State,
    pipeline: String,
    s: FillStatus,
    paused: Bool,
    in_window: Bool,
) raises:
    """Raises 'fill stalled' when no fill run of `pipeline` finished in
    `ORCA_NOTIFY_FILL_STALL_HOURS` (2) although it is unpaused, inside its
    window and not done; resolves it once one finishes."""
    if not configured() or not s.on:
        return
    var limit = Int(_hours("ORCA_NOTIFY_FILL_STALL_HOURS", 2.0) * 3600.0)
    # Since the last fill run that succeeded; else the first fill run, else
    # when the pipeline was first registered.
    var rows = db.conn.query(
        (
            "SELECT extract(epoch FROM now() - coalesce((SELECT max(ended)"
            " FROM orca.runs WHERE pipeline = $1 AND trigger = 'fill' AND"
            " status = 'succeeded'), (SELECT min(created) FROM orca.runs"
            " WHERE pipeline = $1 AND trigger = 'fill'), (SELECT first_seen"
            " FROM orca.pipelines WHERE name = $1)))::bigint AS ago, EXISTS"
            " (SELECT 1 FROM orca.runs WHERE pipeline = $1 AND trigger ="
            " 'fill' AND status = 'succeeded') AS ever"
        ),
        Params().text(pipeline),
    )
    if len(rows) != 1:
        return
    var ago = Int(rows.row(0).int64("ago"))
    var ever = rows.row(0).bool("ever")
    var unfinished = s.done < s.total and (
        s.left + s.retrying + s.live + s.running > 0
    )
    if not unfinished or ago < limit:
        resolve(
            db,
            "fill-stalled",
            pipeline,
            String("orca: fill of ", pipeline, " is moving again"),
            String(s.done, " of ", s.total, " partitions done."),
            ui_link(String("/pipelines/", pipeline)),
        )
        return
    if paused or not in_window:
        return
    _ = alert(
        db,
        "fill-stalled",
        pipeline,
        "high",
        "warning,hourglass",
        String("orca: fill of ", pipeline, " stalled"),
        String(
            s.done,
            " of ",
            s.total,
            " partitions done; ",
            String("last success ", _duration(ago), " ago") if ever else (
                String("no fill run has succeeded in ", _duration(ago))
            ),
            ".",
            String("\n", s.idle) if s.idle else String(),
        ),
        ui_link(String("/pipelines/", pipeline)),
    )


# ---------------------------------------------------------------------------
# The control host
# ---------------------------------------------------------------------------


def disk(mut db: State) raises:
    """Raises 'disk' when the control host's fullest disk, from the /system
    sampler, is over `ORCA_NOTIFY_DISK_PERCENT` (85); resolves it when not."""
    if not configured():
        return
    var now = system_now(db)
    if not now or now.value().stale():
        return
    ref n = now.value()
    var limit = _hours("ORCA_NOTIFY_DISK_PERCENT", 85.0)
    if n.disk_pct > limit:
        var gb = n.disk_total // 1000000000
        _ = alert(
            db,
            "disk",
            n.disk_path,
            "high",
            "warning,floppy_disk",
            "orca: control disk filling",
            String(
                n.disk_path,
                " is ",
                Int(n.disk_pct + 0.5),
                "% full (",
                n.disk_used // 1000000000,
                " of ",
                gb,
                " GB), over ",
                Int(limit + 0.5),
                "%.",
            ),
            ui_link("/ops/control"),
        )
        return
    for mount in firing_subjects(db, "disk"):
        resolve(
            db,
            "disk",
            mount,
            "orca: control disk is fine again",
            String(mount, " is ", Int(n.disk_pct + 0.5), "% full."),
            ui_link("/ops/control"),
        )


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


def lost_workers(mut db: State) raises -> Int:
    """Alerts for the workers (`orca.workers`) that last said they were
    running runs and haven't been heard from for `ORCA_NOTIFY_WORKER_SECONDS`
    (300), once per loss. Their runs are requeued when their heartbeats go
    stale (`orca.scheduler`). Returns how many were raised."""
    if not configured():
        return 0
    var quiet = _hours("ORCA_NOTIFY_WORKER_SECONDS", 300.0)
    var n = 0
    for r in db.conn.query(
        (
            "SELECT id, pool, label, coalesce(name, '') AS name, running,"
            " extract(epoch FROM last_seen)::bigint AS seen,"
            " extract(epoch FROM now() - last_seen)::bigint AS ago FROM"
            " orca.workers WHERE revoked IS NULL AND expires > now() AND"
            " last_seen IS NOT NULL AND last_seen < now() -"
            " make_interval(secs => $1) AND last_seen > now() - interval '1"
            " day' AND running IS NOT NULL ORDER BY id"
        ),
        Params().float64(quiet),
    ):
        var subject = String(r.int64("id"), "/", r.int64("seen"))
        if _known(db, "worker-lost", subject):
            continue
        var runs = r.text("running")
        var count = len(runs.split(","))
        var host = r.text("name")
        if alert(
            db,
            "worker-lost",
            subject,
            "default",
            "warning",
            String("orca: worker ", r.text("label"), " lost"),
            String(
                "Pool ",
                r.text("pool"),
                ", worker ",
                r.text("label"),
                String(" (", host, ")") if host else String(),
                " has been quiet for ",
                _duration(Int(r.int64("ago"))),
                " while holding ",
                count,
                " run" if count == 1 else " runs",
                " (",
                runs,
                "); they are requeued when their heartbeats go stale.",
            ),
            ui_link("/ops/workers"),
        ):
            n += 1
    return n
