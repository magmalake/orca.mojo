"""A run's heartbeat, kept fresh from a thread while the run's steps work.

Steps run on the main thread and can take minutes, so the heartbeat can't
wait for the next attempt to be recorded. Started as
`TypedPool.start[beat](1, b)` with a `Beat`, one thread opens a connection
of its own and updates `runs.heartbeat` every `b.interval` seconds until the
pool is shut down, which joins it. A failed update is retried on the next beat: a run whose
heartbeat goes stale can be resumed elsewhere, and this process then finds
out at its next attempt (see `orca.state`).

**Stopping a step.** A step runs on the main thread and can't be
interrupted there, so the heartbeat thread is also the run's watchdog. On
each beat it looks for two things, and stops the process for either:

- an attempt of a step declared `timeout_seconds=N` that started more than
  N seconds ago: the attempt and its step are `failed` ("timed out after
  N s") and the run `failed`, with that as its error, so the scheduler
  retries it as it retries any failed run;
- a cancel (`runs.cancel_requested`) whose `cancel_at` has passed, set to
  `ORCA_CANCEL_KILL_SECONDS` after the cancel by whoever asked: the
  running attempt is `failed` ("cancelled") and the run `cancelled`.

Each is one transaction, with an `orca.audit` row (user `orca`, action
`stop`) saying why, and then the process exits at once, with code 1,
taking a local run's dista head and workers with it. On Kubernetes the
Job's pod ends, and on EC2 the head's script shuts its instance down;
either way the scheduler's sweep removes the rest of the run's cluster, as
for any run that isn't running. A commit the step made before it was
stopped is recognised by the attempt that runs it again (DESIGN.md,
"Snapshots").
"""

from std.ffi import external_call

from std.time import sleep

from postgres import Connection, Params
from threads import AtomicFlag, TypedPool


struct Beat(Movable):
    """What the heartbeat thread needs."""

    var url: String
    var run: Int
    var owner: String
    var interval: Float64
    var timeouts: String
    """`step=seconds` for each step with a timeout, comma-separated."""

    def __init__(
        out self,
        url: String,
        run: Int,
        owner: String,
        interval: Float64,
        timeouts: String = "",
    ):
        self.url = url
        self.run = run
        self.owner = owner
        self.interval = interval
        self.timeouts = timeouts


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
"""A reason to stop the run's process now, if there is one: a cancel whose
time has come, or an attempt past its step's timeout."""


def _stop(mut conn: Connection, b: Beat) raises -> String:
    """Ends the run as the watchdog finds it should end, and returns why,
    or empty if it shouldn't."""
    var rows = conn.query(
        _WATCH_SQL, Params().int64(Int64(b.run)).text(b.owner).text(b.timeouts)
    )
    if len(rows) == 0:
        return ""
    var r = rows.row(0)
    var tx = conn.begin()
    var why: String
    if r.text("why") == "timeout":
        var step = r.text("step")
        if r.text("partition"):
            step.write(" ", r.text("partition"))
        why = String(
            "timed out after ",
            Int(r.float64("secs")),
            " s (timeout_seconds), so its process was stopped",
        )
        var n = tx.execute(
            (
                "UPDATE orca.attempts SET status = 'failed', ended = now(),"
                " error = $2 WHERE id = $1 AND status = 'running'"
            ),
            Params().int64(r.int64("attempt")).text(why),
        )
        if n != 1:
            tx.rollback()
            return ""  # it ended as we looked
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = 'failed', error = $2 WHERE id ="
                " (SELECT step FROM orca.attempts WHERE id = $1)"
            ),
            Params().int64(r.int64("attempt")).text(why),
        )
        why = String("step ", step, " ", why)
        _ = tx.execute(
            (
                "UPDATE orca.runs SET status = 'failed', ended = now(),"
                " heartbeat = NULL, error = $3 WHERE id = $1 AND owner = $2"
            ),
            Params().int64(Int64(b.run)).text(b.owner).text(why),
        )
    else:
        why = String(
            "cancelled: its step hadn't finished when the cancel's",
            " ORCA_CANCEL_KILL_SECONDS ran out, so its process was stopped",
        )
        _ = tx.execute(
            (
                "UPDATE orca.attempts a SET status = 'failed', ended = now(),"
                " error = $2 FROM orca.steps s WHERE a.step = s.id AND s.run ="
                " $1 AND a.status = 'running'"
            ),
            Params().int64(Int64(b.run)).text(why),
        )
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = 'failed', error = $2 WHERE run"
                " = $1 AND status = 'running'"
            ),
            Params().int64(Int64(b.run)).text(why),
        )
        _ = tx.execute(
            (
                "UPDATE orca.runs SET status = 'cancelled', ended = now(),"
                " heartbeat = NULL WHERE id = $1 AND owner = $2"
            ),
            Params().int64(Int64(b.run)).text(b.owner),
        )
    _ = tx.execute(
        (
            "INSERT INTO orca.audit (username, action, run, detail) VALUES"
            " ('orca', 'stop', $1, $2)"
        ),
        Params().int64(Int64(b.run)).text(String(b.owner, ": ", why)),
    )
    tx.commit()
    return why^


def beat(worker: Int, mut b: Beat, stop: AtomicFlag) -> None:
    var conn: Optional[Connection] = None
    while not stop.is_set():
        try:
            if not conn:
                conn = Connection(b.url)
            _ = conn.value().execute(
                (
                    "UPDATE orca.runs SET heartbeat = now() WHERE id = $1 AND"
                    " owner = $2 AND status = 'running'"
                ),
                Params().int64(Int64(b.run)).text(b.owner),
            )
            var why = _stop(conn.value(), b)
            if why:
                print("run", b.run, why, flush=True)
                external_call["_exit", NoneType](Int32(1))
        except:
            conn = None
        # Sleep in short slices, so stopping doesn't wait out a whole beat.
        var slept = 0.0
        while slept < b.interval and not stop.is_set():
            sleep(0.05)
            slept += 0.05


def heartbeat_interval(stale_seconds: Float64) -> Float64:
    """Five beats per stale limit, so a few missed ones don't lose the run."""
    return stale_seconds / 5
