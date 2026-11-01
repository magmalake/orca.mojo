"""A run's heartbeat, kept fresh from a thread while the run's steps work.

Steps run on the main thread and can take minutes, so the heartbeat can't
wait for the next attempt to be recorded. Started as
`TypedPool.start[beat](1, b)` with a `Beat`, one thread opens a connection
of its own and updates `runs.heartbeat` every `b.interval` seconds until the
pool is shut down, which joins it. A failed update is retried on the next beat: a run whose
heartbeat goes stale can be resumed elsewhere, and this process then finds
out at its next attempt (see `orca.state`).
"""

from std.time import sleep

from postgres import Connection, Params
from threads import AtomicFlag, TypedPool


struct Beat(Movable):
    """What the heartbeat thread needs."""

    var url: String
    var run: Int
    var owner: String
    var interval: Float64

    def __init__(
        out self, url: String, run: Int, owner: String, interval: Float64
    ):
        self.url = url
        self.run = run
        self.owner = owner
        self.interval = interval


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
