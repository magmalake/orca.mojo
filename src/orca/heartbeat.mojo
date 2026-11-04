"""A run's heartbeat, kept fresh from a thread while the run's steps work.

Steps run on the main thread and can take minutes, so the heartbeat can't
wait for the next attempt to be recorded. A `RunStore` keeps one as a
`Heartbeat[B]`: one thread calls `B.beat` every `interval` seconds, on a
connection of its own, until the heartbeat is stopped or dropped, which
joins it. A failed beat is retried on the next one, after `B.reset`: a run
whose heartbeat goes stale can be resumed elsewhere, and this process then
finds out at its next attempt (see `orca.state`).
"""

from std.time import sleep

from threads import AtomicFlag, WorkerPool
from threads.ffi import OpaquePtr, opaque_ptr


trait Beater(Deinitable, Movable):
    """Refreshes one run's heartbeat in a store."""

    def beat(mut self) raises:
        """Refreshes the heartbeat once."""
        ...

    def reset(mut self):
        """Drops the connection a failed beat used, so the next one opens
        another."""
        ...


struct _Beating[B: Beater](Movable):
    var beater: Self.B
    var interval: Float64

    def __init__(out self, var beater: Self.B, interval: Float64):
        self.beater = beater^
        self.interval = interval


def _beat[B: Beater](worker: Int, ctx: OpaquePtr, stop: AtomicFlag) -> None:
    ref s = Pointer[_Beating[B], MutAnyOrigin](unsafe_from_address=Int(ctx))[]
    while not stop.is_set():
        try:
            s.beater.beat()
        except:
            s.beater.reset()
        # Sleep in short slices, so stopping doesn't wait out a whole beat.
        var slept = 0.0
        while slept < s.interval and not stop.is_set():
            sleep(0.05)
            slept += 0.05


struct Heartbeat[B: Beater](Movable):
    """A thread beating `B` until this is dropped."""

    var _state: List[_Beating[Self.B]]
    """One element, which the thread holds by address: a list's elements
    stay where they are when the list is moved."""
    var _pool: WorkerPool

    def __init__(out self, var beater: Self.B, interval: Float64) raises:
        self._state = List[_Beating[Self.B]]()
        self._state.append(_Beating(beater^, interval))
        self._pool = WorkerPool.start[_beat[Self.B]](
            1, opaque_ptr(Int(Pointer(to=self._state[0])))
        )

    def __deinit__(deinit self):
        # Joined before the state it beats with is freed. `_state` is
        # moved out only after the join: unused, it would be destroyed at
        # the start of this body (last-use destruction), freeing the beater
        # and its connection under a thread still beating with them.
        try:
            self._pool.shutdown()
        except:
            pass
        var state = self._state^
        state.clear()


def heartbeat_interval(stale_seconds: Float64) -> Float64:
    """Five beats per stale limit, so a few missed ones don't lose the run."""
    return stale_seconds / 5
