"""A pipeline whose steps take as long as tests/stop.sh says, to stop them:
`slow` sleeps STOP_SLOW seconds with `timeout_seconds=2`, and `wait`, after
it, sleeps STOP_WAIT seconds with no timeout. Runs aren't retried.
"""

from std.os import getenv
from std.time import sleep

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main


def _nap(name: StaticString) raises:
    var s = getenv(name)
    if s:
        sleep(Float64(s))


def slow(mut ctx: Ctx, m: Month) raises -> Int:
    _nap("STOP_SLOW")
    return m.month


def wait(mut ctx: Ctx, n: Int) raises -> Int:
    ctx.log("waiting")
    _nap("STOP_WAIT")
    return n


comptime Slow = Step["slow", Month, Int, slow, Mapped, timeout_seconds=2]
comptime Wait = Step["wait", Int, Int, wait, Mapped, after=Slow]
comptime Stopping = Pipeline[
    "stopping", Slow, Wait, partitions=Monthly["2024-01"], run_retries=0
]


def main() raises:
    morca_main[Stopping]()
