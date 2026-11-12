"""A pipeline whose first step waits, fails and defers as tests/defer.sh
asks, for step backoff, run backoff with a cap, and `defer`.

`fetch` fails its first attempt with DEFER_FLAKY set (and is retried after
`backoff_seconds=1`), fails every attempt for the months in DEFER_FAIL, and
raises `defer(3600)` while the file DEFER_GATE doesn't exist, as a download
that isn't published yet would. Runs are retried after 10 s doubling, at
most 20 s apart.
"""

from std.os import getenv
from std.os.path import exists

from dista import Seq
from morca import (
    Ctx,
    Gather,
    Mapped,
    Month,
    Monthly,
    Pipeline,
    Step,
    defer,
    morca_main,
)


def fetch(mut ctx: Ctx, m: Month) raises -> Int:
    if getenv("DEFER_FLAKY") and ctx.attempt == 1:
        raise Error("the first try fails")
    if String(m) in getenv("DEFER_FAIL"):
        raise Error("failing ", m, " as asked")
    var gate = getenv("DEFER_GATE")
    if gate and not exists(gate):
        raise defer(3600, String(m, " isn't published yet"))
    return m.month


def total(mut ctx: Ctx, ms: Seq[Int]) raises -> Int:
    var t = 0
    for i in range(len(ms)):
        t += ms[i]
    ctx.log(String("total ", t))
    return t


comptime Fetch = Step[
    "fetch", Month, Int, fetch, Mapped, retries=1, backoff_seconds=1
]
comptime Total = Step["total", Seq[Int], Int, total, Gather, after=Fetch]
comptime Deferring = Pipeline[
    "deferring",
    Fetch,
    Total,
    partitions=Monthly["2024-01"],
    run_retries=5,
    retry_seconds=10,
    max_retry_seconds=20,
]


def main() raises:
    morca_main[Deferring]()
