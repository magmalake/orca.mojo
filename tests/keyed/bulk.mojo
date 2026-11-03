"""A keyed sensor over keys.bulk, a hundred keys to a run, four runs at
once, never retried by the scheduler: `work` fails the names listed in
ORCA_KEYED_FAIL and sleeps ORCA_KEYED_SLEEP seconds a key, and `tally`
counts its run's."""

from std.os import getenv
from std.time import sleep

from dista import Seq
from orca import Ctx, Gather, Mapped, Name, Pipeline, Step, orca_main
from orca.keyed import Keyed


def work(mut ctx: Ctx, n: Name) raises -> Int:
    for bad in getenv("ORCA_KEYED_FAIL").split(","):
        if bad == n.text:
            raise Error("told to fail ", n)
    var s = getenv("ORCA_KEYED_SLEEP")
    if s:
        sleep(Float64(s))
    return 1


def tally(mut ctx: Ctx, ones: Seq[Int]) raises -> Int:
    return len(ones)


comptime Work = Step["work", Name, Int, work, Mapped]
comptime Tally = Step["tally", Seq[Int], Int, tally, Gather, after=Work]
comptime Bulk = Pipeline[
    "bulk",
    Work,
    Tally,
    partitions=Keyed["keys.bulk", "log_id"],
    keys_per_run=100,
    max_runs=4,
    run_retries=0,
    sensor=True,
]


def main() raises:
    orca_main[Bulk]()
