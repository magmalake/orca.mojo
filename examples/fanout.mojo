"""A pipeline whose steps call dista: each month's step fans out one task per
day, over the run's workers.

    pixi run fanout
    ./build/fanout run --partitions 2024-01..2024-03
    ORCA_WORKERS=8 ORCA_LOGS=build/logs ./build/fanout run --partitions 2024-01

The run's runtime is started once, before the first step; with ORCA_LOGS set
its timeline is written to <ORCA_LOGS>/fanout/<run>/timeline.json.
ORCA_FANOUT_SLEEP makes every task take that many seconds, so
tests/lost_workers.sh can kill workers while a step waits on them; the step
is retried once in its run. With ORCA_FANOUT_SHOW_HOST set, each task prints
the host it ran on, which tests/kind.sh reads from the worker pods' logs.
ORCA_FANOUT_SECRET names a variable whose value every task prints and
`all_trips` logs, for tests/secrets.sh to look for in the logs.
ORCA_FANOUT_PAUSE makes `all_trips` wait that many seconds first, with no
task running, which tests/autoscale.sh's run drains its idle workers in. On
Kubernetes a run gets 3 workers.
"""

from std.os import getenv
from std.time import sleep

from dista import ObjectRef, Seq, Task
from orca import Ctx, Gather, Mapped, Month, Pipeline, Step, Tasks, orca_main


@fieldwise_init
struct Day(Copyable, Movable):
    var month: Month
    var day: Int


def trips(d: Day) -> Int:
    """A stand-in for a day's work: a number that depends on the date."""
    if getenv("ORCA_FANOUT_SHOW_HOST"):
        print("trips on", getenv("HOSTNAME", "?"), flush=True)
    var secret = getenv("ORCA_FANOUT_SECRET")
    if secret:
        print("secret", getenv(secret), flush=True)
    var s = getenv("ORCA_FANOUT_SLEEP")
    if s:
        try:
            sleep(Float64(s))
        except:
            pass
    return (d.month.year * 13 + d.month.month * 31 + d.day * 7) % 1000


comptime Trips = Task[Day, Int, trips]
comptime FanoutTasks = Tasks[Trips]


def days_in(m: Month) -> Int:
    if m.month == 2:
        var leap = (m.year % 4 == 0 and m.year % 100 != 0) or m.year % 400 == 0
        return 29 if leap else 28
    if m.month in (4, 6, 9, 11):
        return 30
    return 31


def month_trips(mut ctx: Ctx, m: Month) raises -> Int:
    """One dista task per day of the month, summed."""
    ref rt = ctx.runtime[FanoutTasks]()
    var refs = List[ObjectRef[Int]]()
    for d in range(1, days_in(m) + 1):
        refs.append(rt.remote[Trips](Day(m.copy(), d)))
    var total = 0
    for r in refs:
        total += rt.get(r)
    ctx.log(String(len(refs), " days, ", total, " trips"))
    return total


def all_trips(mut ctx: Ctx, months: Seq[Int]) raises -> Int:
    var secret = getenv("ORCA_FANOUT_SECRET")
    if secret:
        var v = getenv(secret)
        print("secret", v, flush=True)
        ctx.log(String("secret ", v, " of length ", v.byte_length()))
    var pause = getenv("ORCA_FANOUT_PAUSE")
    if pause:
        sleep(Float64(pause))
    var t = 0
    for i in range(len(months)):
        t += months[i]
    ctx.log(String(t, " trips in ", len(months), " months"))
    return t


comptime MonthTrips = Step[
    "month_trips", Month, Int, month_trips, Mapped, retries=1
]
comptime AllTrips = Step[
    "all_trips", Seq[Int], Int, all_trips, Gather, after=MonthTrips
]
comptime Fanout = Pipeline[
    "fanout", MonthTrips, AllTrips, tasks=FanoutTasks, workers=3
]


def main() raises:
    orca_main[Fanout]()
