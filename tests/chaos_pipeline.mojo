"""The pipeline tests/chaos.sh runs, kills and resumes.

Four steps over the run's months: `days` (mapped), `fan` (mapped, one dista
task per day over the run's workers), `flaky` (mapped, retries=1, fails at
random) and `total` (gather). Every step naps CHAOS_NAP seconds so kills land
inside steps as well as between them. `flaky` fails an attempt when a hash of
CHAOS_SEED, the partition and the attempt number falls under CHAOS_FLAKY
percent, so the failures are the same for the same seed, and a later attempt
of the same step can pass.

`total` logs `TOTAL <n>`, which the script compares with a run without chaos.
"""

from std.os import getenv
from std.time import sleep

from dista import ObjectRef, Seq, Task
from dista.task import fnv1a64
from orca import Ctx, Gather, Mapped, Month, Pipeline, Step, Tasks, orca_main


def _nap() raises:
    var s = getenv("CHAOS_NAP")
    if s:
        sleep(Float64(s))


def days_in(m: Month) -> Int:
    if m.month == 2:
        var leap = (m.year % 4 == 0 and m.year % 100 != 0) or m.year % 400 == 0
        return 29 if leap else 28
    if m.month in (4, 6, 9, 11):
        return 30
    return 31


@fieldwise_init
struct Day(Copyable, Movable):
    var month: Month
    var day: Int


def weight(d: Day) -> Int:
    return (d.month.year * 13 + d.month.month * 31 + d.day * 7) % 1000


comptime Weight = Task[Day, Int, weight]
comptime ChaosTasks = Tasks[Weight]


def days(mut ctx: Ctx, m: Month) raises -> Day:
    _nap()
    return Day(m.copy(), days_in(m))


def fan(mut ctx: Ctx, d: Day) raises -> Int:
    _nap()
    ref rt = ctx.runtime[ChaosTasks]()
    var refs = List[ObjectRef[Int]]()
    for i in range(1, d.day + 1):
        refs.append(rt.remote[Weight](Day(d.month.copy(), i)))
    var t = 0
    for r in refs:
        t += rt.get(r)
    return t


def flaky(mut ctx: Ctx, n: Int) raises -> Int:
    _nap()
    var pct = Int(getenv("CHAOS_FLAKY", "0"))
    var h = fnv1a64(
        String(getenv("CHAOS_SEED"), ":", ctx.partition, ":", ctx.attempt)
    )
    if Int(h % 100) < pct:
        raise Error("flaky: attempt ", ctx.attempt, " fails")
    return n * 3


def total(mut ctx: Ctx, xs: Seq[Int]) raises -> Int:
    _nap()
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    ctx.log(String("TOTAL ", t))
    return t


comptime Days = Step["days", Month, Day, days, Mapped]
comptime Fan = Step["fan", Day, Int, fan, Mapped, after=Days]
comptime Flaky = Step["flaky", Int, Int, flaky, Mapped, after=Fan, retries=1]
comptime Total = Step["total", Seq[Int], Int, total, Gather, after=Flaky]
comptime Chaos = Pipeline["chaos", Days, Fan, Flaky, Total, tasks=ChaosTasks]


def main() raises:
    orca_main[Chaos]()
