"""A three-step pipeline over months: the one tests/cli.sh drives.

    pixi run toy
    ./build/toy describe
    ./build/toy run --partitions 2024-01..2024-03

A partition whose month is in ORCA_TOY_FAIL (e.g. `2024-02`) fails its
`measure` step, to show the rest of the run carrying on.
"""

from std.os import getenv

from dista import Seq
from orca import Ctx, Gather, Mapped, Month, Pipeline, Step, orca_main


@fieldwise_init
struct Measured(Copyable, Movable):
    var month: Month
    var days: Int


def days(mut ctx: Ctx, m: Month) raises -> Measured:
    """The number of days in the month."""
    var n = 31
    if m.month == 2:
        var leap = (m.year % 4 == 0 and m.year % 100 != 0) or m.year % 400 == 0
        n = 29 if leap else 28
    elif m.month in (4, 6, 9, 11):
        n = 30
    return Measured(m.copy(), n)


def measure(mut ctx: Ctx, m: Measured) raises -> Measured:
    """Hours in the month; fails for the months in ORCA_TOY_FAIL."""
    if String(m.month) in getenv("ORCA_TOY_FAIL"):
        raise Error("failing ", m.month, " as asked")
    ctx.log(String(m.month, " has ", m.days, " days"))
    return Measured(m.month.copy(), m.days * 24)


def total(mut ctx: Ctx, ms: Seq[Measured]) raises -> Int:
    var t = 0
    for i in range(len(ms)):
        t += ms[i].days
    ctx.log(String(len(ms), " months, ", t, " hours"))
    return t


comptime Days = Step["days", Month, Measured, days, Mapped]
comptime Measure = Step[
    "measure", Measured, Measured, measure, Mapped, after=Days
]
comptime Total = Step["total", Seq[Measured], Int, total, Gather, after=Measure]
comptime Toy = Pipeline["toy", Days, Measure, Total]


def main() raises:
    orca_main[Toy]()
