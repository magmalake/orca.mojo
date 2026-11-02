"""A pipeline partitioned by day, from 2024-01-01.

    pixi run daily
    ./build/daily describe
    ./build/daily run --partitions 2024-02-27..2024-03-02

Each day's step makes a number from the date; the gather step sums them.
"""

from dista import Seq
from orca import Ctx, Daily, Day, Gather, Mapped, Pipeline, Step, orca_main


def trips(mut ctx: Ctx, d: Day) raises -> Int:
    """A stand-in for a day's work: a number that depends on the date."""
    return d.month * 100 + d.day


def total(mut ctx: Ctx, days: Seq[Int]) raises -> Int:
    var t = 0
    for i in range(len(days)):
        t += days[i]
    ctx.log(String(len(days), " days, ", t, " trips"))
    return t


comptime Trips = Step["trips", Day, Int, trips, Mapped]
comptime Total = Step["total", Seq[Int], Int, total, Gather, after=Trips]
comptime DailyTrips = Pipeline[
    "daily", Trips, Total, partitions=Daily["2024-01-01"]
]


def main() raises:
    orca_main[DailyTrips]()
