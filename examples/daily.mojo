"""A pipeline partitioned by day, from 2024-01-01.

    pixi run daily
    ./build/daily describe
    ./build/daily run --partitions 2024-02-27..2024-03-02
    ./build/daily fires 3 --after 2024-03-09T12:00Z

Each day's step makes a number from the date; the gather step sums them. It's
scheduled at 02:30 New York time for the day before; on 2024-03-10 the clocks
skip 02:30, so that day's fire is at 03:00. With `catchup=False`, a
scheduler that was down starts only the latest fire it missed.
"""

from dista import Seq
from orca import (
    Cron,
    Ctx,
    Daily,
    Day,
    Gather,
    Mapped,
    Pipeline,
    Step,
    orca_main,
)


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
    "daily",
    Trips,
    Total,
    partitions=Daily["2024-01-01"],
    schedule=Cron["30 2 * * *", "America/New_York"],
    catchup=False,
]


def main() raises:
    orca_main[DailyTrips]()
