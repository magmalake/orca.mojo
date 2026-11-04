"""A daily pipeline whose schedule fires every six hours, each fire for the
day before: four fires per partition, which `orca register` warns of
(tests/one_run_per_key.sh)."""

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


def count(mut ctx: Ctx, d: Day) raises -> Int:
    return d.day


def total(mut ctx: Ctx, days: Seq[Int]) raises -> Int:
    var t = 0
    for i in range(len(days)):
        t += days[i]
    return t


comptime Count = Step["count", Day, Int, count, Mapped]
comptime Total = Step["total", Seq[Int], Int, total, Gather, after=Count]
comptime SixHourly = Pipeline[
    "six_hourly",
    Count,
    Total,
    partitions=Daily["2024-01-01"],
    schedule=Cron["0 */6 * * *"],
]


def main() raises:
    orca_main[SixHourly]()
