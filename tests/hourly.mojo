"""An hourly pipeline polled every five minutes (`fills=Current`): each
fire runs the hour it falls in, the step returning its hour (tests/hourly.sh).
"""

from orca import (
    Cron,
    Ctx,
    Current,
    Hour,
    Hourly,
    Mapped,
    Pipeline,
    Step,
    orca_main,
)


def poll(mut ctx: Ctx, h: Hour) raises -> Int:
    return h.hour


comptime Poll = Step["poll", Hour, Int, poll, Mapped]
comptime Hourly5 = Pipeline[
    "hourly",
    Poll,
    partitions=Hourly["2024-03-01T00"],
    schedule=Cron["*/5 * * * *"],
    fills=Current,
    catchup=False,
]


def main() raises:
    orca_main[Hourly5]()
