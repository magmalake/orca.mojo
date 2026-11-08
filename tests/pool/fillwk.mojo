"""The pipeline of tests/pool_fills.sh: a monthly pipeline from 2024-01
that fills its range on a worker pool. While the file `FILLWK_HOLD` names
exists, its step waits first, so the test can see what's running and kill
a worker mid-run. It writes nothing but its log."""

from std.os import getenv
from std.os.path import exists
from std.time import sleep

from orca import Ctx, Mapped, Month, Monthly, Pipeline, Step, orca_main


def load(mut ctx: Ctx, m: Month) raises -> Int:
    ctx.log(String("month ", m))
    var hold = getenv("FILLWK_HOLD")
    while hold and exists(hold):
        sleep(0.1)
    return m.month


comptime Load = Step["load", Month, Int, load, Mapped]
comptime Fillwk = Pipeline["fillwk", Load, partitions=Monthly["2024-01"]]


def main() raises:
    orca_main[Fillwk]()
