"""The pipeline of tests/worker.sh, run on a remote worker: writes three
rows a month to lake.worker. While the file `WK_HOLD` names exists, its step
waits first, so the test can kill the worker mid-run. It logs whether its
process has a database to write to, which on a worker it mustn't."""

from std.os import getenv
from std.os.path import exists
from std.time import sleep

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main
from morca.tables import append
from rows import dataset, rows


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    ctx.log(
        String(
            "database: ",
            "set" if getenv("MORCA_DATABASE_URL") else "none",
            ", state: ",
            getenv("MORCA_STATE_URL"),
        )
    )
    var hold = getenv("WK_HOLD")
    while hold and exists(hold):
        sleep(0.1)
    var t = dataset(ctx, "lake.worker")
    return append(ctx, t, rows(String(m), 3))


comptime Load = Step["load", Month, Int64, load, Mapped, writes=["lake.worker"]]
comptime Wk = Pipeline["wk", Load, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[Wk]()
