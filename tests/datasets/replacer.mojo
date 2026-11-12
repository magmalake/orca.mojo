"""A one-step pipeline that replaces its partitions of lake.r, for
tests/datasets.sh: `morca backfill --rerun` of it needs no --allow-append."""

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main


def step(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


comptime S = Step["step", Month, Int, step, Mapped, replaces=["lake.r"]]
comptime P = Pipeline["replacer", S, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[P]()
