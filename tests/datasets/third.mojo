"""A one-step pipeline that reads lake.b and writes lake.c, for
tests/datasets.sh: first, second and third each read what the one before
writes, so registering the third closes a cycle."""

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main


def step(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


comptime S = Step[
    "step", Month, Int, step, Mapped, reads=["lake.b"], writes=["lake.c"]
]
comptime P = Pipeline["third", S, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[P]()
