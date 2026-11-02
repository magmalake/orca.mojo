"""A one-step pipeline that reads lake.c and writes lake.a, for
tests/datasets.sh: first, second and third each read what the one before
writes, so registering the third closes a cycle."""

from orca import Ctx, Mapped, Month, Monthly, Pipeline, Step, orca_main


def step(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


comptime S = Step[
    "step", Month, Int, step, Mapped, reads=["lake.c"], writes=["lake.a"]
]
comptime P = Pipeline["first", S, partitions=Monthly["2024-01"]]


def main() raises:
    orca_main[P]()
