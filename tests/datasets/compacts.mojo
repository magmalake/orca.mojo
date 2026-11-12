"""A pipeline that reads and writes one dataset, as a compaction does: not
a cycle, for tests/datasets.sh."""

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main


def step(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


comptime S = Step[
    "step", Month, Int, step, Mapped, reads=["lake.a"], writes=["lake.a"]
]
comptime P = Pipeline["compacts", S, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[P]()
