"""A pipeline that reads and writes one dataset, as a compaction does: not
a cycle, for tests/datasets.sh."""

from orca import Ctx, Mapped, Month, Monthly, Pipeline, Step, orca_main


def step(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


comptime S = Step[
    "step", Month, Int, step, Mapped, reads=["lake.a"], writes=["lake.a"]
]
comptime P = Pipeline["compacts", S, partitions=Monthly["2024-01"]]


def main() raises:
    orca_main[P]()
