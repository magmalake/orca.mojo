"""A sensor: reads raw.events and writes as many rows to lake.clean, a
month at a time."""

from orca import Ctx, Mapped, Month, Monthly, Pipeline, Step, orca_main
from orca.tables import append, table
from rows import count, dataset, rows


def clean(mut ctx: Ctx, m: Month) raises -> Int64:
    var n = count(ctx, table(ctx, "raw.events"))
    var t = dataset(ctx, "lake.clean")
    return append(ctx, t, rows(String(m), n))


comptime Clean = Step[
    "clean",
    Month,
    Int64,
    clean,
    Mapped,
    reads=["raw.events"],
    writes=["lake.clean"],
]
comptime P = Pipeline[
    "clean", Clean, partitions=Monthly["2024-01"], sensor=True
]


def main() raises:
    orca_main[P]()
