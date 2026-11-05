"""Reads lake.events and writes its row count to lake.counts, a month at a
time (tests/expire.sh): its reads pin snapshots of lake.events."""

from orca import Ctx, Mapped, Month, Monthly, Pipeline, Step, orca_main
from orca.tables import append, table
from rows import count, dataset, rows


def count_rows(mut ctx: Ctx, m: Month) raises -> Int64:
    var n = count(ctx, table(ctx, "lake.events"))
    var t = dataset(ctx, "lake.counts")
    return append(ctx, t, rows(String(m), n))


comptime Count = Step[
    "count",
    Month,
    Int64,
    count_rows,
    Mapped,
    reads=["lake.events"],
    writes=["lake.counts"],
]
comptime P = Pipeline["count", Count, partitions=Monthly["2024-01"]]


def main() raises:
    orca_main[P]()
