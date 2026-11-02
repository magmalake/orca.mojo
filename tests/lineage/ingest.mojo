"""Writes three rows a month to lake.raw, as a source would."""

from orca import Ctx, Mapped, Month, Monthly, Pipeline, Step, orca_main
from orca.tables import append
from rows import dataset, rows


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    var t = dataset(ctx, "lake.raw")
    return append(ctx, t, rows(String(m), 3))


comptime Load = Step["load", Month, Int64, load, Mapped, writes=["lake.raw"]]
comptime Ingest = Pipeline["ingest", Load, partitions=Monthly["2024-01"]]


def main() raises:
    orca_main[Ingest]()
