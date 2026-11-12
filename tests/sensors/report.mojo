"""A sensor: reads lake.clean once per run and writes one row to
lake.report."""

from dista import Seq
from morca import Ctx, Gather, Month, Monthly, Pipeline, Step, morca_main
from morca.tables import append, table
from rows import count, dataset, rows


def report(mut ctx: Ctx, ms: Seq[Month]) raises -> Int64:
    var n = count(ctx, table(ctx, "lake.clean"))
    var t = dataset(ctx, "lake.report")
    return append(ctx, t, rows(String(n), 1))


comptime Report = Step[
    "report",
    Seq[Month],
    Int64,
    report,
    Gather,
    reads=["lake.clean"],
    writes=["lake.report"],
]
comptime P = Pipeline[
    "report", Report, partitions=Monthly["2024-01"], sensor=True
]


def main() raises:
    morca_main[P]()
