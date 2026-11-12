"""Writes three rows a month to lake.events, keeping two snapshots of history
(tests/expire.sh)."""

from iceberg.schema import Schema

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main
from morca.tables import append, ensure_table
from rows import SCHEMA, rows


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    var props = Dict[String, String]()
    props["history.expire.max-snapshot-age-ms"] = "0"
    props["history.expire.min-snapshots-to-keep"] = "2"
    var t = ensure_table(
        ctx, "lake.events", Schema.parse(SCHEMA), properties=props
    )
    return append(ctx, t, rows(String(m), 3))


comptime Load = Step["load", Month, Int64, load, Mapped, writes=["lake.events"]]
comptime Ingest = Pipeline["ingest", Load, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[Ingest]()
