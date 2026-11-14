"""A step that replaces its month of lake.w in three commits, for
tests/replace_concurrent.sh, which runs several at once over one month.

    writer init            creates lake.w, partitioned by month
    writer run ...         morca's command line; ids from REPLACE_BASE
    writer check           prints the bases of the ids lake.w holds, and
                           how many ids it has

Each commit writes `ROWS` ids from `REPLACE_BASE`, napping REPLACE_NAP
seconds before each, so the writers' commits interleave and clash.
"""

from std.os import getenv
from std.sys import argv
from std.time import sleep

from iceberg.catalog.filesystem import FilesystemCatalog
from iceberg.schema import Schema as IcebergSchema
from iceberg.transforms import PartitionField, PartitionSpec, parse_transform
from parquet import RecordBatch

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main
from morca.tables import BatchBuilder, Schema, append, load_dataset, table

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"month","required":true,"type":"string"}]}'
)
comptime ROWS = 4
comptime COMMITS = 3


def _rows(month: String, start: Int) raises -> List[RecordBatch]:
    var b = BatchBuilder(Schema.parse(SCHEMA))
    var ids = b.col("id")
    var months = b.col("month")
    for i in range(ROWS):
        b.add_int(ids, Int64(start + i))
        b.add_str(months, month)
    return [b^.build()]


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    var base = Int(getenv("REPLACE_BASE", "0"))
    var nap = Float64(getenv("REPLACE_NAP", "0"))
    var t = table(ctx, "lake.w")
    var id = Int64(-1)
    for c in range(COMMITS):
        sleep(nap)
        id = append(ctx, t, _rows(String(m), base + c * ROWS))
    return id


comptime Load = Step["load", Month, Int64, load, Mapped, replaces=["lake.w"]]
comptime Writer = Pipeline["writer", Load, partitions=Monthly["2024-01"]]


def main() raises:
    var args = argv()
    if len(args) == 2 and String(args[1]) == "init":
        _ = FilesystemCatalog.local(getenv("MORCA_WAREHOUSE")).create_table(
            "lake",
            "w",
            IcebergSchema.parse(SCHEMA),
            PartitionSpec(
                0,
                [
                    PartitionField.single(
                        2, 1000, "month", parse_transform("identity")
                    )
                ],
            ),
        )
        return
    if len(args) == 2 and String(args[1]) == "check":
        var bases = Dict[Int, Bool]()
        var n = 0
        for b in load_dataset("lake.w").scan().to_batches():
            for i in range(b.num_columns()):
                if b.name(i) == "id":
                    for v in b.column_i64(i)[0]:
                        bases[Int(v) // 1000 * 1000] = True
                        n += 1
        var listed = String()
        for e in bases.keys():
            listed.write(" " if listed else "", e)
        print("bases", listed, "ids", n)
        return
    morca_main[Writer]()
