"""What tests/dataview.sh's pipeline writes: 150 rows a month to lake.obs,
partitioned by month and city, with a column of each kind the data view
shows (long, date, string, double, int, and an optional string with
nulls, commas and quotes)."""

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.schema import Schema
from iceberg.transforms import PartitionField, PartitionSpec, parse_transform
from iceberg.types import P_DATE
from iceberg.values import Datum

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main
from morca.civil import days_from_civil
from morca.tables import append, ensure_table

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long","doc":"The row'
    ' number, month * 1000 + i"},'
    '{"id":2,"name":"month","required":true,"type":"string"},'
    '{"id":3,"name":"day","required":true,"type":"date"},'
    '{"id":4,"name":"city","required":true,"type":"string"},'
    '{"id":5,"name":"temp","required":true,"type":"double"},'
    '{"id":6,"name":"n","required":false,"type":"int"},'
    '{"id":7,"name":"note","required":false,"type":"string"}]}'
)
comptime CITIES = ["Lima", "Oslo, NO", "Paris"]


def spec() raises -> PartitionSpec:
    return PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, "month", parse_transform("identity")
            ),
            PartitionField.single(4, 1001, "city", parse_transform("identity")),
        ],
    )


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    var schema = Schema.parse(SCHEMA)
    var t = ensure_table(ctx, "lake.obs", schema, spec())
    var cols = List[ColumnBuilder]()
    for id in range(1, 8):
        cols.append(ColumnBuilder.of(schema, id))
    for i in range(150):
        cols[0].add(Datum.long_(Int64(m.month * 1000 + i)))
        cols[1].add(Datum.string_(String(m)))
        cols[2].add(
            Datum.integral(P_DATE, days_from_civil(m.year, m.month, 1 + i % 5))
        )
        cols[3].add(Datum.string_(String(materialize[CITIES]()[i % 3])))
        cols[4].add(Datum.double_(Float64(i % 10) + 0.5 * Float64(m.month)))
        cols[5].add(Datum.int_(Int64(i)))
        if i % 7 == 0:
            cols[6].add_null()
        else:
            cols[6].add(Datum.string_(String('say "', i, '", ok')))
    return append(ctx, t, [batch_of(cols)])


comptime Load = Step["load", Month, Int64, load, Mapped, writes=["lake.obs"]]
comptime Obs = Pipeline["obs", Load, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[Obs]()
