"""What tests/dataview.sh's pipeline writes: 150 rows a month to lake.obs,
partitioned by month and city, with a column of each kind the data view
shows (long, date, string, double, int, and an optional string with
nulls, commas and quotes)."""

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main
from morca.civil import days_from_civil
from morca.tables import (
    BatchBuilder,
    PartitionField,
    PartitionSpec,
    Schema,
    append,
    ensure_table,
    parse_transform,
)

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
    var b = BatchBuilder(schema)
    var c_id = b.col("id")
    var c_month = b.col("month")
    var c_day = b.col("day")
    var c_city = b.col("city")
    var c_temp = b.col("temp")
    var c_n = b.col("n")
    var c_note = b.col("note")
    for i in range(150):
        b.add_int(c_id, Int64(m.month * 1000 + i))
        b.add_str(c_month, String(m))
        b.add_int(c_day, Int64(days_from_civil(m.year, m.month, 1 + i % 5)))
        b.add_str(c_city, String(materialize[CITIES]()[i % 3]))
        b.add_float(c_temp, Float64(i % 10) + 0.5 * Float64(m.month))
        b.add_int(c_n, Int64(i))
        if i % 7 == 0:
            b.add_null(c_note)
        else:
            b.add_str(c_note, String('say "', i, '", ok'))
    return append(ctx, t, [b^.build()])


comptime Load = Step["load", Month, Int64, load, Mapped, writes=["lake.obs"]]
comptime Obs = Pipeline["obs", Load, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[Obs]()
