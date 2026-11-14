"""What tests/keyed_sensor.sh's pipelines write: rows of one string column,
`repo`."""

from parquet import RecordBatch

from morca import Ctx
from morca.tables import BatchBuilder, Schema, Table, ensure_table, scan

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"repo","required":true,"type":"string"}]}'
)


def repo_rows(names: List[String]) raises -> List[RecordBatch]:
    var b = BatchBuilder(Schema.parse(SCHEMA))
    var col = b.col("repo")
    for n in names:
        b.add_str(col, n)
    return [b^.build()]


def repo_dataset(mut ctx: Ctx, name: String) raises -> Table:
    """The dataset, created if this step writes it and it's missing."""
    return ensure_table(ctx, name, Schema.parse(SCHEMA))


def repo_count(mut ctx: Ctx, t: Table) raises -> Int:
    """Rows of `t` at the step's pinned snapshot."""
    var n = 0
    for b in scan(ctx, t).to_batches():
        n += b.num_rows
    return n
