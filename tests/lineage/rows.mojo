"""What tests/lineage.sh's pipelines write: rows of (id, month)."""

from parquet import RecordBatch

from morca import Ctx
from morca.tables import BatchBuilder, Schema, Table, ensure_table, scan

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"month","required":true,"type":"string"}]}'
)


def rows(month: String, n: Int) raises -> List[RecordBatch]:
    var b = BatchBuilder(Schema.parse(SCHEMA))
    var ids = b.col("id")
    var months = b.col("month")
    for i in range(n):
        b.add_int(ids, Int64(i))
        b.add_str(months, month)
    return [b^.build()]


def dataset(mut ctx: Ctx, name: String) raises -> Table:
    """The dataset, created if this step writes it and it's missing."""
    return ensure_table(ctx, name, Schema.parse(SCHEMA))


def count(mut ctx: Ctx, t: Table) raises -> Int:
    """Rows of `t` at the step's pinned snapshot."""
    var n = 0
    for b in scan(ctx, t).to_batches():
        n += b.num_rows
    return n
