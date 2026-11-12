"""What tests/lineage.sh's pipelines write: rows of (id, month)."""

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import Table
from iceberg.schema import Schema
from iceberg.values import Datum
from parquet import RecordBatch

from morca import Ctx
from morca.tables import ensure_table, scan

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"month","required":true,"type":"string"}]}'
)


def rows(month: String, n: Int) raises -> List[RecordBatch]:
    var schema = Schema.parse(SCHEMA)
    var ids = ColumnBuilder.of(schema, 1)
    var months = ColumnBuilder.of(schema, 2)
    for i in range(n):
        ids.add(Datum.long_(Int64(i)))
        months.add(Datum.string_(month))
    return [batch_of([ids^, months^])]


def dataset(mut ctx: Ctx, name: String) raises -> Table:
    """The dataset, created if this step writes it and it's missing."""
    return ensure_table(ctx, name, Schema.parse(SCHEMA))


def count(mut ctx: Ctx, t: Table) raises -> Int:
    """Rows of `t` at the step's pinned snapshot."""
    var n = 0
    for b in scan(ctx, t).to_batches():
        n += b.num_rows
    return n
