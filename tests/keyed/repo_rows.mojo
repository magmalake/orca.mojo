"""What tests/keyed_sensor.sh's pipelines write: rows of one string column,
`repo`."""

from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import Table
from iceberg.schema import Schema
from iceberg.values import Datum
from parquet import RecordBatch

from morca import Ctx
from morca.tables import ensure_table, scan

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"repo","required":true,"type":"string"}]}'
)


def repo_rows(names: List[String]) raises -> List[RecordBatch]:
    var schema = Schema.parse(SCHEMA)
    var col = ColumnBuilder.of(schema, 1)
    for n in names:
        col.add(Datum.string_(n))
    return [batch_of([col^])]


def repo_dataset(mut ctx: Ctx, name: String) raises -> Table:
    """The dataset, created if this step writes it and it's missing."""
    return ensure_table(ctx, name, Schema.parse(SCHEMA))


def repo_count(mut ctx: Ctx, t: Table) raises -> Int:
    """Rows of `t` at the step's pinned snapshot."""
    var n = 0
    for b in scan(ctx, t).to_batches():
        n += b.num_rows
    return n
