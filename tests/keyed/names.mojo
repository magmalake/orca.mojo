"""Writes the keys dataset `keys.logs`: `log_id` and `frames` columns, 50
logs named `log-00` to `log-49` and shuffled, each written twice, so the
keys are the distinct values and not the rows.

    names <warehouse> [<count>]
"""

from std.sys import argv

from iceberg.append import prepare_append
from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.values import Datum
from iceberg.write import WriteOptions, write_data_files

from orca.cli import seed_random

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"log_id","required":true,"type":"string"},'
    '{"id":2,"name":"frames","required":true,"type":"long"}]}'
)


def main() raises:
    seed_random()  # iceberg draws snapshot IDs from std.random
    var args = argv()
    var n = Int(String(args[2])) if len(args) > 2 else 50
    var cat = FilesystemCatalog.local(String(args[1]))
    var schema = Schema.parse(SCHEMA)
    if not cat.table_exists("keys", "logs"):
        _ = cat.create_table(
            "keys", "logs", schema.copy(), PartitionSpec.unpartitioned()
        )
    var t = cat.load_table("keys", "logs")
    var ids = ColumnBuilder.of(schema, 1)
    var frames = ColumnBuilder.of(schema, 2)
    for copy in range(2):
        for i in range(n):
            # 37 is prime to 50, so this visits every log out of order.
            var k = (i * 37 + copy) % n
            ids.add(Datum.string_(String("log-", "0" if k < 10 else "", k)))
            frames.add(Datum.long_(Int64(k * 10)))
    var files = write_data_files(
        t.io,
        t.metadata.location,
        [batch_of([ids^, frames^])],
        t.metadata.schema(),
        t.metadata.spec(),
        t.metadata.default_sort_order_id,
        WriteOptions.from_properties(t.metadata.properties),
    )
    var result = prepare_append(
        t.io, t.metadata, files^, Dict[String, String]()
    )
    t.commit(result)
    print(result.snapshot_id)
