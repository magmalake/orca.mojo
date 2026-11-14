"""morca.tables' BatchBuilder (#454): a value of each kind into a schema's
columns, through liblake's empty batch, read back with the accessors and
written to a table and scanned. Needs no database: the table is used
outside any step."""

from std.os import getenv, setenv
from std.testing import TestSuite, assert_equal, assert_raises, assert_true

from iceberg.catalog.filesystem import FilesystemCatalog
from iceberg.schema import Schema as IcebergSchema
from iceberg.util import now_ms
from parquet import RecordBatch

from morca.tables import (
    BatchBuilder,
    Schema,
    Table,
    concat_batches,
    ints_of,
    strings_of,
)

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"n","required":false,"type":"int"},'
    '{"id":3,"name":"day","required":false,"type":"date"},'
    '{"id":4,"name":"at","required":false,"type":"timestamptz"},'
    '{"id":5,"name":"x","required":false,"type":"double"},'
    '{"id":6,"name":"f","required":false,"type":"float"},'
    '{"id":7,"name":"ok","required":false,"type":"boolean"},'
    '{"id":8,"name":"s","required":false,"type":"string"},'
    '{"id":9,"name":"tags","required":false,"type":{"type":"list",'
    '"element-id":10,"element":"string","element-required":true}},'
    '{"id":11,"name":"geo","required":false,"type":{"type":"struct",'
    '"fields":[{"id":12,"name":"lat","required":false,"type":"double"}]}}]}'
)


def _rows(n: Int) raises -> BatchBuilder:
    var b = BatchBuilder(Schema.parse(SCHEMA))
    for i in range(n):
        b.add_int(b.col("id"), Int64(i))
        if i == 1:
            b.add_null(b.col("n"))
            b.add_null(b.col("s"))
            b.add_null(b.col("tags"))
            b.add_null(b.col("geo"))
        else:
            b.add_int(b.col("n"), Int64(-i))
            b.add_str(b.col("s"), String("row ", i))
            for k in range(i):
                b.add_str(b.col("tags.element"), String("t", k))
            b.end_list(b.col("tags"))
            b.add_float(b.col("geo.lat"), 0.5 * Float64(i))
            b.end_struct(b.col("geo"))
        b.add_int(b.col("day"), Int64(19000 + i))
        b.add_int(b.col("at"), Int64(1_700_000_000_000_000 + i))
        b.add_float(b.col("x"), Float64(i) + 0.25)
        b.add_float(b.col("f"), Float64(i))
        b.add_bool(b.col("ok"), i % 2 == 0)
    return b^


def test_a_batch_reads_back() raises:
    var batch = _rows(3)^.build()
    assert_equal(batch.num_rows, 3)
    assert_equal(batch.num_columns(), 10)
    var ns = ints_of(batch, "n")
    assert_equal(ns[0].value(), 0)
    assert_true(not ns[1])
    assert_equal(ns[2].value(), -2)
    var ss = strings_of(batch, "s")
    assert_equal(ss[2].value(), "row 2")
    assert_true(not ss[1])
    assert_equal(ints_of(batch, "day")[2].value(), 19002)
    with assert_raises(contains="isn't a string column"):
        _ = strings_of(batch, "id")


def test_columns_must_line_up() raises:
    var b = BatchBuilder(Schema.parse(SCHEMA))
    b.add_int(b.col("id"), 1)
    with assert_raises(contains="has 0 rows"):
        _ = b^.build()


def test_a_batch_writes_and_scans() raises:
    var root = String("build/wh-lake-batch-", now_ms())
    _ = FilesystemCatalog.local(root).create_table(
        "db", "t", IcebergSchema.parse(SCHEMA)
    )
    var t = Table.load_local(String(root, "/db/t"))
    var batches = List[RecordBatch]()
    batches.append(_rows(3)^.build())
    batches.append(_rows(2)^.build())
    var one = concat_batches(batches)
    assert_equal(one.num_rows, 5)
    _ = t.append([one^])
    assert_equal(len(t.metadata.snapshots), 1)
    var n = 0
    var ids = List[Int64]()
    for b in t.scan().select(["id", "s"]).to_batches():
        n += b.num_rows
        for v in ints_of(b, "id"):
            ids.append(v.value())
    assert_equal(n, 5)
    assert_equal(t.scan().count(), 5)
    assert_equal(len(t.scan().plan_files()), 1)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
