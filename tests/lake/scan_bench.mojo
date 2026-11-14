"""What a scan through liblake costs per row, beyond iceberg's own read:
`morca.tables` scans of a table of (long, short string) rows, timed against
iceberg's scan of the same table in this process. Not a test: run it by
hand, `pixi run -e data sh -c 'scripts/build-liblake.sh && scripts/mojo-build.sh
-I src $DEP_INC tests/lake/scan_bench.mojo -o build/scan-bench &&
./build/scan-bench [ROWS]'`."""

from std.sys import argv
from std.time import perf_counter_ns

from iceberg.catalog.filesystem import FilesystemCatalog
from iceberg.catalog.filesystem import Table as IcebergTable
from iceberg.schema import Schema as IcebergSchema
from iceberg.util import now_ms

from morca.tables import BatchBuilder, Schema, Table

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"s","required":true,"type":"string"}]}'
)
comptime ROUNDS = 5


def main() raises:
    var rows = 1_000_000
    if len(argv()) > 1:
        rows = Int(String(argv()[1]))
    var root = String("build/wh-scan-bench-", now_ms())
    _ = FilesystemCatalog.local(root).create_table(
        "db", "t", IcebergSchema.parse(SCHEMA)
    )
    var t = Table.load_local(String(root, "/db/t"))
    var per = 100_000
    var at = 0
    while at < rows:
        var b = BatchBuilder(Schema.parse(SCHEMA))
        var ids = b.col("id")
        var s = b.col("s")
        for i in range(at, min(at + per, rows)):
            b.add_int(ids, Int64(i))
            b.add_str(s, String("row ", i % 1000))
        _ = t.append([b^.build()])
        at += per
    var ice = IcebergTable.load_local(String(root, "/db/t"))
    var best_lake = Int.MAX
    var best_ice = Int.MAX
    for _ in range(ROUNDS):
        var t0 = perf_counter_ns()
        var n = 0
        for b in t.scan().to_batches():
            n += b.num_rows
        var t1 = perf_counter_ns()
        var m = 0
        for b in ice.scan().to_batches():
            m += b.num_rows
        var t2 = perf_counter_ns()
        if n != rows or m != rows:
            raise Error("scanned ", n, " and ", m, " rows of ", rows)
        best_lake = min(best_lake, Int(t1 - t0))
        best_ice = min(best_ice, Int(t2 - t1))
    print(
        rows,
        "rows: through liblake",
        Float64(best_lake) / 1e6,
        "ms, iceberg in-process",
        Float64(best_ice) / 1e6,
        "ms; the boundary",
        Float64(best_lake - best_ice) / Float64(rows),
        "ns/row (best of",
        ROUNDS,
        ")",
    )
