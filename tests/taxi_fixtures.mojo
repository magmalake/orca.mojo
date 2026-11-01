"""Writes tests/fixtures/taxi/: a slice of each of two real TLC files, small
enough to commit, for tests/taxi.sh.

    pixi run -e data sh -c 'mojo build -I src -I examples/taxi $DEP_INC \\
        tests/taxi_fixtures.mojo -o build/taxi-fixtures'
    pixi run -e data ./build/taxi-fixtures <dir with the 2023-01 and 2024-01 files>

Each slice keeps the file's first 500 rows and every row dated outside the
month, at the file's own column names and types, compressed with the codec
the TLC used that year (Snappy in 2023, Zstandard in 2024). Row groups of 128
rows make the load see several batches, so the compaction has work to do.
"""

from std.sys import argv

from iceberg.io import FileIO, join_path
from iceberg.nested import concat_tree, filter_tree
from iceberg.write import codec_value
from parquet import ParquetReader, ParquetWriter, RecordBatch, WriterOptions
from parquet_full import AllCodecs

from orca import Month
from tlc import file_name, month_bounds, pickup_column

comptime KEEP_FIRST = 500


def _slice(path: String, m: Month) raises -> RecordBatch:
    var r = ParquetReader[AllCodecs].open(path)
    var out = RecordBatch()
    var seen = 0
    while r.has_next():
        var b = r.read_batch()
        var c = pickup_column(b)
        var lo, hi = month_bounds(m, b.type(c).unit)
        var got = b.column_i64(c)
        var keep = List[Bool](capacity=b.num_rows)
        var n = 0
        for i in range(b.num_rows):
            var outside = not got[1][i] or got[0][i] < lo or got[0][i] >= hi
            var k = seen + i < KEEP_FIRST or outside
            keep.append(k)
            if k:
                n += 1
        seen += b.num_rows
        if n == 0:
            continue
        var part = RecordBatch()
        for k in range(b.num_columns()):
            part.roots.append(
                filter_tree(b.arena, b.roots[k], keep, n, part.arena)
            )
        part.num_rows = n
        if out.num_rows == 0:
            out = part^
        else:
            for k in range(out.num_columns()):
                concat_tree(out.arena, out.roots[k], part.arena, part.roots[k])
            out.num_rows += part.num_rows
    return out^


def main() raises:
    var src = String(argv()[1])
    var io = FileIO.local()
    for m in [Month(2023, 1), Month(2024, 1)]:
        var b = _slice(join_path(src, file_name(m)), m)
        var opts = WriterOptions()
        opts.codec = codec_value("snappy" if m.year == 2023 else "zstd")
        opts.row_group_size = 128
        var w = ParquetWriter[AllCodecs](opts^)
        w.write_batch(b.arena, b.roots)
        var dst = join_path("tests/fixtures/taxi", file_name(m))
        io.write_all(dst, Span(w^.finish()))
        print(dst, b.num_rows, "rows")
