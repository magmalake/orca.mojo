"""The Iceberg pipeline tests/chaos.sh runs with CHAOS_CHECK=1, in the data
environment: kills that land between a commit and its record must still
leave exactly one snapshot per month.

    chaos_tables init          creates the table under CHAOS_WH
    chaos_tables run ...       orca's command line
    chaos_tables check <run>   checks the table after a run

`load` appends three rows for its month to `out`, napping CHAOS_NAP before
and after the commit, and fails at random after committing (CHAOS_FLAKY
percent of attempts, keyed on CHAOS_SEED, the partition and the attempt), the
case a retry has to recognise. `count` reads `out` at its pinned snapshot and
logs `TOTAL <rows>`.
"""

from std.os import getenv
from std.sys import argv
from std.time import sleep

from dista import Seq
from dista.task import fnv1a64
from iceberg.batch import ColumnBuilder, batch_of
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.schema import Schema
from iceberg.values import Datum
from parquet import RecordBatch
from orca import Ctx, Gather, Mapped, Month, Pipeline, Step, orca_main
from orca.tables import append, scan

comptime SCHEMA = (
    '{"schema-id":0,"type":"struct","fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"month","required":true,"type":"string"}]}'
)


def _nap() raises:
    var s = getenv("CHAOS_NAP")
    if s:
        sleep(Float64(s))


def _out() raises -> Table:
    return Table.load_local(String(getenv("CHAOS_WH"), "/db/out"))


def _rows(month: String) raises -> List[RecordBatch]:
    var schema = Schema.parse(SCHEMA)
    var ids = ColumnBuilder.of(schema, 1)
    var months = ColumnBuilder.of(schema, 2)
    for i in range(3):
        ids.add(Datum.long_(Int64(i)))
        months.add(Datum.string_(month))
    return [batch_of([ids^, months^])]


def load(mut ctx: Ctx, m: Month) raises -> Int64:
    _nap()
    var out = _out()
    var id = append(ctx, out, _rows(String(m)))
    _nap()
    var pct = Int(getenv("CHAOS_FLAKY", "0"))
    var h = fnv1a64(
        String(getenv("CHAOS_SEED"), ":", ctx.partition, ":", ctx.attempt)
    )
    if Int(h % 100) < pct:
        raise Error("flaky: attempt ", ctx.attempt, " fails after committing")
    return id


def count(mut ctx: Ctx, ids: Seq[Int64]) raises -> Int:
    _nap()
    var n = 0
    for b in scan(ctx, _out()).to_batches():
        n += b.num_rows
    ctx.log(String("TOTAL ", n))
    return n


comptime Load = Step["load", Month, Int64, load, Mapped, retries=1]
comptime Count = Step["count", Seq[Int64], Int, count, Gather, after=Load]
comptime ChaosTables = Pipeline["chaos_tables", Load, Count]


def _check(run: String) raises:
    var out = _out()
    var per_month = Dict[String, Int]()
    for s in out.metadata.snapshots:
        if s.summary.get("orca.run", "") != run:
            continue
        var m = s.summary.get("orca.partition", "")
        per_month[m] = per_month.get(m, 0) + 1
    if len(per_month) == 0:
        raise Error("check: no snapshots of run ", run)
    for e in per_month.items():
        if e.value != 1:
            raise Error(
                "check: ", e.value, " snapshots for ", e.key, ", not one"
            )
    print("check: one snapshot for each of", len(per_month), "months")


def main() raises:
    var args = argv()
    if len(args) == 2 and String(args[1]) == "init":
        var root = getenv("CHAOS_WH")
        if not root:
            raise Error("init needs CHAOS_WH")
        _ = FilesystemCatalog.local(root).create_table(
            "db", "out", Schema.parse(SCHEMA)
        )
        return
    if len(args) == 3 and String(args[1]) == "check":
        _check(String(args[2]))
        return
    orca_main[ChaosTables]()
