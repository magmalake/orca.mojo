"""Months, pipelines, describe and the in-memory executor."""

from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from dista import Seq
from orca import (
    FAILED,
    SKIPPED,
    SUCCEEDED,
    Ctx,
    Gather,
    Mapped,
    Month,
    Pipeline,
    Step,
    parse_months,
)


# --- months ---


def test_month_parse_and_write() raises:
    var m = Month.parse("2024-03")
    assert_equal(m.year, 2024)
    assert_equal(m.month, 3)
    assert_equal(String(m), "2024-03")
    assert_equal(String(Month(2024, 12).next()), "2025-01")
    with assert_raises(contains="not a month"):
        _ = Month.parse("2024-13")
    with assert_raises(contains="not a month"):
        _ = Month.parse("2024-3")
    with assert_raises(contains="not a month"):
        _ = Month.parse("24-03")


def test_month_ranges() raises:
    var ms = parse_months("2023-11..2024-02")
    assert_equal(len(ms), 4)
    assert_equal(String(ms[0]), "2023-11")
    assert_equal(String(ms[3]), "2024-02")
    ms = parse_months("2024-05,2024-01..2024-02")
    assert_equal(len(ms), 3)
    assert_equal(String(ms[0]), "2024-05")
    assert_equal(String(ms[2]), "2024-02")
    assert_equal(len(parse_months("2011-01..2024-12")), 168)
    with assert_raises(contains="backwards"):
        _ = parse_months("2024-03..2024-01")
    with assert_raises(contains="given twice"):
        _ = parse_months("2024-01..2024-03,2024-02")


# --- a pipeline: key -> label -> total, plus a gather over the gather ---


@fieldwise_init
struct Labeled(Copyable, Movable):
    var key: String
    var n: Int


def key(mut ctx: Ctx, m: Month) raises -> Int:
    return m.year * 100 + m.month


def label(mut ctx: Ctx, n: Int) raises -> Labeled:
    if n % 100 == 2:
        raise Error("February is broken")
    return Labeled(ctx.partition, n)


def total(mut ctx: Ctx, xs: Seq[Labeled]) raises -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i].n
    return t


def twice(mut ctx: Ctx, t: Int) raises -> Int:
    return t * 2


def count(mut ctx: Ctx, ms: Seq[Month]) raises -> Int:
    return len(ms)


comptime Key = Step["key", Month, Int, key, Mapped]
comptime Label = Step["label", Int, Labeled, label, Mapped, after=Key]
comptime Total = Step["total", Seq[Labeled], Int, total, Gather, after=Label]
comptime Twice = Step["twice", Int, Int, twice, Gather, after=Total]
comptime Labels = Pipeline["labels", Key, Label, Total, Twice]

comptime Count = Step["count", Seq[Month], Int, count, Gather]
comptime Counts = Pipeline["counts", Count]


def test_runs_in_order() raises:
    var res = Labels.run(parse_months("2024-03..2024-04,2024-01"))
    assert_true(res.succeeded())
    # Mapped steps run once per partition, in the run's order, step by step.
    assert_equal(len(res.steps), 3 + 3 + 1 + 1)
    assert_equal(res.steps[0].step, "key")
    assert_equal(res.steps[0].partition, "2024-03")
    assert_equal(res.steps[2].partition, "2024-01")
    assert_equal(res.steps[3].step, "label")
    assert_equal(res.steps[6].step, "total")
    assert_equal(res.steps[6].partition, "")
    var l = res.output[Labeled]("label", "2024-04")
    assert_equal(l.key, "2024-04")
    assert_equal(l.n, 202404)
    assert_equal(res.output[Int]("total"), 202403 + 202404 + 202401)
    assert_equal(res.output[Int]("twice"), 2 * (202403 + 202404 + 202401))


def test_failed_partition_skips_downstream() raises:
    var res = Labels.run(parse_months("2024-01..2024-03"))
    assert_false(res.succeeded())
    assert_equal(res.status("key", "2024-02"), SUCCEEDED)
    assert_equal(res.status("label", "2024-02"), FAILED)
    assert_equal(
        res.steps[res.find("label", "2024-02")].error, "February is broken"
    )
    # The other partitions carry on.
    assert_equal(res.status("label", "2024-01"), SUCCEEDED)
    assert_equal(res.status("label", "2024-03"), SUCCEEDED)
    # A gather over a failed partition is skipped, and so is what follows it.
    assert_equal(res.status("total", ""), SKIPPED)
    assert_true("label 2024-02" in res.steps[res.find("total", "")].error)
    assert_equal(res.status("twice", ""), SKIPPED)
    with assert_raises(contains="skipped"):
        _ = res.output[Int]("total")


def test_first_gather_takes_partitions() raises:
    var res = Counts.run(parse_months("2011-01..2024-12"))
    assert_true(res.succeeded())
    assert_equal(res.output[Int]("count"), 168)


def test_describe() raises:
    var d = Labels.describe()
    assert_true(d.startswith('{"pipeline": "labels", "registry_hash": "0x'))
    assert_true('"name": "key", "kind": "mapped", "after": null' in d)
    assert_true('"name": "total", "kind": "gather", "after": "label"' in d)
    assert_true('"input": "dista.seq.Seq[test_steps.Labeled]"' in d)
    assert_true('"function": "test_steps::label(orca::ctx::Ctx,' in d)


def test_registry_hash() raises:
    # Stable within a binary, and different for a different DAG.
    assert_equal(Labels.registry_hash(), Labels.registry_hash())
    assert_true(Labels.registry_hash() != Counts.registry_hash())
    comptime Fewer = Pipeline["labels", Key, Label, Total]
    assert_true(Labels.registry_hash() != Fewer.registry_hash())
    assert_true(hex(Labels.registry_hash()) in Labels.describe())


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
