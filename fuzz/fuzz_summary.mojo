"""Fuzz: `Ctx.made`, which decides that a snapshot is this step's commit and
must not be written again.

A wrong yes loses a commit (the step returns someone else's snapshot); a
wrong no commits twice. Contexts and summaries are built from a small
alphabet so that near-misses are common: `made` must be true exactly when the
pipeline, run, step, partition and commit number all match and the run isn't
an in-memory one (run 0), and a context always recognises its own summary.

    pixi run -e fuzz fuzz-summary
"""

from harness import check, run_harness, save_last

from orca import Ctx

comptime NAMES: List[String] = [String("a"), String("b"), String("")]
comptime FIELDS: List[String] = [
    String("orca.pipeline"),
    String("orca.run"),
    String("orca.step"),
    String("orca.partition"),
    String("orca.commit"),
]


def _pick(data: List[UInt8], mut i: Int) -> Int:
    var v = Int(data[i % len(data)]) if len(data) > 0 else 0
    i += 1
    return v


def target(data: List[UInt8]) raises:
    save_last("summary", data)
    var i = 0
    var names = materialize[NAMES]()
    var ctx = Ctx(
        names[_pick(data, i) % 2],
        _pick(data, i) % 3,
        names[_pick(data, i) % 3],
        names[_pick(data, i) % 3],
    )
    var table = String("t")
    # Some earlier commits, to t or elsewhere.
    for _ in range(_pick(data, i) % 3):
        ctx.committed(table if _pick(data, i) % 2 == 0 else "u", 1)
    check(
        ctx.made(table, ctx.summary(table)) == (ctx.run > 0),
        "a context doesn't recognise its own summary",
    )
    # A summary that differs in some fields, or lacks them.
    var s = ctx.summary(table)
    var fields = materialize[FIELDS]()
    var same = True
    for f in fields:
        var how = _pick(data, i) % 4
        if how == 1:
            _ = s.pop(f)
            same = False
        elif how == 2:
            var v = names[_pick(data, i) % 3]
            if s[f] != v:
                same = False
            s[f] = v
        elif how == 3:
            var v = String(_pick(data, i) % 3)
            if s[f] != v:
                same = False
            s[f] = v
    check(
        ctx.made(table, s) == (same and ctx.run > 0),
        "made disagrees with the fields",
    )


def main() raises:
    var seeds = List[List[UInt8]]()
    seeds.append([0, 1, 0, 0, 0, 0, 0, 0, 0, 0])
    seeds.append([1, 2, 1, 2, 2, 1, 2, 0, 3, 1, 2, 3, 0])
    run_harness[target]("summary", seeds, max_len=64)
