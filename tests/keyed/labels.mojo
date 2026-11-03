"""A keyed toy: a mapped step per log name, then a gather over the run's.

`label` returns the length of the name, `score` adds them up and prints how
many names it saw.
"""

from dista import Seq
from orca import Ctx, Gather, Mapped, Name, Pipeline, Step, orca_main
from orca.keyed import Keyed


def label(mut ctx: Ctx, n: Name) raises -> Int:
    return n.text.byte_length()


def score(mut ctx: Ctx, lens: Seq[Int]) raises -> Int:
    var t = 0
    for i in range(len(lens)):
        t += lens[i]
    ctx.log(String(len(lens), " names, ", t, " bytes"))
    return t


comptime Label = Step["label", Name, Int, label, Mapped]
comptime Score = Step["score", Seq[Int], Int, score, Gather, after=Label]
comptime Labels = Pipeline[
    "labels",
    Label,
    Score,
    partitions=Keyed["keys.logs", "log_id"],
    keys_per_run=10,
]


def main() raises:
    orca_main[Labels]()
