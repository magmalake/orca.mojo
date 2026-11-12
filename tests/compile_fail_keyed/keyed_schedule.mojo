# expect: a keyed pipeline has no calendar for a schedule to fill
from morca import Cron, Ctx, Mapped, Name, Pipeline, Step, morca_main
from morca.keyed import Keyed


def label(mut ctx: Ctx, n: Name) raises -> Int:
    return 1


comptime Label = Step["label", Name, Int, label, Mapped]
comptime P = Pipeline[
    "p",
    Label,
    partitions=Keyed["keys.logs", "log_id"],
    schedule=Cron["0 3 * * *"],
]


def main() raises:
    morca_main[P]()
