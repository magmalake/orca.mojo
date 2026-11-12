# expect: a first mapped step takes the pipeline's partition key
from morca import Ctx, Mapped, Month, Pipeline, Step, morca_main
from morca.keyed import Keyed


def label(mut ctx: Ctx, m: Month) raises -> Int:
    return 1


comptime Label = Step["label", Month, Int, label, Mapped]
comptime P = Pipeline["p", Label, partitions=Keyed["keys.logs", "log_id"]]


def main() raises:
    morca_main[P]()
