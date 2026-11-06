# expect: a dataset is named namespace.table
from orca import Ctx, Mapped, Month, Pipeline, Step


def key(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month


comptime Key = Step["key", Month, Int, key, Mapped, replaces=["trips"]]
comptime P = Pipeline["p", Key]


def main() raises:
    _ = P.describe()
