"""A daily pipeline that writes the keys a keyed sensor runs: each day adds
two names to keys.repos, `day-<DD>-a` and `day-<DD>-b`, and a row to
lake.days. tests/keyed/history.mojo is keyed on keys.repos."""

from morca import Ctx, Daily, Day, Mapped, Pipeline, Step, morca_main
from morca.tables import append
from repo_rows import repo_dataset, repo_rows


def find(mut ctx: Ctx, d: Day) raises -> Int64:
    var dd = String(d)[byte=8:]
    var t = repo_dataset(ctx, "keys.repos")
    _ = append(
        ctx,
        t,
        repo_rows([String("day-", dd, "-a"), String("day-", dd, "-b")]),
    )
    var days = repo_dataset(ctx, "lake.days")
    return append(ctx, days, repo_rows([String(d)]))


comptime Find = Step[
    "find", Day, Int64, find, Mapped, writes=["keys.repos", "lake.days"]
]
comptime Repos = Pipeline["repos", Find, partitions=Daily["2024-06-01"]]


def main() raises:
    morca_main[Repos]()
