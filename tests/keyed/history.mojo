"""A keyed sensor over keys.repos, which the daily pipeline
tests/keyed/repos.mojo writes: a run for the names each new snapshot adds.
`hist` reads keys.repos (so lineage goes through it) and writes a row per
name to lake.history."""

from morca import Ctx, Mapped, Name, Pipeline, Step, morca_main
from morca.keyed import Keyed
from morca.tables import append, table
from repo_rows import repo_count, repo_dataset, repo_rows


def hist(mut ctx: Ctx, n: Name) raises -> Int64:
    var seen = repo_count(ctx, table(ctx, "keys.repos"))
    ctx.log(String(n, " of ", seen, " rows"))
    var t = repo_dataset(ctx, "lake.history")
    return append(ctx, t, repo_rows([n.text]))


comptime Hist = Step[
    "hist",
    Name,
    Int64,
    hist,
    Mapped,
    reads=["keys.repos"],
    writes=["lake.history"],
]
comptime History = Pipeline[
    "history",
    Hist,
    partitions=Keyed["keys.repos", "repo"],
    keys_per_run=10,
    sensor=True,
]


def main() raises:
    morca_main[History]()
