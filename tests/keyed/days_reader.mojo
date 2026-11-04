"""A keyed sensor over keys.repos that also reads lake.days, which the daily
pipeline tests/keyed/repos.mojo writes: a day isn't a name, so registering
it must still be refused."""

from orca import Ctx, Mapped, Name, Pipeline, Step, orca_main
from orca.keyed import Keyed
from orca.tables import table
from repo_rows import repo_count


def look(mut ctx: Ctx, n: Name) raises -> Int:
    return repo_count(ctx, table(ctx, "lake.days"))


comptime Look = Step["look", Name, Int, look, Mapped, reads=["lake.days"]]
comptime DaysReader = Pipeline[
    "days_reader", Look, partitions=Keyed["keys.repos", "repo"], sensor=True
]


def main() raises:
    orca_main[DaysReader]()
