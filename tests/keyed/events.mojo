"""A keyed pipeline writing raw.events, which tests/sensors' monthly
sensor clean reads: registering both must be refused."""

from orca import Ctx, Mapped, Name, Pipeline, Step, orca_main
from orca.keyed import Keyed
from orca.tables import append
from rows import dataset, rows


def event(mut ctx: Ctx, n: Name) raises -> Int64:
    var t = dataset(ctx, "raw.events")
    return append(ctx, t, rows(n.text, 1))


comptime Event = Step[
    "event", Name, Int64, event, Mapped, writes=["raw.events"]
]
comptime Events = Pipeline[
    "events", Event, partitions=Keyed["keys.logs", "log_id"]
]


def main() raises:
    orca_main[Events]()
