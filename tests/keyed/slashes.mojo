"""A keyed pipeline over names with a `/`, `org/repo-<n>` in keys.slashed:
`touch` logs its name and fails those listed in ORCA_SLASH_FAIL."""

from std.os import getenv

from orca import Ctx, Mapped, Name, Pipeline, Step, orca_main
from orca.keyed import Keyed


def touch(mut ctx: Ctx, n: Name) raises -> Int:
    for bad in getenv("ORCA_SLASH_FAIL").split(","):
        if bad == n.text:
            raise Error("told to fail ", n)
    ctx.log(String("touched ", n))
    return 1


comptime Touch = Step["touch", Name, Int, touch, Mapped]
comptime Slashes = Pipeline[
    "slashes",
    Touch,
    partitions=Keyed["keys.slashed", "log_id"],
    keys_per_run=10,
]


def main() raises:
    orca_main[Slashes]()
