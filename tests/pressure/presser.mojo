"""The pipeline of tests/pressure.sh: one step that writes the memory
pressure file its run was given (`ORCA_MEMORY_PRESSURE_FILE`) and what
`orca.memory_pressure` reads from it, then takes a while, so the test can
see which runs overlap.

For a month, the step writes `$PRESSER_OUT/<month>.env` with the file's
path (`-` when unset) and `some avg10` as read (`-` for none), then sleeps
`PRESSER_SLEEP` (2) seconds.
"""

from std.os import getenv
from std.time import sleep

from orca import (
    Ctx,
    Mapped,
    Month,
    Monthly,
    Pipeline,
    Step,
    memory_pressure,
    orca_main,
)


def presser(mut ctx: Ctx, m: Month) raises -> Int:
    var out = getenv("PRESSER_OUT")
    if out:
        var file = getenv("ORCA_MEMORY_PRESSURE_FILE")
        var p = memory_pressure()
        with open(String(out, "/", m, ".env"), "w") as f:
            f.write(
                "ORCA_MEMORY_PRESSURE_FILE=",
                file if file else String("-"),
                "\nsome_avg10=",
                String(p.value().some.avg10) if p else String("-"),
                "\n",
            )
    sleep(Float64(getenv("PRESSER_SLEEP", "2")))
    return m.month


comptime Presser = Step["presser", Month, Int, presser, Mapped]
comptime Pressers = Pipeline["presser", Presser, partitions=Monthly["2024-01"]]


def main() raises:
    orca_main[Pressers]()
