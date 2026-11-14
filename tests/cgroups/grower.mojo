"""The pipeline of tests/cgroups.sh: one step that holds `GROW_MB` (300) MiB,
every page written, for a second (`GROW_SECONDS`), so a run whose cgroup
limit is under that is killed by the kernel and its retry, with a bigger
limit, isn't. tests/run_nice.sh holds it longer, to look at its process."""

from std.os import getenv
from std.time import perf_counter_ns

from morca import Ctx, Mapped, Month, Monthly, Pipeline, Step, morca_main


def grow(mut ctx: Ctx, m: Month) raises -> Int:
    """Holds `GROW_MB` MiB for `GROW_SECONDS` (1)."""
    var mb = Int(getenv("GROW_MB", "300"))
    var secs = Float64(getenv("GROW_SECONDS", "1"))
    var b = List[UInt8](length=mb * 1024 * 1024, fill=1)
    var t0 = Int(perf_counter_ns())
    var x = 0
    while Float64(Int(perf_counter_ns()) - t0) / 1e9 < secs:
        for i in range(0, len(b), 4096):
            x += Int(b[i])
            b[i] = UInt8(x & 0xFF)
    ctx.log(String("held ", mb, " MiB for ", m))
    return x


comptime Grow = Step["grow", Month, Int, grow, Mapped]
comptime Grower = Pipeline["grower", Grow, partitions=Monthly["2024-01"]]


def main() raises:
    morca_main[Grower]()
