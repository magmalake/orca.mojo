"""A run's CPU priority (#229): runs may use every idle core, but the
scheduler, the UI and Postgres win when the cores are busy.

- **In a cgroup** (`morca._internal.cgroup`, where the host delegates the
  `cpu` controller): the run's cgroup has `cpu.weight`
  `MORCA_RUN_CPU_WEIGHT` (20; the scheduler's leaf has the kernel's
  default, 100), and with a CPU grant (`morca._internal.resources`)
  `cpu.max` at that many cores, so it can't exceed its grant either.
- **Otherwise** (macOS, no delegation, `MORCA_CGROUPS=0`, or no `cpu`
  controller): the run's process starts at `nice` `MORCA_RUN_NICE` (10; 0:
  off), which everything it starts inherits, dista's local workers
  included. A control-placed run keeps `MORCA_CONTROL_NICE`.

`morca worker` does the same for the runs it starts on its own host.
"""

from std.os import getenv

comptime CPU_PERIOD_US = 100000
"""`cpu.max`'s period: the kernel's default, 100 ms."""


def _env_int(
    name: StaticString, default: Int, least: Int, most: Int
) raises -> Int:
    var s = getenv(name)
    if not s:
        return default
    try:
        var n = Int(s)
        if n >= least and n <= most:
            return n
    except:
        pass
    raise Error(
        "morca: ",
        name,
        " must be a number from ",
        least,
        " to ",
        most,
        ", not ",
        s,
    )


@fieldwise_init
struct CpuKnobs(ImplicitlyCopyable, Movable):
    """The scheduler's (or worker's) settings for its runs' CPU priority."""

    var nice: Int
    """`MORCA_RUN_NICE` (10): the `nice` a run without a `cpu.weight`
    starts at; 0: off."""
    var weight: Int
    """`MORCA_RUN_CPU_WEIGHT` (20): a run's cgroup's `cpu.weight`, against
    the kernel's default of 100."""

    @staticmethod
    def defaults() -> CpuKnobs:
        return CpuKnobs(10, 20)

    @staticmethod
    def from_env() raises -> CpuKnobs:
        return CpuKnobs(
            _env_int("MORCA_RUN_NICE", 10, 0, 19),
            _env_int("MORCA_RUN_CPU_WEIGHT", 20, 1, 10000),
        )

    @staticmethod
    def settings() -> CpuKnobs:
        """This process's settings, for the pages: `from_env`, or the
        defaults where a setting is malformed (the scheduler refuses to
        start with one, so it isn't what its runs got)."""
        try:
            return CpuKnobs.from_env()
        except:
            return CpuKnobs.defaults()

    def json(self) -> String:
        """`{"cpu_weight": .., "nice": ..}`."""
        return String(
            '{"cpu_weight": ', self.weight, ', "nice": ', self.nice, "}"
        )


def status_text(weight: Int, nice: Int) -> String:
    """`morca status`'s line for the runs' CPU priority."""
    var t = String("run CPU priority: cpu.weight ", weight, " in a cgroup")
    if nice > 0:
        t.write(", else nice ", nice)
    else:
        t.write(", else not lowered (MORCA_RUN_NICE=0)")
    return t^


def cpu_max(cpus: Int) -> String:
    """`cpu.max` for a grant of `cpus` cores; `max` (no limit) for none."""
    if cpus <= 0:
        return "max"
    return String(cpus * CPU_PERIOD_US, " ", CPU_PERIOD_US)
