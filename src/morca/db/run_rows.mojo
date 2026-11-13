"""The rows the run stores' queries return, the same for both dialects
(`morca.db.runs` and `morca.db.steps` on Postgres, `morca.db.sqlite_runs`
on SQLite). Declared here, with no import, so that `morca.store` takes them
from here (`from morca.store import RunInfo` keeps working) without
depending on either database's library."""


@fieldwise_init
struct RunInfo(Copyable, Movable):
    var id: Int
    var pipeline: String
    var registry_hash: String
    var partitions: String
    var status: String
    var owner: String
    var error: String
    """Why the run ended without its steps saying so, or empty."""


@fieldwise_init
struct Usage(ImplicitlyCopyable, Movable):
    """What an attempt's processes used (`morca.usage`): the peak resident
    bytes of its process tree, its CPU seconds, and the most cores it kept
    busy over a sample's interval. Each is -1 where it isn't known: a run
    on EC2 or Kubernetes, one no scheduler or worker started, or on macOS
    the peak cores, which only samples give."""

    var peak_rss_bytes: Int
    var cpu_seconds: Float64
    var peak_cpus: Float64

    @staticmethod
    def unknown() -> Usage:
        return Usage(-1, -1.0, -1.0)

    def known(self) -> Bool:
        return self.peak_rss_bytes >= 0 or self.cpu_seconds >= 0


@fieldwise_init
struct StepInfo(Copyable, Movable):
    var step: String
    var partition: String
    var status: String
    var attempts: Int
    var error: String
    var log: String
    """The latest attempt's log file, or empty."""
    var usage: Usage
    """What the latest attempt used, as far as it's known."""


@fieldwise_init
struct Succeeded(Copyable, Movable):
    """A step of a run that succeeded, with its encoded output: what
    resuming the run starts from (`RunStore.prior`)."""

    var step: String
    var partition: String
    var output: List[UInt8]


@fieldwise_init
struct Fence(Copyable, Movable):
    """What a run's `fence` checks before a commit: who holds the run, its
    status, and whether its heartbeat is younger than the limit asked."""

    var owner: String
    var status: String
    var fresh: Bool


@fieldwise_init
struct Owned(Copyable, Movable):
    """An owner check of a run (`still_owner`): whether this process still
    owns it, running, and whether it was asked to cancel."""

    var owned: Bool
    var cancel_requested: Bool
