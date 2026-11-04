"""Where the executor records what it does.

`Pipeline.execute` reports every attempt it starts and finishes, and every
step it skips, to a `Store`; a step's reads and commits reach the same store
through the `Recorder` its `Ctx` holds. `MemoryStore` only counts attempts,
for a run that lives as long as its process; `orca.state.PgStore` writes them
to Postgres, so a run can be resumed by another process.

`RunStore` is a `Store` that also keeps runs: the rest of what a pipeline
binary (`orca.cli`) asks of persistence, from registering the pipeline to
ending its run, so the binary depends on this trait and nothing else.
The scheduler and the UI query `orca.state.State`'s SQL directly.
"""

from std.ffi import external_call

from dista.codec import decode

comptime RUNNING = "running"
comptime CANCELLED = "cancelled"


comptime SUCCEEDED = "succeeded"
comptime FAILED = "failed"
comptime SKIPPED = "skipped"


struct StepRun(Copyable, Movable):
    """One step for one partition (or for the run, for a gather step)."""

    var step: String
    var partition: String
    """Empty for a gather step."""
    var status: String
    var output: List[UInt8]
    """The encoded output, when the step succeeded."""
    var error: String
    var attempt: Int
    """The number of the attempt that settled it; 0 for a skipped step."""

    def __init__(
        out self,
        step: String,
        partition: String,
        status: String,
        var output: List[UInt8],
        error: String,
        attempt: Int = 0,
    ):
        self.step = step
        self.partition = partition
        self.status = status
        self.output = output^
        self.error = error
        self.attempt = attempt


struct RunResult(Movable):
    """What a run did: every step and partition, in the order they ran."""

    var steps: List[StepRun]

    def __init__(out self):
        self.steps = List[StepRun]()

    def succeeded(self) -> Bool:
        for i in range(len(self.steps)):
            if self.steps[i].status != SUCCEEDED:
                return False
        return True

    def find(self, step: StringSlice, partition: StringSlice) -> Int:
        """The index of `step` for `partition`, or -1."""
        for i in range(len(self.steps)):
            ref s = self.steps[i]
            if s.step == step and s.partition == partition:
                return i
        return -1

    def status(
        self, step: StringSlice, partition: StringSlice
    ) raises -> String:
        var i = self.find(step, partition)
        if i < 0:
            raise Error("no step ", step, " for '", partition, "'")
        return self.steps[i].status

    def output[
        T: Movable
    ](self, step: StringSlice, partition: StringSlice = "") raises -> T:
        """The decoded output of `step` for `partition`."""
        var i = self.find(step, partition)
        if i < 0:
            raise Error("no step ", step, " for '", partition, "'")
        ref s = self.steps[i]
        if s.status != SUCCEEDED:
            raise Error("step ", step, " for '", partition, "' ", s.status)
        var pos = 0
        return decode[T](Span(s.output), pos)


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
struct StepInfo(Copyable, Movable):
    var step: String
    var partition: String
    var status: String
    var attempts: Int
    var error: String
    var log: String
    """The latest attempt's log file, or empty."""


def process_owner() -> String:
    """`host:pid`, naming this process as a run's owner."""
    var buf = List[UInt8](length=256, fill=0)
    _ = external_call["gethostname", Int32](buf.unsafe_ptr(), Int(255))
    var n = 0
    while n < 255 and buf[n] != 0:
        n += 1
    var host = String(from_utf8_lossy=Span(buf)[:n])
    return String(host, ":", external_call["getpid", Int32]())


@fieldwise_init
struct TableSnapshot(Copyable, Movable):
    """A table, by its location, at a snapshot."""

    var table: String
    var snapshot_id: Int64
    """-1 for a table that had no snapshot yet."""
    var dataset: String
    """The table's dataset name, `namespace.table`, or empty for a table
    outside the warehouse."""


def _record_nothing(store: Int, commit: Bool, t: TableSnapshot) raises:
    pass


def _record_in[S: Store](store: Int, commit: Bool, t: TableSnapshot) raises:
    Pointer[S, MutAnyOrigin](unsafe_from_address=store)[].record(commit, t)


struct Recorder(Movable):
    """Records an attempt's reads and commits in the store it came from, as
    they happen, so a process killed right after a commit still leaves it on
    record.

    It holds the store by address, as `Ctx` holds the run's dista runtime:
    `Ctx` isn't generic over the store, and the store outlives every `Ctx`
    `Pipeline.execute` makes from it."""

    var _store: Int
    """The store's address; 0 when nothing is recorded."""
    var _record: def(Int, Bool, TableSnapshot) raises thin -> None

    def __init__(out self):
        """A recorder that records nothing."""
        self._store = 0
        self._record = _record_nothing

    def __init__[S: Store](out self, mut store: S):
        """A recorder that records in `store`."""
        self._store = Int(Pointer(to=store))
        self._record = _record_in[S]

    def read(mut self, t: TableSnapshot) raises:
        self._record(self._store, False, t)

    def commit(mut self, t: TableSnapshot) raises:
        self._record(self._store, True, t)


trait Store:
    def start_attempt(
        mut self,
        step: String,
        partition: String,
        ord: Int,
        input: Span[UInt8, _],
    ) raises -> Int:
        """Records that `step` for `partition` is starting, with its encoded
        input, and returns the attempt's number (1 for the first). `ord` is the
        step's place in the run order."""
        ...

    def finish_attempt(
        mut self,
        step: String,
        partition: String,
        attempt: Int,
        status: String,
        output: Span[UInt8, _],
        error: String,
    ) raises:
        """Records how attempt `attempt` ended: `succeeded` with its encoded
        output, or `failed` with its error."""
        ...

    def skip(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        """Records that `step` for `partition` didn't run, and why."""
        ...

    def pinned_reads(
        mut self, step: String, partition: String
    ) raises -> Dict[String, Int64]:
        """The table snapshots earlier attempts of `step` for `partition`
        read, each table at its first attempt's snapshot."""
        ...

    def record(mut self, commit: Bool, t: TableSnapshot) raises:
        """Records that the attempt last started read `t` (or, if `commit`,
        committed it). What a `Recorder` from this store calls."""
        ...

    def recorder(mut self) raises -> Recorder:
        """What records the reads and commits of the attempt just started."""
        return Recorder(self)

    def set_log(
        mut self, step: String, partition: String, attempt: Int, path: String
    ) raises:
        """Records where attempt `attempt` writes its log."""
        ...


trait RunStore(Store):
    """A `Store` that keeps runs too: everything a pipeline binary records.

    One process is a run's writer, its owner (`process_owner`): it creates
    or claims the run, `own`s it, which points the `Store` methods at it,
    keeps its heartbeat fresh, and ends it with `finish_run`. A store that
    other processes share stops an owner that lost its run, or whose run was
    asked to cancel, at its next attempt."""

    def migrate(mut self) raises:
        """Creates the store's schema, or brings it up to date."""
        ...

    def register(
        mut self, pipeline: String, registry_hash: String, describe: String
    ) raises:
        """Records a pipeline, or updates it to this binary's DAG."""
        ...

    def scheduled_hash(mut self, pipeline: String) raises -> String:
        """The registry hash `orca register` gave `pipeline`, or empty if it
        wasn't registered with a binary."""
        ...

    def version(
        mut self,
        pipeline: String,
        registry_hash: String,
        describe: String,
        image: String,
    ) raises -> Int:
        """The version of `pipeline` that code `registry_hash`, from the
        source `describe` names, in `image` is; recorded the first time."""
        ...

    def create_run(
        mut self,
        pipeline: String,
        registry_hash: String,
        partitions: String,
        trigger: String,
        owner: String,
    ) raises -> Int:
        """A new run, already claimed by `owner`."""
        ...

    def claim(mut self, run: Int, owner: String, stale_seconds: Float64) raises:
        """Makes `owner` the run's writer if no live process holds it: the
        run is `queued`, `failed`, or `running` with a heartbeat older than
        `stale_seconds` or cleared. Raises otherwise, saying why."""
        ...

    def ran_version(mut self, run: Int, version: Int) raises:
        """Records that `run` is running `version`'s code."""
        ...

    def interrupt(mut self, run: Int) raises -> Int:
        """Marks attempts left `running` by a process that died as failed,
        and returns how many there were."""
        ...

    def prior(mut self, run: Int) raises -> RunResult:
        """The run's succeeded steps, with their outputs, for resuming it."""
        ...

    def run_info(mut self, run: Int) raises -> RunInfo:
        """The run; raises if there's no such run."""
        ...

    def runs(mut self, limit: Int) raises -> List[RunInfo]:
        """The latest `limit` runs, newest first."""
        ...

    def steps(mut self, run: Int) raises -> List[StepInfo]:
        """A run's steps in run order, with their attempt counts."""
        ...

    def own(mut self, run: Int, owner: String, registry_hash: String) raises:
        """Records this process's attempts, as `owner` with code
        `registry_hash`, against `run`, which it has created or claimed."""
        ...

    def start_heartbeat(mut self, interval: Float64) raises:
        """Keeps the owned run's heartbeat fresh, every `interval` seconds,
        from a thread, until `stop_heartbeat`."""
        ...

    def stop_heartbeat(mut self):
        """Stops the heartbeat thread, if it's going, and waits for it."""
        ...

    def finish_run(mut self, status: String) raises:
        """Ends the owned run with `status` and clears its heartbeat, so it
        can be resumed at once if it failed."""
        ...

    def cancel_requested(mut self) raises -> Bool:
        """Whether the owned run was asked to cancel."""
        ...


struct MemoryStore(Movable, Store):
    """Keeps nothing but the attempt counts."""

    var _attempts: Dict[String, Int]

    def __init__(out self):
        self._attempts = Dict[String, Int]()

    def start_attempt(
        mut self,
        step: String,
        partition: String,
        ord: Int,
        input: Span[UInt8, _],
    ) raises -> Int:
        var key = String(step, "\t", partition)
        var n = self._attempts.get(key, 0) + 1
        self._attempts[key] = n
        return n

    def finish_attempt(
        mut self,
        step: String,
        partition: String,
        attempt: Int,
        status: String,
        output: Span[UInt8, _],
        error: String,
    ) raises:
        pass

    def skip(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        pass

    def pinned_reads(
        mut self, step: String, partition: String
    ) raises -> Dict[String, Int64]:
        # Within one process the executor carries reads from attempt to
        # attempt itself; there are no earlier processes to ask.
        return Dict[String, Int64]()

    def record(mut self, commit: Bool, t: TableSnapshot) raises:
        pass

    def set_log(
        mut self, step: String, partition: String, attempt: Int, path: String
    ) raises:
        pass
