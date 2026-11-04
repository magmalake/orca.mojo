"""Where the executor records what it does.

`Pipeline.execute` reports every attempt it starts and finishes, and every
step it skips, to a `Store`; a step's reads and commits reach the same store
through the `Recorder` its `Ctx` holds. `MemoryStore` only counts attempts,
for a run that lives as long as its process; `orca.state.PgStore` writes them
to Postgres, so a run can be resumed by another process.
"""


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
