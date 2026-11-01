"""Where the executor records what it does.

`Pipeline.execute` reports every attempt it starts and finishes, and every
step it skips, to a `Store`. `MemoryStore` only counts attempts, for a run
that lives as long as its process; `orca.state.PgStore` writes them to
Postgres, so a run can be resumed by another process.
"""


from orca.ctx import Recorder


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

    def recorder(mut self) raises -> Recorder:
        """What records the reads and commits of the attempt just started."""
        ...

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

    def recorder(mut self) raises -> Recorder:
        return Recorder()

    def set_log(
        mut self, step: String, partition: String, attempt: Int, path: String
    ) raises:
        pass
