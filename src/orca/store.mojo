"""Where the executor records what it does.

`Pipeline.execute` reports every attempt it starts and finishes, and every
step it skips, to a `Store`. `MemoryStore` only counts attempts, for a run
that lives as long as its process; `orca.state.PgStore` writes them to
Postgres, so a run can be resumed by another process.
"""


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
