"""`Ctx`: what a step gets besides its input.

The step's identity (pipeline, run, step, partition, attempt, the code's
registry hash), a logger, and what makes reruns safe: the table snapshots
the step's first attempt read, which every later attempt reads again, and
the record of what each attempt read and committed. `orca.tables` reads and
commits Iceberg tables through it; the dista runtime joins it in a later PR
(see DESIGN.md).
"""

from postgres import Connection, Params


@fieldwise_init
struct TableSnapshot(Copyable, Movable):
    """A table, by its location, at a snapshot."""

    var table: String
    var snapshot_id: Int64
    """-1 for a table that had no snapshot yet."""


struct Recorder(Movable):
    """Writes an attempt's reads and commits to Postgres as they happen, so
    a process killed right after a commit still leaves it on record."""

    var url: String
    """Empty when nothing is recorded (an in-memory run)."""
    var attempt_id: Int64
    var _conn: Optional[Connection]

    def __init__(out self, url: String = "", attempt_id: Int64 = 0):
        self.url = url
        self.attempt_id = attempt_id
        self._conn = None

    def _insert(mut self, into: StaticString, t: TableSnapshot) raises:
        if not self.url:
            return
        if not self._conn:
            self._conn = Connection(self.url)
        _ = self._conn.value().execute(
            String(
                "INSERT INTO orca.",
                into,
                " (attempt, table_name, snapshot_id) VALUES ($1, $2, $3)",
            ),
            Params().int64(self.attempt_id).text(t.table).int64(t.snapshot_id),
        )

    def read(mut self, t: TableSnapshot) raises:
        self._insert("reads", t)

    def commit(mut self, t: TableSnapshot) raises:
        self._insert("commits", t)


struct Ctx(Movable):
    """The run, step and partition a step is running for."""

    var pipeline: String
    var run: Int
    """0 for an in-memory run: nothing is recorded, and commits aren't
    recognised across processes."""
    var step: String
    var partition: String
    """The partition key, e.g. `2024-01`; empty for a gather step."""
    var attempt: Int
    var code: String
    """The pipeline's registry hash, in hex."""
    var pinned: Dict[String, Int64]
    """Table location to the snapshot an earlier attempt of this step read."""
    var reads: List[TableSnapshot]
    var commits: List[TableSnapshot]
    var recorder: Recorder

    def __init__(
        out self,
        pipeline: String,
        run: Int,
        step: String,
        partition: String,
        attempt: Int = 1,
        code: String = "",
        var pinned: Dict[String, Int64] = Dict[String, Int64](),
        var recorder: Recorder = Recorder(),
    ):
        self.pipeline = pipeline
        self.run = run
        self.step = step
        self.partition = partition
        self.attempt = attempt
        self.code = code
        self.pinned = pinned^
        self.reads = List[TableSnapshot]()
        self.commits = List[TableSnapshot]()
        self.recorder = recorder^

    def log(self, msg: String):
        """Writes a line tagged with the step and partition."""
        if self.partition:
            print("[", self.step, " ", self.partition, "] ", msg, sep="")
        else:
            print("[", self.step, "] ", msg, sep="")

    def pin(mut self, table: String, current: Int64) raises -> Int64:
        """The snapshot of `table` this step reads: the one an earlier attempt
        read, or else `current`, which is then recorded as read."""
        for r in self.reads:
            if r.table == table:
                return r.snapshot_id
        var id = self.pinned.get(table, current)
        var t = TableSnapshot(table, id)
        self.recorder.read(t)
        self.reads.append(t^)
        return id

    def committed(mut self, table: String, snapshot_id: Int64) raises:
        """Records that this attempt committed (or found it had committed)
        `snapshot_id` to `table`."""
        var t = TableSnapshot(table, snapshot_id)
        self.recorder.commit(t)
        self.commits.append(t^)

    def commits_to(self, table: String) -> Int:
        """How many commits to `table` this attempt has made so far."""
        var n = 0
        for c in self.commits:
            if c.table == table:
                n += 1
        return n

    def summary(self, table: String) -> Dict[String, String]:
        """The snapshot summary properties of this step's next commit to
        `table`: who made it, from which code, having read what."""
        var reads = String()
        for i in range(len(self.reads)):
            reads.write(
                "," if i > 0 else "",
                self.reads[i].table,
                "@",
                self.reads[i].snapshot_id,
            )
        var s = Dict[String, String]()
        s["orca.pipeline"] = self.pipeline
        s["orca.run"] = String(self.run)
        s["orca.step"] = self.step
        s["orca.partition"] = self.partition
        s["orca.attempt"] = String(self.attempt)
        s["orca.commit"] = String(self.commits_to(table))
        s["orca.code"] = self.code
        s["orca.reads"] = reads^
        return s^

    def made(self, table: String, summary: Dict[String, String]) -> Bool:
        """Whether a snapshot with `summary` is this step's next commit to
        `table`, made by any attempt of this run: the same pipeline, run,
        step, partition and commit number. Never for an in-memory run."""
        if self.run <= 0:
            return False
        return (
            summary.get("orca.pipeline", "") == self.pipeline
            and summary.get("orca.run", "") == String(self.run)
            and summary.get("orca.step", "") == self.step
            and summary.get("orca.partition", "") == self.partition
            and summary.get("orca.commit", "") == String(self.commits_to(table))
        )
