"""`Ctx`: what a step gets besides its input.

The step's identity (pipeline, run, step, partition, attempt, the code's
registry hash), a logger, and what makes reruns safe: the table snapshots
the step's first attempt read, which every later attempt reads again, and
the record of what each attempt read and committed. `orca.tables` reads and
commits Iceberg tables through it, and checks the datasets a step touches
against the ones it declared (`reads=` and `writes=` on its `Step`). `runtime` is the run's dista runtime, for a
pipeline with `tasks=`.

`log` writes to stdout and, when `ORCA_LOGS` is set, appends to the attempt's
log file as well (`orca.logs.attempt_log`).
"""


from std.os import getenv
from std.time import perf_counter_ns, sleep

from postgres import Connection, Params

from orca.logs import mask, utc_now
from orca.tasks import TaskSet, task_list


@fieldwise_init
struct TableSnapshot(Copyable, Movable):
    """A table, by its location, at a snapshot."""

    var table: String
    var snapshot_id: Int64
    """-1 for a table that had no snapshot yet."""
    var dataset: String
    """The table's dataset name, `namespace.table`, or empty for a table
    outside the warehouse."""


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
        """Retried on a new connection for `ORCA_DB_RETRY_SECONDS`, as
        `orca.state.PgStore` is, so a blip of the server doesn't fail the
        step that read or committed."""
        if not self.url:
            return
        var budget: Float64
        try:
            budget = Float64(
                getenv(
                    "ORCA_DB_RETRY_SECONDS", getenv("ORCA_STALE_SECONDS", "30")
                )
            )
        except:
            budget = 30.0
        var started = perf_counter_ns()
        var wait = 0.25
        while True:
            try:
                self._insert_once(into, t)
                return
            except e:
                var spent = Float64(perf_counter_ns() - started) / 1e9
                if spent + wait > budget:
                    raise e^
                print(
                    "orca: Postgres failed (",
                    e,
                    "); retrying in ",
                    wait,
                    " s",
                    sep="",
                )
                sleep(wait)
                wait = min(wait * 2, 4.0)
                self._conn = None

    def _insert_once(mut self, into: StaticString, t: TableSnapshot) raises:
        if not self._conn:
            self._conn = Connection(self.url)
        _ = self._conn.value().execute(
            String(
                "INSERT INTO orca.",
                into,
                " (attempt, table_name, snapshot_id, dataset) VALUES ($1, $2,",
                " $3, nullif($4, ''))",
            ),
            Params()
            .int64(self.attempt_id)
            .text(t.table)
            .int64(t.snapshot_id)
            .text(t.dataset),
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
    var partitions: String
    """The keys this step's commits hold, comma-separated: its partition
    for a mapped step, the run's keys for a gather step."""
    var declared_reads: List[String]
    """The datasets the step declared it reads."""
    var declared_writes: List[String]
    var attempt: Int
    var code: String
    """The pipeline's registry hash, in hex."""
    var pinned: Dict[String, Int64]
    """Table location to the snapshot an earlier attempt of this step read."""
    var reads: List[TableSnapshot]
    var commits: List[TableSnapshot]
    var recorder: Recorder
    var log_path: String
    """The attempt's log file; empty when logs go to stdout only."""
    var _runtime: Int
    """The run's dista runtime, by address; 0 if it has none."""
    var _runtime_tasks: String
    """The runtime's task names, one per line: what `runtime` checks a
    requested task set against (dista runtimes all reflect to one name)."""

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
        log_path: String = "",
        runtime: Int = 0,
        runtime_tasks: String = "",
        partitions: String = "",
        var declared_reads: List[String] = List[String](),
        var declared_writes: List[String] = List[String](),
    ):
        self.pipeline = pipeline
        self.run = run
        self.step = step
        self.partition = partition
        self.partitions = partitions if partitions else partition
        self.declared_reads = declared_reads^
        self.declared_writes = declared_writes^
        self.attempt = attempt
        self.code = code
        self.pinned = pinned^
        self.reads = List[TableSnapshot]()
        self.commits = List[TableSnapshot]()
        self.recorder = recorder^
        self.log_path = log_path
        self._runtime = runtime
        self._runtime_tasks = runtime_tasks

    def log(self, msg: String):
        """Writes a line tagged with the step and partition to stdout, and
        with a timestamp to the attempt's log file."""
        var tag = String("[", self.step)
        if self.partition:
            tag.write(" ", self.partition)
        tag.write("] ")
        print(tag, mask(msg), sep="")
        self.log_to_file(msg)

    def log_to_file(self, msg: String):
        """Writes a line to the attempt's log file only, if it has one."""
        if not self.log_path:
            return
        var tag = String("[", self.step)
        if self.partition:
            tag.write(" ", self.partition)
        tag.write("] ")
        try:
            with open(self.log_path, "a") as f:
                f.write(utc_now(), " ", tag, mask(msg), "\n")
        except e:
            print("orca: can't write ", self.log_path, ": ", e, sep="")

    def runtime[T: TaskSet](self) raises -> ref[MutAnyOrigin] T.RT:
        """The run's dista runtime, for a pipeline declared with `tasks=T`."""
        if self._runtime == 0:
            raise Error("orca: this run has no dista runtime (no tasks=)")
        var asked = task_list[T]()
        if self._runtime_tasks != asked:
            raise Error(
                "orca: the run's runtime has tasks\n",
                self._runtime_tasks,
                "not\n",
                asked,
            )
        return Pointer[T.RT, MutAnyOrigin](unsafe_from_address=self._runtime)[]

    def pin(
        mut self, table: String, current: Int64, dataset: String = ""
    ) raises -> Int64:
        """The snapshot of `table` this step reads: the one an earlier attempt
        read, or else `current`, which is then recorded as read."""
        for r in self.reads:
            if r.table == table:
                return r.snapshot_id
        var id = self.pinned.get(table, current)
        var t = TableSnapshot(table, id, dataset)
        self.recorder.read(t)
        self.reads.append(t^)
        return id

    def committed(
        mut self, table: String, snapshot_id: Int64, dataset: String = ""
    ) raises:
        """Records that this attempt committed (or found it had committed)
        `snapshot_id` to `table`."""
        var t = TableSnapshot(table, snapshot_id, dataset)
        self.recorder.commit(t)
        self.commits.append(t^)

    def commits_to(self, table: String) -> Int:
        """How many commits to `table` this attempt has made so far."""
        var n = 0
        for c in self.commits:
            if c.table == table:
                n += 1
        return n

    def check_declared(self, dataset: String, write: Bool) raises:
        """Raises unless the step declared it reads (or writes) `dataset`;
        a table outside the warehouse (`dataset` empty) needs nothing."""
        if not dataset:
            return
        if dataset in (self.declared_writes if write else self.declared_reads):
            return
        raise Error(
            "orca: step ",
            self.step,
            " of ",
            self.pipeline,
            " writes " if write else " reads ",
            dataset,
            ", which it doesn't declare in ",
            "writes=" if write else "reads=",
        )

    def summary(self, table: String) -> Dict[String, String]:
        """The snapshot summary properties of this step's next commit to
        `table`: who made it, from which code, over which partitions, having
        read what."""
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
        s["orca.partitions"] = self.partitions
        s["orca.attempt"] = String(self.attempt)
        s["orca.commit"] = String(self.commits_to(table))
        s["orca.code"] = self.code
        s["orca.reads"] = reads^
        return s^

    def made(self, table: String, summary: Dict[String, String]) -> Bool:
        """Whether a snapshot with `summary` is this step's next commit to
        `table`, made by any attempt of this run: the same pipeline, run,
        step, partition and commit number. Never for an in-memory run.

        Every key must be there: a missing one doesn't match an empty value,
        such as a gather step's partition (found by fuzz/fuzz_summary.mojo).
        """
        if self.run <= 0:
            return False
        return (
            _has(summary, "orca.pipeline", self.pipeline)
            and _has(summary, "orca.run", String(self.run))
            and _has(summary, "orca.step", self.step)
            and _has(summary, "orca.partition", self.partition)
            and _has(summary, "orca.commit", String(self.commits_to(table)))
        )


def _has(summary: Dict[String, String], key: String, value: String) -> Bool:
    var v = summary.get(key)
    return Bool(v) and v.value() == value
