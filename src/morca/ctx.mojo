"""`Ctx`: what a step gets besides its input.

The step's identity (pipeline, run, step, partition, attempt, the code's
registry hash), a logger, and what makes reruns safe: the table snapshots
the step's first attempt read, which every later attempt reads again, and
the record of what each attempt read and committed. `morca.tables` reads and
commits Iceberg tables through it, and checks the datasets a step touches
against the ones it declared (`reads=` and `writes=` on its `Step`). `runtime` is the run's dista runtime, for a
pipeline with `tasks=`.

`log` writes to stdout and, when `MORCA_LOGS` is set, appends to the attempt's
log file as well (`morca._internal.logs.attempt_log`).

**Commits are fenced.** A process whose heartbeat went stale while it was
alive (the database unreachable for a while, a paused VM) may have lost its
run to another process, which reruns the step. `morca.tables` calls `fence`
right before each commit to Iceberg: it goes ahead only if this process
still owns the run and its heartbeat is younger than half the stale limit,
so no other process can have claimed the run, nor claim it during the
commit unless the commit takes that long. Otherwise the attempt fails
without committing.
"""


from morca._internal.logs import mask, utc_now
from morca._internal.store import Recorder, TableSnapshot
from morca.tasks import TaskSet, task_list


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
    """The datasets it declared it writes, in `writes=` or `replaces=`."""
    var declared_replaces: List[String]
    """The datasets whose partitions a new run of it replaces (`replaces=`):
    `morca.tables.append` and `append_files` replace rather than append."""
    var attempt: Int
    var code: String
    """The pipeline's registry hash, in hex."""
    var token: String
    """The run's random token (`Store.run_token`), which its commits carry
    beside its number; empty for a run without one."""
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
        var declared_replaces: List[String] = List[String](),
        token: String = "",
    ):
        self.pipeline = pipeline
        self.run = run
        self.step = step
        self.partition = partition
        self.partitions = partitions if partitions else partition
        self.declared_reads = declared_reads^
        self.declared_writes = declared_writes^
        for d in declared_replaces:
            if d not in self.declared_writes:
                self.declared_writes.append(d)
        self.declared_replaces = declared_replaces^
        self.attempt = attempt
        self.code = code
        self.token = token
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
            print("morca: can't write ", self.log_path, ": ", e, sep="")

    def runtime[T: TaskSet](self) raises -> ref[MutAnyOrigin] T.RT:
        """The run's dista runtime, for a pipeline declared with `tasks=T`."""
        if self._runtime == 0:
            raise Error("morca: this run has no dista runtime (no tasks=)")
        var asked = task_list[T]()
        if self._runtime_tasks != asked:
            raise Error(
                "morca: the run's runtime has tasks\n",
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

    def fence(mut self) raises:
        """Raises unless this process may still commit for its run (see
        the module's doc); `morca.tables` calls it before each commit."""
        self.recorder.fence()

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
            "morca: step ",
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
        if self.token:
            s["orca.run-token"] = self.token
        return s^

    def replaces(self, dataset: String) -> Bool:
        """Whether the step declared it replaces `dataset`'s partitions."""
        return Bool(dataset) and dataset in self.declared_replaces

    def same_run(self, summary: Dict[String, String]) -> Bool:
        """Whether a snapshot with `summary` was committed by any step of
        this run: the same pipeline and run. Never for an in-memory run."""
        if self.run <= 0:
            return False
        return _has(summary, "orca.pipeline", self.pipeline) and _has(
            summary, "orca.run", String(self.run)
        )

    def made(self, table: String, summary: Dict[String, String]) -> Bool:
        """Whether a snapshot with `summary` is this step's next commit to
        `table`, made by any attempt of this run: the same pipeline, run,
        step, partition and commit number. Never for an in-memory run.

        Every key must be there: a missing one doesn't match an empty value,
        such as a gather step's partition (found by fuzz/fuzz_summary.mojo).

        And not by another run that had this run's number: one a restore of
        morca's database lost, or one of another database over the same
        tables (#292). A snapshot that carries a run token (`orca.run-token`)
        is this run's only if the token is this run's; a snapshot without
        one, or a run without one, is told by the number alone, as before
        tokens.
        """
        if self.run <= 0:
            return False
        return (
            _has(summary, "orca.pipeline", self.pipeline)
            and _has(summary, "orca.run", String(self.run))
            and _has(summary, "orca.step", self.step)
            and _has(summary, "orca.partition", self.partition)
            and _has(summary, "orca.commit", String(self.commits_to(table)))
            and self._token_fits(summary)
        )

    def _token_fits(self, summary: Dict[String, String]) -> Bool:
        """Whether a snapshot with `summary` could be this run's by its run
        token: one of them has none, or they are the same."""
        if not self.token:
            return True
        var theirs = summary.get("orca.run-token")
        if not theirs:
            return True
        return theirs.value() == self.token


def _has(summary: Dict[String, String], key: String, value: String) -> Bool:
    var v = summary.get(key)
    return Bool(v) and v.value() == value
