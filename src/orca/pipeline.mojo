"""Pipelines: a named list of steps, checked and run in order.

```mojo
comptime Taxi = Pipeline["taxi", Download, Load, Compact]

var result = Taxi.run(parse_months("2024-01..2024-03"))
```

Steps are listed after the step they follow, so the declared order is a
valid run order; `check` asserts it at compile time, along with every edge's
types and unique names. Every function that runs a pipeline calls `check`,
so a pipeline that breaks a rule doesn't compile.

`execute` is the executor: it runs the steps that haven't succeeded yet and
reports each attempt to a `Store`, either `MemoryStore` (what `run` uses: a
run lives as long as the process) or `orca.state.PgStore`, which makes a run
resumable. Each step's output is kept encoded by dista's codec, as Postgres
holds it. A step that raises is retried up to its `retries`, then fails its
partition: the steps after it for that partition, and any gather step over
it, are skipped, while other partitions carry on.
"""

from dista.codec import decode, encode, put_scalar
from dista.seq import Seq
from dista.task import fnv1a64

from orca.ctx import Ctx
from orca.json import quote
from orca.month import Month
from orca.step import Gather, Mapped, StepLike
from orca.store import MemoryStore, Store
from orca.logs import attempt_log
from orca.tasks import NoTasks, TaskSet, task_list, workers

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


struct Pipeline[name: StaticString, *Ts: StepLike, tasks: TaskSet = NoTasks]:
    """A pipeline named `name` of the steps `Ts`, in run order, whose steps
    call the dista `tasks`."""

    @staticmethod
    def _find(step: StaticString) -> Int:
        comptime for k in range(Self.Ts.length):
            if Self.Ts[k].step_name() == step:
                return k
        return -1

    @staticmethod
    def check():
        """Asserts at compile time that the pipeline is well formed."""
        comptime assert Self.name != "", "a pipeline needs a name"
        comptime assert Self.Ts.length > 0, "a pipeline needs a step"
        comptime for i in range(Self.Ts.length):
            Self.Ts[i].check()
            comptime assert (
                Self._find(Self.Ts[i].step_name()) == i
            ), "two steps share a name"
            comptime if Self.Ts[i].upstream() != "":
                comptime j = Self._find(Self.Ts[i].upstream())
                comptime assert (
                    j >= 0
                ), "a step follows a step not in its pipeline"
                comptime assert (
                    j < i
                ), "a step must be listed after the step it follows"
                comptime assert (
                    Self.Ts[i].upstream_function() == Self.Ts[j].function()
                ), "a step follows another step of the same name"

    @staticmethod
    def registry_hash() -> UInt64:
        """A hash over the pipeline's name, its steps' names, kinds, edges and
        functions, and its dista tasks: two binaries with the same hash run
        the same DAG."""
        Self.check()
        var all = String(Self.name, "\n")
        comptime for i in range(Self.Ts.length):
            all.write(
                Self.Ts[i].step_name(),
                "\t",
                Self.Ts[i].kind(),
                "\t",
                Self.Ts[i].upstream(),
                "\t",
                Self.Ts[i].function(),
                "\n",
            )
        for t in Self.tasks.names():
            all.write("task\t", t, "\n")
        return fnv1a64(all)

    @staticmethod
    def describe() -> String:
        """The DAG as JSON: the pipeline's name, its registry hash, and each
        step's name, kind, upstream, function and types, in run order."""
        Self.check()
        var out = String("{")
        out.write('"pipeline": ', quote(Self.name), ", ")
        out.write('"registry_hash": ', quote(hex(Self.registry_hash())), ", ")
        out.write('"steps": [')
        comptime for i in range(Self.Ts.length):
            comptime S = Self.Ts[i]
            if i > 0:
                out.write(", ")
            out.write("{")
            out.write('"name": ', quote(S.step_name()), ", ")
            out.write('"kind": ', quote(String(S.kind())), ", ")
            if S.upstream() == "":
                out.write('"after": null, ')
            else:
                out.write('"after": ', quote(S.upstream()), ", ")
            out.write('"function": ', quote(S.function()), ", ")
            out.write('"input": ', quote(S.input_type()), ", ")
            out.write('"output": ', quote(S.output_type()))
            out.write("}")
        out.write('], "tasks": [')
        var names = Self.tasks.names()
        for i in range(len(names)):
            out.write(", " if i > 0 else "", quote(names[i]))
        out.write("]}")
        return out^

    @staticmethod
    def run(partitions: List[Month], run: Int = 0) raises -> RunResult:
        """Runs every step over `partitions`, in order, recording nothing,
        with a dista runtime of its own if the pipeline has tasks."""
        var store = MemoryStore()
        var rt = Self.tasks.start(workers() if Self.tasks.HAS_RUNTIME else 0)
        var res = Self.execute(
            store, RunResult(), partitions, run, Int(Pointer(to=rt))
        )
        _ = rt^  # alive until the steps are done
        return res^

    @staticmethod
    def execute[
        S: Store
    ](
        mut store: S,
        var prior: RunResult,
        partitions: List[Month],
        run: Int,
        runtime: Int = 0,
    ) raises -> RunResult:
        """Runs the steps over `partitions` that haven't succeeded in `prior`,
        in order, reporting each attempt to `store`.

        `prior` holds the steps a run had already finished when it's resumed,
        with their outputs; they aren't run again. A step that raises is
        retried up to its `retries`, then fails its partition. An error from
        the store itself is raised: the run stops where it is, and can be
        resumed from there. `runtime` is the address of the run's
        `tasks.RT`, which steps reach through `Ctx.runtime`.
        """
        Self.check()
        var res = prior^
        comptime for i in range(Self.Ts.length):
            comptime T = Self.Ts[i]
            comptime if T.kind() == Mapped:
                for p in partitions:
                    var key = String(p)
                    if Self._done(res, T.step_name(), key):
                        continue
                    var input = List[UInt8]()
                    var blocked = String()
                    comptime if T.upstream() == "":
                        encode(p, input)
                    else:
                        var j = res.find(T.upstream(), key)
                        if j < 0 or res.steps[j].status != SUCCEEDED:
                            blocked = String(T.upstream(), " ", key)
                        else:
                            input = res.steps[j].output.copy()
                    Self._step[T](
                        store, res, i, run, runtime, key, input, blocked
                    )
            else:
                if Self._done(res, T.step_name(), ""):
                    continue
                var input = List[UInt8]()
                var blocked = String()
                comptime if T.upstream() == "":
                    encode(Seq[Month](partitions.copy()), input)
                elif Self.Ts[Self._find(T.upstream())].kind() == Gather:
                    var j = res.find(T.upstream(), "")
                    if j < 0 or res.steps[j].status != SUCCEEDED:
                        blocked = String(T.upstream())
                    else:
                        input = res.steps[j].output.copy()
                else:
                    # A Seq on the wire is a count, then each element's
                    # encoding, so the upstream outputs are concatenated
                    # as they are.
                    put_scalar[DType.int64](input, Int64(len(partitions)))
                    for p in partitions:
                        var j = res.find(T.upstream(), String(p))
                        if j < 0 or res.steps[j].status != SUCCEEDED:
                            if blocked:
                                blocked.write(", ")
                            blocked.write(T.upstream(), " ", p)
                        else:
                            input.extend(Span(res.steps[j].output))
                Self._step[T](store, res, i, run, runtime, "", input, blocked)
        return res^

    @staticmethod
    def _done(
        res: RunResult, step: StringSlice, partition: StringSlice
    ) -> Bool:
        var j = res.find(step, partition)
        return j >= 0 and res.steps[j].status == SUCCEEDED

    @staticmethod
    def _step[
        T: StepLike, S: Store
    ](
        mut store: S,
        mut res: RunResult,
        ord: Int,
        run: Int,
        runtime: Int,
        partition: String,
        input: List[UInt8],
        blocked: String,
    ) raises:
        """Runs one step for one partition, with its retries, and records the
        outcome in `res` (replacing an earlier one) and in `store`."""
        var outcome: StepRun
        if blocked:
            var reason = String("not run: ", blocked, " didn't succeed")
            store.skip(T.step_name(), partition, ord, reason)
            outcome = StepRun(
                T.step_name(), partition, SKIPPED, List[UInt8](), reason
            )
        else:
            outcome = StepRun(
                T.step_name(), partition, FAILED, List[UInt8](), ""
            )
            # Every attempt reads the snapshots the first one read.
            var pinned = store.pinned_reads(T.step_name(), partition)
            var code = hex(Self.registry_hash())
            for _ in range(T.max_retries() + 1):
                var attempt = store.start_attempt(
                    T.step_name(), partition, ord, Span(input)
                )
                var log = attempt_log(
                    Self.name, run, T.step_name(), partition, attempt
                )
                if log:
                    store.set_log(T.step_name(), partition, attempt, log)
                var ctx = Ctx(
                    Self.name,
                    run,
                    T.step_name(),
                    partition,
                    attempt,
                    code,
                    pinned.copy(),
                    store.recorder(),
                    log,
                    runtime if Self.tasks.HAS_RUNTIME else 0,
                    task_list[Self.tasks](),
                )
                ctx.log_to_file(String("attempt ", attempt, " started"))
                var out = List[UInt8]()
                var error = String()
                try:
                    T.run(ctx, Span(input), out)
                    ctx.log_to_file(String("attempt ", attempt, " succeeded"))
                except e:
                    error = String(e)
                    ctx.log(String("attempt ", attempt, " failed: ", error))
                for r in ctx.reads:
                    if r.table not in pinned:
                        pinned[r.table] = r.snapshot_id
                if error:
                    store.finish_attempt(
                        T.step_name(),
                        partition,
                        attempt,
                        FAILED,
                        Span(out),
                        error,
                    )
                    outcome.error = error
                    outcome.attempt = attempt
                    continue
                store.finish_attempt(
                    T.step_name(), partition, attempt, SUCCEEDED, Span(out), ""
                )
                outcome = StepRun(
                    T.step_name(), partition, SUCCEEDED, out^, "", attempt
                )
                break
        var j = res.find(T.step_name(), partition)
        if j >= 0:
            res.steps[j] = outcome^
        else:
            res.steps.append(outcome^)
