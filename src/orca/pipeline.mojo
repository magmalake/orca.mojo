"""Pipelines: a named list of steps, checked and run in order.

```mojo
comptime Taxi = Pipeline["taxi", Download, Load, Compact]

var result = Taxi.run(parse_months("2024-01..2024-03"))
```

Steps are listed after the step they follow, so the declared order is a
valid run order; `check` asserts it at compile time, along with every edge's
types and unique names. Every function that runs a pipeline calls `check`,
so a pipeline that breaks a rule doesn't compile.

`run` is the in-memory executor: nothing is recorded, and a run lives as long
as the process. Each step's output is kept encoded by dista's codec, which is
also how the state in Postgres will hold it. A step that raises fails its
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

    def __init__(
        out self,
        step: String,
        partition: String,
        status: String,
        var output: List[UInt8],
        error: String,
    ):
        self.step = step
        self.partition = partition
        self.status = status
        self.output = output^
        self.error = error


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


struct Pipeline[name: StaticString, *Ts: StepLike]:
    """A pipeline named `name` of the steps `Ts`, in run order."""

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
        """A hash over the pipeline's name and its steps' names, kinds, edges
        and functions: two binaries with the same hash run the same DAG."""
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
        out.write("]}")
        return out^

    @staticmethod
    def run(partitions: List[Month], run: Int = 0) -> RunResult:
        """Runs every step over `partitions`, in order, in this process."""
        Self.check()
        var res = RunResult()
        comptime for i in range(Self.Ts.length):
            comptime S = Self.Ts[i]
            comptime if S.kind() == Mapped:
                for p in partitions:
                    var key = String(p)
                    var input = List[UInt8]()
                    var blocked = String()
                    comptime if S.upstream() == "":
                        encode(p, input)
                    else:
                        var j = res.find(S.upstream(), key)
                        if res.steps[j].status != SUCCEEDED:
                            blocked = String(S.upstream(), " ", key)
                        else:
                            input = res.steps[j].output.copy()
                    res.steps.append(Self._attempt[S](run, key, input, blocked))
            else:
                var input = List[UInt8]()
                var blocked = String()
                comptime if S.upstream() == "":
                    encode(Seq[Month](partitions.copy()), input)
                elif Self.Ts[Self._find(S.upstream())].kind() == Gather:
                    var j = res.find(S.upstream(), "")
                    if res.steps[j].status != SUCCEEDED:
                        blocked = String(S.upstream())
                    else:
                        input = res.steps[j].output.copy()
                else:
                    # A Seq on the wire is a count, then each element's
                    # encoding, so the upstream outputs are concatenated
                    # as they are.
                    put_scalar[DType.int64](input, Int64(len(partitions)))
                    for p in partitions:
                        var j = res.find(S.upstream(), String(p))
                        if res.steps[j].status != SUCCEEDED:
                            if blocked:
                                blocked.write(", ")
                            blocked.write(S.upstream(), " ", p)
                        else:
                            input.extend(Span(res.steps[j].output))
                res.steps.append(Self._attempt[S](run, "", input, blocked))
        return res^

    @staticmethod
    def _attempt[
        S: StepLike
    ](
        run: Int, partition: String, input: List[UInt8], blocked: String
    ) -> StepRun:
        if blocked:
            return StepRun(
                S.step_name(),
                partition,
                SKIPPED,
                List[UInt8](),
                String("not run: ", blocked, " didn't succeed"),
            )
        var ctx = Ctx(Self.name, run, S.step_name(), partition)
        var out = List[UInt8]()
        try:
            S.run(ctx, Span(input), out)
        except e:
            return StepRun(
                S.step_name(), partition, FAILED, List[UInt8](), String(e)
            )
        return StepRun(S.step_name(), partition, SUCCEEDED, out^, "")
