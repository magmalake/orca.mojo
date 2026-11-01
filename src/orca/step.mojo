"""Steps: the functions a pipeline runs, and the edges between them.

A step wraps a top-level `def` with a name, its input and output types, its
kind, and the step it follows:

```mojo
def download(mut ctx: Ctx, month: Month) raises -> RawFile: ...
def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded: ...
def compact(mut ctx: Ctx, loaded: Seq[Loaded]) raises -> Int64: ...

comptime Download = Step["download", Month, RawFile, download, Mapped]
comptime Load = Step["load", RawFile, Loaded, load, Mapped, after=Download]
comptime Compact = Step["compact", Seq[Loaded], Int64, compact, Gather, after=Load]
```

A mapped step runs once per partition of a run, a gather step once per run.
The edge is checked when the pipeline is compiled:

| Step | Its input must be |
|---|---|
| first, mapped | `Month`, the partition key |
| first, gather | `Seq[Month]`, every partition of the run |
| mapped, after a mapped step | that step's output |
| gather, after a mapped step | `Seq` of that step's output, one per partition in run order |
| gather, after a gather step | that step's output |

A mapped step can't follow a gather step. Types are compared by their
reflected names, which include their module.
"""

from std.reflection import reflect, reflect_fn

from dista.codec import decode, encode
from dista.seq import Seq

from orca.ctx import Ctx
from orca.month import Month


@fieldwise_init
struct StepKind(Equatable, TrivialRegisterPassable, Writable):
    """Whether a step runs once per partition or once per run."""

    var value: Int

    def __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    def write_to(self, mut writer: Some[Writer]):
        writer.write("gather" if self.value == 1 else "mapped")


comptime Mapped = StepKind(0)
"""Runs once per partition of the run."""
comptime Gather = StepKind(1)
"""Runs once per run, over every partition's output of the step before."""


trait StepLike:
    """What a pipeline needs from a step, without knowing its function."""

    comptime A: Copyable & Deinitable
    """The input type."""
    comptime R: Copyable & Deinitable
    """The output type."""

    @staticmethod
    def step_name() -> StaticString:
        """The step's name in its pipeline; empty for `NoStep`."""
        ...

    @staticmethod
    def kind() -> StepKind:
        ...

    @staticmethod
    def upstream() -> StaticString:
        """The name of the step this one follows; empty for a first step."""
        ...

    @staticmethod
    def max_retries() -> Int:
        """How many times a failed attempt is retried, in the same run."""
        ...

    @staticmethod
    def upstream_function() -> StaticString:
        """The function of the step this one follows; empty for a first step."""
        ...

    @staticmethod
    def function() -> StaticString:
        """The step function's linkage name."""
        ...

    @staticmethod
    def input_type() -> StaticString:
        ...

    @staticmethod
    def output_type() -> StaticString:
        ...

    @staticmethod
    def check():
        """Asserts at compile time that the step's input matches its edge."""
        ...

    @staticmethod
    def run(mut ctx: Ctx, input: Span[UInt8, _], mut out: List[UInt8]) raises:
        """Decodes the input, calls the function, appends the encoded output."""
        ...


struct NoStep(StepLike):
    """What a first step follows."""

    comptime A = NoneType
    comptime R = NoneType

    @staticmethod
    def step_name() -> StaticString:
        return ""

    @staticmethod
    def kind() -> StepKind:
        return Mapped

    @staticmethod
    def upstream() -> StaticString:
        return ""

    @staticmethod
    def max_retries() -> Int:
        return 0

    @staticmethod
    def upstream_function() -> StaticString:
        return ""

    @staticmethod
    def function() -> StaticString:
        return ""

    @staticmethod
    def input_type() -> StaticString:
        return ""

    @staticmethod
    def output_type() -> StaticString:
        return ""

    @staticmethod
    def check():
        pass

    @staticmethod
    def run(mut ctx: Ctx, input: Span[UInt8, _], mut out: List[UInt8]) raises:
        raise Error("NoStep doesn't run")


struct Step[
    name: StaticString,
    A_: Copyable & Deinitable,
    R_: Copyable & Deinitable,
    f: def(mut Ctx, A_) thin raises -> R_,
    kind_: StepKind,
    *,
    after: StepLike = NoStep,
    retries: Int = 0,
](StepLike):
    """A step named `name` running `f` on an `A_` to make an `R_`."""

    comptime A = Self.A_
    comptime R = Self.R_

    @staticmethod
    def step_name() -> StaticString:
        return Self.name

    @staticmethod
    def kind() -> StepKind:
        return Self.kind_

    @staticmethod
    def upstream() -> StaticString:
        return Self.after.step_name()

    @staticmethod
    def max_retries() -> Int:
        return Self.retries

    @staticmethod
    def upstream_function() -> StaticString:
        return Self.after.function()

    @staticmethod
    def function() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def input_type() -> StaticString:
        return reflect[Self.A_].name()

    @staticmethod
    def output_type() -> StaticString:
        return reflect[Self.R_].name()

    @staticmethod
    def check():
        comptime assert Self.name != "", "a step needs a name"
        comptime assert Self.retries >= 0, "retries can't be negative"
        comptime a = reflect[Self.A_].name()
        comptime if Self.after.step_name() == "":
            comptime if Self.kind_ == Gather:
                comptime assert (
                    a == reflect[Seq[Month]].name()
                ), "a first gather step takes Seq[Month], the run's partitions"
            else:
                comptime assert (
                    a == reflect[Month].name()
                ), "a first mapped step takes Month, the partition key"
        elif Self.kind_ == Gather:
            comptime if Self.after.kind() == Gather:
                comptime assert (
                    a == reflect[Self.after.R].name()
                ), "a gather step after a gather step takes its output"
            else:
                comptime assert (
                    a == reflect[Seq[Self.after.R]].name()
                ), "a gather step takes a Seq of the mapped step's output"
        else:
            comptime assert (
                Self.after.kind() == Mapped
            ), "a mapped step can't follow a gather step"
            comptime assert (
                a == reflect[Self.after.R].name()
            ), "a mapped step takes the output of the step it follows"

    @staticmethod
    def run(mut ctx: Ctx, input: Span[UInt8, _], mut out: List[UInt8]) raises:
        var pos = 0
        var a = decode[Self.A_](input, pos)
        if pos != len(input):
            raise Error(
                Self.name, ": ", len(input) - pos, " input bytes left over"
            )
        var r = Self.f(ctx, a)
        encode(r, out)
