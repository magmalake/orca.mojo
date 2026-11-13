"""morca: orchestration for Mojo data pipelines.

Declare steps and the pipeline they make, and give the binary morca's
command line:

```mojo
from dista import Seq
from morca import Ctx, Gather, Mapped, Month, Pipeline, Step, morca_main

def size(mut ctx: Ctx, m: Month) raises -> Int:
    return m.month * 100

def total(mut ctx: Ctx, sizes: Seq[Int]) raises -> Int:
    var t = 0
    for i in range(len(sizes)):
        t += sizes[i]
    return t

comptime Size = Step["size", Month, Int, size, Mapped]
comptime Total = Step["total", Seq[Int], Int, total, Gather, after=Size]
comptime Sizes = Pipeline["sizes", Size, Total]

def main() raises:
    morca_main[Sizes]()
```

See DESIGN.md for how it works and where it's going.
"""

from morca._internal.cli import morca_main, report
from morca.ctx import Ctx
from morca.civil import Civil, now, parse_time, utc
from morca.cron import CronExpr
from morca.day import Day
from morca.hour import Hour
from morca.key import Key, TimeKey, parse_keys
from morca.month import Month, days_in, parse_months
from morca._internal.handover import (
    code_version,
    desired_code_version,
    outdated,
    outdated_why,
)
from morca.name import Name, parse_names
from morca.partitions import (
    Daily,
    Hourly,
    Monthly,
    Partitions,
    parse_partitions,
)
from morca.pipeline import (
    FAILED,
    SKIPPED,
    SUCCEEDED,
    Pipeline,
    RunResult,
    StepRun,
)
from morca._internal.pressure import Psi, Stall, memory_pressure
from morca._internal.resources import Grant, auto_count
from morca.schedule import (
    Cron,
    Current,
    Fill,
    NoSchedule,
    Previous,
    Schedule,
    ScheduleLike,
)
from morca._internal.sqlite_store import SqliteStore, sqlite_path
from morca._internal.state import PgStore, State
from morca._internal.store import (
    MemoryStore,
    Recorder,
    RunInfo,
    RunStore,
    StepInfo,
    Store,
    TableSnapshot,
)
from morca.tasks import NoTasks, Tasks, TaskSet, workers
from morca.step import (
    Gather,
    Mapped,
    NoStep,
    Step,
    StepKind,
    StepLike,
    defer,
)
from morca.tz import PosixTZ, Zone
