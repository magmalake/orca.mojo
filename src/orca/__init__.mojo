"""orca: orchestration for Mojo data pipelines.

Declare steps and the pipeline they make, and give the binary orca's
command line:

```mojo
from dista import Seq
from orca import Ctx, Gather, Mapped, Month, Pipeline, Step, orca_main

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
    orca_main[Sizes]()
```

See DESIGN.md for how it works and where it's going.
"""

from orca.cli import orca_main, report
from orca.ctx import Ctx
from orca.civil import Civil, now, parse_time, utc
from orca.cron import CronExpr
from orca.day import Day
from orca.key import Key, TimeKey, parse_keys
from orca.month import Month, days_in, parse_months
from orca.name import Name, parse_names
from orca.partitions import Daily, Monthly, Partitions, parse_partitions
from orca.pipeline import (
    FAILED,
    SKIPPED,
    SUCCEEDED,
    Pipeline,
    RunResult,
    StepRun,
)
from orca.schedule import (
    Cron,
    Current,
    Fill,
    NoSchedule,
    Previous,
    Schedule,
    ScheduleLike,
)
from orca.state import PgStore, RunInfo, State, StepInfo
from orca.store import MemoryStore, Store
from orca.tasks import NoTasks, Tasks, TaskSet, workers
from orca.step import (
    Gather,
    Mapped,
    NoStep,
    Step,
    StepKind,
    StepLike,
    defer,
)
from orca.tz import PosixTZ, Zone
