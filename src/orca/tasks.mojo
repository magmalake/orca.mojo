"""The dista tasks a pipeline's steps call, and the runtime a run gets.

```mojo
comptime Square = Task[Int, Int, square]
comptime TaxiTasks = Tasks[Square]
comptime Taxi = Pipeline["taxi", Download, Load, tasks=TaxiTasks]

def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded:
    ref rt = ctx.runtime[TaxiTasks]()
    var r = rt.remote[Square](7)
    ...
```

A run of a pipeline with tasks starts one dista `Runtime` before its first
step and keeps it until the run ends: in local mode it forks a head and
`ORCA_WORKERS` workers (default 4); with `DISTA_HEAD` set it joins that
cluster instead. The same binary is dista's worker: run with `DISTA_ROLE`
set, `orca_main` hands the process to dista before reading its arguments.

The runtime starts before orca opens a database connection or its heartbeat
thread, so the forked head and workers inherit neither.
"""

from std.os import getenv

from dista import Runtime, TaskLike


trait TaskSet:
    """The tasks a pipeline registers with dista."""

    comptime RT: Movable & Deinitable
    """The runtime type: `Runtime` of the tasks, or `NoneType`."""
    comptime HAS_RUNTIME: Bool

    @staticmethod
    def start(workers: Int) raises -> Self.RT:
        """Starts the runtime: forks a local cluster, or joins `DISTA_HEAD`."""
        ...

    @staticmethod
    def names() -> List[String]:
        """The tasks' linkage names, for the pipeline's registry hash."""
        ...

    @staticmethod
    def timeline(mut rt: Self.RT, path: String) raises -> Int:
        """Writes the run's dista timeline (Chrome's trace format) to `path`
        and returns the tasks in it."""
        ...


struct Tasks[*Ts: TaskLike](TaskSet):
    """A pipeline's dista tasks."""

    comptime RT = Runtime[*Self.Ts]
    comptime HAS_RUNTIME = True

    @staticmethod
    def start(workers: Int) raises -> Self.RT:
        return Self.RT(workers=workers)

    @staticmethod
    def names() -> List[String]:
        var out = List[String]()
        comptime for i in range(Self.Ts.length):
            out.append(String(Self.Ts[i].name()))
        return out^

    @staticmethod
    def timeline(mut rt: Self.RT, path: String) raises -> Int:
        return rt.timeline(path)


struct NoTasks(TaskSet):
    """A pipeline that calls no dista tasks: no runtime is started."""

    comptime RT = NoneType
    comptime HAS_RUNTIME = False

    @staticmethod
    def start(workers: Int) raises -> Self.RT:
        return None

    @staticmethod
    def names() -> List[String]:
        return List[String]()

    @staticmethod
    def timeline(mut rt: Self.RT, path: String) raises -> Int:
        return 0


def task_list[T: TaskSet]() -> String:
    """`T`'s task names, one per line."""
    var out = String()
    for n in T.names():
        out.write(n, "\n")
    return out^


def workers() raises -> Int:
    """`ORCA_WORKERS`, the local workers a run forks (default 4)."""
    var s = getenv("ORCA_WORKERS", "4")
    try:
        var n = Int(s)
        if n >= 1:
            return n
    except:
        pass
    raise Error("orca: ORCA_WORKERS must be a positive number, got ", s)
