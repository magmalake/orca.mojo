"""`orca_main`: the command line every pipeline binary gets.

```
taxi describe                              the DAG as JSON
taxi run --partitions 2024-01..2024-03     starts a run and runs it
taxi resume <run> [--allow-new-code]       continues a run at what's unfinished
taxi status [<run>]                        the latest runs, or one run's steps
```

With `ORCA_DATABASE_URL` set, runs are recorded in Postgres and can be
resumed; without it, `run` runs in memory and `resume` and `status` aren't
available. `ORCA_STALE_SECONDS` (default 30) is how old a run's heartbeat must
be before another process may resume it.

A pipeline with dista tasks gets a runtime per run, started before anything
else so its forked processes inherit no connection or thread; run with
`DISTA_ROLE` set, the binary is a dista head or worker instead (see
`orca.tasks`). With `ORCA_LOGS` set, attempts log to files there, the run's
dista timeline is written beside them, and `status <run>` shows each step's
latest log.

`run` and `resume` print one line per step and partition, then the run's
status. Exit codes: 0 when the run succeeded, 1 when a step didn't, 2 for a
usage error, 3 for any other error (the database, a run that can't be
resumed).
"""

from std.ffi import external_call
from std.os import getenv
from std.random import seed
from std.sys import argv, exit

from threads import TypedPool

from orca.heartbeat import Beat, beat, heartbeat_interval
from orca.logs import run_dir
from orca.tasks import workers
from orca.civil import Civil, now, parse_time
from orca.partitions import parse_partitions
from orca.pipeline import FAILED, SUCCEEDED, Pipeline, RunResult
from orca.state import PgStore, State, process_owner

comptime USAGE = """usage:
  <pipeline> describe
  <pipeline> run --partitions KEY[..KEY][,...]   (YYYY-MM, or YYYY-MM-DD daily)
  <pipeline> resume <run> [--allow-new-code]
  <pipeline> status [<run>]
  <pipeline> fires [<count>] [--after YYYY-MM-DDTHH:MM[:SS]Z]"""


def _usage(msg: String = ""):
    if msg:
        print(msg)
    print(USAGE)
    exit(2)


def seed_random():
    """Seeds `std.random` from the clock and the process ID.

    Unseeded, every process starts from the same state, so pipeline
    processes committing to one table at once draw the same Iceberg snapshot
    IDs and file UUIDs (iceberg takes both from `std.random` and doesn't
    seed it), and overwrite each other's manifest lists.
    """
    var ts = List[Int64](length=2, fill=0)
    _ = external_call["clock_gettime", Int32](Int32(0), ts.unsafe_ptr())
    var pid = Int(external_call["getpid", Int32]())
    seed(Int(ts[0]) * 1_000_000_000 + Int(ts[1]) ^ (pid << 40))


def orca_main[P: type_of(Pipeline)]() raises:
    """Parses the command line and runs the command on `P`."""
    seed_random()
    comptime if P.tasks.HAS_RUNTIME:
        if getenv("DISTA_ROLE"):
            _ = P.tasks.start(1)  # serves as a head or worker, then exits
    var args = List[String]()
    for a in argv():
        args.append(String(a))
    if len(args) < 2:
        _usage()
        return
    var code: Int
    try:
        code = _command[P](args)
    except e:
        print(e)
        code = 3
    exit(code)


def _command[P: type_of(Pipeline)](args: List[String]) raises -> Int:
    var cmd = args[1]
    var url = getenv("ORCA_DATABASE_URL")
    # A schedule is checked on every command, so a binary whose cron
    # expression or zone is bad can't be registered or run.
    comptime if P.scheduled():
        _ = P.load_schedule()
    if cmd == "fires":
        return _fires[P](args)
    if cmd == "describe" and len(args) == 2:
        print(P.describe())
        return 0
    if cmd == "run":
        var partitions = String()
        var i = 2
        while i < len(args):
            if args[i] == "--partitions" and i + 1 < len(args):
                partitions = args[i + 1]
                i += 2
            else:
                _usage(String("unknown argument: ", args[i]))
        if not partitions:
            _usage("run needs --partitions")
        var keys: List[P.K]
        try:
            keys = parse_partitions[P.partitions](partitions)
        except e:
            _usage(String(e))
            return 2
        if not url:
            var res = P.run(keys)
            report(res)
            return 0 if res.succeeded() else 1
        var rt = P.tasks.start(workers() if P.tasks.HAS_RUNTIME else 0)
        var db = State(url)
        db.migrate()
        var hash = hex(P.registry_hash())
        db.register(P.name, hash, P.describe())
        var owner = process_owner()
        var key = String()
        for j in range(len(keys)):
            key.write("," if j > 0 else "", keys[j])
        var run = db.create_run(P.name, hash, key, "manual", owner)
        print("run", run)
        return _execute[P](rt, db^, url, run, owner, keys, RunResult())
    if cmd == "resume":
        if not url:
            _usage("resume needs ORCA_DATABASE_URL")
        var run = -1
        var allow_new_code = False
        for i in range(2, len(args)):
            if args[i] == "--allow-new-code":
                allow_new_code = True
            elif run < 0:
                try:
                    run = Int(args[i])
                except:
                    _usage(String("not a run: ", args[i]))
            else:
                _usage(String("unknown argument: ", args[i]))
        if run < 0:
            _usage("resume needs a run")
        var rt = P.tasks.start(workers() if P.tasks.HAS_RUNTIME else 0)
        var db = State(url)
        db.migrate()
        var info = db.run_info(run)
        if info.pipeline != P.name:
            raise Error(
                "orca: run ",
                run,
                " is of pipeline ",
                info.pipeline,
                ", not ",
                P.name,
            )
        var hash = hex(P.registry_hash())
        if info.registry_hash != hash and not allow_new_code:
            raise Error(
                "orca: run ",
                run,
                " ran code ",
                info.registry_hash,
                " and this binary is ",
                hash,
                "; pass --allow-new-code to resume it anyway",
            )
        var owner = process_owner()
        db.claim(run, owner, _stale_seconds())
        var interrupted = db.interrupt(run)
        var prior = db.prior(run)
        print(
            "resuming run",
            run,
            "with",
            len(prior.steps),
            "steps done and",
            interrupted,
            "interrupted",
        )
        return _execute[P](
            rt,
            db^,
            url,
            run,
            owner,
            parse_partitions[P.partitions](info.partitions),
            prior^,
        )
    if cmd == "status":
        if not url:
            _usage("status needs ORCA_DATABASE_URL")
        var db = State(url)
        db.migrate()
        if len(args) == 2:
            for r in db.runs():
                print(r.id, r.pipeline, r.status, r.partitions)
            return 0
        if len(args) == 3:
            var run: Int
            try:
                run = Int(args[2])
            except:
                _usage(String("not a run: ", args[2]))
                return 2
            var info = db.run_info(run)
            print("run", run, info.pipeline, info.status, info.partitions)
            _print_steps(db, run, show_logs=True)
            return 0
    _usage()
    return 2


def _fires[P: type_of(Pipeline)](args: List[String]) raises -> Int:
    """The schedule's next fires after now (or `--after`), each with the
    partition it fills: UTC time, wall-clock time, partition."""
    comptime if not P.scheduled():
        _usage(String("pipeline ", P.name, " has no schedule"))
        return 2
    var count = 5
    var after = now()
    var i = 2
    while i < len(args):
        if args[i] == "--after" and i + 1 < len(args):
            try:
                after = parse_time(args[i + 1])
            except e:
                _usage(String(e))
            i += 2
        else:
            try:
                count = Int(args[i])
            except:
                _usage(String("not a count: ", args[i]))
            i += 1
    var s = P.load_schedule()
    var t = after
    for _ in range(count):
        t = s.next_fire(t)
        print(
            String(Civil.of(t), "Z"),
            String(Civil.of(s.zone.to_local(t))),
            P.fills_for(t, s),
        )
    return 0


def _stale_seconds() raises -> Float64:
    var s = getenv("ORCA_STALE_SECONDS", "30")
    try:
        return Float64(s)
    except:
        raise Error("orca: ORCA_STALE_SECONDS is not a number: ", s)


def _execute[
    P: type_of(Pipeline)
](
    mut rt: P.tasks.RT,
    var db: State,
    url: String,
    run: Int,
    owner: String,
    keys: List[P.K],
    var prior: RunResult,
) raises -> Int:
    """Runs what's left of `run` as its owner, with a heartbeat, then ends it.

    If this raises (the database went away, or another process took the run
    over), the run is left `running`; once its heartbeat is stale it can be
    resumed.
    """
    var b = Beat(url, run, owner, heartbeat_interval(_stale_seconds()))
    var pool = TypedPool.start[beat](1, b)
    var store = PgStore(db^, url, run, owner, hex(P.registry_hash()))
    var res = P.execute(store, prior^, keys, run, Int(Pointer(to=rt)))
    pool.shutdown()
    var dir = run_dir(P.name, run)
    comptime if P.tasks.HAS_RUNTIME:
        if dir:
            try:
                _ = P.tasks.timeline(rt, String(dir, "/timeline.json"))
            except e:
                print("orca: no timeline:", e)
    var ok = res.succeeded()
    store.db.finish_run(run, owner, SUCCEEDED if ok else FAILED)
    _print_steps(store.db, run)
    print("run", SUCCEEDED if ok else FAILED)
    return 0 if ok else 1


def _print_steps(mut db: State, run: Int, show_logs: Bool = False) raises:
    """The run's steps in run order, as recorded."""
    for s in db.steps(run):
        var line = String(s.step)
        if s.partition:
            line.write(" ", s.partition)
        line.write(" ", s.status)
        if s.attempts > 1:
            line.write(" after ", s.attempts, " attempts")
        if s.error:
            line.write(": ", s.error)
        if show_logs and s.log:
            line.write(" [", s.log, "]")
        print(line)


def report(res: RunResult):
    """One line per step and partition, then the run's status."""
    for i in range(len(res.steps)):
        ref s = res.steps[i]
        var line = String(s.step)
        if s.partition:
            line.write(" ", s.partition)
        line.write(" ", s.status)
        if s.error:
            line.write(": ", s.error)
        print(line)
    print("run", SUCCEEDED if res.succeeded() else "failed")
