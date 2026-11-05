"""`orca_main`: the command line every pipeline binary gets.

```
taxi describe                              the DAG as JSON
taxi run --partitions 2024-01..2024-03     starts a run and runs it
taxi run ... --allow-new-code              ... with code the scheduler wasn't given
taxi resume <run> [--allow-new-code]       continues a run at what's unfinished
taxi status [<run>]                        the latest runs, or one run's steps
taxi sense                                 the current snapshot of each dataset it reads
labels keys                                a keyed pipeline's keys snapshot, then its keys
```

For a keyed pipeline (`orca.keyed`), `--partitions` takes names: a list,
`@all` or `@file:<path>`. A name its keys dataset doesn't have now is
refused, as a malformed key is, with exit code 2.

With `ORCA_DATABASE_URL` set, runs are recorded in Postgres and can be
resumed, or, for `sqlite:<path>` (`sqlite:///abs/path` for an absolute
one), in that SQLite file (`orca.sqlite_store`); without it, `run` records its run in memory (`MemoryStore`, run 0)
on the same path, and `resume` and `status` aren't available. `run` records the pipeline the first time; once `orca register`
has given it a binary, `run` leaves the registration as it is, since what
the scheduler queues and counts as done is that code's. A binary whose code
isn't the registered one's is refused (exit code 2) unless `run` is given
`--allow-new-code`; its run then counts as that code's, not the
pipeline's. `ORCA_STALE_SECONDS` (default 30) is how old a run's heartbeat must
be before another process may resume it.

A pipeline with dista tasks gets a runtime per run, started before anything
else so its forked processes inherit no connection or thread; run with
`DISTA_ROLE` set, the binary is a dista head or worker instead (see
`orca.tasks`). With `ORCA_LOGS` set, a directory or a location in object
storage (`orca.logs`), attempts log there, the run's dista timeline is
written beside them, and `status <run>` shows each step's latest log.

`sense` prints a line per dataset the pipeline reads and doesn't write,
`<dataset> <snapshot> <orca|other> <partitions|->`: its table's current
snapshot in `ORCA_WAREHOUSE` (-1 for none), whether an orca step made it,
and the partition keys its summary says it holds. The scheduler polls it for
a sensor pipeline (`orca.sensors`).

`run` and `resume` print one line per step and partition, then the run's
status. A run whose step raised `defer` (and none failed) ends `deferred`,
and the scheduler starts it again when the step asked. Exit codes: 0 when
the run succeeded, 1 when a step didn't, the run deferred or was cancelled,
2 for a
usage error, 3 for any other error (the database, a run that can't be
resumed).
"""

from std.ffi import external_call
from std.os import getenv, makedirs
from std.random import seed
from std.sys import argv, exit

from dista.client import Observer

from orca.heartbeat import heartbeat_interval
from orca.logs import (
    location_exists,
    publish,
    read_location,
    run_dir,
    stage_of,
    write_location,
)
from orca.tasks import workers
from orca.civil import Civil, now, parse_time
from orca.crash import crash_summary
from orca.pipeline import Pipeline
from orca.sample import sample_main
from orca.sqlite_store import SqliteStore, sqlite_path
from orca.state import PgStore
from orca.store import (
    CANCELLED,
    DEFERRED,
    FAILED,
    RUNNING,
    SUCCEEDED,
    MemoryStore,
    RunResult,
    RunStore,
    process_owner,
)
from orca.warehouse import Sensed, current_snapshot

comptime USAGE = """usage:
  <pipeline> describe
  <pipeline> run --partitions KEY[..KEY][,...] [--allow-new-code]   (YYYY-MM, or YYYY-MM-DD daily)
  <pipeline> run --partitions NAME[,...]|@all|@file:<path> [--allow-new-code]   (keyed)
  <pipeline> resume <run> [--allow-new-code]
  <pipeline> status [<run>]
  <pipeline> sense
  <pipeline> keys
  <pipeline> fires [<count>] [--after YYYY-MM-DDTHH:MM[:SS]Z]
  <pipeline> sample --out <location>|- [--pid PID] [--seconds S] ..."""


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
    if cmd == "sample":
        # For the runners: this machine's metrics beside a run (orca.sample).
        return sample_main(args)
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
    if cmd == "keep-log" and len(args) == 5:
        # For the ec2 runner's head instance: the driver's output, appended
        # to the run's driver.log wherever ORCA_LOGS is (orca.ec2).
        var data = read_location(args[2])
        var before = List[UInt8]()
        if location_exists(args[3]):
            before = read_location(args[3])
        before.extend(String("--- start ", args[4], "\n").as_bytes())
        before.extend(Span(data))
        write_location(args[3], Span(before))
        # A driver that crashed left its attempts running: they fail with
        # the crash's first frames (orca.crash) while the run is running.
        var why = crash_summary(String(from_utf8_lossy=Span(data)))
        var run = getenv("ORCA_RUN")
        if why and run and url:
            try:
                if sqlite_path(url):
                    var file = SqliteStore(sqlite_path(url))
                    _crashed(file, Int(run), why)
                else:
                    var store = PgStore(url)
                    _crashed(store, Int(run), why)
            except e:
                print("orca: can't record run", run, "crashing:", e)
        return 0
    if cmd == "dista-status" and len(args) == 3:
        # For the scheduler, scaling a run (orca.ec2): its head's demand.
        # The pipeline's binary links dista; orca's own doesn't, since
        # dista's sockets and flare's can't share one binary.
        var o = Observer(args[2])
        print(o.status_json(), end="")
        return 0
    if cmd == "dista-drain" and len(args) == 4:
        var o = Observer(args[2])
        try:
            o.drain(args[3])
        except e:
            print(e)
            return 1
        return 0
    if cmd == "put-log" and len(args) == 4:
        # For the ec2 runner's instances: a head's or worker's output so
        # far, replacing what was kept of it before (orca.ec2).
        var data = read_location(args[2])
        write_location(args[3], Span(data))
        return 0
    if cmd == "keys" and len(args) == 2:
        comptime if P.partitions.kind() != "keyed":
            _usage(String("pipeline ", P.name, " isn't keyed"))
            return 2
        else:
            var got = P.partitions.keys()
            print(got[0])
            for k in got[1]:
                print(k)
            return 0
    if cmd == "sense" and len(args) == 2:
        var wh = getenv("ORCA_WAREHOUSE")
        var names = P.sensed_datasets()
        if len(names) > 0 and not wh:
            raise Error("orca: sense needs ORCA_WAREHOUSE")
        for d in names:
            var c: Sensed
            try:
                c = current_snapshot(wh, d)
            except e:
                raise Error("orca: can't read ", d, "'s snapshot: ", e)
            print(
                c.dataset,
                c.snapshot,
                "orca" if c.by_orca else "other",
                c.partitions if c.partitions else String("-"),
            )
        return 0
    if cmd == "run":
        var partitions = String()
        var new_code = False
        var i = 2
        while i < len(args):
            if args[i] == "--partitions" and i + 1 < len(args):
                partitions = args[i + 1]
                i += 2
            elif args[i] == "--allow-new-code":
                new_code = True
                i += 1
            else:
                _usage(String("unknown argument: ", args[i]))
        if not partitions:
            _usage("run needs --partitions")
        var keys: List[P.K]
        try:
            keys = P.partitions.parse(partitions, check=True)
        except e:
            _usage(String(e))
            return 2
        var rt = P.tasks.start(workers() if P.tasks.HAS_RUNTIME else 0)
        if not url:
            var mem = MemoryStore()
            return _run[P](rt, mem, keys, new_code)
        if sqlite_path(url):
            var file = SqliteStore(sqlite_path(url))
            return _run[P](rt, file, keys, new_code)
        var store = PgStore(url)
        return _run[P](rt, store, keys, new_code)
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
        if sqlite_path(url):
            var file = SqliteStore(sqlite_path(url))
            return _resume[P](rt, file, run, allow_new_code)
        var store = PgStore(url)
        return _resume[P](rt, store, run, allow_new_code)
    if cmd == "status":
        if not url:
            _usage("status needs ORCA_DATABASE_URL")
        if sqlite_path(url):
            var file = SqliteStore(sqlite_path(url))
            return _status(file, args)
        var store = PgStore(url)
        return _status(store, args)
    _usage()
    return 2


def _crashed[S: RunStore](mut store: S, run: Int, why: String) raises:
    """`keep-log`'s: fails the attempts a crashed driver left running."""
    if store.run_info(run).status != RUNNING:
        return
    var n = store.interrupt(run, why)
    print("run", run, "crashed,", n, "attempts failed:", why)


def _run[
    P: type_of(Pipeline), S: RunStore
](
    mut rt: P.tasks.RT, mut store: S, keys: List[P.K], new_code: Bool
) raises -> Int:
    """`run`: records the pipeline and a new run of `keys` in `store`, and
    runs it."""
    var hash = hex(P.registry_hash())
    store.migrate()
    # Only `orca register` moves a scheduled pipeline's code.
    var current = store.scheduled_hash(P.name)
    if not current:
        store.register(P.name, hash, P.describe())
    elif current != hash and not new_code:
        print(
            "orca: pipeline ",
            P.name,
            " is registered with code ",
            current,
            " and this binary is ",
            hash,
            "; its run wouldn't count as the pipeline's. Pass",
            " --allow-new-code to run it anyway, or register it with",
            " `orca register`",
            sep="",
        )
        return 2
    var owner = process_owner()
    var key = String()
    for j in range(len(keys)):
        key.write("," if j > 0 else "", keys[j])
    var run = store.create_run(P.name, hash, key, "manual", owner)
    store.ran_version(
        run, store.version(P.name, hash, P.describe(), getenv("ORCA_IMAGE"))
    )
    print("run", run)
    return _execute[P](rt, store, run, owner, keys, RunResult())


def _resume[
    P: type_of(Pipeline), S: RunStore
](
    mut rt: P.tasks.RT, mut store: S, run: Int, allow_new_code: Bool
) raises -> Int:
    """`resume`: claims `run` from `store` and runs what's left of it."""
    store.migrate()
    var info = store.run_info(run)
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
    store.claim(run, owner, _stale_seconds())
    store.ran_version(
        run, store.version(P.name, hash, P.describe(), getenv("ORCA_IMAGE"))
    )
    var interrupted = store.interrupt(run)
    var prior = store.prior(run)
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
        store,
        run,
        owner,
        P.partitions.parse(info.partitions, check=False),
        prior^,
    )


def _status[S: RunStore](mut store: S, args: List[String]) raises -> Int:
    """`status`: the latest runs, or one run's steps."""
    store.migrate()
    if len(args) == 2:
        for r in store.runs(20):
            print(r.id, r.pipeline, r.status, r.partitions)
        return 0
    if len(args) == 3:
        var run: Int
        try:
            run = Int(args[2])
        except:
            _usage(String("not a run: ", args[2]))
            return 2
        var info = store.run_info(run)
        print("run", run, info.pipeline, info.status, info.partitions)
        if info.error:
            print("error:", info.error)
        _print_steps(store, run, show_logs=True)
        return 0
    _usage()
    return 2


def _fires[P: type_of(Pipeline)](args: List[String]) raises -> Int:
    """The schedule's next fires after now (or `--after`), each with the
    partition it fills: UTC time, wall-clock time, partition."""
    comptime if not P.scheduled():
        _usage(String("pipeline ", P.name, " has no schedule"))
        return 2
    else:
        return _print_fires[P](args)


def _print_fires[P: type_of(Pipeline)](args: List[String]) raises -> Int:
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


def _parent(path: String) -> String:
    var at = path.rfind("/")
    return String(path[byte=:at]) if at > 0 else String(".")


def _stale_seconds() raises -> Float64:
    var s = getenv("ORCA_STALE_SECONDS", "30")
    try:
        return Float64(s)
    except:
        raise Error("orca: ORCA_STALE_SECONDS is not a number: ", s)


def _execute[
    P: type_of(Pipeline), S: RunStore
](
    mut rt: P.tasks.RT,
    mut store: S,
    run: Int,
    owner: String,
    keys: List[P.K],
    var prior: RunResult,
) raises -> Int:
    """Runs what's left of `run` as its owner, with a heartbeat, then ends it.

    A run asked to cancel stops before its next attempt and ends
    `cancelled`. The store retries its writes for the run (`PgStore`: on a
    new connection for `ORCA_DB_RETRY_SECONDS`, `orca.state.DbRetry`). If
    this raises otherwise (the database stayed away, or another process took
    the run over), the run is left `running`; once its heartbeat is stale it
    can be resumed.
    """
    store.own(run, owner, hex(P.registry_hash()))
    store.start_heartbeat(heartbeat_interval(_stale_seconds()), P.timeouts())
    var res: RunResult
    try:
        res = P.execute(store, prior^, keys, run, Int(Pointer(to=rt)))
    except e:
        store.stop_heartbeat()
        if not store.cancel_requested():
            raise e^
        store.finish_run(CANCELLED)
        _print_steps_after(store, run)
        print("run", CANCELLED)
        return 1
    store.stop_heartbeat()
    var dir = run_dir(P.name, run)
    comptime if P.tasks.HAS_RUNTIME:
        if dir:
            var at = String(dir, "/timeline.json")
            try:
                makedirs(_parent(stage_of(at)), exist_ok=True)
                _ = P.tasks.timeline(rt, stage_of(at))
                publish(at)
            except e:
                print("orca: no timeline:", e)
    var outcome = res.outcome()
    if outcome == DEFERRED:
        store.defer_run(res.deferred_until())
    else:
        store.finish_run(outcome)
    _print_steps_after(store, run)
    if outcome == DEFERRED:
        print("run deferred until", String(Civil.of(res.deferred_until()), "Z"))
    else:
        print("run", outcome)
    return 0 if outcome == SUCCEEDED else 1


def _print_steps_after[S: RunStore](mut store: S, run: Int):
    """`_print_steps` once the run has ended: it's recorded, so not being
    able to list its steps doesn't change how it ended."""
    try:
        _print_steps(store, run)
    except e:
        print("orca: can't list run", run, "steps:", e)


def _print_steps[
    S: RunStore
](mut store: S, run: Int, show_logs: Bool = False) raises:
    """The run's steps in run order, as recorded."""
    for s in store.steps(run):
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
    var outcome = res.outcome()
    if outcome == DEFERRED:
        print("run deferred until", String(Civil.of(res.deferred_until()), "Z"))
    else:
        print("run", outcome)
