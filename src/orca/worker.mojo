"""`orca worker`: runs a pool's runs on this machine, talking only to
`orca ui`'s API.

```
ORCA_WORKER_TOKEN=orca_... orca worker --pool bgent --remote https://orca.mlake.dev
```

Every `--poll-seconds` (2) the worker posts its heartbeat to
`/api/worker/claim` (`orca.workers`), its name, image, CPUs, memory and
the runs it has, and, while it has fewer than `--max-runs` (1), asks for
one. A run it's given starts as the local runner starts one: the
pipeline's binary (the path it was registered with, which the worker's
image has too) with `resume <run>`, in a process group of its own, its
output staged under `--dir` (`$TMPDIR/orca-worker`), with the pipeline's
`env`, its secrets read here with `ORCA_SECRET_FETCH` and named in
`ORCA_SECRETS`, and `ORCA_STATE_URL` and `ORCA_STATE_TOKEN` in place of a
database: the run records itself over the API (`orca.http_store`). Its
data and logs go where the worker's own `ORCA_WAREHOUSE` and `ORCA_LOGS`
say, as any run's do. When the process ends, its output is appended to
the run's `driver.log` by `<binary> keep-log`, which also fails the
attempts a crash left running; a process that ended before claiming its
run has the run failed, saying why (`/api/worker/exited`). Beside each
run's process it starts `<binary> sample` (`orca.sample`, unless
`ORCA_SAMPLE_SECONDS=0`), which writes this machine's metrics to
`<ORCA_LOGS>/<pipeline>/<run>/metrics/worker-<name>.jsonl`, so the run's
page has its Machines card, as a local run's does. Each poll the
worker samples its runs' process trees, as the scheduler samples a local
run's, and sends the samples with `/api/worker/exited`, so their attempts
get their peak memory and CPU (`orca.usage`). Where the worker can make
cgroups (`orca.cgroup`), each run gets one at its grant, and an
out-of-memory kill is reported with `/api/worker/exited`, which fails the
attempts saying so and raises what the pipeline asks for.

**No database.** The worker refuses to start with `ORCA_DATABASE_URL` set:
nothing on a worker holds database credentials. Its token is
`ORCA_WORKER_TOKEN`, or the first line of `--token-file`; one that's
revoked, expired or for another pool than `--pool` is refused, and the
worker exits (code 3).

**Versions.** The worker's image is `--image`, else `ORCA_IMAGE`. A run it's
given whose pipeline's image isn't that is put back (`/api/worker/release`)
rather than started, though the UI shouldn't hand it one
(`orca.workers.version_refusal`).

**Dying.** A worker that's killed takes its runs with it: each run's
heartbeat thread exits its process once its parent isn't the worker
(`ORCA_WORKER_PID`), so the run's heartbeat goes stale and the scheduler
queues it again, for this worker or another, whatever its trigger
(`orca.scheduler`'s `_requeue_lost`); a run this worker claimed and never
started has its start given back.

**`orca runner`** (docs/runner.md) is this worker for the control host:
pool `local`, its row made by name (`orca.workers.runner_auth`), with the
worker API called on its own database connection
(`orca.workers.runner_call`) rather than over HTTPS. It claims what the
scheduler assigned it with `ORCA_LOCAL_RUNNER=runner`, starts a
control-placed run as the scheduler did (`nice`, `oom_score_adj`,
`ORCA_CONTROL_WORKERS`, its cgroup at most `ORCA_CONTROL_MEMORY_MB`, and
uncontained, killed over it), gives runs its own environment (the
database included) and no `ORCA_WORKER_PID`, so they outlive it, kills a
run whose heartbeat went stale, and records its runs' memory every 5 s.
"""

from std.ffi import c_int, external_call
from std.os import getenv, makedirs, remove
from std.os.path import exists
from std.sys.info import CompilationTarget, num_logical_cores
from std.time import perf_counter_ns, sleep

from json import Value, loads
from postgres import Params

from orca.admission import Report
from orca.cgroup import Cgroups, Contained, MemoryKnobs, put as put_cgroup
from orca.cgroup import start_limit_mb
from orca.control import CONTROL, ControlLimits, fetch_secret, run_env
from orca.http_store import Body, StateApi
from orca.logs import run_dir
from orca.pressure import (
    Admission,
    Source,
    find_source,
    pct_text,
    pressure_env,
    read_psi,
)
from orca.proc import Reaped, reap, spawn
from orca.resources import host_room, run_env as grant_env
from orca.sample import (
    _hostname,
    _sysctl_u64,
    machine_name,
    metrics_location,
    parse_meminfo,
    sample_seconds,
    tree_usage,
)
from orca.store import FINAL
from orca.usage import Meter, marks_json
from orca.live import RUNNER_POOL, start_grace, stale_limit
from orca.sample import tree_rss
from orca.state import State
from orca.workers import (
    WorkerAuth,
    WorkerRun,
    pool_problem,
    runner_auth,
    runner_call,
    version_refusal,
    worker_run_of,
)

comptime WORKER_USAGE = """usage:
  orca worker --pool <pool> [--remote <url>] [--token-file <path>]
              [--max-runs N] [--poll-seconds S] [--dir <dir>]
              [--image <image>] [--name <name>]
              (the token: ORCA_WORKER_TOKEN, or --token-file)"""


@fieldwise_init
struct _Running(Copyable, Movable):
    var spec: WorkerRun
    var stage: String
    var env: List[String]
    var control: Bool
    """A control-placed run of `orca runner`."""
    var over: String
    """Why the runner killed it, over the control placement's memory cap,
    or empty."""


def _memory_mb() -> Int:
    comptime if CompilationTarget.is_linux():
        try:
            with open("/proc/meminfo", "r") as f:
                var m = parse_meminfo(f.read())
                return m.get("MemTotal", 0) // (1024 * 1024)
        except:
            return 0
    else:
        return _sysctl_u64("hw.memsize") // (1024 * 1024)


def _last_line(path: String) -> String:
    """The last non-empty line of `path`, at most 300 bytes of it."""
    try:
        with open(path, "r") as f:
            var text = f.read()
            var lines = text.split("\n")
            var i = len(lines) - 1
            while i >= 0:
                var l = String(lines[i].strip())
                if l:
                    if l.byte_length() > 300:
                        return String(l[byte=:300])
                    return l^
                i -= 1
    except:
        pass
    return ""


struct Worker(Movable):
    var api: StateApi
    var token: String
    var pool: String
    var name: String
    var image: String
    var max_runs: Int
    var poll: Float64
    var dir: String
    var running: Dict[Int, _Running]
    """The runs' processes, by pid."""
    var keeping: Dict[Int, String]
    """`keep-log`s going, by pid, with the stage they're keeping."""
    var meters: Dict[Int, Meter]
    """The runs' processes' samples, by pid, sent when each ends
    (`orca.usage`)."""
    var psi: Source
    """Where this host's memory pressure is read from (`orca.pressure`)."""
    var admission: Admission
    """Whether this worker claims runs for its host's memory pressure."""
    var cgroups: Cgroups
    """Where the runs' cgroups go (`orca.cgroup`), or why they don't."""
    var contained: Dict[Int, Contained]
    """The runs' processes' cgroups, by pid."""
    var unswept: List[String]
    """Cgroups to remove once empty."""
    var room_mb: Int
    """The memory this host can grant, the limit of a run without a
    grant."""
    var db: Optional[State]
    """`orca runner`'s own connection: the worker API is called on it
    (`orca.workers.runner_call`) rather than over HTTPS, and its runs write
    to Postgres themselves. None for a remote worker."""
    var auth: WorkerAuth
    """The runner's `orca.workers` row (pool `local`)."""
    var limits: ControlLimits
    """What a control-placed run of the runner may use (`orca.control`)."""
    var stale: Float64
    var grace: Float64
    var _cgroups_next: Int
    """When the runner next records its runs' memory, on the monotonic
    clock."""

    def __init__(
        out self,
        url: String,
        token: String,
        pool: String,
        name: String,
        image: String,
        max_runs: Int,
        poll: Float64,
        dir: String,
    ) raises:
        self.api = StateApi(url, token)
        self.token = token
        self.pool = pool
        self.name = name
        self.image = image
        self.max_runs = max_runs
        self.poll = poll
        self.dir = dir
        self.running = Dict[Int, _Running]()
        self.keeping = Dict[Int, String]()
        self.meters = Dict[Int, Meter]()
        self.psi = find_source()
        self.admission = Admission.from_env()
        self.cgroups = Cgroups.off("off: not serving")
        self.contained = Dict[Int, Contained]()
        self.unswept = List[String]()
        self.room_mb = 0
        self.db = None
        self.auth = WorkerAuth(0, pool, name)
        self.limits = ControlLimits.from_env()
        self.stale = stale_limit()
        self.grace = start_grace(self.stale)
        self._cgroups_next = 0

    def runner(mut self, url: String) raises:
        """Makes this `orca runner` (docs/runner.md): a worker of pool
        `local` on its own database connection, its row made or found by
        its name."""
        var db = State(url)
        db.migrate()
        self.auth = runner_auth(db, self.name)
        self.db = db^
        # The scheduler held its starts for this host's memory pressure
        # already: the runner claims every start it was assigned.
        self.admission = Admission(0)

    def _running_ids(self) -> String:
        var ids = String()
        for e in self.running.items():
            ids.write("," if ids else "", e.value.spec.run)
        return ids^

    def _post(mut self, path: String, var body: Body) raises -> Value:
        var text = (
            body^.text("pool", self.pool).text("name", self.name).finish()
        )
        if self.db:
            return loads(
                runner_call(self.db.value(), self.auth, path, loads(text))
            )
        return self.api.request_once(path, text^)

    def _start(
        mut self,
        spec: WorkerRun,
        placement: String = "",
        request_max_mb: Int = 0,
        raised_mb: Int = 0,
    ) raises:
        # The runner's runs are the control host's: the image is the
        # scheduler's own, as for a run it forks.
        var why = String() if self.db else version_refusal(
            spec.image, self.image
        )
        if why:
            print("orca worker: refusing run", spec.run, ":", why)
            _ = self._post(
                "/api/worker/release",
                Body()
                .int("run", Int64(spec.run))
                .int("start", Int64(spec.start))
                .text("why", why),
            )
            return
        var control = Bool(self.db) and placement == CONTROL
        # Its own variables and its grant of this host (orca.resources).
        var extra = pressure_env(self.psi)
        if control:
            # Its local dista workers (orca.control).
            extra.extend(run_env(self.limits, "", []))
        var env = grant_env(spec.env, spec.grant, extra)
        var names = String()
        for s in spec.secrets:
            env.append(String(s[0], "=", fetch_secret(s[1])))
            names.write("," if names else "", s[0])
        if names:
            env.append(String("ORCA_SECRETS=", names))
        if self.image:
            env.append(String("ORCA_IMAGE=", self.image))
        if not self.db:
            # The runner's runs write to Postgres themselves, and outlive
            # it (docs/runner.md); a remote worker's go through the API.
            env.append(String("ORCA_STATE_URL=", self.api.url))
            env.append(String("ORCA_STATE_TOKEN=", self.token))
            env.append(
                String("ORCA_WORKER_PID=", external_call["getpid", c_int]())
            )
        env.append(String("ORCA_RUN=", spec.run))
        makedirs(self.dir, exist_ok=True)
        var stage = String(
            self.dir,
            "/",
            spec.pipeline,
            "-",
            spec.run,
            "-",
            spec.start,
            "-driver.log",
        )
        with open(stage, "w") as f:
            f.write("")  # keep-log heads it with the start
        var argv: List[String] = [spec.binary, "resume", String(spec.run)]
        if spec.allow_new_code:
            argv.append("--allow-new-code")
        var cg = Contained(
            spec.run,
            spec.start,
            spec.pipeline,
            "",
            0,
            0,
            0,
            spec.grant.memory_mb,
        )
        if self.cgroups.on():
            # The runner's as the scheduler's (orca.cgroup): at most the
            # request's maximum, at least what an OOM raised it to, and on
            # the control placement at most its cap.
            var cap = self.limits.memory_mb if control else 0
            var default = self.cgroups.knobs.default_mb
            if default <= 0:
                default = cap if control else self.room_mb
            cg.limit_mb = start_limit_mb(
                spec.grant.memory_mb,
                request_max_mb,
                default,
                raised_mb,
                cap,
            )
            if cg.limit_mb > 0:
                try:
                    var made = self.cgroups.make(
                        spec.run, spec.start, cg.limit_mb
                    )
                    cg.path = made[0]
                    cg.kill_mb = made[1]
                    if self.db:
                        _ = self.db.value().conn.execute(
                            (
                                "UPDATE orca.runs SET memory_limit_mb = $3,"
                                " memory_max_mb = $4, memory_bytes = NULL"
                                " WHERE id = $1 AND starts = $2"
                            ),
                            Params()
                            .int64(Int64(spec.run))
                            .int64(Int64(spec.start))
                            .int64(Int64(cg.limit_mb))
                            .int64(Int64(cg.kill_mb)),
                        )
                except e:
                    print("orca worker: run", spec.run, "without a cgroup:", e)
        var pid = spawn(
            argv,
            stage,
            env=env,
            nice=self.limits.nice if control else 0,
            group=True,
            first_to_go=control,
            cgroup=cg.path,
        )
        if cg.path:
            self.contained[pid] = cg^
        self.meters[pid] = Meter()
        print(
            "started",
            spec.pipeline,
            "run",
            spec.run,
            "start",
            spec.start,
            "pid",
            pid,
            flush=True,
        )
        self.running[pid] = _Running(spec.copy(), stage, env^, control, "")
        # The machine's metrics while the run's process lives, as the
        # scheduler's beside a local run (orca.sample): the Machines card.
        var metrics = metrics_location(
            spec.pipeline,
            spec.run,
            (
                String(CONTROL) if control else String("local")
            ) if self.db else machine_name(self.name),
        )
        if metrics and sample_seconds() > 0:
            try:
                _ = spawn(
                    [
                        spec.binary,
                        "sample",
                        "--out",
                        metrics,
                        "--pid",
                        String(pid),
                    ]
                )
            except e:
                print("orca worker: no sampler for run", spec.run, ":", e)

    def _sample(mut self) -> Dict[Int, Tuple[Int, Float64]]:
        """Samples each run's process tree (none on macOS); the readings,
        by pid."""
        var pids = List[Int]()
        for e in self.meters.items():
            pids.append(e.key)
        var readings = tree_usage(pids)
        for e in readings.items():
            try:
                self.meters[e.key].sample(e.value[0], e.value[1])
            except:
                pass
        return readings^

    def _tend(mut self, readings: Dict[Int, Tuple[Int, Float64]]) raises:
        """What the scheduler did for the processes it forked, which the
        runner now does for its own (docs/runner.md): kills an uncontained
        control-placed run over `ORCA_CONTROL_MEMORY_MB`, kills a run whose
        heartbeat went stale (wedged, or cut off from the database: it
        can't be left beside the start that replaces it), and every 5 s
        records each contained run's memory."""
        var cap = self.limits.memory_mb * 1024 * 1024
        var over = List[Int]()
        for e in self.running.items():
            if (
                e.value.control
                and not e.value.over
                and e.key not in self.contained
            ):
                var rss = readings[e.key][0] if e.key in readings else tree_rss(
                    e.key
                )
                if rss > cap:
                    over.append(e.key)
                    print(
                        "run",
                        e.value.spec.run,
                        "pid",
                        e.key,
                        "over the control cap:",
                        rss // (1024 * 1024),
                        "MB",
                        flush=True,
                    )
        for pid in over:
            var r = self.running.pop(pid)
            r.over = String(
                "killed: its processes held more than the control instance's",
                " cap of ",
                self.limits.memory_mb,
                " MB a run (ORCA_CONTROL_MEMORY_MB)",
            )
            _ = external_call["kill", c_int](c_int(-pid), c_int(9))
            self.running[pid] = r^
        if len(self.running) == 0:
            return
        var ids = self._running_ids()
        var stale = self.db.value().conn.query(
            (
                "SELECT id FROM orca.runs WHERE id = ANY"
                " (string_to_array($1, ',')::bigint[]) AND status = 'running'"
                " AND ((heartbeat IS NOT NULL AND heartbeat <= now() -"
                " make_interval(secs => $2)) OR (heartbeat IS NULL AND"
                " started <= now() - make_interval(secs => $3)))"
            ),
            Params().text(ids).float64(self.stale).float64(self.grace),
        )
        for row in stale:
            var id = Int(row.int64("id"))
            for e in self.running.items():
                if e.value.spec.run == id:
                    _ = external_call["kill", c_int](c_int(-e.key), c_int(9))
                    print(
                        "killed pid",
                        e.key,
                        "of run",
                        id,
                        "(its heartbeat went stale)",
                        flush=True,
                    )
        var t = Int(perf_counter_ns())
        if t < self._cgroups_next or len(self.contained) == 0:
            return
        self._cgroups_next = t + 5_000_000_000
        for e in self.contained.items():
            var b = Cgroups.current(e.value.path)
            if b >= 0:
                _ = self.db.value().conn.execute(
                    (
                        "UPDATE orca.runs SET memory_bytes = $3 WHERE id = $1"
                        " AND starts = $2"
                    ),
                    Params()
                    .int64(Int64(e.value.run))
                    .int64(Int64(e.value.start))
                    .int64(Int64(b)),
                )

    def _ended(mut self, done: Reaped) raises:
        var pid = done.pid
        var code = done.code
        if pid in self.keeping:
            var stage = self.keeping.pop(pid)
            if code != 0:
                print("orca worker: keep-log exited", code, "keeping", stage)
            else:
                try:
                    remove(stage)
                except:
                    pass
            return
        if pid not in self.running:
            return
        var r = self.running.pop(pid)
        print("run", r.spec.run, "pid", pid, "exited", code, flush=True)
        var m = self.meters.pop(pid, Meter())
        m.end(done.max_rss, done.cpu_seconds)
        var body = (
            Body()
            .int("run", Int64(r.spec.run))
            .int("start", Int64(r.spec.start))
            .int("code", Int64(code))
            .text("last", _last_line(r.stage))
            .int("pid", Int64(pid))
            .int("max_rss", Int64(m.max_rss))
            .raw("marks", marks_json(m.aged()))
        )
        if r.over:
            body = body^.text("over", r.over)
        if pid in self.contained:
            var cg = self.contained.pop(pid)
            var oom = code != 0 and Cgroups.oom_kills(cg.path) > 0
            if oom:
                print(
                    "run", r.spec.run, "pid", pid, "out of memory", flush=True
                )
            body = body^.int("memory_limit_mb", Int64(cg.limit_mb)).int(
                "memory_max_mb", Int64(cg.kill_mb)
            ).int("oom", Int64(1 if oom else 0))
            _ = put_cgroup(String(cg.path, "/cgroup.kill"), "1")
            if not Cgroups.remove(cg.path):
                self.unswept.append(cg.path)
        try:
            _ = self._post("/api/worker/exited", body^)
        except e:
            print("orca worker: can't report run", r.spec.run, "ending:", e)
        var location = run_dir(r.spec.pipeline, r.spec.run)
        if location:
            # The output, masked, after what earlier starts left there; and
            # a crash's attempts failed with its summary (orca.crash).
            try:
                var kpid = spawn(
                    [
                        r.spec.binary,
                        "keep-log",
                        r.stage,
                        String(location, "/driver.log"),
                        String(r.spec.start),
                    ],
                    env=r.env,
                )
                self.keeping[kpid] = r.stage
            except e:
                print("orca worker: can't keep run", r.spec.run, "log:", e)
        else:
            print(
                "orca worker: run",
                r.spec.run,
                "output (no ORCA_LOGS) is in",
                r.stage,
            )

    def serve(mut self) raises -> Int:
        if self.db:
            print(
                "orca runner",
                self.name,
                "(orca.workers row",
                String(self.auth.id, ", pool local): starts what the"),
                "scheduler assigns it, on its own database connection",
                flush=True,
            )
        else:
            print(
                "orca worker",
                self.name,
                "pool",
                self.pool,
                "image",
                self.image if self.image else String("(none)"),
                "on",
                self.api.url,
                flush=True,
            )
        var cpus = num_logical_cores()
        var mem = _memory_mb()
        # What this host can grant the runs it takes (orca.resources).
        # The runner's host is the scheduler's, with its reserve.
        var room = host_room(worker=not self.db)
        print(
            "orca worker: grants up to",
            room.cpus,
            "CPUs and",
            room.memory_mb,
            "MB (ORCA_RESERVE_CPUS, ORCA_RESERVE_MEMORY_MB)",
            flush=True,
        )
        if not self.psi:
            print(
                "orca worker: no memory pressure to read here (no PSI);",
                " runs are claimed on --max-runs and grants alone",
                sep="",
                flush=True,
            )
        else:
            print(
                "orca worker: memory pressure from",
                self.psi.describe(),
                "- claims nothing while some avg10 is over",
                String(pct_text(self.admission.limit), "%"),
                flush=True,
            )
        self.room_mb = room.memory_mb
        self.cgroups = Cgroups.setup(MemoryKnobs.from_env())
        print("orca worker: run cgroups", self.cgroups.why, flush=True)
        self.unswept = self.cgroups.stale()
        var said = String()
        while True:
            for done in reap():
                try:
                    self._ended(done)
                except e:
                    print("orca worker:", e)
            if len(self.unswept) > 0:
                var left = List[String]()
                for path in self.unswept:
                    if not Cgroups.remove(path):
                        left.append(path)
                self.unswept = left^
            var readings = self._sample()
            if self.db:
                try:
                    self._tend(readings)
                except e:
                    print("orca runner:", e, flush=True)
            var slots = self.max_runs - len(self.running)
            # Memory pressure (orca.pressure): no claim while this host is
            # over the limit, but one at a time once a hold has lasted its
            # grace with none of its runs going.
            var now = Float64(Int(perf_counter_ns())) / 1e9
            var idle = len(self.running) == 0
            var was = self.admission.held()
            if self.psi:
                self.admission.observe(read_psi(self.psi.path), now, idle)
            var allowed = self.admission.allows(idle)
            if allowed == 0:
                slots = 0
            elif allowed > 0:
                slots = min(slots, allowed)
            if self.admission.held() and not was:
                print(
                    "orca worker:",
                    self.admission.hold,
                    "- claiming no runs",
                    flush=True,
                )
            elif was and not self.admission.held():
                print("orca worker: memory pressure eased", flush=True)
            if self.admission.stuck:
                print(
                    "orca worker: memory pressure held past",
                    Int(self.admission.grace),
                    "s with none of its runs going: one at a time",
                    flush=True,
                )
            var got: Value
            try:
                got = self._post(
                    "/api/worker/claim",
                    Body()
                    .text("image", self.image)
                    .int("slots", Int64(slots))
                    .text("running", self._running_ids())
                    .int("cpus", Int64(cpus))
                    .int("memory_mb", Int64(mem))
                    .int("room_cpus", Int64(room.cpus))
                    .int("room_memory_mb", Int64(room.memory_mb))
                    .raw(
                        "pressure",
                        Report.of(self.psi, self.admission, now).json(),
                    ),
                )
            except e:
                var msg = String(e)
                if msg.startswith(FINAL):
                    print(
                        "orca worker: refused:",
                        String(msg[byte = FINAL.byte_length() :]),
                        flush=True,
                    )
                    return 3
                if msg != said:
                    print("orca worker:", msg, flush=True)
                    said = msg
                sleep(self.poll)
                continue
            said = ""
            if String("refused") in got:
                var why = got["refused"].string_value()
                print("orca worker: not given a run:", why, flush=True)
            if not got["run"].is_null():
                var spec = worker_run_of(got["run"])
                var placement = String()
                var request_max_mb = 0
                var raised_mb = 0
                if String("local") in got:
                    ref l = got["local"]
                    placement = l["placement"].string_value()
                    request_max_mb = Int(l["memory_max_mb"].int_value())
                    raised_mb = Int(l["memory_raised_mb"].int_value())
                try:
                    self._start(spec, placement, request_max_mb, raised_mb)
                except e:
                    print("orca worker: can't start run", spec.run, ":", e)
                    try:
                        _ = self._post(
                            "/api/worker/exited",
                            Body()
                            .int("run", Int64(spec.run))
                            .int("start", Int64(spec.start))
                            .int("code", Int64(127))
                            .text("last", String(e)),
                        )
                    except:
                        pass
                continue  # ask again at once while there's room
            sleep(self.poll)


def _usage(msg: String) -> Int:
    print(msg)
    print(WORKER_USAGE)
    return 2


def worker_command(args: List[String], target: String) raises -> Int:
    """`orca worker ...`, to the UI `target` unless `--remote` follows."""
    var url = target
    var pool = String()
    var token_file = String()
    var max_runs = 1
    var poll = 2.0
    var dir = String(getenv("TMPDIR", "/tmp").rstrip("/"), "/orca-worker")
    var image = getenv("ORCA_IMAGE")
    var name = _hostname()
    var i = 2
    while i < len(args):
        var a = args[i]
        if i + 1 >= len(args):
            return _usage(String(a, " needs a value"))
        var val = args[i + 1]
        if a == "--pool":
            pool = val
        elif a == "--remote" or a == "--url":
            url = val
        elif a == "--token-file":
            token_file = val
        elif a == "--max-runs":
            try:
                max_runs = Int(val)
            except:
                return _usage("--max-runs is a count")
        elif a == "--poll-seconds":
            try:
                poll = Float64(val)
            except:
                return _usage("--poll-seconds is a number")
        elif a == "--dir":
            dir = val
        elif a == "--image":
            image = val
        elif a == "--name":
            name = val
        else:
            return _usage(String("unknown argument: ", a))
        i += 2
    if getenv("ORCA_DATABASE_URL"):
        print(
            "orca worker: ORCA_DATABASE_URL is set, and a worker talks only",
            " to orca ui's API: unset it",
            sep="",
        )
        return 2
    if not pool:
        return _usage("orca worker needs --pool")
    var why = pool_problem(pool)
    if why:
        return _usage(String("orca worker: ", why))
    if max_runs < 1:
        return _usage("--max-runs is at least 1")
    if not url:
        return _usage(
            "orca worker needs the UI: --remote <url>, or ORCA_REMOTE"
        )
    if not url.startswith("http://") and not url.startswith("https://"):
        url = String("https://", url)
    var token = getenv("ORCA_WORKER_TOKEN")
    if token_file:
        with open(token_file, "r") as f:
            token = String(f.read().split("\n")[0].strip())
    if not token:
        return _usage(
            "orca worker needs a token: ORCA_WORKER_TOKEN, or --token-file"
        )
    var w = Worker(url, token, pool, name, image, max_runs, poll, dir)
    return w.serve()


comptime RUNNER_USAGE = """usage:
  orca runner [--name <name>] [--max-runs N] [--poll-seconds S] [--dir <dir>]
              (needs ORCA_DATABASE_URL; the scheduler assigns it runs with
              ORCA_LOCAL_RUNNER=runner: docs/runner.md)"""


def runner_command(args: List[String], url: String) raises -> Int:
    """`orca runner ...`: starts the local and control-placed runs the
    scheduler assigns it, as a worker of pool `local` on the database at
    `url` (docs/runner.md)."""
    var name = String("runner")
    var max_runs = 64
    var poll = 1.0
    var dir = String(getenv("TMPDIR", "/tmp").rstrip("/"), "/orca-runner")
    var i = 2
    while i < len(args):
        var a = args[i]
        if i + 1 >= len(args):
            print(a, "needs a value")
            print(RUNNER_USAGE)
            return 2
        var val = args[i + 1]
        if a == "--name":
            name = val
        elif a == "--max-runs":
            try:
                max_runs = Int(val)
            except:
                max_runs = 0
            if max_runs < 1:
                print("--max-runs is a count from 1")
                print(RUNNER_USAGE)
                return 2
        elif a == "--poll-seconds":
            try:
                poll = Float64(val)
            except:
                print("--poll-seconds is a number")
                print(RUNNER_USAGE)
                return 2
        elif a == "--dir":
            dir = val
        else:
            print("unknown argument:", a)
            print(RUNNER_USAGE)
            return 2
        i += 2
    var w = Worker(
        "http://127.0.0.1:1",
        "unused",
        RUNNER_POOL,
        name,
        getenv("ORCA_IMAGE"),
        max_runs,
        poll,
        dir,
    )
    w.runner(url)
    return w.serve()
