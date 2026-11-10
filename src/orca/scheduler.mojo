"""The scheduler: fires schedules, senses datasets, queues backfills and
rebuilds, and starts runs.

```
orca register ./build/taxi             runs `taxi describe`, records the binary
orca backfill taxi 2011-01..2024-12    a run per partition that isn't done
orca backfill labels @all              keyed: runs of keys_per_run undone keys
orca rebuild taxi.trips 2024-01        the pipelines downstream, in order
orca tick                              one pass, then exits
orca scheduler                         a pass every second
```

The scheduler links no pipeline. It knows one by the `describe` it printed,
kept in `orca.pipelines` with the binary's path, and reads the schedule,
partitions and limits from that JSON in SQL. Everything it decides is in
Postgres, so it can be stopped and restarted, or run twice, at any time:

- **A fire is a row.** `orca.fires` is keyed by (pipeline, fire time), and a
  fire's row and its `queued` run are inserted in one transaction, so a fire
  is never made twice. Fires are counted from the last one recorded (or from
  when the pipeline was registered), so the fires missed while the scheduler
  was down are made once each, or, with `catchup=False`, only the latest.
  A fire whose partition is before the pipeline's first is recorded without
  a run.
- **One live run per partition.** Every way a run is queued checks, under
  a lock on the pipeline's row, for a live run (`orca.live`) of the same
  keys: a fire whose partition a live run has records that run as its own,
  a sensor leaves such keys to it (the snapshot's run, if it left nothing
  else), a rebuild's level waits for it, the UI refuses to start one, and a
  backfill skips the key. A partition that's done is run again by a fire,
  sensor, rebuild or start, and its steps append again; `orca register`
  warns of a schedule that fires more than once for one partition.
- **Starting a run is `resume`.** A queued run has nothing done, or what a
  resume or rerun from `orca ui` left it, which is what `<binary> resume
  <run>` takes (with `--allow-new-code` when the resume said so). The scheduler marks the run `running`,
  with no heartbeat and itself as owner, and execs the binary, which claims
  it. It doesn't wait: the run records its own progress.
- **`max_runs`.** Under a lock on the pipeline's row, the runs that are live
  (a fresh heartbeat, or started by the scheduler less than the stale limit
  ago) are counted, and only that many fewer than `max_runs` are started.
- **Memory pressure** (`orca.pressure`, `orca.admission`). On the local
  runner and for control-placed runs, none starts while this host's PSI
  `some avg10` is over `ORCA_ADMIT_MEMORY_PRESSURE` (10%), each queued run
  `held` saying so; then the grants of a pipeline with a request
  (`orca.resources`).
- **Backfills queue what isn't done.** A key is done when every mapped
  step has succeeded for it in a run of the pipeline's current code
  (`DONE_SQL`). A backfill drops the keys that are done or in a live run
  (queued, or running with its process alive: `live_sql`), sorts the rest
  and queues a run per `keys_per_run` of them; `orca backfill` says on
  stderr which keys it dropped and why.
- **Where runs run.** `ORCA_RUNNER` is `local` (a process here, the
  default), `kubernetes` (a dista cluster of pods per run, `orca.kube`) or
  `ec2` (a head instance and worker instances per run, `orca.ec2`).
  A pipeline registered with `--placement control` runs here whatever the
  runner, within the control instance's limits (`orca.control`); one with
  `--placement ec2` waits on a scheduler whose runner isn't `ec2`. One with
  `--placement worker:<pool>` is never started here: a remote worker of the
  pool claims its queued runs over the UI's API (`orca.workers`), and the
  scheduler queues its due retries and its dead runs again for one, its
  fill runs too (held for a worker, `fill_max_runs` across the pool), and
  a run whose worker is gone whatever its trigger.
- **Overlapping instances** (`orca.instances`, docs/overlap.md). With
  `ORCA_OVERLAP=on` a deploy starts the new image's scheduler beside the
  old one: the newest live instance leads and alone decides, and an older
  one starts nothing more, supervises its runs until they end (or kills
  them after `ORCA_DRAIN_SECONDS`) and exits. A local or control-placed
  run records the instance that started it; while that instance lists it
  the run is live (`orca.live.held_sql`), counted against `max_runs`, the
  control slots and the grants, and left alone. Once the instance is gone
  and the run's heartbeat stale, it's queued again whatever its trigger,
  audited.
- **Sensors and rebuilds** (`orca.sensors`). A sensor pipeline's pass
  looks for snapshots of the datasets it reads, in `orca.commits` on every
  tick and from `<binary> sense` every `ORCA_SENSE_SECONDS` (asked in a
  child the next ticks check on, killed after `ORCA_BINARY_SECONDS`), and
  queues a
  run, trigger `sensor`, for those it hasn't seen. A keyed sensor's keys
  dataset is sensed too: a new snapshot of it queues the keys that aren't
  done, in runs of `keys_per_run`. Each rebuild queues its
  next level once the one before has succeeded.
- **A snapshot it can't run** is seen all the same. One whose keys aren't
  partitions of the sensor's kind (a week in `orca.partitions`, or names
  for a dated sensor) is recorded in `orca.sensed` with no run and why, and
  the others the tick saw are run. A pass that fails, such as a `sense` that
  exits non-zero, is kept as the pipeline's `sense_error`, which `orca ui`
  shows, and printed once rather than on every tick.
- **Fills** (`orca.fill`). A pipeline registered with `fills = "all"` in
  orca.toml has a second pass after its other runs': runs with trigger
  `fill`, one partition each, over the partitions of its range no run has
  taken (`fill_status`), up to `fill_max_runs` beside `max_runs`, none
  while its other runs are queued, it's paused, outside `fill_window` or
  over `fill_budget_per_day`.
- **Retries.** A scheduled, backfilled, sensed or fill run that failed is started again
  after the pipeline's `retry_seconds` (else `ORCA_RETRY_SECONDS`, default
  60) doubling with each start, at most its `max_retry_seconds` if set,
  plus up to a tenth more as jitter, up to `run_retries` times. One whose process
  died (its heartbeat went stale) is started again at once, within the same
  budget. A manual, UI or rerun run is never retried by the scheduler: one
  whose process died ends `failed`, with the error `interrupted: its
  process stopped`, so it can be resumed and holds its keys no longer. A
  run whose step raised `defer` ends `deferred` with `retry_at` the time it
  asked for, and is started again then, whatever its trigger; those starts
  (`deferrals`) don't count against `run_retries`. A run asked to cancel
  whose process died ends `cancelled`.
- **New code.** `orca register` of a new build moves the queued runs to it,
  and the failed ones waiting for a retry that have no step done. A retry
  of a run with steps done by other code would be refused by the binary's
  `resume`, so the scheduler fails it at once instead of starting it, with
  an error (`runs.error`) saying to resume it with `--allow-new-code`. A
  local run whose process exits before claiming its run fails with the
  exit code and the last line of its driver log as its error; one whose
  process crashes after (a segfault, `abort`) has the attempts it left
  running failed with the crash's first frames (`orca.crash`).

The scheduler's clock is `orca.civil.now`, which `ORCA_NOW` fixes; fire
times and retry times are on it. Heartbeats are on the database's clock.
A run's output goes to `<ORCA_LOGS>/<pipeline>/<run>/driver.log` when
`ORCA_LOGS` is set: written there by a local run, published there when the
process is reaped if `ORCA_LOGS` is in object storage, or kept from the
driver pod's log on Kubernetes (`orca.kube`). Beside a local run's process
the scheduler starts `<binary> sample` (`orca.sample`), which writes the
machine's metrics to `.../<run>/metrics/local.jsonl` until the process ends;
`ORCA_SAMPLE_SECONDS=0` turns it off, on every runner. Apart from that, each
tick reads each local and control-placed run's process tree in /proc, and
once the process is reaped its attempts get their peak memory and CPU
(`orca.usage`). `orca scheduler` gives each local and control-placed run a
cgroup of its own where it can (`orca.cgroup`): a memory limit at its
grant, an out-of-memory kill that fails its attempts saying so, and a retry
that asks for more.
"""

from std.ffi import c_int, external_call
from std.os import getenv, makedirs, remove
from std.os.path import exists
from std.time import perf_counter_ns, sleep

from postgres import Params, Transaction

from orca.expiry import expirable, expire_argv, maintain_binary
from orca.civil import Civil
from orca.cgroup import (
    Cgroups,
    Contained,
    MemoryKnobs,
    HeldWatch,
    at_limit,
    held_text,
    ceiling_mb,
    oom_text,
    put as put_cgroup,
    raised_mb,
    raised_request,
    start_limit_mb,
)
from orca.control import (
    CONTROL,
    ControlLimits,
    control_stage,
    fetch_secret,
    pool_of,
    pressure_reason,
    run_env,
    secrets_env,
    slots_reason,
)
from orca.config import (
    ProjectConfig,
    RegisterFlags,
    env_json,
    parse_env,
    parse_max_runs,
    parse_worker_procs,
    parse_workers,
    resolve,
)
from orca.crash import crash_summary, crashed_by_signal
from orca.alerts import (
    disk as alert_disk,
    failed_runs,
    fill as alert_fill,
    lost_workers,
    watchdog_recovered,
)
from orca.notify import configured as notify_configured, flush as flush_notify
from orca.fill import (
    Fill,
    FillPipeline,
    FillStatus,
    fill_refusal,
    fill_status,
    parse_fill_budget,
    parse_fill_max_runs,
)
from orca.cron import CronExpr
from orca.day import Day
from orca.key import parse_keys
from orca.ec2 import Ec2Runner, Ec2Spec
from orca.fleet import ingest
from orca.system import (
    Archive,
    keep as keep_system,
    system_now,
    trim as trim_system,
)
from orca.kube import KubeRunner, RunSpec, cpu_millis
from orca.instances import (
    beat,
    end as end_instance,
    join as join_instance,
    newer,
    overlap_on,
    step_down,
    try_lead,
)
from orca.live import (
    dead_sql,
    live_runs,
    live_sql,
    stale_limit,
    stale_sql,
    start_grace,
)
from orca.logs import is_remote, publish, read_location, run_dir, stage_of
from orca.month import Month
from orca.name import Name, parse_names, sort_texts
from orca.proc import Capture, absolute, binary_seconds, capture, reap, spawn
from orca.sample import metrics_location, sample_seconds, tree_rss, tree_usage
from orca.schedule import Schedule
from orca.secrets import parse_secrets, secrets_json
from orca.sensors import (
    advance_rebuilds,
    join_keys,
    map_keys,
    parse_sense,
    split_keys,
)
from orca.resources import (
    Grant,
    Request,
    Room,
    admit,
    host_room,
    resolve_auto,
    run_env as grant_env,
)
from orca.admission import (
    SCHEDULER_HOST,
    Report,
    alert_stuck,
    record as record_pressure,
    resolve_stuck,
)
from orca.pressure import (
    Admission,
    Source,
    find_source,
    pct_text,
    pressure_env,
    read_psi,
)
from orca.recommend import (
    DEFAULT,
    LEARN_RUNS,
    Recommendation,
    RunRequest,
    expire_raised,
    learned,
    resolve_request,
)
from orca.state import State
from orca.watchdog import check as check_watchdog, config as watchdog_config
from orca.tz import Zone
from orca.usage import Meter, record as record_usage

comptime _BUDGET_LOCK = 0x6F726362  # "orcb"
"""Held while a start is weighed against `ORCA_VCPU_BUDGET`."""

comptime _CONTROL_LOCK = 0x6F726363  # "orcc"
"""Held while a control-placed start is weighed against
`ORCA_CONTROL_MAX_RUNS`."""

comptime _HOST_LOCK = 0x6F726364  # "orcd"
"""Held while a start is weighed against what this host's running runs
weren't granted (`orca.resources`)."""

comptime _WORKER_OWNER = "worker"
"""A run's owner once a worker claimed it and before its process did
(`orca.workers.WORKER_OWNER`, which imports this module)."""
comptime _RETRIED = "trigger IN ('schedule', 'backfill', 'sensor', 'fill')"
"""The runs the scheduler retries."""
comptime RETRIED_SQL = _RETRIED
"""`_RETRIED`, for `orca.actions`."""


@fieldwise_init
struct Registered(Copyable, Movable):
    """A registered pipeline, as the scheduler reads it from `describe`."""

    var name: String
    var binary: String
    var registry_hash: String
    var since: Int64
    var cron: String
    var tz: String
    var fills: String
    var catchup: Bool
    var kind: String
    var start: String
    var max_runs: Int
    var run_retries: Int
    var retry_seconds: Int
    """The first retry's wait, doubling; 0 for `ORCA_RETRY_SECONDS`."""
    var max_retry_seconds: Int
    """The longest wait between retries; 0 for no limit."""
    var sensor: Bool
    var keys_per_run: Int
    var keys_dataset: String
    """A keyed pipeline's keys dataset; empty for time partitions."""
    var image: String
    """The image its runs' pods run (`orca register --image`), or empty."""
    var workers: Int
    """Its cluster's dista workers; 0 if it has no tasks."""
    var head_type: String
    """On EC2, its runs' head instance type; empty for the default."""
    var worker_type: String
    var spot: Bool
    """On EC2, whether its workers are Spot instances."""
    var min_workers: Int
    """On EC2, its runs' fewest workers, scaling up to `workers`; equal to
    `workers` for a fixed count."""
    var secrets: List[Tuple[String, String]]
    """Its runs' secrets: environment variable and Secrets Manager ID."""
    var paused: Bool
    """`orca pause`: its runs are queued as ever, but none is started."""
    var worker_procs: Int
    """On EC2, the dista worker processes on each worker instance
    (`orca register --worker-procs`); 0 is one per vCPU."""
    var placement: String
    """Where its runs run (`orca register --placement`): `control` (the
    scheduler's host, `orca.control`), `ec2`, or empty for the scheduler's
    runner."""
    var env: List[Tuple[String, String]]
    """Its runs' own variables (`env` in orca.toml, `orca.config`), given
    to its runs alone, over those `ORCA_RUN_ENV` names."""
    var fill: Fill
    """How it fills its range by itself (`orca.fill`)."""
    var cpus: String
    """The CPUs it asks for, `N` or `MIN..MAX` (`orca.resources`); empty
    for none."""
    var memory_gb: String
    """The memory it asks for, in GB; empty for none."""
    var memory_raised_mb: Int
    """What an out-of-memory kill raised its memory to (`orca.cgroup`),
    which its runs ask for at least; 0 for none."""
    var learn: Bool
    """Whether `auto` uses what its runs suggest (`learn` in orca.toml)."""
    var learn_key: String
    """For a pipeline asking for `auto`, what a learned request now would be
    learned from: its latest successful run (and whether that's measured
    yet), its out-of-memory bump and its declared request; else empty."""
    var learned: Recommendation
    """The learned request cached in `orca.pipelines.learned` (its runs,
    memory and CPUs), learned from `learned_key`."""
    var learned_key: String

    def request(self) raises -> Request:
        """As declared: `auto` not resolved (`run_request` does)."""
        return Request.of(self.cpus, self.memory_gb)


def recorded_cpus(p: Registered, sized: RunRequest) -> String:
    """The CPUs a run records it asked for (`orca.runs.request_cpus`): as
    registered, or what `auto` resolved to."""
    return sized.cpus_text() if p.cpus.startswith("auto") else p.cpus


def recorded_memory_gb(p: Registered, sized: RunRequest) -> String:
    """The memory a run records it asked for, as `recorded_cpus`."""
    return sized.memory_gb_text() if p.memory_gb.startswith(
        "auto"
    ) else p.memory_gb


def run_request(
    mut db: State, p: Registered, most_cpus: Int = 0
) raises -> RunRequest:
    """The request a start of `p` gets now (`orca.recommend`'s
    `resolve_request`): as declared, or for `auto` what its runs suggest. That is cached on the
    pipeline (`orca.pipelines.learned`) and learned again only when what it
    was learned from (`learn_key`) has changed, so a start doesn't query
    its runs' peaks each tick. Learned CPUs are at most `most_cpus`, what
    the host grants a run."""
    var declared = p.request()
    if not declared.auto() or not p.learn:
        return resolve_request(declared, Recommendation.none(), p.learn)
    if p.learned_key == p.learn_key:
        return resolve_request(declared, p.learned, True, most_cpus)
    var rec = learned(db, p.name, p.cpus, p.memory_gb)
    _ = db.conn.execute(
        (
            "UPDATE orca.pipelines SET learned = jsonb_build_object('key',"
            " $2::text, 'runs', $3::bigint, 'memory_mb', $4::bigint, 'cpus',"
            " $5::bigint) WHERE name = $1"
        ),
        Params()
        .text(p.name)
        .text(p.learn_key)
        .int64(Int64(rec.runs))
        .int64(Int64(rec.memory_mb))
        .int64(Int64(rec.cpus)),
    )
    return resolve_request(declared, rec, True, most_cpus)


def _ec2_spec(p: Registered, run: Int, start: Int, new_code: Bool) -> Ec2Spec:
    """Start `start` of `p`'s run `run`, as the ec2 runner starts it."""
    return Ec2Spec(
        run,
        start,
        p.name,
        p.image,
        p.binary,
        p.workers > 0,
        p.head_type,
        p.worker_type,
        p.workers,
        p.spot,
        new_code,
        p.min_workers,
        p.secrets.copy(),
        p.worker_procs,
        p.registry_hash,
        resolve_auto(p.env),
    )


def fill_key(kind: String, fills: String, local: Civil) raises -> String:
    """The partition a fire fills, given its time on the schedule's wall
    clock: the key the time falls in, or the one before it."""
    if kind == "daily":
        var d = Day.containing(local.year, local.month, local.day)
        return String(d.prev() if fills == "previous" else d^)
    if kind == "monthly":
        var m = Month.containing(local.year, local.month, local.day)
        return String(m.prev() if fills == "previous" else m^)
    raise Error("orca: unknown partitions '", kind, "'")


def repeated_fill(p: Registered, after: Int64) raises -> String:
    """How `p`'s schedule fires more than once for one partition in the
    year or so after `after` (such as `0 */6 * * *` on daily partitions), or
    empty if it doesn't."""
    if not p.cron or p.kind == "keyed":
        return ""
    var s = Schedule(CronExpr.parse(p.cron), Zone.load(p.tz))
    var t = after
    var last = String()
    for _ in range(2000):
        t = s.next_fire(t)
        if t > after + 400 * 86400:
            break
        var key = fill_key(p.kind, p.fills, Civil.of(s.zone.to_local(t)))
        if key == last:
            return String(
                "the schedule of ",
                p.name,
                " (",
                p.cron,
                ") fires more than once for ",
                key,
                ": a fire while a run of the partition is live is left to",
                " it, but one after that run has ended runs the partition",
                " again, and its steps append again",
            )
        last = key^
    return ""


def registered(mut db: State, only: String = "") raises -> List[Registered]:
    """Every pipeline registered with a binary, or the one named."""
    var out = List[Registered]()
    var rows = db.conn.query(
        (
            "SELECT name, binary_path, registry_hash, coalesce(since, 0) AS"
            " since, coalesce(describe->'schedule'->>'cron', '') AS cron,"
            " coalesce(describe->'schedule'->>'tz', 'UTC') AS tz,"
            " coalesce(describe->'schedule'->>'fills', 'previous') AS fills,"
            " coalesce((describe->'schedule'->>'catchup')::boolean, true) AS"
            " catchup, coalesce(describe->'partitions'->>'kind', 'monthly') AS"
            " kind, coalesce(describe->'partitions'->>'start', '') AS start,"
            " coalesce(max_runs, (describe->>'max_runs')::bigint, 1) AS"
            " max_runs, coalesce((describe->>'run_retries')::bigint, 3) AS"
            " run_retries, coalesce((describe->>'retry_seconds')::bigint, 0) AS"
            " retry_seconds, coalesce((describe->>'max_retry_seconds')::bigint,"
            " 0) AS max_retry_seconds, coalesce((describe->>'sensor')::boolean,"
            " false) AS sensor, coalesce((describe->>'keys_per_run')::bigint,"
            " 1) AS keys_per_run, coalesce(describe->'partitions'->>'dataset',"
            " '') AS keys_dataset, coalesce(image, '') AS image, CASE WHEN"
            " jsonb_array_length(coalesce(describe->'tasks', '[]')) > 0 THEN"
            " coalesce(ec2_workers, (describe->>'workers')::bigint, 2) ELSE 0"
            " END AS workers, coalesce(head_type, '') AS head_type,"
            " coalesce(worker_type, '') AS worker_type, coalesce(spot, false)"
            " AS spot, coalesce(ec2_min_workers, -1) AS min_workers,"
            " coalesce(secrets::text, '') AS secrets, paused,"
            " coalesce(worker_procs, 1) AS worker_procs, coalesce(placement,"
            " '') AS placement, coalesce(env::text, '') AS env, coalesce(fills,"
            " '') AS fill_mode, coalesce(fill_order, '') AS fill_order,"
            " coalesce(fill_max_runs, 0) AS fill_max_runs,"
            " coalesce(fill_budget_per_day, -1) AS fill_budget,"
            " coalesce(fill_window, '') AS fill_window, coalesce(fill_lag, '')"
            " AS fill_lag, coalesce(cpus, '') AS cpus, coalesce(memory_gb, '')"
            " AS memory_gb,"
            " coalesce(memory_raised_mb, 0) AS memory_raised_mb,"
            " coalesce(learn, true) AS learn, CASE WHEN cpus LIKE 'auto%' OR"
            " memory_gb LIKE 'auto%' THEN concat_ws('/', coalesce((SELECT"
            " r.id::text || CASE WHEN EXISTS (SELECT 1 FROM orca.steps s JOIN"
            " orca.attempts a ON a.step = s.id WHERE s.run = r.id AND"
            " a.peak_rss_bytes IS NOT NULL) THEN 'm' ELSE '' END FROM"
            " orca.runs r WHERE r.pipeline = orca.pipelines.name AND r.status"
            " = 'succeeded' ORDER BY r.id DESC LIMIT 1), ''),"
            " coalesce(memory_raised_mb, 0), coalesce(cpus, ''),"
            " coalesce(memory_gb, '')) ELSE '' END AS learn_key,"
            " coalesce(learned->>'key', '') AS learned_key,"
            " coalesce((learned->>'runs')::bigint, 0) AS learned_runs,"
            " coalesce((learned->>'memory_mb')::bigint, 0) AS"
            " learned_memory_mb, coalesce((learned->>'cpus')::bigint, 0) AS"
            " learned_cpus FROM orca.pipelines"
            " WHERE binary_path IS NOT NULL AND ($1 = '' OR name = $1) ORDER BY"
            " name"
        ),
        Params().text(only),
    )
    for r in rows:
        out.append(
            Registered(
                r.text("name"),
                r.text("binary_path"),
                r.text("registry_hash"),
                r.int64("since"),
                r.text("cron"),
                r.text("tz"),
                r.text("fills"),
                r.bool("catchup"),
                r.text("kind"),
                r.text("start"),
                Int(r.int64("max_runs")),
                Int(r.int64("run_retries")),
                Int(r.int64("retry_seconds")),
                Int(r.int64("max_retry_seconds")),
                r.bool("sensor"),
                Int(r.int64("keys_per_run")),
                r.text("keys_dataset"),
                r.text("image"),
                Int(r.int64("workers")),
                r.text("head_type"),
                r.text("worker_type"),
                r.bool("spot"),
                Int(r.int64("min_workers")),
                parse_secrets(r.text("secrets")),
                r.bool("paused"),
                Int(r.int64("worker_procs")),
                r.text("placement"),
                parse_env(r.text("env")),
                Fill.of(
                    r.text("fill_mode"),
                    r.text("fill_order"),
                    Int(r.int64("fill_max_runs")),
                    r.float64("fill_budget"),
                    r.text("fill_window"),
                    r.text("fill_lag"),
                ),
                r.text("cpus"),
                r.text("memory_gb"),
                Int(r.int64("memory_raised_mb")),
                r.bool("learn"),
                r.text("learn_key"),
                Recommendation(
                    Int(r.int64("learned_runs")),
                    0,
                    Int(r.int64("learned_memory_mb")),
                    Int(r.int64("learned_cpus")),
                    "",
                    "",
                    False,
                ),
                r.text("learned_key"),
            )
        )
    # A fixed count, or no tasks: no scaling.
    for ref p in out:
        if p.min_workers < 0 or p.workers == 0:
            p.min_workers = p.workers
        p.min_workers = min(p.min_workers, p.workers)
    return out^


def placed_of(p: Registered) -> String:
    """Where `p`'s runs start under a scheduler with this process's
    `ORCA_RUNNER`: `control`, `ec2`, `kubernetes` or `local`."""
    if p.placement == CONTROL or pool_of(p.placement):
        return p.placement
    var runner = getenv("ORCA_RUNNER", "local")
    if p.placement == "ec2":
        return "ec2"
    return runner


def fill_pipeline(
    p: Registered, placed: String = "", pool_seconds: Int = -1
) -> FillPipeline:
    """What `orca.fill.fill_status` needs of `p`; where its runs start and
    the warm pool's seconds from this process's environment unless
    given."""
    var pool = pool_seconds
    if pool < 0:
        try:
            pool = Int(Float64(getenv("ORCA_EC2_POOL_SECONDS", "150")))
        except:
            pool = 150
    return FillPipeline(
        p.name,
        p.fill.copy(),
        p.kind,
        p.start,
        p.tz,
        p.paused,
        placed if placed else placed_of(p),
        p.head_type,
        p.worker_type,
        p.workers,
        pool,
    )


def dataset_cycle(mut db: State, describe: String) raises -> String:
    """How the pipeline `describe` gives would close a cycle across
    pipelines, through the datasets they write and read, or empty if it
    wouldn't. A pipeline reading a dataset it writes itself isn't one."""
    var rows = db.conn.query(
        (
            "WITH d AS (SELECT name, describe FROM orca.pipelines WHERE name"
            " <> $1::jsonb->>'pipeline' UNION ALL SELECT $1::jsonb->>"
            "'pipeline', $1::jsonb), io AS (SELECT DISTINCT d.name, k.kind,"
            " x.ds FROM d, jsonb_array_elements(d.describe->'steps') AS s(st),"
            " (VALUES ('reads'), ('writes')) AS k(kind),"
            " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
            " x(ds)) SELECT w.name AS src, r.name AS dst, w.ds, $1::jsonb->>"
            "'pipeline' AS me FROM io w JOIN io r ON r.ds = w.ds AND r.kind ="
            " 'reads' WHERE w.kind = 'writes' AND w.name <> r.name ORDER BY"
            " 1, 2, 3"
        ),
        Params().text(describe),
    )
    if len(rows) == 0:
        return ""
    var me = rows.row(0).text("me")
    var src = List[String]()
    var dst = List[String]()
    var via = List[String]()
    for r in rows:
        src.append(r.text("src"))
        dst.append(r.text("dst"))
        via.append(r.text("ds"))
    # A path from `me` back to `me`, breadth first, by the edge taken.
    var came = List[Int](length=len(src), fill=-2)
    var frontier = List[Int]()
    for i in range(len(src)):
        if src[i] == me:
            came[i] = -1
            frontier.append(i)
    while len(frontier) > 0:
        var next = List[Int]()
        for e in frontier:
            if dst[e] == me:
                var path = List[Int]()
                var at = e
                while at >= 0:
                    path.append(at)
                    at = came[at]
                var out = String()
                for k in range(len(path) - 1, -1, -1):
                    var i = path[k]
                    out.write(
                        src[i],
                        " writes ",
                        via[i],
                        ", which ",
                        dst[i],
                        " reads",
                        "; " if k > 0 else "",
                    )
                return out^
            for i in range(len(src)):
                if src[i] == dst[e] and came[i] == -2:
                    came[i] = e
                    next.append(i)
        frontier = next^
    return ""


def parse_keys_output(
    binary: String, text: String
) raises -> Tuple[Int64, List[String]]:
    """What `<binary> keys` printed: its keys snapshot, then its keys."""
    var lines = text.splitlines()
    if len(lines) == 0:
        raise Error("orca: ", binary, " keys printed nothing")
    var keys = List[String]()
    for i in range(1, len(lines)):
        if lines[i]:
            keys.append(String(lines[i]))
    return (Int64(Int(lines[0])), keys^)


def current_keys(
    p: Registered, timeout: Float64 = -1
) raises -> Tuple[Int64, List[String]]:
    """A keyed pipeline's keys snapshot and keys, as `<binary> keys`
    prints them, killed past `timeout` seconds (by default
    `ORCA_BINARY_SECONDS`)."""
    var got = capture(
        [p.binary, "keys"], timeout if timeout >= 0 else binary_seconds()
    )
    if got[0] != 0:
        raise Error("orca: ", p.binary, " keys exited ", got[0], ": ", got[1])
    return parse_keys_output(p.binary, got[1])


def keep_keys(
    mut db: State, pipeline: String, keys: Tuple[Int64, List[String]]
) raises:
    """Records the keys `<binary> keys` printed, in `orca.keys`."""
    var text = String()
    for i in range(len(keys[1])):
        text.write("\n" if i else "", keys[1][i])
    _ = db.conn.execute(
        (
            "INSERT INTO orca.keys (pipeline, snapshot_id, keys) VALUES ($1,"
            " $2, $3) ON CONFLICT (pipeline) DO UPDATE SET snapshot_id ="
            " EXCLUDED.snapshot_id, keys = EXCLUDED.keys, read_at = now()"
        ),
        Params().text(pipeline).int64(keys[0]).text(text),
    )


@fieldwise_init
struct KeptKeys(Copyable, Movable):
    """A pipeline's keys as `orca.keys` has them."""

    var snapshot: Int64
    var keys: List[String]
    var read_at: String
    var age: Float64
    """Seconds since they were read."""


def kept_keys(mut db: State, pipeline: String) raises -> Optional[KeptKeys]:
    var rows = db.conn.query(
        (
            "SELECT snapshot_id, keys, to_char(read_at AT TIME ZONE 'UTC',"
            ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS at, extract(epoch FROM'
            " now() - read_at)::float8 AS age FROM orca.keys WHERE pipeline ="
            " $1"
        ),
        Params().text(pipeline),
    )
    if len(rows) == 0:
        return None
    var r = rows.row(0)
    var keys = List[String]()
    for k in r.text("keys").split("\n"):
        if k:
            keys.append(String(k))
    return KeptKeys(
        r.int64("snapshot_id"), keys^, r.text("at"), r.float64("age")
    )


def file_names(path: String) raises -> String:
    """The names in the file at `path`, a line each, as `NAME,NAME,...`
    (`@file:<path>`)."""
    var text: String
    with open(path, "r") as f:
        text = f.read()
    var joined = String()
    var at = 0
    for line in text.splitlines():
        at += 1
        var n = line.strip()
        if n:
            # The error names the line, not its text: a file's other
            # contents stay out of the message.
            try:
                _ = Name.parse(n)
            except:
                raise Error("orca: line ", at, " of ", path, " isn't a name")
            joined.write("," if joined else "", n)
    if not joined:
        raise Error("orca: no names in ", path)
    return joined^


def kind_mismatch(mut db: State, describe: String) raises -> String:
    """How the pipeline `describe` gives would make a sensor run keys of the
    other kind, a keyed sensor reading a dataset a time-partitioned
    pipeline writes or the other way round, or empty if it wouldn't.

    A keyed sensor's own keys dataset needs no mapping, whoever writes it:
    a new snapshot of it offers the keys it holds, which are names already.
    So a dated pipeline may write the keys a keyed sensor runs."""
    var rows = db.conn.query(
        (
            "WITH d AS (SELECT name, describe FROM orca.pipelines WHERE name"
            " <> $1::jsonb->>'pipeline' UNION ALL SELECT $1::jsonb->>"
            "'pipeline', $1::jsonb), io AS (SELECT DISTINCT d.name,"
            " coalesce(d.describe->'partitions'->>'kind', 'monthly') AS"
            " pkind, coalesce((d.describe->>'sensor')::boolean, false) AS"
            " sensor, k.kind, x.ds, coalesce(d.describe->'partitions'->>"
            "'dataset', '') AS keys FROM d, jsonb_array_elements(d.describe->"
            "'steps') AS s(st), (VALUES ('reads'), ('writes')) AS k(kind),"
            " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
            " x(ds) UNION SELECT d.name, 'keyed', coalesce((d.describe->>"
            "'sensor')::boolean, false), 'reads', d.describe->'partitions'->>"
            "'dataset', d.describe->'partitions'->>'dataset' FROM d WHERE"
            " d.describe->'partitions'->>'dataset' IS NOT NULL) SELECT w.name"
            " AS writer, w.pkind AS wkind, r.name AS reader, r.pkind AS rkind,"
            " w.ds FROM io w JOIN io r ON r.ds = w.ds AND r.kind = 'reads'"
            " WHERE w.kind = 'writes' AND w.name <> r.name AND r.sensor AND"
            " (w.pkind = 'keyed') <> (r.pkind = 'keyed') AND NOT (r.pkind ="
            " 'keyed' AND r.ds = r.keys) AND $1::jsonb->>'pipeline' IN"
            " (w.name, r.name) ORDER BY 1, 3, 5 LIMIT 1"
        ),
        Params().text(describe),
    )
    if len(rows) == 0:
        return ""
    var r = rows.row(0)
    return String(
        "the sensor ",
        r.text("reader"),
        " (",
        r.text("rkind"),
        ") reads ",
        r.text("ds"),
        ", which ",
        r.text("writer"),
        " (",
        r.text("wkind"),
        ") writes, and no rule turns a name into a date or a date into a",
        " name",
    )


def partition_keys(p: Registered, keys: String) raises -> List[String]:
    """`keys` as the pipeline's partition keys, each checked: for time keys
    `KEY[..KEY][,...]` from its first partition on; for keyed partitions a
    list of names, `@all` or `@file:<path>`, each a current key."""
    var texts = List[String]()
    if p.kind == "keyed":
        var current = current_keys(p)[1].copy()
        if keys == "@all":
            return current^
        var have = Dict[String, Bool]()
        for k in current:
            have[k] = True
        var list = file_names(String(keys[byte=6:])) if keys.startswith(
            "@file:"
        ) else keys
        for n in parse_names(list):
            if n.text not in have:
                raise Error("orca: ", n, " isn't a key of ", p.keys_dataset)
            texts.append(n.text)
        return texts^
    if p.kind == "daily":
        for k in parse_keys[Day](keys, p.start):
            texts.append(String(k))
    else:
        for k in parse_keys[Month](keys, p.start):
            texts.append(String(k))
    return texts^


comptime DONE_SQL = (
    "WITH k AS (SELECT DISTINCT unnest(string_to_array($2, ',')) AS key), p"
    " AS (SELECT registry_hash, describe FROM orca.pipelines WHERE name ="
    " $1), m AS (SELECT x->>'name' AS step FROM p,"
    " jsonb_array_elements(p.describe->'steps') AS x WHERE x->>'kind' ="
    " 'mapped') SELECT k.key FROM k WHERE CASE WHEN EXISTS (SELECT 1 FROM m)"
    " THEN NOT EXISTS (SELECT 1 FROM m WHERE NOT EXISTS (SELECT 1 FROM"
    " orca.steps st JOIN orca.runs r ON r.id = st.run JOIN p ON"
    " r.registry_hash = p.registry_hash WHERE r.pipeline = $1 AND st.step ="
    " m.step AND st.partition = k.key AND st.status = 'succeeded')) ELSE"
    " false END UNION ALL SELECT k.key FROM k WHERE k.key IN (SELECT"
    " unnest(string_to_array(r.partitions, ',')) FROM orca.runs r JOIN p ON"
    " r.registry_hash = p.registry_hash WHERE NOT EXISTS (SELECT 1 FROM m)"
    " AND r.pipeline = $1 AND r.status = 'succeeded')"
)
"""Of the comma-separated keys `$2`, those done for pipeline `$1`: every
mapped step has succeeded for the key in a run of the pipeline's current
code. A pipeline with no mapped step has a key done when a run of its
current code over it succeeded. The two are separate halves, each empty
when the other applies. The first looks each key's steps up by index (the
`CASE` keeps Postgres from hashing every step of every pipeline instead);
the second joins the keys to its runs' keys, read once, rather than
scanning the runs again for each key (1.2 s for 1,500 keys over 30,000
runs when it was a `CASE` branch too, 10 ms now)."""


@fieldwise_init
struct Queued(Movable):
    """What a backfill or a keyed sensor queued, and the keys it left."""

    var runs: List[Int]
    var done: List[String]
    """Keys left because they're done."""
    var live: List[String]
    """Keys left because a live run has them, that run in `live_runs`."""
    var live_runs: List[Int]
    var keys: List[String]
    """The keys queued (or, in a preview, that would be), sorted."""
    var batches: Int
    """How many runs those keys make, `keys_per_run` to a run; a preview
    counts them without queueing any."""

    def __init__(out self):
        self.runs = List[Int]()
        self.done = List[String]()
        self.live = List[String]()
        self.live_runs = List[Int]()
        self.keys = List[String]()
        self.batches = 0

    def skipped(self) -> List[String]:
        """A line for each reason keys were left, or none."""
        var out = List[String]()
        if len(self.done) > 0:
            var line = String("skipped ", len(self.done), " done: ")
            _list(line, self.done)
            out.append(line^)
        if len(self.live) > 0:
            var line = String("skipped ", len(self.live), " in live runs: ")
            var named = List[String]()
            for i in range(len(self.live)):
                named.append(
                    String(self.live[i], " (run ", self.live_runs[i], ")")
                )
            _list(line, named)
            out.append(line^)
        return out^


def _list(mut line: String, items: List[String]):
    """The first ten of `items`, comma-separated, and how many more."""
    for i in range(min(len(items), 10)):
        line.write(", " if i else "", items[i])
    if len(items) > 10:
        line.write(", and ", len(items) - 10, " more")


def done_keys(
    mut db: State, pipeline: String, keys: List[String]
) raises -> Dict[String, Bool]:
    """Which of `keys` are done for `pipeline` (`DONE_SQL`)."""
    var joined = String()
    for i in range(len(keys)):
        joined.write("," if i else "", keys[i])
    var out = Dict[String, Bool]()
    for r in db.conn.query(DONE_SQL, Params().text(pipeline).text(joined)):
        out[r.text("key")] = True
    return out^


def queue_keys(
    mut db: State,
    p: Registered,
    keys: List[String],
    trigger: String,
    rerun: Bool = False,
) raises -> Queued:
    """Queues runs of `p` over those of `keys` that aren't done and aren't
    in a live run, sorted, `keys_per_run` to a run; returns the runs, and
    the keys it left and why. With
    `rerun`, keys that are done are queued too, but never one in a live
    run. Under a lock on the pipeline's row, so two backfills don't both
    queue a key."""
    var tx = db.conn.begin()
    var got = _queue(tx, p, keys, trigger, rerun)
    tx.commit()
    return got^


def _queue(
    mut tx: Transaction,
    p: Registered,
    keys: List[String],
    trigger: String,
    rerun: Bool = False,
    dry: Bool = False,
) raises -> Queued:
    """`queue_keys` in `tx`; with `dry`, what it would queue, queueing
    nothing."""
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    var got = Queued()
    var live = live_runs(tx, p.name)
    var joined = String()
    for i in range(len(keys)):
        joined.write("," if i else "", keys[i])
    var done = Dict[String, Bool]()
    if not rerun:
        for r in tx.query(DONE_SQL, Params().text(p.name).text(joined)):
            done[r.text("key")] = True
    var seen = Dict[String, Bool]()
    var todo = List[String]()
    for k in keys:
        if k in seen:
            continue
        seen[k] = True  # each once
        var in_run = live.get(k)
        if in_run:
            got.live.append(k)
            got.live_runs.append(in_run.value())
        elif k in done:
            got.done.append(k)
        else:
            todo.append(k)
    sort_texts(todo)
    got.keys = todo.copy()
    ref runs = got.runs
    var i = 0
    while i < len(todo):
        var batch = String()
        var j = i
        while j < len(todo) and j < i + p.keys_per_run:
            batch.write("," if j > i else "", todo[j])
            j += 1
        got.batches += 1
        if dry:
            i = j
            continue
        runs.append(
            Int(
                tx.query(
                    (
                        "INSERT INTO orca.runs (pipeline, registry_hash,"
                        " partitions, trigger, status) VALUES ($1, $2, $3, $4,"
                        " 'queued') RETURNING id"
                    ),
                    Params()
                    .text(p.name)
                    .text(p.registry_hash)
                    .text(batch)
                    .text(trigger),
                )
                .row(0)
                .int64("id")
            )
        )
        i = j
    return got^


comptime APPENDING_SQL = (
    "SELECT x.st->>'name' AS step, w.ds FROM orca.pipelines p,"
    " jsonb_array_elements(p.describe->'steps') WITH ORDINALITY AS x(st, n),"
    " jsonb_array_elements_text(coalesce(x.st->'writes', '[]'::jsonb)) AS"
    " w(ds) WHERE p.name = $1 AND x.n > $2 AND NOT coalesce(x.st->'replaces',"
    " '[]'::jsonb) ? w.ds ORDER BY x.n, w.ds"
)
"""`step` and `ds` for each dataset a step of pipeline `$1` after its
first `$2` declares in `writes=` and not in `replaces=`."""


def appending_writes(
    mut db: State, pipeline: String, from_step: Int = 0
) raises -> List[String]:
    """`step <name> writes <dataset>` for each dataset a step of `pipeline`
    writes without replacing it (in `writes=`, not `replaces=`), from its
    `from_step`th step on (0, the first): what a new run over a key that
    already has rows appends to again. A pipeline registered before
    `replaces=` existed appends to all it writes."""
    var out = List[String]()
    for r in db.conn.query(
        APPENDING_SQL, Params().text(pipeline).int64(Int64(from_step))
    ):
        out.append(String("step ", r.text("step"), " writes ", r.text("ds")))
    return out^


def append_again(
    pipeline: String, what: String, writes: List[String], how: String
) -> String:
    """Why a rerun of `what` is refused: it appends again (`writes`, from
    `appending_writes`). `how` says how to rerun anyway."""
    var line = String(
        "orca: rerunning ",
        what,
        " appends again to what ",
        pipeline,
        " appended to before, duplicating those rows: ",
    )
    _list(line, writes)
    line.write(
        ". Declare a dataset in its step's replaces= to have a new run",
        " replace its partitions (orca.tables), or ",
        how,
    )
    return line^


comptime ALLOW_APPEND_HOW = "pass --allow-append to append anyway"


def rerun_refusal(mut db: State, p: Registered, keys: String) raises -> String:
    """Why `orca backfill --rerun` of `keys` is refused without
    `--allow-append`, or empty: some are done, and the pipeline's steps
    append to datasets (`appending_writes`), so a new run of them would
    leave their rows twice."""
    var writes = appending_writes(db, p.name)
    if len(writes) == 0:
        return ""
    var done = done_keys(db, p.name, partition_keys(p, keys))
    if len(done) == 0:
        return ""
    var names = List[String]()
    for k in done.keys():
        names.append(k)
    sort_texts(names)
    var what = String()
    _list(what, names)
    what.write(" (done)")
    return append_again(p.name, what, writes, ALLOW_APPEND_HOW)


def backfill(
    mut db: State,
    p: Registered,
    keys: String,
    rerun: Bool = False,
    allow_append: Bool = False,
) raises -> Queued:
    """Queues runs for the keys in `keys` that aren't done and aren't in a
    live run, `keys_per_run` to a run; returns the runs it queued and the
    keys it left. With `rerun` (`orca backfill --rerun`) the done ones are
    queued again too, as for a day whose source has changed since; unless
    `allow_append`, that's refused for a pipeline whose steps append to
    what they write (`rerun_refusal`)."""
    if rerun and not allow_append:
        var why = rerun_refusal(db, p, keys)
        if why:
            raise Error(why)
    return queue_keys(db, p, partition_keys(p, keys), "backfill", rerun)


def preview_backfill(
    mut db: State,
    p: Registered,
    keys: String,
    rerun: Bool = False,
    allow_append: Bool = False,
) raises -> Queued:
    """What `backfill` would queue now, queueing nothing: the keys, how many
    runs they make, and the keys it would leave and why; or its refusal."""
    if rerun and not allow_append:
        var why = rerun_refusal(db, p, keys)
        if why:
            raise Error(why)
    var texts = partition_keys(p, keys)
    var tx = db.conn.begin()
    var got = _queue(tx, p, texts, "backfill", rerun, dry=True)
    tx.rollback()
    return got^


def _last_line(path: String) -> String:
    """The last line with text in the file at `path`, at most 300 bytes of
    it, or empty if there's none or it can't be read."""
    if not path or not exists(path):
        return ""
    try:
        var text: String
        with open(path, "r") as f:
            text = f.read()
        var lines = text.splitlines()
        for i in range(len(lines) - 1, -1, -1):
            var line = String(lines[i].strip())
            if line and not line.startswith("--- start "):
                var n = line.byte_length()
                if n > 300:
                    # Cut where a character starts.
                    n = 300
                    while n > 0 and (line.as_bytes()[n] & 0xC0) == 0x80:
                        n -= 1
                    return String(line[byte=:n])
                return line^
    except:
        pass
    return ""


@fieldwise_init
struct _ControlProc(Copyable, Movable):
    """A control-placed run's process, as the scheduler keeps it."""

    var run: Int
    var binary: String
    var stage: String
    """Where its output goes, or empty without `ORCA_LOGS`."""
    var location: String
    """The run's `driver.log`, which `keep-log` appends `stage` to."""
    var start: Int
    var env: List[String]
    """What it was given beside the scheduler's environment: its secrets'
    values among them, which `keep-log` masks."""
    var over: String
    """Why the scheduler killed it, over its memory cap, or empty."""


struct Scheduler(Movable):
    var db: State
    var stale: Float64
    var retry_seconds: Int64
    var sense_seconds: Float64
    var start_grace: Float64
    """How long a run the scheduler started may go without a heartbeat."""
    var budget: Int
    """`ORCA_VCPU_BUDGET` in thousandths of a vCPU; 0 for none."""
    var runner: Optional[KubeRunner]
    """Set with `ORCA_RUNNER=kubernetes`."""
    var ec2: Optional[Ec2Runner]
    """Set with `ORCA_RUNNER=ec2`; with neither, runs are local processes."""
    var logs: String
    var _polled: Dict[String, Int]
    """When each sensor's binary was last asked to `sense`, on the monotonic
    clock in nanoseconds."""
    var _driver_logs: Dict[Int, String]
    """A local run's process to where its driver log goes, when that's in
    object storage: it's published once the process is reaped."""
    var _spawned: Dict[Int, Tuple[Int, Int, String]]
    """A local run's process to its run, the run's start count when it was
    started, and the local file its output goes to (or empty)."""
    var _asked: Dict[String, Capture]
    """The `sense` and `keys` of pipeline binaries still going, by
    `<command> <pipeline>`: a tick starts one and later ticks check on it,
    so a slow binary holds up no other pipeline."""
    var _unsensed: Dict[String, String]
    """A keyed sensor's `sense` output, kept until its keys are read."""
    var _keys: Dict[String, Tuple[Int64, List[String]]]
    """Each keyed sensor's keys as last read, by pipeline."""
    var wait_for_binaries: Bool
    """Whether a tick waits for the binaries it asks (`orca tick`, which
    has no later tick to collect them), rather than checking next tick."""
    var expire_seconds: Float64
    """How often every dataset is expired by its own policy
    (`ORCA_EXPIRE_HOURS`, 24; 0 turns it off)."""
    var maintain: String
    """The `orca_maintain` binary that expires, or empty for none."""
    var _expire_next: Int
    """When the next expiry sweep starts, on the monotonic clock."""
    var _expiring: List[String]
    """The datasets this sweep has still to expire, the first one going."""
    var fleet_seconds: Float64
    """How often the fleet page's samples are read from the instances'
    files (`ORCA_FLEET_SECONDS`, 30; 0 turns it off)."""
    var _fleet_next: Int
    """When the next pass starts, on the monotonic clock."""
    var _archive: Archive
    """The control instance's day files between passes (`orca.system`)."""
    var watchdog_seconds: Float64
    """How often the watchdog's rules are checked
    (`ORCA_WATCHDOG_SECONDS`, 60; 0 turns it off): `orca.watchdog`."""
    var _watchdog_next: Int
    """When the next check starts, on the monotonic clock."""
    var _notify_next: Int
    """When the alerts' checks (`orca.alerts`) run next, on the monotonic
    clock; they are every `ORCA_NOTIFY_CHECK_SECONDS` (10)."""
    var limits: ControlLimits
    """What a control-placed run may use (`orca.control`)."""
    var room: Room
    """What this host can grant the runs of pipelines with a request
    (`orca.resources`): its capacity less its reserve."""
    var _control: Dict[Int, _ControlProc]
    """A control-placed run's process to what its end needs."""
    var _kept: Dict[Int, _ControlProc]
    """A local run's process whose output, staged for its secrets, `keep-log`
    appends to its `driver.log` once it ends."""
    var _keeping: Dict[Int, String]
    """A `keep-log` still going to the staging file it keeps, removed once
    it has ended."""
    var _meters: Dict[Int, Meter]
    """A local or control-placed run's process to its samples, recorded on
    its attempts when it's reaped (`orca.usage`)."""
    var psi: Source
    """Where this host's memory pressure is read from (`orca.pressure`),
    found once; none on macOS or without PSI."""
    var admission: Admission
    """Whether runs may start on this host for its memory pressure, from
    each tick's reading."""
    var _pressure_kept: Int
    """When the reading was last kept in `orca.pressure`, on the monotonic
    clock."""
    var _pressure_unread: Bool
    """The file was found but last couldn't be read (said once)."""
    var cgroups: Cgroups
    """Where its runs' cgroups go (`orca.cgroup`); off until `contain`."""
    var _contained: Dict[Int, Contained]
    """A local or control-placed run's process to its cgroup."""
    var _unswept: List[String]
    """Runs' cgroups to remove once their processes have left them."""
    var _stalled: Dict[Int, Bool]
    """Contained runs' processes killed while held at their memory limit,
    for a stale heartbeat or by `_kill_held`: out of memory, as a kill at
    memory.max is."""
    var _held: HeldWatch
    """Since when contained runs have been held at their memory limit."""
    var _cgroups_next: Int
    """When the runs' cgroups' memory is next recorded, on the monotonic
    clock."""
    var instance: Int
    """This scheduler's row of `orca.schedulers` (`orca.instances`), or 0
    when it isn't an instance: `ORCA_OVERLAP` off, or `orca tick`."""
    var leading: Bool
    """Whether this instance holds the lead (always, for no instance)."""
    var draining: Bool
    """Whether this instance drains: it starts nothing, and exits once the
    processes it started have ended."""
    var drain_seconds: Float64
    """`ORCA_DRAIN_SECONDS` (3600): how long a drain waits for its runs
    before it kills what's left."""
    var _drain_start: Int
    """When the drain began, on the monotonic clock."""
    var finished: Bool
    """Whether a drained instance has ended: `orca scheduler` exits."""
    var _going: Int
    """For an instance, the local and control-placed runs going on this
    host across every instance, as this tick counted them: memory
    pressure's "none of orca's runs going"."""

    def __init__(out self, url: String) raises:
        self.db = State(url)
        self.db.migrate()
        self.stale = stale_limit()
        self.retry_seconds = Int64(Int(getenv("ORCA_RETRY_SECONDS", "60")))
        self.sense_seconds = Float64(getenv("ORCA_SENSE_SECONDS", "60"))
        var runner = getenv("ORCA_RUNNER", "local")
        self.runner = None
        self.ec2 = None
        if runner == "kubernetes":
            self.runner = KubeRunner()
        elif runner == "ec2":
            self.ec2 = Ec2Runner()
        elif runner != "local":
            raise Error(
                "orca: ORCA_RUNNER is local, kubernetes or ec2, not ", runner
            )
        # A pod can take minutes to be scheduled and pull its image.
        self.start_grace = start_grace(self.stale)
        var budget = getenv("ORCA_VCPU_BUDGET")
        self.budget = cpu_millis(budget) if budget else 0
        self.logs = getenv("ORCA_LOGS")
        self._polled = Dict[String, Int]()
        self._asked = Dict[String, Capture]()
        self._unsensed = Dict[String, String]()
        self._keys = Dict[String, Tuple[Int64, List[String]]]()
        self.wait_for_binaries = False
        self._driver_logs = Dict[Int, String]()
        self._spawned = Dict[Int, Tuple[Int, Int, String]]()
        self.expire_seconds = Float64(getenv("ORCA_EXPIRE_HOURS", "24")) * 3600
        self.maintain = maintain_binary() if self.expire_seconds > 0 else ""
        # The first sweep a few minutes after starting
        # (`ORCA_EXPIRE_DELAY_SECONDS`, 300), not at once: a scheduler
        # restarted in a loop shouldn't expire on every start.
        self._expire_next = Int(perf_counter_ns()) + Int(
            Float64(getenv("ORCA_EXPIRE_DELAY_SECONDS", "300")) * 1e9
        )
        self._expiring = List[String]()
        self.fleet_seconds = Float64(getenv("ORCA_FLEET_SECONDS", "30"))
        self._fleet_next = 0
        self._archive = Archive()
        self.watchdog_seconds = Float64(getenv("ORCA_WATCHDOG_SECONDS", "60"))
        self._watchdog_next = 0
        self._notify_next = 0
        self.limits = ControlLimits.from_env()
        self.room = host_room()
        self._control = Dict[Int, _ControlProc]()
        self._kept = Dict[Int, _ControlProc]()
        self._keeping = Dict[Int, String]()
        self._meters = Dict[Int, Meter]()
        self.psi = find_source()
        self.admission = Admission.from_env()
        self._pressure_kept = 0
        self._pressure_unread = False
        self.cgroups = Cgroups.off("off: not the scheduler's loop")
        self._contained = Dict[Int, Contained]()
        self._unswept = List[String]()
        self._stalled = Dict[Int, Bool]()
        self._held = HeldWatch()
        self._cgroups_next = 0
        self.instance = 0
        self.leading = True
        self.draining = False
        self.drain_seconds = Float64(getenv("ORCA_DRAIN_SECONDS", "3600"))
        self._drain_start = 0
        self.finished = False
        self._going = 0

    def join(mut self) raises:
        """With `ORCA_OVERLAP=on`, makes this scheduler an instance
        (`orca.instances`): it leads only once it holds the lead."""
        if not overlap_on():
            return
        self.instance = join_instance(self.db)
        self.leading = False
        print("orca: scheduler instance", self.instance, "joined")

    def reconnect(mut self, url: String) raises:
        """A new connection after a tick failed, keeping what this instance
        supervises; the lead, which was the old connection's, is taken
        again on the next tick if no newer instance has it."""
        self.db = State(url)
        self.db.migrate()
        self.leading = False

    def _idle(self) -> Bool:
        """Whether none of orca's runs is going on this host: none of its
        own children, or for an instance none of any instance's."""
        if self.instance > 0:
            return self._going == 0
        return len(self._spawned) == 0

    def _lead(mut self) raises:
        """An instance's turn at the lead: marks it seen with its runs,
        drains once a newer instance is live or it's asked to, takes the
        lead when free otherwise, and while draining ends once nothing it
        started is left (killing what is, after `drain_seconds`)."""
        var running = String()
        for e in self._spawned.items():
            if running:
                running.write(",")
            running.write(e.value[0])
        var asked = beat(self.db, self.instance, running)
        if not self.draining:
            var n = newer(self.db, self.instance)
            if asked or n > 0:
                step_down(self.db, self.instance, self.leading)
                self.leading = False
                self.draining = True
                self._drain_start = Int(perf_counter_ns())
                var why = String("instance ", n, " is newer") if n > 0 else (
                    String("asked to")
                )
                print(
                    String("orca: scheduler instance ", self.instance),
                    String(" drains (", why, "): "),
                    len(self._spawned),
                    " runs going",
                    sep="",
                )
            elif not self.leading and try_lead(self.db, self.instance):
                self.leading = True
                print("orca: scheduler instance", self.instance, "leads")
        if not self.draining:
            return
        if len(self._spawned) == 0 and len(self._keeping) == 0:
            end_instance(self.db, self.instance)
            self.finished = True
            print("orca: scheduler instance", self.instance, "drained")
            return
        var waited = Float64(Int(perf_counter_ns()) - self._drain_start) / 1e9
        if waited < self.drain_seconds:
            return
        for e in self._spawned.items():
            if e.key in self._control:
                _ = external_call["kill", c_int](c_int(-e.key), c_int(9))
            _ = external_call["kill", c_int](c_int(e.key), c_int(9))
            print(
                "killed pid",
                e.key,
                "of run",
                e.value[0],
                "(the drain ran out after",
                Int(self.drain_seconds),
                "s)",
            )
        end_instance(self.db, self.instance)
        self.finished = True
        print(
            "orca: scheduler instance",
            self.instance,
            "ended its drain; the leader queues its runs again",
        )

    def _kill_held(mut self):
        """Kills each contained run held at its memory limit (`held`) for
        `ORCA_RUN_MEMORY_HELD_SECONDS` in a row: throttled at memory.high,
        it may never reach memory.max, and its heartbeat, kept on another
        thread, stays fresh. Reaped, it's out of memory (`_uncontain`)."""
        var held_s = self.cgroups.knobs.held_s
        for pid in self._held.due(self._contained, held_s):
            self._stalled[pid] = True
            if pid in self._control:
                _ = external_call["kill", c_int](c_int(-pid), c_int(9))
            _ = external_call["kill", c_int](c_int(pid), c_int(9))
            try:
                ref c = self._contained[pid]
                print(
                    "killed pid",
                    pid,
                    "of",
                    c.pipeline,
                    "run",
                    c.run,
                    String("(", held_text(c, held_s), ")"),
                )
            except:
                pass

    def _kill_wedged(mut self) raises:
        """Kills each process of ours whose run's heartbeat went stale: it's
        wedged, or stopped, rather than gone (it would have been reaped),
        and mustn't go on beside its run's next start. Once reaped, its run
        is cancelled, failed or started again as a dead one."""
        if len(self._spawned) == 0:
            return
        var ids = String()
        for e in self._spawned.items():
            if ids:
                ids.write(",")
            ids.write(e.value[0])
        for r in self.db.conn.query(
            String(
                "SELECT id, pipeline FROM orca.runs WHERE status = 'running'",
                " AND id::text = ANY (string_to_array($2, ',')) AND ",
                stale_sql("$1", self.start_grace),
            ),
            Params().float64(self.stale).text(ids),
        ):
            var id = Int(r.int64("id"))
            var pid = 0
            for e in self._spawned.items():
                if e.value[0] == id:
                    pid = e.key
            if pid <= 0:
                continue
            if pid in self._contained:
                # Throttled at its limit (memory.high) rather than wedged:
                # out of memory.
                try:
                    ref c = self._contained[pid]
                    if at_limit(Cgroups.current(c.path), c.limit_mb):
                        self._stalled[pid] = True
                except:
                    pass
            if pid in self._control:
                # Its own process group: its forked workers too.
                _ = external_call["kill", c_int](c_int(-pid), c_int(9))
            _ = external_call["kill", c_int](c_int(pid), c_int(9))
            print(
                "killed pid",
                pid,
                "of",
                r.text("pipeline"),
                "run",
                id,
                "(its heartbeat went stale)",
            )

    def say_pressure(self):
        """Says once, for `orca scheduler`, where this host's memory
        pressure is read from and what it holds (`orca.pressure`)."""
        if not self.psi:
            print(
                "orca: no memory pressure to read here (no PSI: macOS, or a",
                " kernel without it); runs start on counts and grants alone",
                sep="",
            )
        elif self.admission.limit <= 0:
            print(
                "orca: memory pressure from",
                self.psi.describe(),
                "- admission by it is off (ORCA_ADMIT_MEMORY_PRESSURE=0)",
            )
        else:
            print(
                "orca: memory pressure from",
                self.psi.describe(),
                "- no new runs start here while some avg10 is over",
                String(pct_text(self.admission.limit), "%"),
            )

    def contain(mut self) raises:
        """Turns on a cgroup per run (`orca.cgroup`) where this host lets
        orca make them, saying once which, and picks up the empty cgroups
        an earlier scheduler left. `orca scheduler` does this; `orca tick`
        and the other commands don't, as they don't reap what they
        start."""
        self.cgroups = Cgroups.setup(MemoryKnobs.from_env())
        print("orca: run cgroups", self.cgroups.why)
        if not self.cgroups.on():
            print(
                "orca: runs are contained as before: first to go if memory",
                "runs out, and control-placed runs killed over",
                "ORCA_CONTROL_MEMORY_MB",
            )
        self._unswept = self.cgroups.stale()
        self._sweep_cgroups()

    def _sweep_cgroups(mut self):
        """Removes the runs' cgroups their processes have left."""
        var left = List[String]()
        for path in self._unswept:
            if not Cgroups.remove(path):
                left.append(path)
        self._unswept = left^

    def _contain(
        mut self,
        p: Registered,
        run: Int,
        start: Int,
        grant: Grant,
        control: Bool,
    ) -> Contained:
        """Makes a start's cgroup at its limit (`orca.cgroup`) and records
        the limit on the run; with cgroups off, or if it can't, one with no
        path, and the run starts as before."""
        var c = Contained(run, start, p.name, "", 0, 0, 0, grant.memory_mb)
        if not self.cgroups.on():
            return c^
        try:
            var req = p.request()
            var cap = self.limits.memory_mb if control else 0
            var default = self.cgroups.knobs.default_mb
            if default <= 0:
                default = cap if control else self.room.memory_mb
            c.limit_mb = start_limit_mb(
                grant.memory_mb,
                req.memory_max_mb,
                default,
                p.memory_raised_mb,
                cap,
            )
            c.ceiling_mb = ceiling_mb(
                req.memory_max_mb, self.room.memory_mb, cap
            )
            if c.limit_mb <= 0:
                return c^
            var made = self.cgroups.make(run, start, c.limit_mb)
            c.path = made[0]
            c.kill_mb = made[1]
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.runs SET memory_limit_mb = $3, memory_max_mb"
                    " = $4, memory_bytes = NULL WHERE id = $1 AND starts = $2"
                ),
                Params()
                .int64(Int64(run))
                .int64(Int64(start))
                .int64(Int64(c.limit_mb))
                .int64(Int64(c.kill_mb)),
            )
        except e:
            print("orca: run", run, "starts without a cgroup:", e)
            if c.path and not Cgroups.remove(c.path):
                self._unswept.append(c.path)
            c.path = ""
        return c^

    def _uncontain(mut self, pid: Int, code: Int, mut over: String):
        """At the reap of `pid`: if the kernel killed its run's processes
        for their memory, `over` says so, and its pipeline asks for more
        from now on; its attempts get its limit; the cgroup is emptied of
        what's left of the run, and removed."""
        if pid not in self._contained:
            return
        var c: Contained
        try:
            c = self._contained.pop(pid)
        except:
            return
        var stalled = self._stalled.pop(pid, False)
        if (
            code != 0
            and not over
            and (stalled or Cgroups.oom_kills(c.path) > 0)
        ):
            var next = raised_mb(
                c.limit_mb, self.cgroups.knobs.growth_pct, c.ceiling_mb
            )
            over = oom_text(
                c.limit_mb,
                c.kill_mb,
                next,
                stalled and Cgroups.oom_kills(c.path) == 0,
            )
            print("run", c.run, "pid", pid, over)
            try:
                _ = self.db.conn.execute(
                    (
                        "UPDATE orca.pipelines SET memory_raised_mb ="
                        " greatest(coalesce(memory_raised_mb, 0), $2),"
                        " memory_raised_run = $3 WHERE name = $1"
                    ),
                    Params()
                    .text(c.pipeline)
                    .int64(Int64(next))
                    .int64(Int64(c.run)),
                )
            except e:
                print("orca: can't raise", c.pipeline, "memory:", e)
        var stall = Cgroups.stall_ms(c.path)
        try:
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.attempts a SET memory_limit_mb = $3,"
                    " grant_memory_mb = nullif($4, 0), memory_stall_ms ="
                    " nullif($5, -1), memory_stall_full_ms = nullif($6, -1)"
                    " FROM orca.steps st WHERE st.id = a.step AND st.run = $1"
                    " AND a.host LIKE '%:' || $2 AND a.memory_limit_mb IS NULL"
                ),
                Params()
                .int64(Int64(c.run))
                .text(String(pid))
                .int64(Int64(c.limit_mb))
                .int64(Int64(c.grant_mb))
                .int64(Int64(stall[0]))
                .int64(Int64(stall[1])),
            )
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.runs SET memory_bytes = NULL WHERE id = $1"
                    " AND starts = $2"
                ),
                Params().int64(Int64(c.run)).int64(Int64(c.start)),
            )
        except e:
            print("orca: can't record run", c.run, "memory limit:", e)
        # The run is over: what it left running goes with it.
        _ = put_cgroup(String(c.path, "/cgroup.kill"), "1")
        if not Cgroups.remove(c.path):
            self._unswept.append(c.path)

    def _record_cgroups(mut self):
        """Every 5 s, each contained run's memory now, for Ops > Control;
        and the cgroups of ended runs removed once empty."""
        if len(self._unswept) > 0:
            self._sweep_cgroups()
        if len(self._contained) == 0:
            return
        var t = Int(perf_counter_ns())
        if t < self._cgroups_next:
            return
        self._cgroups_next = t + 5_000_000_000
        for e in self._contained.items():
            var b = Cgroups.current(e.value.path)
            if b < 0:
                continue
            try:
                _ = self.db.conn.execute(
                    (
                        "UPDATE orca.runs SET memory_bytes = $3 WHERE id = $1"
                        " AND starts = $2"
                    ),
                    Params()
                    .int64(Int64(e.value.run))
                    .int64(Int64(e.value.start))
                    .int64(Int64(b)),
                )
            except:
                pass

    def register(
        mut self,
        binary: String,
        now: Int64,
        flags: RegisterFlags = RegisterFlags(),
        file: ProjectConfig = ProjectConfig(),
    ) raises -> String:
        """Records the pipeline `binary` describes, with its path; its
        schedule counts from `now` the first time, and a sensor from now the
        first time it's one, with the snapshots its datasets have now seen.
        Queued runs move to the new code, and so do failed ones waiting for
        a retry with no step done. How its runs run is `flags` over what
        `file`, the project's orca.toml (`orca.config`), says for it: for a
        pipeline the file names, exactly that, a setting neither gives
        going back to its default; for one it doesn't, the flags given,
        each left out keeping what was registered (`--workers MIN..MAX`
        scales an EC2 run's workers, `--worker-procs` is the dista worker
        processes per EC2 worker instance, `--secret`s replace the
        pipeline's, `--placement` is `control`, `ec2` or `default`).
        Returns the pipeline's name."""
        var path = absolute(binary)
        var got = capture([path, "describe"], binary_seconds())
        if got[0] != 0 or not got[1].startswith('{"pipeline": '):
            raise Error(
                "orca: ", path, " describe exited ", got[0], ": ", got[1]
            )
        var cycle = dataset_cycle(self.db, got[1])
        if cycle:
            raise Error("orca: ", path, " can't be registered: ", cycle)
        var mismatch = kind_mismatch(self.db, got[1])
        if mismatch:
            raise Error("orca: ", path, " can't be registered: ", mismatch)
        var info = self.db.conn.query(
            (
                "SELECT $1::jsonb->>'pipeline' AS name,"
                " coalesce(($1::jsonb->>'sensor')::boolean, false) AS"
                " sensor, NOT EXISTS (SELECT 1 FROM orca.pipelines WHERE name"
                " = $1::jsonb->>'pipeline' AND sensing_since IS NOT NULL) AS"
                " new_sensor, coalesce($1::jsonb->'partitions'->>'kind',"
                " 'monthly') AS kind, coalesce($1::jsonb->'partitions'->>"
                "'start', '') AS start"
            ),
            Params().text(got[1]),
        ).row(0)
        var sensor = info.bool("sensor")
        var r = resolve(info.text("name"), flags, file)
        var workers = 0
        var min_workers = -1
        if r.text("workers"):
            var w = parse_workers(r.text("workers"))
            min_workers = w[0]
            workers = w[1]
        var worker_procs = -1
        if r.text("worker_procs"):
            worker_procs = parse_worker_procs(r.text("worker_procs"))
        var max_runs = -1
        if r.text("max_runs"):
            max_runs = parse_max_runs(r.text("max_runs"))
        # Fills (orca.fill): only from the file.
        if r.text("fills") == "all":
            var why = fill_refusal(
                info.text("kind"), info.text("start"), sensor, r.name
            )
            if why:
                var at = file.where(r.name, "fills", r.values["fills"].copy())
                raise Error(
                    "orca: ",
                    at,
                    ' fills = "all" can\'t be registered: ',
                    why,
                    '. Set fills = "since_registration" for it in its',
                    " own table",
                )
        var fill_max_runs = -1
        if r.text("fill_max_runs"):
            fill_max_runs = parse_fill_max_runs(r.text("fill_max_runs"))
        var fill_budget = -1.0
        if r.text("fill_budget_per_day"):
            fill_budget = parse_fill_budget(r.text("fill_budget_per_day"))
        var seen = String()
        if sensor:
            var sensed = capture([path, "sense"], binary_seconds())
            if sensed[0] != 0:
                raise Error(
                    "orca: ", path, " sense exited ", sensed[0], ": ", sensed[1]
                )
            if len(parse_sense(sensed[1])) == 0:
                raise Error(
                    "orca: ",
                    path,
                    " is a sensor, but reads no dataset it doesn't write",
                )
            if info.bool("new_sensor"):
                seen = sensed[1]
        # $14: the file names it, so what it and the flags say is all of
        # it; otherwise a value not given keeps what was registered.
        var res = self.db.conn.query(
            (
                "INSERT INTO orca.pipelines (name, registry_hash, describe,"
                " binary_path, since, sensing_since, image, head_type,"
                " worker_type, ec2_workers, spot, ec2_min_workers, secrets,"
                " worker_procs, placement, max_runs, env, config, fills,"
                " fill_order, fill_max_runs, fill_budget_per_day, fill_window,"
                " cpus, memory_gb, fill_lag, learn) SELECT d->>'pipeline',"
                " d->>'registry_hash',"
                " d, $2, $3, CASE WHEN $4 THEN now() END, nullif($5, ''),"
                " nullif($6, ''), nullif($7, ''), nullif($8, 0), $9,"
                " nullif($10, -1), nullif($11, '')::jsonb, nullif($12, -1),"
                " nullif(nullif($13, ''), 'default'), nullif($15, -1),"
                " nullif($16, '')::jsonb, $17::jsonb, nullif($18, ''),"
                " nullif($19, ''), nullif($20, -1), nullif($21, -1::float8),"
                " nullif($22, ''), nullif($23, ''), nullif($24, ''),"
                " nullif($25, ''), nullif($26, '')::boolean FROM"
                " (SELECT $1::jsonb AS d) AS x ON CONFLICT (name) DO UPDATE SET"
                " registry_hash = EXCLUDED.registry_hash, describe ="
                " EXCLUDED.describe, binary_path = EXCLUDED.binary_path, since"
                " = coalesce(orca.pipelines.since, EXCLUDED.since),"
                " sensing_since = CASE WHEN $4 THEN"
                " coalesce(orca.pipelines.sensing_since, now()) END, image ="
                " coalesce(EXCLUDED.image, orca.pipelines.image), head_type ="
                " CASE WHEN $14 THEN EXCLUDED.head_type ELSE"
                " coalesce(EXCLUDED.head_type, orca.pipelines.head_type) END,"
                " worker_type = CASE WHEN $14 THEN EXCLUDED.worker_type ELSE"
                " coalesce(EXCLUDED.worker_type, orca.pipelines.worker_type)"
                " END, ec2_workers = CASE WHEN $14 THEN EXCLUDED.ec2_workers"
                " ELSE coalesce(EXCLUDED.ec2_workers,"
                " orca.pipelines.ec2_workers) END, ec2_min_workers = CASE WHEN"
                " $14 OR $8 > 0 THEN EXCLUDED.ec2_min_workers ELSE"
                " orca.pipelines.ec2_min_workers END, secrets = CASE WHEN $14"
                " THEN EXCLUDED.secrets ELSE coalesce(EXCLUDED.secrets,"
                " orca.pipelines.secrets) END, worker_procs = CASE WHEN $14"
                " THEN EXCLUDED.worker_procs WHEN $12 >= 0 THEN $12 ELSE"
                " orca.pipelines.worker_procs END, placement = CASE WHEN $14"
                " THEN EXCLUDED.placement WHEN $13 = '' THEN"
                " orca.pipelines.placement ELSE nullif($13, 'default') END,"
                " max_runs = CASE WHEN $14 OR $15 > 0 THEN EXCLUDED.max_runs"
                " ELSE orca.pipelines.max_runs END, env = CASE WHEN $14 THEN"
                " EXCLUDED.env ELSE orca.pipelines.env END, config = CASE WHEN"
                " $14 THEN EXCLUDED.config ELSE jsonb_build_object('sources',"
                " coalesce(orca.pipelines.config->'sources', '{}'::jsonb) ||"
                " coalesce(EXCLUDED.config->'sources', '{}'::jsonb)) END, spot"
                " = CASE WHEN $6 <> '' OR $7 <> '' OR $8 > 0 OR $9 THEN $9 ELSE"
                " orca.pipelines.spot END, fills = CASE WHEN $14 THEN"
                " EXCLUDED.fills ELSE orca.pipelines.fills END, fill_order ="
                " CASE WHEN $14 THEN EXCLUDED.fill_order ELSE"
                " orca.pipelines.fill_order END, fill_max_runs = CASE WHEN $14"
                " THEN EXCLUDED.fill_max_runs ELSE orca.pipelines.fill_max_runs"
                " END, fill_budget_per_day = CASE WHEN $14 THEN"
                " EXCLUDED.fill_budget_per_day ELSE"
                " orca.pipelines.fill_budget_per_day END, fill_window = CASE"
                " WHEN $14 THEN EXCLUDED.fill_window ELSE"
                " orca.pipelines.fill_window END, fill_lag = CASE WHEN $14 THEN"
                " EXCLUDED.fill_lag ELSE orca.pipelines.fill_lag END, learn ="
                " CASE WHEN $14 THEN EXCLUDED.learn ELSE orca.pipelines.learn"
                " END, cpus ="
                " CASE WHEN $14 OR $23"
                " <> '' THEN EXCLUDED.cpus ELSE orca.pipelines.cpus END,"
                " memory_gb = CASE WHEN $14 OR $24 <> '' THEN"
                " EXCLUDED.memory_gb ELSE orca.pipelines.memory_gb END,"
                # A new request forgets what out-of-memory kills raised.
                " memory_raised_mb = CASE WHEN (CASE WHEN $14 OR $24 <> ''"
                " THEN EXCLUDED.memory_gb ELSE orca.pipelines.memory_gb END)"
                " IS DISTINCT FROM orca.pipelines.memory_gb THEN NULL ELSE"
                " orca.pipelines.memory_raised_mb END,"
                " last_seen = now() RETURNING name, registry_hash,"
                " coalesce(image, '') AS image"
            ),
            Params()
            .text(got[1])
            .text(path)
            .int64(now)
            .bool(sensor)
            .text(flags.image)
            .text(r.text("head_type"))
            .text(r.text("worker_type"))
            .int64(Int64(workers))
            .bool(flags.spot)
            .int64(Int64(min_workers))
            .text(secrets_json(r.secret_specs()))
            .int64(Int64(worker_procs))
            .text(r.text("placement"))
            .bool(r.in_file)
            .int64(Int64(max_runs))
            .text(r.env_json())
            .text(r.config_json(file))
            .text(r.text("fills"))
            .text(r.text("fill_order"))
            .int64(Int64(fill_max_runs))
            .float64(fill_budget)
            .text(r.text("fill_window"))
            .text(r.text("cpus"))
            .text(r.text("memory_gb"))
            .text(r.text("fill_lag"))
            .text(r.text("learn")),
        )
        var name = res.row(0).text("name")
        var version = self.db.version(
            name,
            res.row(0).text("registry_hash"),
            got[1],
            res.row(0).text("image"),
        )
        _ = self.db.conn.execute(
            "UPDATE orca.pipelines SET version = $2 WHERE name = $1",
            Params().text(name).int64(Int64(version)),
        )
        # Where its functions are, if `orca locate` left a map beside it.
        var map = String(path, ".source.json")
        if exists(map):
            var text: String
            with open(map, "r") as f:
                text = f.read()
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.pipeline_versions SET locations = $2::jsonb"
                    " WHERE id = $1"
                ),
                Params().int64(Int64(version)).text(text),
            )
        # What its datasets hold now isn't new to it.
        for c in parse_sense(seen):
            if c.snapshot >= 0:
                _ = self.db.conn.execute(
                    (
                        "INSERT INTO orca.sensed (pipeline, dataset,"
                        " snapshot_id) VALUES ($1, $2, $3) ON CONFLICT DO"
                        " NOTHING"
                    ),
                    Params().text(name).text(c.dataset).int64(c.snapshot),
                )
        for q in registered(self.db, name):
            var twice = repeated_fill(q, now)
            if twice:
                print("orca: warning:", twice)
            if q.fill.all and q.fill.budget < 0:
                print(
                    "orca: warning:",
                    q.name,
                    "fills its range with no fill_budget_per_day: its fill",
                    "runs start only where they cost nothing (placement",
                    "control, or the local runner); on ec2 or kubernetes",
                    "they wait for a budget",
                )
        # A retry of a run with steps done would run the new code over the
        # old code's outputs, which only a resume that says so may do.
        _ = self.db.conn.execute(
            String(
                "UPDATE orca.runs r SET registry_hash = $2 WHERE pipeline =",
                " $1 AND registry_hash <> $2 AND (status = 'queued' OR",
                " ((status = 'deferred' OR (status = 'failed' AND ",
                _RETRIED,
                " AND starts - deferrals BETWEEN 1 AND (SELECT",
                " coalesce((describe->>'run_retries')::bigint, 3) FROM",
                " orca.pipelines WHERE name = $1))) AND NOT EXISTS (SELECT 1",
                " FROM orca.steps s WHERE s.run = r.id AND s.status =",
                " 'succeeded')))",
            ),
            Params().text(name).text(res.row(0).text("registry_hash")),
        )
        return name^

    def tick(mut self, now: Int64) raises:
        """One pass: reap ended runs, advance rebuilds, make due fires,
        sense new snapshots, schedule retries, and start what may start."""
        for done in reap():
            var asked = String()
            for e in self._asked.items():
                if e.value.pid == done.pid:
                    asked = e.key
            if asked:
                var c = self._asked.pop(asked)
                c.code = done.code
                self._asked[asked] = c^
                continue
            if done.pid in self._keeping:
                var stage = self._keeping.pop(done.pid)
                if done.code != 0:
                    print("orca: keep-log exited", done.code, "keeping", stage)
                try:
                    remove(stage)
                except:
                    pass
                continue
            print("reaped pid", done.pid, "exit", done.code)
            var spawned = self._spawned.pop(done.pid, (-1, 0, String()))
            var over = String()
            var ctl = Optional[_ControlProc](None)
            if done.pid in self._control:
                ctl = self._control.pop(done.pid)
                over = ctl.value().over
            self._uncontain(done.pid, done.code, over)
            if spawned[0] >= 0:
                try:
                    self._exited(spawned, done.pid, done.code, over)
                except e:
                    print("orca: can't record run", spawned[0], "exiting:", e)
            if done.pid in self._meters:
                var m = self._meters.pop(done.pid)
                m.end(done.max_rss, done.cpu_seconds)
                try:
                    _ = record_usage(
                        self.db, spawned[0], done.pid, m.aged(), m.max_rss
                    )
                except e:
                    print("orca: can't record run", spawned[0], "usage:", e)
            if ctl:
                self._keep_control_log(ctl.value())
            if done.pid in self._kept:
                self._keep_control_log(self._kept.pop(done.pid))
            var log = self._driver_logs.pop(done.pid, "")
            if log:
                try:
                    publish(log)
                except e:
                    print("orca: can't keep the driver log at", log, ":", e)
        var readings = self._sample()
        self._kill_held()
        try:
            self._kill_wedged()
        except e:
            print("orca: can't check its runs' heartbeats:", e)
        if self.instance > 0:
            self._lead()
            if not self.leading:
                # Supervising what it started, deciding nothing.
                self._record_cgroups()
                try:
                    self._check_control_memory(readings)
                except e:
                    print("orca: can't check control-placed runs' memory:", e)
                return
            try:
                self._going = Int(
                    self.db.conn.query(
                        String(
                            "SELECT count(*) AS n FROM orca.runs WHERE status",
                            " = 'running' AND placement IN ('local',",
                            " 'control') AND NOT ",
                            self._dead("$1"),
                        ),
                        Params().float64(self.stale),
                    )
                    .row(0)
                    .int64("n")
                )
            except e:
                print("orca: can't count this host's runs:", e)
        try:
            self._pressure()
        except e:
            print("orca: can't keep the memory pressure:", e)
        self._record_cgroups()
        if self.runner:
            try:
                var n = self.runner.value().sweep(self.db)
                if n:
                    print("deleted", n, "objects of runs that ended")
            except e:
                print("orca: can't sweep run clusters:", e)
        if self.ec2:
            try:
                var n = self.ec2.value().sweep(self.db)
                if n:
                    print("terminated", n, "instances of runs that ended")
            except e:
                print("orca: can't sweep run instances:", e)
            try:
                var moved = self.ec2.value().scale(self.db)
                if moved[0] or moved[1]:
                    print(
                        "scaled run workers:",
                        moved[0],
                        "started,",
                        moved[1],
                        "drained",
                    )
            except e:
                print("orca: can't scale run workers:", e)
        try:
            self._check_control_memory(readings)
        except e:
            print("orca: can't check control-placed runs' memory:", e)
        try:
            self._fleet()
        except e:
            print("orca: can't keep the fleet's samples:", e)
        try:
            self._watchdog()
        except e:
            print("orca: can't check the watchdog's rules:", e)
        try:
            self._notify(now)
        except e:
            print("orca: can't raise alerts:", e)
        advance_rebuilds(self.db)
        try:
            self._expire()
        except e:
            print("orca: can't expire snapshots:", e)
        try:
            for name in expire_raised(self.db):
                print(
                    "orca:",
                    name,
                    (
                        "no longer asks for the memory an out-of-memory kill"
                        " raised it to:"
                    ),
                    LEARN_RUNS,
                    "runs have succeeded since",
                )
        except e:
            print("orca: can't expire out-of-memory raises:", e)
        for p in registered(self.db):
            if p.cron:
                try:
                    self._fire(p, now)
                except e:
                    print("orca: pipeline", p.name, "can't fire:", e)
            if p.sensor:
                var error = String()
                try:
                    self._sense(p, now)
                except e:
                    error = String(e)
                self._sensed(p, error)
            self._schedule_retries(p, now)
            if notify_configured():
                try:
                    _ = failed_runs(self.db, p.name, p.run_retries, _RETRIED)
                except e:
                    print("orca: can't alert on", p.name, "failures:", e)
            self._start(p, now)
            # After its other runs, so a fill never takes their place.
            if p.fill.all:
                self._start(p, now, fill=True)

    def _pressure(mut self) raises:
        """Reads this host's memory pressure (`orca.pressure`) for this
        tick's starts, says when a hold starts and ends, alerts once when
        one has nothing of orca's to wait for, and keeps the reading in
        `orca.pressure` every 5 s and on each change of hold (with no
        reading where there's no PSI, so Ops says so)."""
        var t = Int(perf_counter_ns())
        var psi = read_psi(self.psi.path)
        if self.psi and not psi and not self._pressure_unread:
            print(
                "orca: can't read memory pressure from",
                self.psi.path,
                "- nothing is held until it reads",
            )
        self._pressure_unread = not psi
        var was = self.admission.hold
        var now = Float64(t) / 1e9
        self.admission.observe(psi, now, self._idle())
        ref a = self.admission
        var r = Report.of(self.psi, a, now)
        if a.held() and not was:
            print("orca:", a.hold, "- no new runs start on this host")
        if a.released:
            print(
                "orca: memory pressure",
                String(pct_text(r.some10), "%"),
                "- runs start on this host again",
            )
            resolve_stuck(self.db, "the scheduler's host", r)
        if a.stuck:
            alert_stuck(self.db, "the scheduler's host", r)
        var changed = (was.byte_length() > 0) != a.held()
        if changed or t - self._pressure_kept >= 5_000_000_000:
            record_pressure(self.db, SCHEDULER_HOST, r)
            self._pressure_kept = t

    def _exited(
        mut self,
        spawned: Tuple[Int, Int, String],
        pid: Int,
        code: Int,
        over: String = "",
    ) raises:
        """Fails the run a reaped process was started for, if the process
        never claimed it: the run is still the scheduler's, at the start it
        was given. Its error is the exit code and the last line its process
        printed, such as `resume`'s refusal. A process that claimed it and
        crashed has the attempts it left running failed with the crash's
        summary (`orca.crash`), or with `over` if the scheduler killed it
        for its memory (`orca.control`)."""
        var why = String(
            "its process (pid ", pid, ") exited ", code, " before claiming it"
        )
        var last = over if over else _last_line(spawned[2])
        if last:
            why.write(": ", last)
        var n = self.db.conn.execute(
            (
                "UPDATE orca.runs SET status = 'failed', ended = now(),"
                " heartbeat = NULL, error = $3 WHERE id = $1 AND status ="
                " 'running' AND owner = 'scheduler' AND starts = $2"
            ),
            Params()
            .int64(Int64(spawned[0]))
            .int64(Int64(spawned[1]))
            .text(why),
        )
        if n == 1:
            print("run", spawned[0], "failed:", why)
        elif crashed_by_signal(code) or over:
            self._crashed(spawned, pid, code, over)

    def _crashed(
        mut self,
        spawned: Tuple[Int, Int, String],
        pid: Int,
        code: Int,
        over: String = "",
    ) raises:
        """Fails the attempts a crashed process left running, if the run is
        still its at that start, with what its driver log says of the crash
        or, with no log or no dump in it, the signal."""
        var held = self.db.conn.query(
            (
                "SELECT id FROM orca.runs WHERE id = $1 AND status ="
                " 'running' AND starts = $2 AND owner LIKE '%:' || $3"
            ),
            Params()
            .int64(Int64(spawned[0]))
            .int64(Int64(spawned[1]))
            .text(String(pid)),
        )
        if len(held) == 0:
            return
        var why = over
        if not why and spawned[2] and exists(spawned[2]):
            try:
                with open(spawned[2], "r") as f:
                    why = crash_summary(f.read())
            except e:
                print("orca: can't read", spawned[2], ":", e)
        if not why:
            why = String(
                "crashed: killed by signal ",
                code - 128,
                ", with no stack dump in its output",
            )
        var n = self.db.interrupt(spawned[0], why)
        print("run", spawned[0], "crashed,", n, "attempts failed:", why)

    def _keep_control_log(mut self, c: _ControlProc):
        """Appends a control-placed run's output, or a local run's with
        secrets, to its `driver.log`, masked (`<binary> keep-log`, given the
        run's secrets as the run was); the staging file is removed once
        that has ended."""
        if not c.location:
            return
        try:
            var pid = spawn(
                [c.binary, "keep-log", c.stage, c.location, String(c.start)],
                env=c.env,
            )
            self._keeping[pid] = c.stage
        except e:
            print("orca: can't keep run", c.run, "driver log:", e)

    def _sample(mut self) -> Dict[Int, Tuple[Int, Float64]]:
        """Samples each local and control-placed run's process tree, in one
        pass over /proc (`orca.usage`); the readings, by pid, are empty on
        macOS."""
        var pids = List[Int]()
        for e in self._meters.items():
            pids.append(e.key)
        var readings = tree_usage(pids)
        for e in readings.items():
            try:
                self._meters[e.key].sample(e.value[0], e.value[1])
            except:
                pass
        return readings^

    def _check_control_memory(
        mut self, readings: Dict[Int, Tuple[Int, Float64]]
    ) raises:
        """Kills each control-placed run's processes once they hold more
        than `ORCA_CONTROL_MEMORY_MB` resident; the reap fails its attempts
        saying so. The tick's `readings` say what they hold, or on macOS
        `tree_rss`."""
        if len(self._control) == 0:
            return
        var cap = self.limits.memory_mb * 1024 * 1024
        var pids = List[Int]()
        for e in self._control.items():
            # A run in a cgroup has its limit there (orca.cgroup).
            if not e.value.over and e.key not in self._contained:
                pids.append(e.key)
        for pid in pids:
            var rss = readings[pid][0] if pid in readings else tree_rss(pid)
            if rss <= cap:
                continue
            var c = self._control.pop(pid)
            c.over = String(
                "killed: its processes held ",
                rss // (1024 * 1024),
                " MB, over the control instance's cap of ",
                self.limits.memory_mb,
                " MB a run (ORCA_CONTROL_MEMORY_MB)",
            )
            _ = external_call["kill", c_int](c_int(-pid), c_int(9))
            _ = external_call["kill", c_int](c_int(pid), c_int(9))
            print("run", c.run, "pid", pid, c.over)
            self._control[pid] = c^

    def _sensed(mut self, p: Registered, error: String) raises:
        """Records how `p`'s pass went: the error of one that failed, which
        is printed when it's new rather than on every tick, or none. A
        pass still waiting for its `sense` or `keys` changes nothing."""
        if not error and (
            String("sense ", p.name) in self._asked
            or String("keys ", p.name) in self._asked
        ):
            return
        if not error:
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.pipelines SET sense_error = NULL,"
                    " sense_error_at = NULL WHERE name = $1 AND sense_error IS"
                    " NOT NULL"
                ),
                Params().text(p.name),
            )
            return
        var changed = self.db.conn.query(
            (
                "UPDATE orca.pipelines SET sense_error = $2, sense_error_at ="
                " now() WHERE name = $1 AND sense_error IS DISTINCT FROM $2"
                " RETURNING name"
            ),
            Params().text(p.name).text(error),
        )
        if len(changed) > 0:
            print("orca: pipeline", p.name, "can't sense:", error)

    def _sense(mut self, p: Registered, now: Int64) raises:
        """Queues a run for the snapshots of `p`'s datasets it hasn't seen:
        those other pipelines' succeeded runs committed since it became a
        sensor, and, when its binary is due to be asked, the current ones
        something other than orca committed. A snapshot with no partitions
        stands for the one `p` would fill now."""
        var datasets = List[String]()
        var snapshots = List[Int64]()
        var keys = List[String]()
        for r in self.db.conn.query(
            (
                "WITH io AS (SELECT k.kind, x.ds FROM orca.pipelines p,"
                " jsonb_array_elements(p.describe->'steps') AS s(st), (VALUES"
                " ('reads'), ('writes')) AS k(kind),"
                " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
                " x(ds) WHERE p.name = $1 UNION SELECT 'reads',"
                " describe->'partitions'->>'dataset' FROM orca.pipelines WHERE"
                " name = $1 AND describe->'partitions'->>'dataset' IS NOT"
                " NULL) SELECT DISTINCT ON (c.dataset,"
                " c.snapshot_id) c.dataset, c.snapshot_id,"
                " coalesce(nullif(s.partition, ''), r.partitions) AS keys FROM"
                " orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN"
                " orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id ="
                " s.run JOIN orca.pipelines me ON me.name = $1 WHERE"
                " r.pipeline <> $1 AND r.status = 'succeeded' AND a.status ="
                " 'succeeded' AND a.ended >= me.sensing_since AND c.dataset IN"
                " (SELECT ds FROM io WHERE kind = 'reads' EXCEPT SELECT ds FROM"
                " io WHERE kind = 'writes') AND NOT EXISTS (SELECT 1 FROM"
                " orca.sensed x WHERE x.pipeline = $1 AND x.dataset ="
                " c.dataset AND x.snapshot_id = c.snapshot_id) AND NOT EXISTS"
                " (SELECT 1 FROM orca.rebuild_pipelines rp WHERE rp.rebuild ="
                " r.rebuild AND rp.pipeline = $1) ORDER BY c.dataset,"
                " c.snapshot_id, c.attempt"
            ),
            Params().text(p.name),
        ):
            datasets.append(r.text("dataset"))
            snapshots.append(r.int64("snapshot_id"))
            keys.append(r.text("keys"))
        var polled = Optional[String](None)
        if p.name in self._unsensed:
            polled = self._unsensed.pop(p.name)
        else:
            var job = String("sense ", p.name)
            var t = Int(perf_counter_ns())
            var last = self._polled.get(p.name)
            if (
                job in self._asked
                or not last
                or Float64(t - last.value()) >= self.sense_seconds * 1e9
            ):
                if job not in self._asked:
                    self._polled[p.name] = t
                polled = self._ask(job, "sense", [p.binary, "sense"])
        if polled:
            for c in parse_sense(polled.value()):
                if c.snapshot >= 0 and not c.by_orca:
                    datasets.append(c.dataset)
                    snapshots.append(c.snapshot)
                    keys.append(c.partitions)
        if len(datasets) == 0:
            return
        if p.kind == "keyed":
            var current = self._keys_for(p, datasets, snapshots)
            if not current:
                # Its keys are still being read: this pass waits for them,
                # with what `sense` said kept for it.
                if polled:
                    self._unsensed[p.name] = polled.value()
                return
            self._sense_keyed(
                p, current.value()[1].copy(), datasets, snapshots, keys
            )
            return
        var fallback: String
        if p.cron:
            var z = Zone.load(p.tz)
            fallback = fill_key(p.kind, p.fills, Civil.of(z.to_local(now)))
        else:
            fallback = fill_key(p.kind, "current", Civil.of(now))
        var tx = self.db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        var taken = List[Int]()
        var unusable = List[Tuple[Int, String]]()
        var all = List[String]()
        for i in range(len(datasets)):
            # Seen by another tick already, or twice in this one.
            var new = tx.query(
                (
                    "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " snapshot_id"
                ),
                Params().text(p.name).text(datasets[i]).int64(snapshots[i]),
            )
            if len(new) == 0:
                continue
            var ks = split_keys(keys[i])
            if len(ks) == 0:
                ks.append(fallback)
            # Keys that aren't partitions, such as a week another writer
            # put in `orca.partitions`, run nothing: the snapshot is seen,
            # with why, so it doesn't stop the others or the next tick.
            try:
                _ = map_keys(ks, p.kind, p.start)
            except e:
                _ = tx.execute(
                    (
                        "UPDATE orca.sensed SET error = $4 WHERE pipeline = $1"
                        " AND dataset = $2 AND snapshot_id = $3"
                    ),
                    Params()
                    .text(p.name)
                    .text(datasets[i])
                    .int64(snapshots[i])
                    .text(String(e)),
                )
                unusable.append((i, String(e)))
                continue
            taken.append(i)
            all.extend(ks^)
        var run = Int64(-1)
        # Keys a live run has are left to it, which becomes the snapshot's
        # run if it left nothing else.
        var live = live_runs(tx, p.name)
        var mapped = List[String]()
        var held = Int64(-1)
        for k in map_keys(all, p.kind, p.start):
            var in_run = live.get(k)
            if not in_run:
                mapped.append(k)
            elif held < 0:
                held = Int64(in_run.value())
        if len(mapped) == 0:
            run = held
        if len(mapped) > 0:
            run = (
                tx.query(
                    (
                        "INSERT INTO orca.runs (pipeline, registry_hash,"
                        " partitions, trigger, status) VALUES ($1, $2, $3,"
                        " 'sensor', 'queued') RETURNING id"
                    ),
                    Params()
                    .text(p.name)
                    .text(p.registry_hash)
                    .text(join_keys(mapped)),
                )
                .row(0)
                .int64("id")
            )
        if run >= 0:
            for i in taken:
                _ = tx.execute(
                    (
                        "UPDATE orca.sensed SET run = $4 WHERE pipeline = $1"
                        " AND dataset = $2 AND snapshot_id = $3"
                    ),
                    Params()
                    .text(p.name)
                    .text(datasets[i])
                    .int64(snapshots[i])
                    .int64(run),
                )
        tx.commit()
        for i in taken:
            print(
                "sensed",
                String(datasets[i], "@", snapshots[i]),
                "for",
                p.name,
                String("run ", run, " for ", join_keys(mapped)) if len(mapped)
                > 0 else (
                    String("left to live run ", run) if run
                    >= 0 else "before its first partition"
                ),
            )
        for u in unusable:
            print(
                "sensed",
                String(datasets[u[0]], "@", snapshots[u[0]]),
                "for",
                p.name,
                "and ran nothing:",
                u[1],
            )

    def _fire(mut self, p: Registered, now: Int64) raises:
        var s = Schedule(CronExpr.parse(p.cron), Zone.load(p.tz))
        var last = (
            self.db.conn.query(
                (
                    "SELECT coalesce(max(fire_at), $2) AS last FROM orca.fires"
                    " WHERE pipeline = $1"
                ),
                Params().text(p.name).int64(p.since),
            )
            .row(0)
            .int64("last")
        )
        var times = List[Int64]()
        var t = last
        # Many missed fires are made a thousand at a time, one per tick.
        while len(times) < 1000:
            t = s.next_fire(t)
            if t > now:
                break
            times.append(t)
        if not p.catchup and len(times) > 1:
            times = [times[len(times) - 1]]
        for t in times:
            var key = fill_key(p.kind, p.fills, Civil.of(s.zone.to_local(t)))
            var tx = self.db.conn.begin()
            _ = tx.query(
                "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
                Params().text(p.name),
            )
            var made = tx.query(
                (
                    "INSERT INTO orca.fires (pipeline, fire_at, partition)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " fire_at"
                ),
                Params().text(p.name).int64(t).text(key),
            )
            var run = Int64(-1)
            var held = False
            # Keys of one kind order as text: `YYYY-MM`, `YYYY-MM-DD`.
            if len(made) == 1 and (not p.start or key >= p.start):
                # A live run of the key is the fire's run: a second one
                # would run beside it, and both would append.
                var live = live_runs(tx, p.name).get(key)
                held = Bool(live)
                run = Int64(live.value()) if live else self._queue_fire(
                    tx, p, key
                )
                _ = tx.execute(
                    (
                        "UPDATE orca.fires SET run = $3 WHERE pipeline = $1"
                        " AND fire_at = $2"
                    ),
                    Params().text(p.name).int64(t).int64(run),
                )
            tx.commit()
            if len(made) == 1:
                print(
                    "fired",
                    p.name,
                    "at",
                    String(Civil.of(t), "Z"),
                    "for",
                    key,
                    String("left to live run ", run) if held else (
                        String("run ", run) if run >= 0 else "before the start"
                    ),
                )

    def _queue_fire(
        mut self, mut tx: Transaction, p: Registered, key: String
    ) raises -> Int64:
        """A queued run of `p` over `key`, trigger `schedule`."""
        return (
            tx.query(
                (
                    "INSERT INTO orca.runs (pipeline, registry_hash,"
                    " partitions, trigger, status) VALUES ($1, $2, $3,"
                    " 'schedule', 'queued') RETURNING id"
                ),
                Params().text(p.name).text(p.registry_hash).text(key),
            )
            .row(0)
            .int64("id")
        )

    def _fleet(mut self) raises:
        """Every `fleet_seconds`, keeps the new samples of the live
        instances and the control instance for the fleet page
        (`orca.fleet`)."""
        if self.fleet_seconds <= 0 or not (
            self.ec2 or getenv("ORCA_CONTROL_SAMPLES")
        ):
            return
        var t = Int(perf_counter_ns())
        if t < self._fleet_next:
            return
        self._fleet_next = t + Int(self.fleet_seconds * 1e9)
        _ = ingest(self.db)
        var control = getenv("ORCA_CONTROL_SAMPLES")
        if control:
            # The same file the fleet read, for the system page's history.
            var text = String()
            try:
                text = String(from_utf8_lossy=Span(read_location(control)))
            except:
                pass  # not written yet
            if text:
                _ = keep_system(self.db, text, self._archive)
            trim_system(self.db)

    def _watchdog(mut self) raises:
        """Every `watchdog_seconds`, checks the watchdog's rules and pauses
        the pipelines of those over their limits (`orca.watchdog`)."""
        if self.watchdog_seconds <= 0:
            return
        var t = Int(perf_counter_ns())
        if t < self._watchdog_next:
            return
        self._watchdog_next = t + Int(self.watchdog_seconds * 1e9)
        _ = check_watchdog(self.db, watchdog_config())

    def _notify(mut self, now: Int64) raises:
        """Alerts to a person (`orca.notify`): sends what is pending and
        due, and every `ORCA_NOTIFY_CHECK_SECONDS` (10) checks what the
        alerts of `orca.alerts` are about that the scheduler's other steps
        don't decide: a watchdog trip that ended, the control host's disk,
        workers that went quiet, and fills that stalled. A failure to send
        is logged and tried again later, never raised."""
        if not notify_configured():
            return
        _ = flush_notify(self.db)
        var t = Int(perf_counter_ns())
        if t < self._notify_next:
            return
        var every = Float64(getenv("ORCA_NOTIFY_CHECK_SECONDS", "10"))
        self._notify_next = t + Int(every * 1e9)
        watchdog_recovered(self.db)
        alert_disk(self.db)
        _ = lost_workers(self.db)
        var hour = Int((now % 86400) // 3600)
        for p in registered(self.db):
            if not p.fill.all:
                continue
            var tx = self.db.conn.begin()
            var status = fill_status(
                tx,
                fill_pipeline(
                    p,
                    placed_of(p),
                    Int(self.ec2.value().pool_seconds) if self.ec2 else 0,
                ),
                now,
                p.run_retries,
            )
            tx.commit()
            alert_fill(
                self.db, p.name, status, p.paused, p.fill.in_window(hour)
            )
        _ = flush_notify(self.db)

    def _expire(mut self) raises:
        """Every `expire_seconds`, expires each dataset orca commits to by
        the table's own `history.expire.*` properties, one at a time in the
        background (`orca_maintain expire --if-policy`), with the snapshots
        live runs need retained (`orca.expiry.retained`). `orca tick` waits
        for the whole sweep."""
        if self.expire_seconds <= 0 or not self.maintain:
            return
        while True:
            if len(self._expiring) == 0:
                var t = Int(perf_counter_ns())
                if t < self._expire_next:
                    return
                self._expire_next = t + Int(self.expire_seconds * 1e9)
                self._expiring = expirable(self.db)
                if len(self._expiring) == 0:
                    return
            var ds = self._expiring[0]
            var job = String("expire ", ds)
            var argv = List[String]()
            if job not in self._asked:
                argv = expire_argv(self.db, self.maintain, ds)
            var out: Optional[String]
            try:
                out = self._ask(job, String("orca: expire ", ds), argv)
            except e:
                _ = self._expiring.pop(0)
                raise e
            if not out:
                return
            _ = self._expiring.pop(0)
            var line = String(out.value().strip())
            if line:
                print(line)
            if len(self._expiring) == 0:
                return

    def _ask(
        mut self, job: String, what: String, argv: List[String]
    ) raises -> Optional[String]:
        """What `argv` printed, once it has ended: started the first time
        `job` is asked for, and checked on by later calls, which get None
        while it's going (unless `wait_for_binaries`). One that exits
        non-zero raises `<what> exited <code>: <output>`; one going longer
        than `ORCA_BINARY_SECONDS` is killed, and raises saying so."""
        if job not in self._asked:
            self._asked[job] = Capture.start(argv)
        var limit = binary_seconds()
        while True:
            var c = self._asked.pop(job)
            if c.ended():
                var out = c.output()
                if c.code != 0:
                    raise Error(what, " exited ", c.code, ": ", out)
                return out^
            if limit > 0 and c.seconds() > limit:
                c.kill()
                raise c.timed_out(limit)
            self._asked[job] = c^
            if not self.wait_for_binaries:
                return None
            sleep(0.01)

    def _keys_for(
        mut self,
        p: Registered,
        datasets: List[String],
        snapshots: List[Int64],
    ) raises -> Optional[Tuple[Int64, List[String]]]:
        """A keyed sensor's current keys: those last read if no snapshot of
        its keys dataset among `snapshots` is newer than theirs, else from
        `<binary> keys`, which may still be going (None)."""
        var keys_snap = Int64(-1)
        for i in range(len(datasets)):
            if datasets[i] == p.keys_dataset:
                keys_snap = snapshots[i]
        var have = self._keys.get(p.name)
        if have and (keys_snap < 0 or have.value()[0] == keys_snap):
            return have.value().copy()
        var out = self._ask(
            String("keys ", p.name),
            String("orca: ", p.binary, " keys"),
            [p.binary, "keys"],
        )
        if not out:
            return None
        var got = parse_keys_output(p.binary, out.value())
        keep_keys(self.db, p.name, got)
        self._keys[p.name] = got.copy()
        return got^

    def _sense_keyed(
        mut self,
        p: Registered,
        var all_keys: List[String],
        datasets: List[String],
        snapshots: List[Int64],
        keys: List[String],
    ) raises:
        """For a keyed sensor whose current keys are `all_keys`: a snapshot
        of its keys dataset offers every current key, and one of another
        dataset the names it holds that are current keys; the offered keys
        that aren't done or in a live run are queued, `keys_per_run` to a
        run."""
        var cur = Dict[String, Bool]()
        for k in all_keys:
            cur[k] = True
        var tx = self.db.conn.begin()
        var taken = List[Int]()
        var everything = False
        var offer = List[String]()
        for i in range(len(datasets)):
            var new = tx.query(
                (
                    "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " snapshot_id"
                ),
                Params().text(p.name).text(datasets[i]).int64(snapshots[i]),
            )
            if len(new) == 0:
                continue
            taken.append(i)
            if datasets[i] == p.keys_dataset:
                everything = True
            else:
                # Names a keyed pipeline upstream committed; others aren't
                # this pipeline's to run.
                for k in split_keys(keys[i]):
                    if k in cur:
                        offer.append(k)
        var runs = _queue(
            tx, p, all_keys if everything else offer, "sensor"
        ).runs.copy()
        if len(runs) > 0:
            for i in taken:
                _ = tx.execute(
                    (
                        "UPDATE orca.sensed SET run = $4 WHERE pipeline = $1"
                        " AND dataset = $2 AND snapshot_id = $3"
                    ),
                    Params()
                    .text(p.name)
                    .text(datasets[i])
                    .int64(snapshots[i])
                    .int64(Int64(runs[0])),
                )
        tx.commit()
        for i in taken:
            print(
                "sensed",
                String(datasets[i], "@", snapshots[i]),
                "for",
                p.name,
                String(len(runs), " runs from run ", runs[0]) if len(runs)
                > 0 else "no new keys",
            )

    def _schedule_retries(mut self, p: Registered, now: Int64) raises:
        """Gives each failed run that may be retried its time to start:
        the pipeline's `retry_seconds` (or `ORCA_RETRY_SECONDS`) doubling
        with each start that wasn't after a deferral, up to its
        `max_retry_seconds`, plus up to a tenth as jitter."""
        var base = Int64(p.retry_seconds) if p.retry_seconds > 0 else (
            self.retry_seconds
        )
        _ = self.db.conn.execute(
            String(
                (
                    "UPDATE orca.runs SET retry_at = $2 + (SELECT CASE WHEN $5"
                    " > 0 THEN least(w, $5) ELSE w END FROM (SELECT least($3 *"
                    " power(2, least(starts - deferrals - 1, 60)),"
                    " 1e15)::bigint AS w) AS x) + (id * 7919 % 1000) * least($3"
                    " * power(2, least(starts - deferrals - 1, 60)), CASE WHEN"
                    " $5 > 0 THEN $5 ELSE 1e15 END)::bigint / 10000 WHERE"
                    " pipeline = $1 AND status = 'failed' AND "
                ),
                _RETRIED,
                " AND retry_at IS NULL AND starts - deferrals BETWEEN 1 AND $4",
                " AND (registry_hash = $6 OR allow_new_code)",
            ),
            Params()
            .text(p.name)
            .int64(now)
            .int64(base)
            .int64(Int64(p.run_retries))
            .int64(Int64(p.max_retry_seconds))
            .text(p.registry_hash),
        )

    def _dead(self, stale_param: StaticString) -> String:
        """SQL true for a running run whose process died: its heartbeat is
        older than the stale limit, the parameter `stale_param`, or it has
        none and was started longer ago than the start grace."""
        return dead_sql(String(stale_param), self.start_grace)

    def _end_dead(
        mut self, mut tx: Transaction, p: Registered, dead: String
    ) raises:
        """Ends what of `p`'s runs can't go on: a run asked to cancel whose
        process died is cancelled; one of code that isn't the pipeline's
        any more fails, saying how to resume it; one whose process died
        with no retries left, or that the scheduler doesn't retry, fails
        (and so does a fill run's while fills are off)."""
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'cancelled', ended = now(),",
                " heartbeat = NULL WHERE pipeline = $1 AND status = 'running'",
                " AND cancel_requested AND ",
                dead,
            ),
            Params().text(p.name).float64(self.stale),
        )
        # A run of code that isn't the pipeline's any more would be refused
        # by the binary's `resume`, and started again until its retries were
        # spent, so it fails here, saying how to resume it.
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'failed', ended =",
                " coalesce(ended, now()), heartbeat = NULL, retry_at = NULL,",
                " error = 'not started: its code is ' || registry_hash || ',",
                " and the pipeline''s binary is now ' || $4 || '; resume it",
                " with the new code from orca ui, or with <binary> resume",
                " ' || id || ' --allow-new-code' WHERE pipeline = $1 AND",
                " registry_hash <> $4 AND NOT allow_new_code AND error IS NULL",
                " AND (status IN ('queued', 'deferred') OR (status = 'failed'",
                " AND ",
                _RETRIED,
                " AND starts - deferrals BETWEEN 1 AND $3) OR (status =",
                " 'running' AND ",
                _RETRIED,
                " AND NOT cancel_requested AND ",
                dead,
                "))",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries))
            .text(p.registry_hash),
        )
        # A run whose process died and has no retries left has failed, and
        # so has one the scheduler doesn't retry (a manual, UI or rerun
        # run), so it holds its keys no longer and can be resumed.
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'failed', ended = now(),",
                " heartbeat = NULL, error = CASE WHEN ",
                _RETRIED,
                " THEN 'interrupted: its process stopped, and its retries are",
                " spent' ELSE 'interrupted: its process stopped' END WHERE",
                " pipeline = $1 AND status = 'running' AND NOT",
                " cancel_requested AND ",
                dead,
                " AND (starts - deferrals > $3 OR NOT ",
                _RETRIED,
                " OR (trigger = 'fill' AND NOT $4))",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries))
            .bool(p.fill.all),
        )

    def _start(mut self, p: Registered, now: Int64, fill: Bool = False) raises:
        """Starts queued runs, retries that are due and runs whose process
        died, up to `max_runs` live at once; none while `p` is paused, when
        they wait (runs already going go on). A control-placed pipeline's
        runs start here (`orca.control`), within the control instance's
        limits; a queued run those hold, or that's placed on a runner this
        scheduler hasn't, is `held` with the reason.

        With `fill`, the same for its fill runs (trigger `fill`), up to
        `fill_max_runs` beside the others: due retries first, then new
        runs over the partitions `orca.fill.fill_status` gives, within its
        window and budget."""
        var pool = pool_of(p.placement)
        if pool:
            # A remote worker of the pool starts them (orca.workers).
            if fill:
                self._fill_for_workers(p, now, pool)
            else:
                self._requeue_for_workers(p, now, pool)
            return
        var dead = self._dead("$2")
        # Which runs this pass is for: its fill runs, or all the others.
        var which = String(" AND trigger = 'fill'") if fill else String(
            " AND trigger <> 'fill'"
        )
        var control = p.placement == CONTROL
        var placed = String(CONTROL) if control else (
            String("ec2") if self.ec2 else (
                String("kubernetes") if self.runner else String("local")
            )
        )
        var hold = String()
        if p.placement == "ec2" and not self.ec2:
            hold = String(
                "held: placed on ec2, but this scheduler's runner",
                " (ORCA_RUNNER) is ",
                placed,
            )
        var pressure = String()
        if control:
            try:
                pressure = pressure_reason(system_now(self.db))
            except e:
                print("orca: can't read the control instance's samples:", e)
        if (
            not control
            and not hold
            and (self.runner or self.ec2)
            and not p.image
        ):
            if fill:
                return  # said once, by the pass for its other runs
            print(
                "orca: pipeline",
                p.name,
                "has no image (orca register --image), so its runs wait",
            )
            return
        # On EC2 with the warm pool, the keys of the instances its runs may
        # take, and whether some are still on their way to the pool from a
        # run that just ended: then nothing starts this tick, so the next
        # run takes them rather than new ones.
        var keys = List[String]()
        var settling = False
        if self.ec2 and not control:
            keys = self.ec2.value().pool_keys(_ec2_spec(p, 0, 0, False))
            settling = self.ec2.value().settling(self.db, keys)
        # Its request on this host (orca.resources), `auto` learned from its
        # runs (orca.recommend): resolved here, once for the starts this
        # pass makes, each granted from it for its life.
        var sized = RunRequest(Request(0, 0, 0, 0), "", 0, "")
        if control or placed == "local":
            sized = run_request(self.db, p, self.room.cpus)
        var tx = self.db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        # A run of a scheduler instance that's gone, whose heartbeat is
        # stale, is queued again whatever its trigger, as a dead worker's
        # is; one its live instance still holds isn't dead
        # (orca.live.held_sql), so nothing below ends or restarts it.
        var lost = List[Tuple[Int, String]]()
        var local = control or placed == "local"
        if not fill:
            lost = self._requeue_orphans(tx, p)
            self._end_dead(tx, p, dead)
        var live = Int(
            tx.query(
                String(
                    "SELECT count(*) AS n FROM orca.runs WHERE pipeline = $1",
                    " AND status = 'running' AND NOT ",
                    dead,
                    which,
                ),
                Params().text(p.name).float64(self.stale),
            )
            .row(0)
            .int64("n")
        )
        var free = 0 if p.paused or settling or hold else p.max_runs - live
        var status = Optional[FillStatus](None)
        if fill:
            status = fill_status(
                tx,
                fill_pipeline(
                    p,
                    placed,
                    Int(self.ec2.value().pool_seconds) if self.ec2 else 0,
                ),
                now,
                p.run_retries,
            )
            free = 0 if settling or hold else status.value().slots
        var need = 0
        if control:
            need = 0  # no instance: nothing of the budget
        elif self.runner:
            need = self.runner.value().cpu_of(p.workers)
        elif self.ec2:
            need = self.ec2.value().cpu_of(
                p.workers > 0, p.head_type, p.worker_type, p.workers
            )
        if control and free > 0:
            if pressure:
                hold = pressure
                free = 0
            else:
                # Across pipelines: one start at a time counts the slots.
                _ = tx.execute(
                    "SELECT pg_advisory_xact_lock($1)",
                    Params().int64(_CONTROL_LOCK),
                )
                var used = Int(
                    tx.query(
                        String(
                            "SELECT count(*) AS n FROM orca.runs WHERE status",
                            " = 'running' AND placement = 'control' AND NOT ",
                            self._dead("$1"),
                        ),
                        Params().float64(self.stale),
                    )
                    .row(0)
                    .int64("n")
                )
                if used >= self.limits.max_runs:
                    hold = slots_reason(used, self.limits.max_runs)
                    free = 0
                else:
                    free = min(free, self.limits.max_runs - used)
        # Memory pressure (orca.pressure): on this host, no start while it's
        # over the limit, but one at a time once a hold has lasted its
        # grace with none of orca's runs going.
        if (control or placed == "local") and free > 0 and not hold:
            var allowed = self.admission.allows(self._idle())
            if allowed == 0:
                hold = self.admission.hold
                free = 0
            elif allowed > 0:
                free = min(free, allowed)
        # A request (orca.resources): on this host, a start only if its
        # minimum fits what the running runs weren't granted, each with its
        # grant. A pipeline without one is started as ever.
        var request = sized.request
        if request.memory_min_mb > 0:
            # At least what an out-of-memory kill raised it to.
            request.memory_min_mb = raised_request(
                request.memory_min_mb,
                request.memory_max_mb,
                p.memory_raised_mb,
            )
        var grants = List[Grant]()
        var admitted = request.any() and (control or placed == "local")
        if admitted and free > 0:
            # Across pipelines: one start at a time weighs the grants.
            _ = tx.execute(
                "SELECT pg_advisory_xact_lock($1)",
                Params().int64(_HOST_LOCK),
            )
            var g = tx.query(
                String(
                    "SELECT coalesce(sum(grant_cpus), 0) AS cpus,",
                    " coalesce(sum(grant_memory_mb), 0) AS mb FROM orca.runs",
                    " WHERE status = 'running' AND placement IN ('local',",
                    " 'control') AND NOT ",
                    self._dead("$1"),
                ),
                Params().float64(self.stale),
            ).row(0)
            var got = admit(
                request,
                self.room,
                Int(g.int64("cpus")),
                Int(g.int64("mb")),
                free,
            )
            grants = got[0].copy()
            if len(grants) < free:
                # The ones that don't start say why; those that do, not.
                hold = got[1]
            free = len(grants)
        # Its queued runs say why they wait, or no longer do.
        _ = tx.execute(
            String(
                (
                    "UPDATE orca.runs SET held = nullif($2, '') WHERE pipeline"
                    " = $1"
                ),
                " AND status = 'queued' AND held IS DISTINCT FROM nullif($2,",
                " '')",
                which,
            ),
            Params().text(p.name).text(hold),
        )
        if self.budget > 0 and free > 0 and not control:
            # Across pipelines: one start at a time weighs the budget.
            _ = tx.execute(
                "SELECT pg_advisory_xact_lock($1)",
                Params().int64(_BUDGET_LOCK),
            )
            var used = Int(
                tx.query(
                    String(
                        "SELECT coalesce(sum(cpu_millis), 0) AS n FROM",
                        " orca.runs WHERE status = 'running' AND NOT ",
                        self._dead("$1"),
                    ),
                    Params().float64(self.stale),
                )
                .row(0)
                .int64("n")
            )
            # The pool's idle instances count too, but those a run of `p`
            # would take are its own: k runs fit if the budget holds the
            # others and k runs' worth or those, whichever is more.
            var others = 0
            var mine = 0
            if len(keys) > 0:
                var b = keys[1] if len(keys) > 1 else keys[0]
                var pr = tx.query(
                    (
                        "SELECT coalesce(sum(i.cpu_millis), 0) AS n,"
                        " coalesce(sum(i.cpu_millis) FILTER (WHERE i.pool_key"
                        " IN ($1, $2)), 0) AS mine FROM orca.instances i JOIN"
                        " orca.runs r ON r.id = i.run WHERE i.ended IS NULL AND"
                        " i.pool_key IS NOT NULL AND NOT (r.status = 'running'"
                        " AND r.starts = i.start)"
                    ),
                    Params().text(keys[0]).text(b),
                ).row(0)
                mine = Int(pr.int64("mine"))
                others = Int(pr.int64("n")) - mine
            var room = self.budget - used - others
            if room < mine:
                free = 0
            else:
                free = min(free, room // need if need > 0 else free)
        var ids = List[Int64]()
        var new_code = List[Bool]()
        var starts = List[Int]()
        if free > 0:
            var rows = tx.query(
                String(
                    (
                        "SELECT id, allow_new_code FROM orca.runs WHERE"
                        " pipeline = $1 AND (status"
                    ),
                    (
                        " = 'queued' OR (status = 'deferred' AND retry_at <="
                        " $4) OR"
                    ),
                    " (status = 'failed' AND ",
                    _RETRIED,
                    " AND retry_at <= $4) OR (status = 'running' AND ",
                    _RETRIED,
                    " AND ",
                    dead,
                    " AND starts - deferrals <= $3))",
                    which,
                    " ORDER BY id LIMIT $5",
                ),
                Params()
                .text(p.name)
                .float64(self.stale)
                .int64(Int64(p.run_retries))
                .int64(now)
                .int64(Int64(free)),
            )
            for r in rows:
                ids.append(r.int64("id"))
                new_code.append(r.bool("allow_new_code"))
            if status:
                # New fill runs, over the partitions nothing has filled.
                ref next = status.value().next_keys
                for i in range(min(free - len(ids), len(next))):
                    ids.append(
                        tx.query(
                            (
                                "INSERT INTO orca.runs (pipeline,"
                                " registry_hash, partitions, trigger, status)"
                                " VALUES ($1, $2, $3, 'fill', 'queued')"
                                " RETURNING id"
                            ),
                            Params()
                            .text(p.name)
                            .text(p.registry_hash)
                            .text(next[i]),
                        )
                        .row(0)
                        .int64("id")
                    )
                    new_code.append(False)
                    print("fill", p.name, next[i], "run", ids[len(ids) - 1])
            for i in range(len(ids)):
                var id = ids[i]
                var grant = grants[i] if admitted else Grant.none()
                # `started` is the clock's now, not now(): that is when this
                # transaction began, before it waited on the locks above and
                # counted the slots and the budget, so a run that waited for
                # another to end could be recorded as started before it did.
                starts.append(
                    Int(
                        tx.query(
                            (
                                "UPDATE orca.runs SET status = 'running', owner"
                                " = 'scheduler', heartbeat = NULL, started ="
                                " clock_timestamp(), ended = NULL, starts ="
                                " starts + 1,"
                                " retry_at = NULL, error = NULL, waiting_since"
                                " = NULL, cpu_millis = $2, placement = $3, held"
                                " = NULL, request_cpus = nullif($4, ''),"
                                " request_memory_gb = nullif($5, ''),"
                                " grant_cpus = nullif($6, 0), grant_memory_mb ="
                                " nullif($7, 0), memory_limit_mb = NULL,"
                                " memory_max_mb = NULL, memory_bytes = NULL,"
                                " instance = nullif($8, 0), request_source ="
                                " nullif($9, ''), request_runs = nullif($10,"
                                " 0) WHERE id = $1 RETURNING starts"
                            ),
                            Params()
                            .int64(id)
                            .int64(Int64(need))
                            .text(placed)
                            .text(
                                recorded_cpus(
                                    p, sized
                                ) if admitted else String()
                            )
                            .text(
                                recorded_memory_gb(
                                    p, sized
                                ) if admitted else String()
                            )
                            .int64(Int64(grant.cpus))
                            .int64(Int64(grant.memory_mb))
                            .int64(Int64(self.instance if local else 0))
                            .text(sized.source)
                            .int64(Int64(sized.runs)),
                        )
                        .row(0)
                        .int64("starts")
                    )
                )
        tx.commit()
        for l in lost:
            print(
                String("requeued run ", l[0], " of ", p.name),
                ": ",
                l[1],
                sep="",
            )
        for i in range(len(ids)):
            var id = ids[i]
            var grant = grants[i] if admitted else Grant.none()
            if admitted:
                print(
                    "granted",
                    p.name,
                    "run",
                    id,
                    grant.cpus,
                    "CPUs and",
                    grant.memory_mb,
                    "MB (0: not asked for)",
                    sized.text(),
                )
            elif sized.source == DEFAULT:
                print("run", id, "of", p.name, "requests", sized.text())
            if control:
                self._start_control(p, Int(id), starts[i], new_code[i], grant)
                continue
            if self.runner:
                try:
                    self.runner.value().start(
                        RunSpec(
                            Int(id),
                            starts[i],
                            p.name,
                            p.image,
                            p.binary,
                            p.workers,
                            new_code[i],
                            resolve_auto(p.env),
                        )
                    )
                    print("started", p.name, "run", id, "on its cluster")
                except e:
                    print("orca: run", id, "of", p.name, "can't start:", e)
                    _ = self.db.conn.execute(
                        (
                            "UPDATE orca.runs SET status = 'failed', ended ="
                            " now(), error = $2 WHERE id = $1 AND status ="
                            " 'running' AND heartbeat IS NULL"
                        ),
                        Params().int64(id).text(String("can't start: ", e)),
                    )
                continue
            if self.ec2:
                try:
                    self.ec2.value().start(
                        self.db, _ec2_spec(p, Int(id), starts[i], new_code[i])
                    )
                    print("started", p.name, "run", id, "on its instances")
                except e:
                    # Retried as a run whose process failed is.
                    print("orca: run", id, "of", p.name, "can't start:", e)
                    _ = self.db.conn.execute(
                        (
                            "UPDATE orca.runs SET status = 'failed', ended ="
                            " now(), error = $2 WHERE id = $1 AND status ="
                            " 'running' AND heartbeat IS NULL"
                        ),
                        Params().int64(id).text(String("can't start: ", e)),
                    )
                continue
            # Its own variables, from orca.toml, its grant
            # (orca.resources), and its secrets, read now and named in
            # ORCA_SECRETS for masking; the rest it inherits.
            var env = grant_env(p.env, grant, pressure_env(self.psi))
            try:
                var secrets = List[Tuple[String, String]]()
                for s in p.secrets:
                    secrets.append((s[0], fetch_secret(s[1])))
                env.extend(secrets_env(secrets))
            except e:
                # Retried as a run whose process failed is.
                print("orca: run", id, "of", p.name, "can't start:", e)
                _ = self.db.conn.execute(
                    (
                        "UPDATE orca.runs SET status = 'failed', ended ="
                        " now(), error = $2 WHERE id = $1 AND status ="
                        " 'running' AND heartbeat IS NULL"
                    ),
                    Params().int64(id).text(String("can't start: ", e)),
                )
                continue
            var log = String()
            var stage = String()
            # A run with secrets prints what it likes: its output is
            # staged outside ORCA_LOGS and appended to its driver.log,
            # masked, by keep-log once it ends, as a control-placed run's.
            var kept = len(p.secrets) > 0
            if self.logs:
                log = String(run_dir(p.name, Int(id)), "/driver.log")
                stage = control_stage(
                    p.name, Int(id), starts[i]
                ) if kept else stage_of(log)
                makedirs(
                    String(stage[byte = : stage.rfind("/")]), exist_ok=True
                )
            var argv: List[String] = [p.binary, "resume", String(id)]
            if new_code[i]:
                argv.append("--allow-new-code")
            if log:
                if kept:
                    # keep-log heads it with the start.
                    with open(stage, "w") as f:
                        f.write("")
                else:
                    # Each start's output after a line of its own, as on
                    # the other runners: a crash's dump is looked for after
                    # the last.
                    with open(stage, "a") as f:
                        f.write("--- start ", starts[i], "\n")
            var cg = self._contain(p, Int(id), starts[i], grant, False)
            var pid = spawn(argv, stage, env=env, cgroup=cg.path)
            if cg.path:
                self._contained[pid] = cg^
            self._meters[pid] = Meter()
            self._spawned[pid] = (Int(id), starts[i], stage)
            if log and kept:
                self._kept[pid] = _ControlProc(
                    Int(id), p.binary, stage, log, starts[i], env^, ""
                )
            elif is_remote(log):
                self._driver_logs[pid] = log
            print("started", p.name, "run", id, "pid", pid)
            if log and sample_seconds() > 0:
                # The machine's metrics while the run's process lives
                # (orca.sample); it ends once that process has.
                try:
                    _ = spawn(
                        [
                            p.binary,
                            "sample",
                            "--out",
                            metrics_location(p.name, Int(id), "local"),
                            "--pid",
                            String(pid),
                        ]
                    )
                except e:
                    print("orca: no sampler for run", id, ":", e)

    def _requeue_for_workers(
        mut self, p: Registered, now: Int64, pool: String
    ) raises:
        """What `_start` does for a pipeline placed `worker:<pool>`, whose
        runs a worker of the pool claims (`orca.workers`): queues again
        the runs of a worker that's gone (`_requeue_lost`), whatever their
        trigger; ends the runs that can't go on as for any pipeline; and
        queues again, for a worker to claim, the retries and deferred runs
        that are due and the runs whose process died with retries left.
        Its queued runs say they wait for a worker (its fill runs, while
        fills are off, that they wait for fills)."""
        var dead = self._dead("$2")
        var waiting = String("held: waiting for a worker of pool ", pool)
        var tx = self.db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        var lost = self._requeue_lost(tx, p, dead, waiting)
        self._end_dead(tx, p, dead)
        if not p.fill.all:
            _ = tx.execute(
                (
                    "UPDATE orca.runs SET held = $2 WHERE pipeline = $1 AND"
                    " status = 'queued' AND trigger = 'fill' AND held IS"
                    " DISTINCT FROM $2"
                ),
                Params()
                .text(p.name)
                .text(
                    'held: fills are off (fills isn\'t "all"), so no worker'
                    " is given it"
                ),
            )
        var n = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'queued', owner = NULL,",
                " heartbeat = NULL, retry_at = NULL, held = $5 WHERE pipeline",
                " = $1 AND trigger <> 'fill' AND ((status = 'deferred' AND",
                " retry_at <= $4) OR (status = 'failed' AND ",
                _RETRIED,
                " AND retry_at <= $4) OR (status = 'running' AND ",
                _RETRIED,
                " AND ",
                dead,
                " AND starts - deferrals <= $3))",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries))
            .int64(now)
            .text(waiting),
        )
        _ = tx.execute(
            (
                "UPDATE orca.runs SET held = $2 WHERE pipeline = $1 AND status"
                " = 'queued' AND held IS NULL"
            ),
            Params().text(p.name).text(waiting),
        )
        tx.commit()
        for l in lost:
            print(
                String(
                    "requeued run ", l[0], " of ", p.name, " for pool ", pool
                ),
                ": ",
                l[1],
                sep="",
            )
        if n > 0:
            print("requeued", n, "runs of", p.name, "for pool", pool)

    def _requeue_lost(
        mut self,
        mut tx: Transaction,
        p: Registered,
        dead: String,
        waiting: String,
    ) raises -> List[Tuple[Int, String]]:
        """Queues again, for the pool, `p`'s runs whose worker is gone,
        whatever their trigger (a manual, UI or rerun run too, which a dead
        process would fail): a running run whose worker hasn't been seen
        for half the stale limit (killed, or its machine asleep or off the
        network), or whose process died while the worker, seen since,
        doesn't say it has it (restarted). Nothing ran it past its last
        step, so its next start resumes it. A run the worker claimed but
        never started has its start given back; one that started counts
        it, and once `starts - deferrals` is past `run_retries` it is left
        to `_end_dead`, so a run that kills its workers can't loop. Each is
        a row of `orca.audit` (user `scheduler`, action
        `/api/worker/requeue`); returns each run with why."""
        var out = List[Tuple[Int, String]]()
        # In this query orca.runs is `r` (`dead` names it by the table).
        var gone = dead_sql("$2", self.start_grace, "r")
        var rows = tx.query(
            String(
                "WITH lost AS (SELECT r.id, r.owner = '",
                _WORKER_OWNER,
                "' AS unstarted, CASE WHEN w.last_seen IS NULL OR w.last_seen",
                " <= now() - make_interval(secs => $2 / 2) THEN 'worker ' ||",
                " w.label || ' (token ' || w.id || ') stopped: not seen for '",
                " || coalesce(extract(epoch FROM now() -",
                " w.last_seen)::bigint::text || ' s', 'ever') ELSE 'worker '",
                " || w.label || ' (token ' || w.id || ') no longer runs it'",
                " END AS why FROM orca.runs r JOIN orca.workers w ON w.id =",
                " r.worker WHERE r.pipeline = $1 AND r.status = 'running' AND",
                " r.placement LIKE 'worker:%' AND NOT r.cancel_requested AND",
                " (r.owner = '",
                _WORKER_OWNER,
                "' OR r.starts - r.deferrals <= $3) AND (((w.last_seen IS NULL",
                " OR w.last_seen <= now() - make_interval(secs => $2 / 2)) AND",
                " ((r.owner = '",
                _WORKER_OWNER,
                "' AND r.started <= now() - make_interval(secs => $2)) OR ",
                gone,
                ")) OR (",
                gone,
                " AND NOT (r.id::text = ANY (string_to_array(coalesce(",
                "w.running, ''), ','))))) FOR UPDATE OF r) UPDATE orca.runs u",
                " SET status = 'queued', owner = NULL, heartbeat = NULL,",
                " retry_at = NULL, held = $4, starts = CASE WHEN",
                " lost.unstarted THEN u.starts - 1 ELSE u.starts END FROM lost",
                " WHERE u.id = lost.id RETURNING u.id, lost.why,",
                " lost.unstarted",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries))
            .text(waiting),
        )
        for r in rows:
            var why = r.text("why")
            if r.bool("unstarted"):
                why.write(", before starting it")
            out.append((Int(r.int64("id")), why))
            _ = tx.execute(
                (
                    "INSERT INTO orca.audit (username, action, run, detail,"
                    " source) VALUES ('scheduler', '/api/worker/requeue', $1,"
                    " $2, 'scheduler')"
                ),
                Params()
                .int64(r.int64("id"))
                .text(String("queued again: ", why)),
            )
        return out^

    def _requeue_orphans(
        mut self, mut tx: Transaction, p: Registered
    ) raises -> List[Tuple[Int, String]]:
        """Queues again `p`'s local and control-placed runs whose scheduler
        instance (`runs.instance`, docs/overlap.md) is gone (ended, or not
        seen for the stale limit) and whose heartbeat is stale,
        whatever their trigger: the deploy, or a crash, stopped them, not
        their own code. Nothing ran them past their last step, so the next
        start resumes them. The start counts, and once `starts - deferrals`
        is past `run_retries` a run is left to `_end_dead`, so a run that
        kills its scheduler can't loop. Each is a row of `orca.audit` (user
        `scheduler`, action `/api/scheduler/requeue`); returns each with
        why."""
        var out = List[Tuple[Int, String]]()
        var rows = tx.query(
            String(
                "WITH lost AS (SELECT r.id, 'scheduler instance ' || s.id ||",
                " ' on ' || s.host || CASE WHEN s.ended IS NOT NULL THEN",
                " ' ended' ELSE ' stopped: not seen for ' || extract(epoch",
                " FROM now() - s.last_seen)::bigint || ' s' END AS why FROM",
                (
                    " orca.runs r JOIN orca.schedulers s ON s.id = r.instance"
                    " WHERE"
                ),
                " r.pipeline = $1 AND r.status = 'running' AND r.placement IN",
                " ('local', 'control') AND NOT r.cancel_requested AND",
                " r.starts - r.deferrals <= $3 AND (s.ended IS NOT NULL OR",
                " s.last_seen <= now() - make_interval(secs => $2)) AND ",
                stale_sql("$2", self.start_grace, "r"),
                " FOR UPDATE OF r) UPDATE orca.runs u SET status = 'queued',",
                (
                    " owner = NULL, heartbeat = NULL, retry_at = NULL, held ="
                    " NULL,"
                ),
                " instance = NULL FROM lost WHERE u.id = lost.id RETURNING",
                " u.id, lost.why",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries)),
        )
        for r in rows:
            var why = r.text("why")
            out.append((Int(r.int64("id")), why))
            _ = tx.execute(
                (
                    "INSERT INTO orca.audit (username, action, run, detail,"
                    " source) VALUES ('scheduler', '/api/scheduler/requeue',"
                    " $1, $2, 'scheduler')"
                ),
                Params()
                .int64(r.int64("id"))
                .text(String("queued again: ", why)),
            )
        return out^

    def _fill_for_workers(
        mut self, p: Registered, now: Int64, pool: String
    ) raises:
        """`_start`'s fill pass for a pipeline placed `worker:<pool>`: as
        for a local one, but what may start is queued for a worker of the
        pool to claim rather than started. Within `orca.fill.fill_status`'s
        slots (`fill_max_runs` less its fill runs running or queued, so the
        cap holds across the whole pool), the due retries, deferred runs
        and fill runs whose process died with retries left are queued
        again first, then new fill runs over the partitions nothing has
        filled, in `fill_order`."""
        var dead = self._dead("$2")
        var waiting = String("held: waiting for a worker of pool ", pool)
        var tx = self.db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        var status = fill_status(tx, fill_pipeline(p), now, p.run_retries)
        var free = status.slots
        var again = 0
        if free > 0:
            again = tx.execute(
                String(
                    "UPDATE orca.runs SET status = 'queued', owner = NULL,",
                    " heartbeat = NULL, retry_at = NULL, held = $5 WHERE id IN",
                    " (SELECT id FROM orca.runs WHERE pipeline = $1 AND",
                    " trigger = 'fill' AND ((status = 'deferred' AND retry_at",
                    " <= $4) OR (status = 'failed' AND retry_at <= $4 AND",
                    " starts - deferrals BETWEEN 1 AND $3) OR (status =",
                    " 'running' AND ",
                    dead,
                    " AND starts - deferrals <= $3)) ORDER BY id LIMIT $6)",
                ),
                Params()
                .text(p.name)
                .float64(self.stale)
                .int64(Int64(p.run_retries))
                .int64(now)
                .text(waiting)
                .int64(Int64(free)),
            )
        var ids = List[Int64]()
        ref next = status.next_keys
        for i in range(min(free - again, len(next))):
            ids.append(
                tx.query(
                    (
                        "INSERT INTO orca.runs (pipeline, registry_hash,"
                        " partitions, trigger, status, held) VALUES ($1, $2,"
                        " $3, 'fill', 'queued', $4) RETURNING id"
                    ),
                    Params()
                    .text(p.name)
                    .text(p.registry_hash)
                    .text(next[i])
                    .text(waiting),
                )
                .row(0)
                .int64("id")
            )
        tx.commit()
        if again > 0:
            print("requeued", again, "fill runs of", p.name, "for pool", pool)
        for i in range(len(ids)):
            print("fill", p.name, next[i], "run", ids[i], "for pool", pool)

    def _start_control(
        mut self,
        p: Registered,
        id: Int,
        start: Int,
        new_code: Bool,
        grant: Grant = Grant.none(),
    ):
        """Starts start `start` of `p`'s run `id` on this host
        (`orca.control`): its binary's `resume`, in a process group of its
        own, at a lower CPU priority, first to go if memory runs out, with
        its secrets read now and given to it alone, and its output staged
        for `keep-log`. A run that can't start fails, and is retried as a
        run whose process failed is."""
        var c: _ControlProc
        var pid: Int
        var cg = self._contain(p, id, start, grant, True)
        try:
            var secrets = List[Tuple[String, String]]()
            for s in p.secrets:
                secrets.append((s[0], fetch_secret(s[1])))
            var extra = run_env(self.limits, p.image, secrets)
            extra.extend(pressure_env(self.psi))
            var env = grant_env(p.env, grant, extra)
            var location = String()
            var stage = String()
            if self.logs:
                location = String(run_dir(p.name, id), "/driver.log")
                stage = control_stage(p.name, id, start)
                makedirs(
                    String(stage[byte = : stage.rfind("/")]), exist_ok=True
                )
                with open(stage, "w") as f:
                    f.write("")
            var argv: List[String] = [p.binary, "resume", String(id)]
            if new_code:
                argv.append("--allow-new-code")
            pid = spawn(
                argv,
                stage,
                env=env,
                nice=self.limits.nice,
                group=True,
                first_to_go=True,
                cgroup=cg.path,
            )
            c = _ControlProc(id, p.binary, stage, location, start, env^, "")
        except e:
            print("orca: run", id, "of", p.name, "can't start:", e)
            if cg.path and not Cgroups.remove(cg.path):
                self._unswept.append(cg.path)
            try:
                _ = self.db.conn.execute(
                    (
                        "UPDATE orca.runs SET status = 'failed', ended ="
                        " now(), error = $2 WHERE id = $1 AND status ="
                        " 'running' AND heartbeat IS NULL"
                    ),
                    Params().int64(Int64(id)).text(String("can't start: ", e)),
                )
            except e2:
                print("orca: can't record run", id, "failing:", e2)
            return
        self._spawned[pid] = (id, start, c.stage)
        self._meters[pid] = Meter()
        if cg.path:
            self._contained[pid] = cg^
        var sampled = c.location != ""
        self._control[pid] = c^
        print("started", p.name, "run", id, "pid", pid, "on the control host")
        if sampled and sample_seconds() > 0:
            try:
                _ = spawn(
                    [
                        p.binary,
                        "sample",
                        "--out",
                        metrics_location(p.name, id, CONTROL),
                        "--pid",
                        String(pid),
                    ],
                    nice=self.limits.nice,
                )
            except e:
                print("orca: no sampler for run", id, ":", e)

    def backfill(
        mut self,
        pipeline: String,
        keys: String,
        rerun: Bool = False,
        allow_append: Bool = False,
    ) raises -> Queued:
        """Queues a run for each partition in `keys` that has none; see
        `backfill`."""
        var ps = registered(self.db, pipeline)
        if len(ps) == 0:
            raise Error("orca: pipeline ", pipeline, " isn't registered")
        return backfill(self.db, ps[0], keys, rerun, allow_append)
