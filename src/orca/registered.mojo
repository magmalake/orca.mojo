"""A registered pipeline as the scheduler reads it (`Registered`,
`registered`), and what its runs ask for (`run_request`).
"""

from std.os import getenv
from postgres import Params
from orca.civil import Civil
from orca.control import (
    CONTROL,
    pool_of,
)
from orca.config import (
    parse_env,
    max_runs_auto,
)
from orca.fill import (
    Fill,
    FillPipeline,
)
from orca.cron import CronExpr
from orca.day import Day
from orca.ec2 import Ec2Spec
from orca.month import Month
from orca.schedule import Schedule
from orca.secrets import parse_secrets
from orca.resources import (
    Request,
    resolve_auto,
)
from orca.recommend import (
    Recommendation,
    RunRequest,
    learned,
    resolve_request,
)
from orca.state import State
from orca.tz import Zone


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
    var max_runs_auto: Bool
    """`max_runs = "auto"` (#266): as many runs at once as this host has
    room for, `max_runs` the ceiling."""

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
            " max_runs, coalesce(max_runs_auto, false) AS max_runs_auto,"
            " coalesce((describe->>'run_retries')::bigint, 3) AS"
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
                r.bool("max_runs_auto"),
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
