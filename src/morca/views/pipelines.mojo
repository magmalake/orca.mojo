"""Pipelines as the pages and their JSON read them: `PipelineRow` (a
registration with its schedule and live runs), its steps (`StepDef`), its
next fires, a keyed pipeline's counts (`KeyCounts`), its fill, and the
Resources card's summary of what its runs used (`UsageSummary`).
"""

from std.os import getenv

from postgres import Params

from orca.civil import Civil, now
from orca.config_show import (
    Shown,
    shown,
)
from orca.coverage import Coverage, coverage
from orca.cron import CronExpr
from orca.fill import FillStatus, fill_status
from orca.follow import Follow, follow
from orca.lineage import SnapshotRef
from orca.live import live_sql
from orca.recommend import Recommendation
from orca.schedule import Schedule
from orca.queueing import (
    current_keys,
    done_keys,
    keep_keys,
    kept_keys,
)
from orca.registered import (
    fill_key,
    fill_pipeline,
    registered,
)
from orca.state import State
from orca.tz import Zone
from orca.views.runs import (
    RunsPage,
    VersionRow,
    int_or_zero,
    runs_page,
    runs_query,
    utc_sql,
    version_rows,
)
from orca.views.start import StartPreview, start_preview


def utc_text(t: Int64) -> String:
    """Seconds since the epoch as `YYYY-MM-DDTHH:MM:SSZ`."""
    return String(Civil.of(t), "Z")


@fieldwise_init
struct PipelineRow(Copyable, Movable):
    var name: String
    var registry_hash: String
    var binary: String
    var kind: String
    var start: String
    var cron: String
    var tz: String
    var fills: String
    var max_runs: Int
    var run_retries: Int
    var running: Int
    var queued: Int
    var last_run: Int
    """0 when it has none."""
    var last_status: String
    var last_partitions: String
    var next_fire: String
    var next_partition: String
    var paused: Bool
    """`orca pause`: the scheduler starts none of its runs."""
    var sense_error: String
    """Why the sensor's last pass failed, or empty."""
    var sense_error_at: String
    var placement: String
    """`orca register --placement`: `control`, `ec2`, or empty for the
    scheduler's runner."""
    var cpus: String
    """The CPUs it asks for (`orca.resources`), or empty."""
    var memory_gb: String
    """The memory it asks for, in GB, or empty."""
    var waiting: String
    """Why its oldest queued run that waits for room on its host waits
    (`orca.resources`), or empty."""
    var fill_lag: String
    """`fill_lag` as registered (`orca.fill`), or empty."""


@fieldwise_init
struct StepDef(Copyable, Movable):
    var name: String
    var kind: String
    var after: String
    var reads: List[String]
    var writes: List[String]
    var function: String
    """Its function's linkage name, as `describe` records it."""
    var input: String
    var output: String


@fieldwise_init
struct Fire(Copyable, Movable):
    var at: String
    var local: String
    var partition: String


def comma_list(s: String) -> List[String]:
    """A comma-separated list, empty for an empty string."""
    var out = List[String]()
    if s:
        for p in s.split(","):
            out.append(String(p))
    return out^


def next_fires(p: PipelineRow, after: Int64, count: Int) raises -> List[Fire]:
    var out = List[Fire]()
    if not p.cron:
        return out^
    var s = Schedule(CronExpr.parse(p.cron), Zone.load(p.tz))
    var t = after
    for _ in range(count):
        t = s.next_fire(t)
        var local = Civil.of(s.zone.to_local(t))
        var key = fill_key(p.kind, p.fills, local)
        out.append(Fire(utc_text(t), String(local), key))
    return out^


def pipeline_rows(mut db: State, name: String = "") raises -> List[PipelineRow]:
    """Every registered pipeline, or the one named."""
    var rows = db.conn.query(
        String(
            (
                "SELECT p.name, p.registry_hash, coalesce(p.binary_path, '') AS"
                " binary, coalesce(p.describe->'partitions'->>'kind', '') AS"
                " kind, coalesce(p.describe->'partitions'->>'start', '') AS"
                " start, coalesce(p.describe->'schedule'->>'cron', '') AS cron,"
                " coalesce(p.describe->'schedule'->>'tz', '') AS tz,"
                " coalesce(p.describe->'schedule'->>'fills', '') AS fills,"
                " coalesce(p.max_runs::text, p.describe->>'max_runs', '1') AS"
                " max_runs, coalesce(p.describe->>'run_retries', '0') AS"
                " run_retries, (SELECT count(*) FROM orca.runs r WHERE"
                " r.pipeline = p.name AND r.status = 'running')::text AS"
                " running, (SELECT count(*) FROM orca.runs r WHERE r.pipeline ="
                " p.name AND r.status = 'queued')::text AS queued,"
                " coalesce(l.id, 0)::text AS last_run, coalesce(l.status, '')"
                " AS last_status, coalesce(l.partitions, '') AS"
                " last_partitions, p.paused, coalesce(p.sense_error, '') AS"
                " sense_error, coalesce(p.placement, '') AS placement,"
                " coalesce(p.cpus, '') AS cpus, coalesce(p.memory_gb, '') AS"
                " memory_gb, coalesce((SELECT r.held FROM orca.runs r WHERE"
                " r.pipeline = p.name AND r.status = 'queued' AND r.held LIKE"
                " 'waiting: %' ORDER BY r.id LIMIT 1), '') AS waiting,"
                " coalesce(p.fill_lag, '') AS fill_lag, "
            ),
            utc_sql("p.sense_error_at"),
            (
                " AS sense_error_at FROM orca.pipelines p LEFT JOIN LATERAL"
                " (SELECT id, status, partitions FROM orca.runs r WHERE"
                " r.pipeline = p.name ORDER BY id DESC LIMIT 1) l ON true WHERE"
                " ($1 = '' OR p.name = $1) ORDER BY p.name"
            ),
        ),
        Params().text(name),
    )
    var out = List[PipelineRow]()
    var t = now()
    for r in rows:
        var p = PipelineRow(
            r.text("name"),
            r.text("registry_hash"),
            r.text("binary"),
            r.text("kind"),
            r.text("start"),
            r.text("cron"),
            r.text("tz"),
            r.text("fills"),
            int_or_zero(r.text("max_runs")),
            int_or_zero(r.text("run_retries")),
            int_or_zero(r.text("running")),
            int_or_zero(r.text("queued")),
            int_or_zero(r.text("last_run")),
            r.text("last_status"),
            r.text("last_partitions"),
            "",
            "",
            r.bool("paused"),
            r.text("sense_error"),
            r.text("sense_error_at"),
            r.text("placement"),
            r.text("cpus"),
            r.text("memory_gb"),
            r.text("waiting"),
            r.text("fill_lag"),
        )
        try:
            var f = next_fires(p, t, 1)
            if len(f) > 0:
                p.next_fire = f[0].at
                p.next_partition = f[0].partition
        except:
            pass
        out.append(p^)
    return out^


def step_defs(mut db: State, name: String) raises -> List[StepDef]:
    var rows = db.conn.query(
        (
            "SELECT s->>'name' AS name, s->>'kind' AS kind,"
            " coalesce(s->>'after', '') AS after, coalesce((SELECT"
            " string_agg(v, ',') FROM jsonb_array_elements_text(coalesce(s->"
            "'reads', '[]')) AS v), '') AS reads, coalesce((SELECT"
            " string_agg(v, ',') FROM jsonb_array_elements_text(coalesce(s->"
            "'writes', '[]')) AS v), '') AS writes, coalesce(s->>'function',"
            " '') AS function, coalesce(s->>'input', '') AS input,"
            " coalesce(s->>'output', '') AS output FROM orca.pipelines p,"
            " jsonb_array_elements(p.describe->'steps') WITH ORDINALITY AS x(s,"
            " n) WHERE p.name = $1 ORDER BY n"
        ),
        Params().text(name),
    )
    var out = List[StepDef]()
    for r in rows:
        out.append(
            StepDef(
                r.text("name"),
                r.text("kind"),
                r.text("after"),
                comma_list(r.text("reads")),
                comma_list(r.text("writes")),
                r.text("function"),
                r.text("input"),
                r.text("output"),
            )
        )
    return out^


@fieldwise_init
struct KeyCounts(Copyable, Movable):
    """A keyed pipeline's keys and how far it is through them."""

    var dataset: String
    var snapshot: Int64
    var total: Int
    var done: Int
    var live: Int
    var failed: List[String]
    """Keys not done or in a live run, with a failed or skipped step."""
    var error: String
    """Why its keys couldn't be read, or empty."""
    var read_at: String
    """When `<binary> keys` printed them."""
    var note: String
    """Why they're older than they'd be, or empty."""


comptime _UI_KEYS_SECONDS = 10.0
"""How long the page waits for `<binary> keys` before showing the keys
last read, if any: it runs on the server's one thread."""


def _keys_seconds() -> Float64:
    """How old kept keys may be before the page reads them again: the
    scheduler's `ORCA_SENSE_SECONDS` (60)."""
    try:
        return Float64(getenv("ORCA_SENSE_SECONDS", "60"))
    except:
        return 60.0


comptime _FAILED_SQL = (
    "SELECT DISTINCT st.partition AS key FROM orca.steps st JOIN orca.runs r"
    " ON r.id = st.run WHERE r.pipeline = $1 AND st.status IN ('failed',"
    " 'skipped') AND st.partition = ANY (string_to_array($2, ',')) ORDER BY 1"
)


def key_counts(mut db: State, name: String) raises -> KeyCounts:
    var ps = registered(db, name)
    if len(ps) == 0:
        return KeyCounts(
            "", -1, 0, 0, 0, List[String](), "not registered", "", ""
        )
    ref p = ps[0]
    # The keys last read (by the scheduler, or this page) while they're
    # fresh, so a view doesn't scan the keys column each time.
    var got: Tuple[Int64, List[String]]
    var note = String()
    var kept = kept_keys(db, name)
    if kept and kept.value().age < _keys_seconds():
        got = (kept.value().snapshot, kept.value().keys.copy())
    else:
        try:
            got = current_keys(p, _UI_KEYS_SECONDS)
            keep_keys(db, name, got)
            kept = kept_keys(db, name)
        except e:
            if not kept:
                return KeyCounts(
                    p.keys_dataset,
                    -1,
                    0,
                    0,
                    0,
                    List[String](),
                    String(e),
                    "",
                    "",
                )
            got = (kept.value().snapshot, kept.value().keys.copy())
            note = String("reading them again failed: ", e)
    var read_at = kept.value().read_at if kept else String()
    var done = done_keys(db, name, got[1])
    var live = Dict[String, Bool]()
    for r in db.conn.query(live_sql(), Params().text(name)):
        live[r.text("key")] = True
    var n_live = 0
    var open = String()
    for k in got[1]:
        if k in live:
            n_live += 1
        elif k not in done:
            open.write("," if open else "", k)
    var failed = List[String]()
    if open:
        for r in db.conn.query(_FAILED_SQL, Params().text(name).text(open)):
            failed.append(r.text("key"))
    return KeyCounts(
        p.keys_dataset,
        got[0],
        len(got[1]),
        len(done),
        n_live,
        failed^,
        "",
        read_at,
        note,
    )


def pipeline_coverage(mut db: State, p: PipelineRow) -> Optional[Coverage]:
    """`p`'s coverage (`orca.coverage`), None if it has none or it can't
    be read: a list of pipelines still shows the rest."""
    try:
        return coverage(db, p.name, p.kind, p.start, p.tz, p.fill_lag)
    except:
        return None


def fill_of(mut db: State, name: String) raises -> Optional[FillStatus]:
    """`name`'s fill (`orca.fill`), if it fills its range or has fill runs
    from when it did."""
    var regs = registered(db, name)
    if len(regs) == 0:
        return None
    if not regs[0].fill.all:
        var any = db.conn.query(
            (
                "SELECT 1 FROM orca.runs WHERE pipeline = $1 AND trigger ="
                " 'fill' LIMIT 1"
            ),
            Params().text(name),
        )
        if len(any) == 0:
            return None
    var tx = db.conn.begin()
    var s = fill_status(tx, fill_pipeline(regs[0]), now(), regs[0].run_retries)
    tx.rollback()
    return s^


comptime USAGE_RUNS = 20
"""How many of a pipeline's latest measured runs its Resources card
summarizes."""


@fieldwise_init
struct UsageSummary(Copyable, Movable):
    """What a pipeline's latest measured runs used: the median and the most
    of each run's peak memory, CPU seconds and peak cores; -1 for none."""

    var runs: Int
    var peak_p50: Float64
    var peak_max: Float64
    var cpu_p50: Float64
    var cpu_max: Float64
    var cpus_p50: Float64
    var cpus_max: Float64

    def json(self) -> String:
        """`{"runs": n, "peak_rss_bytes": {"p50": .., "max": ..}, ...}`."""
        var out = String('{"runs": ', self.runs)
        var parts = [
            ("peak_rss_bytes", self.peak_p50, self.peak_max),
            ("cpu_seconds", self.cpu_p50, self.cpu_max),
            ("peak_cpus", self.cpus_p50, self.cpus_max),
        ]
        for p in parts:
            out.write(', "', p[0], '": {"p50": ')
            out.write(_json_num(p[1]), ', "max": ', _json_num(p[2]), "}")
        out.write("}")
        return out^


def _json_num(x: Float64) -> String:
    """`x` to the thousandth as JSON, or null if it's negative."""
    if x < 0:
        return "null"
    return String(Float64(Int(x * 1000.0 + 0.5)) / 1000.0)


def usage_summary(mut db: State, name: String) raises -> UsageSummary:
    """The pipeline's latest `USAGE_RUNS` runs that recorded what they used,
    summarized."""
    var r = db.conn.query(
        (
            "SELECT count(*) AS n, coalesce(percentile_cont(0.5) WITHIN GROUP"
            " (ORDER BY peak), -1) AS peak_p50, coalesce(max(peak), -1)::float8"
            " AS peak_max, coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY"
            " cpu), -1) AS cpu_p50, coalesce(max(cpu), -1) AS cpu_max,"
            " coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY cpus), -1)"
            " AS cpus_p50, coalesce(max(cpus), -1) AS cpus_max FROM (SELECT"
            " max(a.peak_rss_bytes) AS peak, sum(a.cpu_seconds) AS cpu,"
            " max(a.peak_cpus) AS cpus FROM orca.runs r JOIN orca.steps s ON"
            " s.run = r.id JOIN orca.attempts a ON a.step = s.id WHERE"
            " r.pipeline = $1 GROUP BY r.id HAVING max(a.peak_rss_bytes) IS"
            " NOT NULL OR sum(a.cpu_seconds) IS NOT NULL ORDER BY r.id DESC"
            " LIMIT $2) x"
        ),
        Params().text(name).int64(Int64(USAGE_RUNS)),
    ).row(0)
    return UsageSummary(
        Int(r.int64("n")),
        r.float64("peak_p50"),
        r.float64("peak_max"),
        r.float64("cpu_p50"),
        r.float64("cpu_max"),
        r.float64("cpus_p50"),
        r.float64("cpus_max"),
    )


@fieldwise_init
struct PipelineAsk(Copyable, Movable):
    """What a pipeline's page was asked for, from its URL's query: which
    tab (`?tab=`); Start runs' preview over `partitions` (`?preview=1`,
    `&rerun=1`, `&allow_append=1`); the month or year its coverage shows
    (`?cal=`) and which of its tabs (`?cov=`); the view of its steps shown
    without scripts (`?steps=`); and, in `url`, the page of its runs."""

    var name: String
    var partitions: String
    var rerun: Bool
    var preview: Bool
    var cal: String
    var allow_append: Bool
    var steps: String
    var tab: String
    var cov: String
    var url: String

    def shown_tab(self) -> String:
        """`overview`, `runs` or `operate`: as asked, or Operate for a
        preview or a coverage month that names no tab."""
        if self.tab == "runs" or self.tab == "operate":
            return self.tab
        if not self.tab and (
            self.preview or self.cal or self.cov or self.partitions
        ):
            return "operate"
        return "overview"


@fieldwise_init
struct NotRun(Copyable, Movable):
    """A snapshot a pipeline's sensor saw and couldn't run, and why."""

    var snapshot: SnapshotRef
    var at: String
    var error: String


struct PipelineView(Movable):
    """A pipeline as its page and its JSON show it, read once:
    `pipeline_json` (`orca.api.pipelines`) and `pipeline_html`
    (`orca.ui.pipeline`) both draw this. The JSON is the same on every
    tab."""

    var pipeline: PipelineRow
    var steps: List[StepDef]
    var fires: List[Fire]
    """Its next five fires, if it's scheduled."""
    var runs: RunsPage
    """The page of its runs `url` asks for."""
    var fill: Optional[FillStatus]
    var following: Optional[Follow]
    """How far a sensor is behind what it follows (`orca.follow`)."""
    var usage: UsageSummary
    var learned: Optional[Recommendation]
    """The request its runs' peaks suggest (`orca.recommend`), from its
    `orca config show`, applied where it asks for `auto`."""
    var versions: List[VersionRow]
    """The code it's run, newest first."""
    var configs: List[Shown]
    """What `orca config show <name>` prints (`orca.config`)."""
    var keys: Optional[KeyCounts]
    """A keyed pipeline's counts."""
    var coverage: Optional[Coverage]
    """Its partitions or keys done of those it has (`orca.coverage`)."""
    var not_run: List[NotRun]
    """The snapshots its sensor saw and couldn't run, latest first."""
    var start: Optional[StartPreview]
    """What Start runs would queue, when a preview was asked for."""

    def __init__(
        out self,
        var pipeline: PipelineRow,
        var runs: RunsPage,
        var usage: UsageSummary,
    ):
        self.pipeline = pipeline^
        self.steps = List[StepDef]()
        self.fires = List[Fire]()
        self.runs = runs^
        self.fill = None
        self.following = None
        self.usage = usage^
        self.learned = None
        self.versions = List[VersionRow]()
        self.configs = List[Shown]()
        self.keys = None
        self.coverage = None
        self.not_run = List[NotRun]()
        self.start = None


def pipeline_view(
    mut db: State, ask: PipelineAsk
) raises -> Optional[PipelineView]:
    """The pipeline `ask` names as its page shows it, or None if there's no
    such pipeline."""
    ref name = ask.name
    var ps = pipeline_rows(db, name)
    if len(ps) == 0:
        return None
    var v = PipelineView(
        ps[0].copy(),
        runs_page(db, runs_query(ask.url, "pipeline", name)),
        usage_summary(db, name),
    )
    ref p = v.pipeline
    if ask.preview and ask.partitions.strip():
        v.start = start_preview(
            db, name, ask.partitions, ask.rerun, ask.allow_append
        )
    v.steps = step_defs(db, name)
    try:
        v.fires = next_fires(p, now(), 5)
    except:
        pass
    v.fill = fill_of(db, name)
    v.following = follow(db, name, p.kind, p.start)
    v.versions = version_rows(db, pipeline=name)
    v.configs = shown(db, name)
    if len(v.configs) > 0:
        v.learned = v.configs[0].learned.copy()
    if p.kind == "keyed":
        v.keys = key_counts(db, name)
    # After the keys: their read keeps them in orca.keys, which this counts.
    v.coverage = pipeline_coverage(db, p)
    for x in db.conn.query(
        String(
            "SELECT dataset, snapshot_id, error, ",
            utc_sql("created"),
            " AS at FROM orca.sensed WHERE pipeline = $1 AND error IS NOT",
            " NULL ORDER BY created DESC, dataset LIMIT 10",
        ),
        Params().text(name),
    ):
        v.not_run.append(
            NotRun(
                SnapshotRef(x.text("dataset"), x.int64("snapshot_id")),
                x.text("at"),
                x.text("error"),
            )
        )
    return v^
