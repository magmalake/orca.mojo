"""Requests learned from what a pipeline's runs used (#269, part 1): shown,
applied nowhere.

From the peaks of a pipeline's last `LEARN_RUNS` successful measured runs
(`orca.attempts.peak_rss_bytes` and `peak_cpus`, #200), `recommend` works out
the request they suggest:

- **memory** is the `MEMORY_PERCENTILE` of the runs' peak memory times
  `MEMORY_MARGIN`, rounded up to `MEMORY_STEP_MB`, and never below what an
  out-of-memory kill raised the pipeline to (`orca.pipelines.memory_raised_mb`,
  `orca.cgroup`);
- **CPUs** are the `CPU_PERCENTILE` of the runs' peak cores, rounded up, at
  least 1; none where peak cores aren't measured (macOS);
- each is clamped to the pipeline's declared range (`cpus`, `memory_gb`),
  saying so;
- with fewer than `MIN_RUNS` runs there's none.

This is the simplest rule that can work: a percentile over a fixed window,
over all of a pipeline's runs alike (fill, manual or scheduled), with no
decay. The Resources card and `orca config show` show it beside the declared
request; admission and grants don't read it yet. `"auto"` requests, which
would, are part 2.
"""

from std.math import ceil

from postgres import Params

from orca.json import quote
from orca.resources import Request
from orca.state import State

comptime LEARN_RUNS = 20
"""How many of the latest runs it learns from: the window the Resources card
summarizes (`orca.views.pipelines.USAGE_RUNS`)."""
comptime MIN_RUNS = 3
"""Fewer runs than this and one odd run would be the whole percentile."""
comptime MEMORY_PERCENTILE: Float64 = 0.95
"""High enough that a run larger than most still fits, without taking the
one largest run as the rule."""
comptime MEMORY_MARGIN: Float64 = 1.15
"""Headroom over the percentile for a run a little larger than any seen,
under the 25% between a run's limit and its kill (`ORCA_RUN_MEMORY_MARGIN`)."""
comptime MEMORY_STEP_MB = 256
"""Rounded up to a quarter GB, which `memory_gb` writes exactly, so the
number doesn't move with every run."""
comptime CPU_PERCENTILE: Float64 = 0.90
"""CPU runs short of cores slow down instead of dying, so a lower percentile
than memory's."""
comptime CPU_SLACK: Float64 = 0.1
"""Peak cores a sample's second over-counts by: 2.05 cores is two busy
ones, not three."""


@fieldwise_init
struct RunPeaks(Copyable, Movable):
    """One run's peaks: its attempts' largest memory, in bytes, and peak
    cores; -1 for what wasn't measured."""

    var peak_rss_bytes: Float64
    var peak_cpus: Float64


@fieldwise_init
struct Recommendation(Copyable, Movable):
    """What a pipeline's runs suggest it asks for. `memory_mb` and `cpus` are
    0 where there's none."""

    var runs: Int
    """How many runs' peak memory it's from."""
    var cpu_runs: Int
    """How many of them measured peak cores."""
    var memory_mb: Int
    var cpus: Int
    var source: String
    """What it is: the rule, or why there's none."""
    var clamped: String
    """Which of the declared range's ends it was clamped to, or empty."""

    def any(self) -> Bool:
        return self.memory_mb > 0 or self.cpus > 0

    def request_text(self) -> String:
        """`3.25 GB, 2 CPUs`, as `memory_gb` and `cpus` would say it."""
        var out = String()
        if self.memory_mb > 0:
            out.write(memory_gb_text(self.memory_mb), " GB")
        if self.cpus > 0:
            out.write(", " if self.memory_mb > 0 else "", self.cpus)
            out.write(" CPU" if self.cpus == 1 else " CPUs")
        return out^

    def text(self) -> String:
        """`learned from 20 runs: 3.25 GB, 2 CPUs`, with what clamped it; or
        why there's none."""
        if not self.any():
            return self.source
        var out = String(
            "learned from ",
            self.runs,
            " run" if self.runs == 1 else " runs",
            ": ",
            self.request_text(),
        )
        if self.clamped:
            out.write(" (clamped to ", self.clamped, ")")
        return out^

    def json(self) -> String:
        """`{"runs": n, "memory_mb": .., "cpus": .., ..., "applied": false}`,
        null for what there's none of."""
        var out = String(
            '{"runs": ', self.runs, ', "cpu_runs": ', self.cpu_runs
        )
        out.write(', "memory_mb": ')
        if self.memory_mb > 0:
            out.write(self.memory_mb)
        else:
            out.write("null")
        out.write(', "cpus": ')
        if self.cpus > 0:
            out.write(self.cpus)
        else:
            out.write("null")
        out.write(', "text": ', quote(self.text()))
        out.write(', "source": ', quote(self.source), ', "clamped": ')
        out.write(quote(self.clamped) if self.clamped else String("null"))
        out.write(', "applied": false}')
        return out^


def memory_gb_text(mb: Int) -> String:
    """`mb` as GB to the hundredth, as `memory_gb` takes it: `3.25`, `3.5`,
    `4`."""
    var hundredths = (max(mb, 0) * 100 + 512) // 1024
    var out = String(hundredths // 100)
    var frac = hundredths % 100
    if frac:
        out.write(".", frac // 10)
        if frac % 10:
            out.write(frac % 10)
    return out^


def percentile(var xs: List[Float64], p: Float64) -> Float64:
    """The nearest-rank `p` percentile of `xs`: the smallest value at least
    `p` of them are at or below, so always one of them. -1 for none."""
    if len(xs) == 0:
        return -1
    sort(xs)
    # Less a hair, so 0.95 × 20 (18.999... or 19.000...1) is rank 19.
    var rank = Int(ceil(p * Float64(len(xs)) - 1e-9))
    return xs[min(max(rank, 1), len(xs)) - 1]


def recommend(
    peaks: List[RunPeaks], raised_mb: Int, request: Request
) -> Recommendation:
    """The request `peaks` (the latest runs first, at most `LEARN_RUNS`)
    suggest, at least `raised_mb` (what an out-of-memory raised the pipeline
    to, 0 for nothing) and within `request`'s range."""
    var mem = List[Float64]()
    var cores = List[Float64]()
    for p in peaks:
        if p.peak_rss_bytes >= 0:
            mem.append(p.peak_rss_bytes)
            if p.peak_cpus >= 0:
                cores.append(p.peak_cpus)
    var r = Recommendation(len(mem), len(cores), 0, 0, "", "")
    if len(mem) < MIN_RUNS:
        r.source = String(
            "not learned yet: ",
            len(mem),
            " of the ",
            MIN_RUNS,
            " successful measured runs it learns from",
        )
        return r^
    var mb = percentile(mem^, MEMORY_PERCENTILE) / 1048576.0 * MEMORY_MARGIN
    r.memory_mb = Int(ceil(mb / Float64(MEMORY_STEP_MB))) * MEMORY_STEP_MB
    r.memory_mb = max(r.memory_mb, MEMORY_STEP_MB)
    r.source = String(
        "p",
        Int(MEMORY_PERCENTILE * 100 + 0.5),
        " of the runs' peak memory × ",
        MEMORY_MARGIN,
    )
    if raised_mb > r.memory_mb:
        r.memory_mb = raised_mb
        r.source = String("what an out-of-memory raised it to")
    if len(cores) >= MIN_RUNS:
        var c = percentile(cores^, CPU_PERCENTILE)
        r.cpus = max(Int(ceil(c - CPU_SLACK)), 1)
        r.source.write(
            "; p", Int(CPU_PERCENTILE * 100 + 0.5), " of their peak cores"
        )
    var clamped = List[String]()
    if request.memory_max_mb > 0:
        if r.memory_mb > request.memory_max_mb:
            r.memory_mb = request.memory_max_mb
            clamped.append("memory_gb's maximum")
        elif r.memory_mb < request.memory_min_mb:
            r.memory_mb = request.memory_min_mb
            clamped.append("memory_gb's minimum")
    if request.cpus_max > 0 and r.cpus > 0:
        if r.cpus > request.cpus_max:
            r.cpus = request.cpus_max
            clamped.append("cpus' maximum")
        elif r.cpus < request.cpus_min:
            r.cpus = request.cpus_min
            clamped.append("cpus' minimum")
    for i in range(len(clamped)):
        r.clamped.write(" and " if i else "", clamped[i])
    return r^


def learned(
    mut db: State, name: String, cpus: String, memory_gb: String
) raises -> Recommendation:
    """What pipeline `name`'s latest successful measured runs suggest it
    asks for, within its declared `cpus` and `memory_gb` (as registered,
    each empty for none)."""
    var peaks = List[RunPeaks]()
    for r in db.conn.query(
        (
            "SELECT max(a.peak_rss_bytes)::float8 AS peak,"
            " coalesce(max(a.peak_cpus), -1)::float8 AS cpus FROM orca.runs r"
            " JOIN orca.steps s ON s.run = r.id JOIN orca.attempts a ON a.step"
            " = s.id WHERE r.pipeline = $1 AND r.status = 'succeeded' GROUP BY"
            " r.id HAVING max(a.peak_rss_bytes) IS NOT NULL ORDER BY r.id DESC"
            " LIMIT $2"
        ),
        Params().text(name).int64(Int64(LEARN_RUNS)),
    ):
        peaks.append(RunPeaks(r.float64("peak"), r.float64("cpus")))
    var raised = db.conn.query(
        (
            "SELECT coalesce(memory_raised_mb, 0) AS raised FROM"
            " orca.pipelines WHERE name = $1"
        ),
        Params().text(name),
    )
    var raised_mb = Int(raised.row(0).int64("raised")) if len(raised) else 0
    return recommend(peaks, raised_mb, Request.of(cpus, memory_gb))
