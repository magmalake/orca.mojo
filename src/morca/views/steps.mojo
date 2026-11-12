"""What the step cards show (`orca.ui.step_cards`): each step's card,
with what its attempts in the pipeline's latest runs did, and on a run's
page each attempt's.
"""

from postgres import Params

from orca.sample import fixed1
from orca.state import State
from orca.ui.step_cards import AttemptCard, StepCard, duration_text
from orca.ui.terms import value_help
from orca.usage import bytes_text, cpu_text
from orca.views.code import CodeLinks
from orca.views.pipelines import StepDef
from orca.views.runs import int_or_zero, usage_columns, utc_sql


comptime STEP_RUNS = 20
"""How many of a pipeline's latest runs a step's card summarizes."""


def step_cards_of(
    mut db: State,
    pipeline: String,
    defs: List[StepDef],
    links: CodeLinks,
    recent: Bool,
) raises -> List[StepCard]:
    """Each step's card (`orca.ui.step_cards`); with `recent`, what its
    attempts in the pipeline's latest `STEP_RUNS` runs did: how many failed,
    the p50 and most its succeeded ones took, and its peak memory's."""
    var got = Dict[String, StepCard]()
    var out = List[StepCard]()
    for d in defs:
        var at = links.step(d.name)
        out.append(
            StepCard(
                d.name,
                d.kind,
                String(value_help("kind", d.kind)),
                d.after,
                d.function,
                d.input,
                d.output,
                d.reads.copy(),
                d.writes.copy(),
                at.url,
                at.at,
                at.commit,
                0,
                0,
                0,
                "",
                "",
            )
        )
    if not recent:
        return out^
    var rows = db.conn.query(
        (
            "SELECT s.step, count(DISTINCT s.run) AS runs, count(*) AS"
            " attempts, count(*) FILTER (WHERE a.status = 'failed') AS failed,"
            " coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY"
            " extract(epoch FROM a.ended - a.started)::float8) FILTER (WHERE"
            " a.status = 'succeeded' AND a.ended IS NOT NULL), -1) AS"
            " took_p50, coalesce(max(extract(epoch FROM a.ended -"
            " a.started)::float8) FILTER (WHERE a.status = 'succeeded'), -1)"
            " AS took_max, coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER"
            " BY a.peak_rss_bytes), -1) AS peak_p50,"
            " coalesce(max(a.peak_rss_bytes), -1)::float8 AS peak_max FROM"
            " orca.steps s JOIN orca.attempts a ON a.step = s.id WHERE s.run"
            " IN (SELECT id FROM orca.runs WHERE pipeline = $1 ORDER BY id"
            " DESC LIMIT $2) GROUP BY s.step"
        ),
        Params().text(pipeline).int64(Int64(STEP_RUNS)),
    )
    for r in rows:
        var name = r.text("step")
        for ref c in out:
            if c.name != name:
                continue
            c.recent_runs = Int(r.int64("runs"))
            c.recent_attempts = Int(r.int64("attempts"))
            c.recent_failed = Int(r.int64("failed"))
            var t50 = r.float64("took_p50")
            var tmax = r.float64("took_max")
            if tmax >= 0:
                c.took = String(
                    "p50 ", duration_text(t50), " · max ", duration_text(tmax)
                )
            var p50 = r.float64("peak_p50")
            var pmax = r.float64("peak_max")
            if pmax >= 0:
                c.peak = String(
                    "p50 ",
                    bytes_text(Int(p50)),
                    " · max ",
                    bytes_text(Int(pmax)),
                )
    return out^


def attempt_cards(mut db: State, run: Int) raises -> List[AttemptCard]:
    """What each of a run's attempts adds to its step's card."""
    var rows = db.conn.query(
        String(
            "SELECT a.id::text AS id, s.step, s.partition, a.attempt::text AS",
            " attempt, a.status, ",
            utc_sql("a.started"),
            " AS started, ",
            utc_sql("a.ended"),
            " AS ended, coalesce(extract(epoch FROM coalesce(a.ended, now())",
            " - a.started)::float8, -1) AS took, coalesce(a.error, '') AS",
            " error, coalesce(a.peak_rss_bytes, -1) AS peak,",
            " coalesce(a.cpu_seconds, -1) AS cpu, coalesce(a.peak_cpus, -1)",
            " AS cpus FROM orca.attempts a JOIN orca.steps s ON s.id = a.step",
            " WHERE s.run = $1 ORDER BY s.ord, a.started",
        ),
        Params().int64(Int64(run)),
    )
    var out = List[AttemptCard]()
    for r in rows:
        var u = usage_columns(r)
        var id = int_or_zero(r.text("id"))
        var status = r.text("status")
        out.append(
            AttemptCard(
                id,
                r.text("step"),
                r.text("partition"),
                int_or_zero(r.text("attempt")),
                status,
                String(value_help("status", status)),
                r.text("started"),
                r.text("ended"),
                duration_text(r.float64("took")),
                bytes_text(u.peak_rss_bytes) if u.peak_rss_bytes >= 0 else "",
                cpu_text(u.cpu_seconds) if u.cpu_seconds >= 0 else "",
                fixed1(u.peak_cpus) if u.peak_cpus >= 0 else "",
                r.text("error"),
                String("/runs/", run, "/attempts/", id),
            )
        )
    return out^
