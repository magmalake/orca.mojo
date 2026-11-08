"""`/`, the home page: the pipelines, with their coverage
(`orca.coverage`), schedules, next and last runs, the recent runs, and
the Timeline tab (`orca.ui.viz`);
`/home/pipelines` is its table of pipelines, as the fragment it refreshes
from.
"""

from postgres import Params

from orca.api.pipelines import json_pipeline
from orca.api.runs import json_runs
from orca.chart import DayRuns, runs_chart
from orca.coverage import Coverage
from orca.fleet import fleet_summary, summary_html, summary_json
from orca.html import Html, url_encode
from orca.json import quote
from orca.state import State
from orca.system import alert_html, system_now, warnings, warnings_json
from orca.ui.coverage import coverage_href
from orca.ui.layout import IDLE_EVERY, LIVE_EVERY, page_html
from orca.ui.parts import pause_button
from orca.ui.runs import runs_list
from orca.ui.viz import gantt_panel, gantt_tabs
from orca.views.pipelines import PipelineRow, pipeline_coverage, pipeline_rows
from orca.views.runs import HOME_RUNS, RunsQuery, runs_page
from orca.watchdog import active_trips, banner_html, trips_json
from orca.web.page import Page


def _runs_per_day(mut db: State, days: Int) raises -> List[DayRuns]:
    """Runs by the UTC day they started (or were made, if they haven't),
    for the last `days` days up to today."""
    var out = List[DayRuns]()
    for r in db.conn.query(
        (
            "SELECT d::date::text AS day, count(r.id) FILTER (WHERE r.status ="
            " 'succeeded') AS ok, count(r.id) FILTER (WHERE r.status ="
            " 'failed') AS bad, count(r.id) FILTER (WHERE r.status NOT IN"
            " ('succeeded', 'failed')) AS other FROM generate_series((now() AT"
            " TIME ZONE 'UTC')::date - ($1::int - 1), (now() AT TIME ZONE"
            " 'UTC')::date, interval '1 day') AS d LEFT JOIN orca.runs r ON"
            " (coalesce(r.started, r.created) AT TIME ZONE 'UTC')::date ="
            " d::date GROUP BY d ORDER BY d"
        ),
        Params().int64(Int64(days)),
    ):
        out.append(
            DayRuns(
                r.text("day"),
                Int(r.int64("ok")),
                Int(r.int64("bad")),
                Int(r.int64("other")),
            )
        )
    return out^


def _pipelines_live(ps: List[PipelineRow]) -> Bool:
    """Whether any of these pipelines has a run queued or running."""
    for p in ps:
        if p.running > 0 or p.queued > 0:
            return True
    return False


def _coverages(
    mut db: State, ps: List[PipelineRow]
) -> List[Optional[Coverage]]:
    """Each pipeline's coverage (`orca.coverage`): a query each."""
    var out = List[Optional[Coverage]]()
    for p in ps:
        out.append(pipeline_coverage(db, p))
    return out^


def _coverage_cell(mut h: Html, p: PipelineRow, c: Optional[Coverage]):
    """A Coverage cell: the percent with a bar, the counts on hover, linking
    to the year calendar (a keyed pipeline's Keys); "—" for none."""
    if not c:
        h.raw('<td class="num muted">—</td>')
        return
    ref v = c.value()
    h.raw('<td class="num"><a class="covpct" href="')
    h.text(coverage_href(p.name, p.kind))
    h.raw('" title="')
    h.text(v.text())
    h.raw('">')
    h.text(v.pct(), "%")
    h.raw('<span class="covpct__bar" aria-hidden="true"><span style="width:')
    h.raw(String(v.pct()), '%"></span></span><span class="sr">')
    h.text(", ", v.done, " of ", v.total, " ", v.units(), " done")
    h.raw("</span></a></td>")


def _home_pipelines(
    mut h: Html, ps: List[PipelineRow], covs: List[Optional[Coverage]]
):
    """The home page's table of pipelines: partitions and coverage,
    schedules, next and last runs."""
    h.raw(
        '<section class="card"><div class="scroll"><table><thead><tr>',
        "<th>Pipeline</th><th>Partitions</th>",
        '<th class="num">Coverage</th><th>Schedule</th>',
        "<th>Next run</th><th>Running</th><th>Queued</th><th>Last run</th>",
        "</tr></thead><tbody>",
    )
    for i in range(len(ps)):
        ref p = ps[i]
        h.raw("<tr><td>")
        h.link(String("/pipelines/", url_encode(p.name)), p.name)
        if p.paused:
            h.raw(" ")
            h.status("paused")
        pause_button(h, p.name, p.paused)
        h.raw("</td>")
        h.cell(p.kind, " from ", p.start if p.start else String("the start"))
        _coverage_cell(h, p, covs[i])
        if p.cron:
            h.raw("<td><code>")
            h.text(p.cron)
            h.raw("</code> ")
            h.text(p.tz)
            h.raw("</td>")
            h.mono(p.next_fire, " → ", p.next_partition)
        else:
            h.raw('<td class="muted">none</td><td></td>')
        # The counts as badges, lit while they're not zero.
        h.raw(
            '<td><span class="badge',
            " badge--running" if p.running > 0 else "",
            '">',
        )
        h.text(p.running, " / ", p.max_runs)
        h.raw(
            '</span></td><td><span class="badge',
            " badge--queued" if p.queued > 0 else "",
            '">',
        )
        h.text(p.queued)
        h.raw("</span></td><td>")
        if p.last_run:
            h.chip(String("/runs/", p.last_run), String(p.last_run))
            h.raw(" ")
            h.status(p.last_status)
            h.raw(' <span class="mono muted">')
            h.text(p.last_partitions)
            h.raw("</span>")
        h.raw("</td></tr>")
    h.raw("</tbody></table></div></section>")


def home_pipelines(mut db: State) raises -> Page:
    """`/home/pipelines`: the home page's table of pipelines, as the
    fragment it refreshes from; ask again in `LIVE_EVERY` seconds while a
    run is queued or running, else `IDLE_EVERY`."""
    var ps = pipeline_rows(db)
    var h = Html()
    _home_pipelines(h, ps, _coverages(db, ps))
    return Page(
        200,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
        poll_every=LIVE_EVERY if _pipelines_live(ps) else IDLE_EVERY,
    )


def home(mut db: State) raises -> Page:
    var ps = pipeline_rows(db)
    var covs = _coverages(db, ps)
    var recent = runs_page(db, RunsQuery.newest("home", HOME_RUNS))
    var per_day = _runs_per_day(db, 30)
    var fleet = fleet_summary(db)
    var alerts = warnings(system_now(db))
    var trips = active_trips(db)
    var j = String(
        '{"warnings": ',
        warnings_json(alerts),
        ', "watchdog": ',
        trips_json(trips),
        ', "pipelines": [',
    )
    for i in range(len(ps)):
        if i:
            j.write(", ")
        json_pipeline(j, ps[i], covs[i])
    j.write('], "runs": ')
    json_runs(j, recent.rows)
    j.write(', "runs_per_day": [')
    for i in range(len(per_day)):
        ref d = per_day[i]
        j.write(
            ", " if i else "",
            '{"day": ',
            quote(d.day),
            ', "succeeded": ',
            d.succeeded,
            ', "failed": ',
            d.failed,
            ', "other": ',
            d.other,
            "}",
        )
    j.write('], "fleet": ', summary_json(fleet), "}")

    var h = Html()
    h.raw("<h1>Pipelines</h1>")
    banner_html(h, trips)
    alert_html(h, alerts)
    summary_html(h, fleet, True)
    if len(ps) == 0:
        h.raw(
            '<p class="empty">No pipelines yet: <code>orca register',
            " &lt;binary&gt;</code> adds one.</p>",
        )
    else:
        # List and Timeline tabs over the table (orca.ui.viz).
        gantt_tabs(h)
        # Refreshed while the page is open: often while a run is going on.
        var live = _pipelines_live(ps)
        h.raw(
            '<div data-poll="/home/pipelines" data-poll-every="',
            LIVE_EVERY if live else IDLE_EVERY,
            '">',
        )
        _home_pipelines(h, ps, covs)
        h.raw("</div>")
        gantt_panel(h)
    h.raw(
        '<section class="card"><h2>Runs per day</h2>',
        '<p class="muted">The last 30 days, by the day each run started',
        " (UTC).</p>",
        '<div class="scroll">',
        runs_chart(per_day),
        "</div></section>",
    )
    h.raw('<section class="card"><h2>Recent runs</h2>')
    runs_list(h, recent)
    h.raw('<p class="more"><a href="/runs">All runs</a></p></section>')
    return Page(200, j^, page_html("Pipelines", "/", h))
