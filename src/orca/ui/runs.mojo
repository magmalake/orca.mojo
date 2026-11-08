"""`/runs`, the runs list: newest first, filtered by pipeline, status
and trigger, a page at a time (`orca.views.runs.RunsPage`); `/runs/table`
is a list's page as the fragment it refreshes from, for `/runs`, a
pipeline's Runs tab and the home page.
"""

from orca.api.runs import json_runs, json_runs_page
from orca.html import Html, query_param
from orca.json import quote
from orca.state import State
from orca.ui.layout import IDLE_EVERY, LIVE_EVERY, page_html
from orca.ui.parts import runs_table_html
from orca.views.runs import (
    RUNS_PAGE,
    RunsPage,
    RunsQuery,
    runs_page,
    runs_query,
)
from orca.web.page import Page


comptime _RUN_STATUSES: List[String] = [
    "queued",
    "running",
    "deferred",
    "succeeded",
    "failed",
    "cancelled",
]
"""The statuses a run takes (`orca.runs.status`), in the order a run goes
through them; a deferred run waits for its `retry_at`."""


comptime _RUN_TRIGGERS: List[String] = [
    "schedule",
    "sensor",
    "backfill",
    "manual",
    "ui",
    "rerun",
    "rebuild",
    "fill",
]
"""The triggers orca writes (`orca.runs.trigger`): the scheduler's fires and
sensors, `backfill`, a binary's `run`, the UI's start button, a rerun, a
rebuild and a fill (`orca.fill`). The filter adds any others the table has."""


comptime _SUBMIT_ON_CHANGE = (
    '<script>document.querySelector("form.filters").addEventListener('
    '"change", (e) => e.currentTarget.submit());</script>'
)
"""Filtering as soon as a choice changes; the Filter button does the same
without JavaScript."""


def _grouped(n: Int) -> String:
    """`n` with commas between thousands: 1,234."""
    var digits = String(n if n >= 0 else -n)
    var out = String("-" if n < 0 else "")
    var first = digits.byte_length() % 3
    if first == 0:
        first = 3
    out.write(digits[byte=:first])
    var i = first
    while i < digits.byte_length():
        out.write(",", digits[byte = i : i + 3])
        i += 3
    return out^


def pager_link(mut h: Html, rel: StaticString, href: String, label: String):
    h.raw("<a")
    if rel:
        h.raw(' rel="', rel, '"')
    h.raw(' href="')
    h.text(href)
    h.raw('">', label, "</a>")


def _runs_pager(mut h: Html, p: RunsPage):
    """Newest, Newer and Older, and where the page is among the runs that
    match; nothing if it's all of them."""
    var newer = p.newer_href()
    var older = p.older_href()
    if not newer and not older:
        return
    h.raw('<nav class="pager" aria-label="Pages of runs">')
    if p.newer > p.q.size and len(p.rows) > 0:
        pager_link(h, "", p.q.href(), "« Newest")
    if newer:
        pager_link(h, "prev", newer, "← Newer")
    h.raw('<span class="pager__at">')
    if len(p.rows) > 0:
        h.text(
            _grouped(p.newer + 1),
            "–",
            _grouped(p.newer + len(p.rows)),
            " of ",
            _grouped(p.total),
        )
    else:
        h.text("none here, of ", _grouped(p.total))
    h.raw("</span>")
    if older:
        pager_link(h, "next", older, "Older →")
    h.raw("</nav>")


def _runs_list_html(mut h: Html, p: RunsPage):
    """A runs table and its pager: what `/runs/table` answers."""
    runs_table_html(h, p.rows, p.q.view != "pipeline")
    if p.q.view != "home":
        _runs_pager(h, p)


def runs_list(mut h: Html, p: RunsPage):
    """A runs table and its pager, refreshed from `/runs/table` while a run
    on it is queued or running, and slowly while it's the newest page, so
    new runs join it; an older page with nothing going on stays as it is."""
    var live = p.live()
    if live or p.q.first():
        h.raw('<div data-poll="')
        h.text(p.q.fragment())
        h.raw(
            '" data-poll-every="',
            LIVE_EVERY if live else IDLE_EVERY,
            '">',
        )
    else:
        h.raw("<div>")
    _runs_list_html(h, p)
    h.raw("</div>")


def runs_fragment_query(url: String) -> RunsQuery:
    """The list a `/runs/table` URL names."""
    return runs_query(
        url, query_param(url, "view"), query_param(url, "pipeline")
    )


def runs_table(mut db: State, url: String) raises -> Page:
    """`/runs/table`: a runs list's page (`?view=list`, `pipeline` or
    `home`, its filters, `limit` and cursor) as an HTML fragment. While it
    has a run going on it says to ask again in `LIVE_EVERY` seconds; else
    the newest page says `IDLE_EVERY`, and an older one answers 286."""
    var p = runs_page(db, runs_fragment_query(url))
    var h = Html()
    _runs_list_html(h, p)
    var live = p.live()
    return Page(
        200 if live or p.q.first() else 286,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
        poll_every=LIVE_EVERY if live else IDLE_EVERY,
    )


def _select(
    mut h: Html, name: StaticString, options: List[String], current: String
):
    """A labelled `<select>` of `options` after "any", `current` selected;
    `current` is added if it isn't one of them, so the page says what it
    filters by."""
    h.raw("<label>", name, ' <select name="', name, '">')
    h.raw(
        '<option value=""', " selected" if not current else "", ">any</option>"
    )
    var found = False
    for o in options:
        h.raw('<option value="')
        h.text(o)
        h.raw('"')
        if o == current:
            h.raw(" selected")
            found = True
        h.raw(">")
        h.text(o)
        h.raw("</option>")
    if current and not found:
        h.raw('<option value="')
        h.text(current)
        h.raw('" selected>')
        h.text(current)
        h.raw("</option>")
    h.raw("</select></label>")


def _with_seen(
    known: List[String], mut db: State, sql: String
) raises -> List[String]:
    """`known`, then the values `sql` returns (as `v`) that aren't in it."""
    var out = known.copy()
    for r in db.conn.query(sql):
        var v = r.text("v")
        if v and v not in out:
            out.append(v)
    return out^


def runs(mut db: State, var q: RunsQuery) raises -> Page:
    """`/runs`: a page of runs newest first, filtered, by id under
    `q.before` or just above `q.after` (`runs_page`); with how many match
    and links to the newest, newer and older pages, and refreshed while it
    can change (`runs_list`)."""
    var p = runs_page(db, q^)
    var j = String('{"runs": ')
    json_runs(j, p.rows)
    j.write(
        ', "filters": {"pipeline": ',
        quote(p.q.pipeline),
        ', "status": ',
        quote(p.q.status),
        ', "trigger": ',
        quote(p.q.trigger),
        "}, ",
    )
    json_runs_page(j, p)
    j.write("}")

    var pipelines = List[String]()
    for r in db.conn.query("SELECT name FROM orca.pipelines ORDER BY name"):
        pipelines.append(r.text("name"))
    var triggers = _with_seen(
        materialize[_RUN_TRIGGERS](),
        db,
        "SELECT DISTINCT trigger AS v FROM orca.runs ORDER BY v",
    )

    var h = Html()
    h.raw("<h1>Runs</h1>")
    h.raw('<form class="filters" method="get" action="/runs">')
    _select(h, "pipeline", pipelines, p.q.pipeline)
    _select(h, "status", materialize[_RUN_STATUSES](), p.q.status)
    _select(h, "trigger", triggers, p.q.trigger)
    if p.q.size != RUNS_PAGE:
        h.raw('<input type="hidden" name="limit" value="', p.q.size, '">')
    h.raw('<button type="submit">Filter</button></form>', _SUBMIT_ON_CHANGE)
    runs_list(h, p)
    return Page(200, j^, page_html("Runs", "/runs", h))
