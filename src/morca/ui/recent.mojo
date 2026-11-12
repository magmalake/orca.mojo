"""The home page's Recent runs (`orca.views.recent`): a row per batch, with
its counts and how far it has got, linking to `/runs?batch=`, and a run
with no batch as the runs lists show it; refreshed from
`/runs/table?view=home`.
"""

from orca.html import Html, url_encode
from orca.state import State
from orca.ui.layout import IDLE_EVERY, LIVE_EVERY
from orca.ui.parts import (
    run_row_html,
    runs_table_head,
    took_text,
    when_cell,
)
from orca.views.code import runs_code
from orca.views.recent import RecentRow, recent_rows
from orca.views.runs import RunRow, RunsQuery
from orca.web.page import Page


def _live(rows: List[RecentRow]) -> Bool:
    for r in rows:
        if r.live():
            return True
    return False


def _first_key(partitions: String) -> String:
    var parts = partitions.split(",")
    return String(parts[0]) if len(parts) > 0 else String()


def _last_key(partitions: String) -> String:
    var parts = partitions.split(",")
    return String(parts[len(parts) - 1]) if len(parts) > 0 else String()


def _batch_row(mut h: Html, r: RecentRow):
    """A batch's row: its runs (linking to them on `/runs`), pipeline,
    when it was made and how long it has taken, its first and last
    partitions, trigger, and each status's count over a bar of the runs
    that have ended."""
    h.raw("<tr><td>")
    h.chip(String("/runs?batch=", r.batch), String(r.runs, " runs"))
    h.raw('</td><td><a href="')
    h.text(String("/pipelines/", url_encode(r.pipeline)))
    h.raw('">')
    h.text(r.pipeline)
    h.raw("</a></td>")
    when_cell(h, r.created)
    h.num(took_text(r.seconds))
    var span = String(
        _first_key(r.first_partitions), " … ", _last_key(r.last_partitions)
    )
    h.raw('<td class="mono parts" title="')
    h.text(span)
    h.raw('">')
    h.text(span)
    h.raw("</td><td>")
    h.term("trigger", r.trigger)
    h.raw('</td><td class="batch">')
    var shown = 0
    for c in [
        (StaticString("running"), r.running),
        (StaticString("queued"), r.queued),
        (StaticString("succeeded"), r.succeeded),
        (StaticString("failed"), r.failed),
        (StaticString("cancelled"), r.cancelled),
    ]:
        if c[1] == 0:
            continue
        h.raw(" " if shown else "", '<span class="status status--', c[0], '">')
        h.text(c[1], " ", c[0])
        h.raw("</span>")
        shown += 1
    var pct = r.done() * 100 // r.runs if r.runs > 0 else 0
    h.raw(
        '<span class="covpct__bar batch__bar" aria-hidden="true"><span',
        ' style="width:',
        pct,
        '%"></span></span><span class="sr">',
    )
    h.text(r.done(), " of ", r.runs, " ended")
    h.raw('</span></td><td class="num"></td><td class="num"></td></tr>')


def recent_table_html(mut h: Html, mut db: State, rows: List[RecentRow]):
    """The rows as a table of runs: a batch's row (`_batch_row`), or the
    run's own row, its pipeline and failed step showing their code on
    hover (`runs_code`)."""
    if len(rows) == 0:
        h.raw('<p class="empty">No runs.</p>')
        return
    var singles = List[RunRow]()
    for r in rows:
        if r.run:
            singles.append(r.run.value().copy())
    var code = runs_code(db, singles)
    runs_table_head(h, True)
    for r in rows:
        if r.run:
            ref run = r.run.value()
            run_row_html(h, run, True, rc=code.get(run.id))
        else:
            _batch_row(h, r)
    h.raw("</tbody></table></div>")


def recent_list(mut h: Html, mut db: State, rows: List[RecentRow], size: Int):
    """The home page's Recent runs, refreshed from `/runs/table?view=home`:
    every `LIVE_EVERY` seconds while a run in them is queued or running,
    else `IDLE_EVERY`."""
    h.raw('<div data-poll="')
    h.text(RunsQuery.newest("home", size).fragment())
    h.raw(
        '" data-poll-every="',
        LIVE_EVERY if _live(rows) else IDLE_EVERY,
        '">',
    )
    recent_table_html(h, db, rows)
    h.raw("</div>")


def recent_table(mut db: State, size: Int) raises -> Page:
    """`/runs/table?view=home`: the home page's Recent runs as the fragment
    it refreshes from."""
    var rows = recent_rows(db, size)
    var h = Html()
    recent_table_html(h, db, rows)
    return Page(
        200,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
        poll_every=LIVE_EVERY if _live(rows) else IDLE_EVERY,
    )
