"""`/runs/<id>/attempts/<attempt>`, an attempt's page: its log, with its
error, and its facts, which refresh from `/facts` while it runs.
"""

from postgres import Params

from orca.html import Html
from orca.json import quote
from orca.logs import read_location
from orca.state import State
from orca.ui.layout import LIVE_EVERY, act_button, not_found, page_html
from orca.ui.parts import code_link, usage_facts
from orca.usage import bytes_text, usage_json
from orca.views.code import CodeLinks, code_of
from orca.views.runs import AttemptRow, attempt_rows, live_status
from orca.web.page import Page


comptime _LOG_LIMIT = 1 << 20
"""The most of a log a page shows: its last MiB."""


def read_log(path: String) -> Tuple[String, Bool]:
    """The log's text, its last MiB if it's longer, and whether it was cut;
    empty if it can't be read."""
    if not path:
        return (String(), False)
    try:
        var b = read_location(path)
        if len(b) <= _LOG_LIMIT:
            return (String(from_utf8_lossy=Span(b)), False)
        var start = len(b) - _LOG_LIMIT
        return (String(from_utf8_lossy=Span(b)[start:]), True)
    except:
        return (String(), False)


def _log_html(mut h: Html, a: AttemptRow):
    var got = read_log(a.log)
    if got[1]:
        h.raw('<p class="muted">Its last MiB:</p>')
    h.raw('<pre class="log">')
    if got[0]:
        h.text(got[0])
    elif a.log:
        h.raw('<span class="muted">Nothing logged yet.</span>')
    else:
        h.raw('<span class="muted">No log file: ORCA_LOGS wasn\'t set.</span>')
    h.raw("</pre>")


def _attempt_facts(
    mut h: Html,
    mut db: State,
    run: Int,
    a: AttemptRow,
    siblings: List[AttemptRow],
) raises:
    """An attempt's error and facts: its host, times, use, log file, code
    and the step's other attempts."""
    if a.error:
        h.raw('<pre class="error">')
        h.text(a.error)
        h.raw("</pre>")
    h.raw('<dl class="facts"><dt>Host</dt><dd>')
    h.text(a.host)
    h.raw("</dd><dt>Started</dt><dd>")
    h.text(a.started)
    h.raw("</dd><dt>Ended</dt><dd>")
    h.text(a.ended)
    h.raw("</dd>")
    usage_facts(h, a.usage, live_status(a.status))
    h.raw("<dt>Log file</dt><dd><code>")
    h.text(a.log)
    h.raw("</code></dd>")
    var cache = Dict[String, CodeLinks]()
    var at = code_of(db, cache, "", run).step(a.step)
    if at:
        h.raw("<dt>Code</dt><dd>")
        code_link(h, at)
        h.raw("</dd>")
    if len(siblings) > 1:
        h.raw("<dt>Attempts</dt><dd>")
        for i in range(len(siblings)):
            if i:
                h.raw(" · ")
            ref s = siblings[i]
            if s.id == a.id:
                h.text(s.attempt)
            else:
                h.link(
                    String("/runs/", run, "/attempts/", s.id), String(s.attempt)
                )
            h.raw(" ")
            h.status(s.status)
            if s.usage.peak_rss_bytes >= 0:
                h.raw(' <span class="muted">')
                h.text(bytes_text(s.usage.peak_rss_bytes))
                h.raw("</span>")
        h.raw("</dd>")
    h.raw("</dl>")


def attempt_facts(mut db: State, run: Int, id: Int) raises -> Page:
    """An attempt's error and facts, as a fragment; 286 once it's ended."""
    var found = attempt_rows(db, run, "a.id = $2", id)
    if len(found) == 0:
        return not_found(String("no attempt ", id, " in run ", run))
    var siblings = attempt_rows(
        db,
        run,
        "a.step = (SELECT step FROM orca.attempts WHERE id = $2)",
        id,
    )
    var h = Html()
    _attempt_facts(h, db, run, found[0], siblings)
    return Page(
        200 if live_status(found[0].status) else 286,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
    )


def attempt(mut db: State, run: Int, id: Int) raises -> Page:
    var found = attempt_rows(db, run, "a.id = $2", id)
    if len(found) == 0:
        return not_found(String("no attempt ", id, " in run ", run))
    ref a = found[0]
    var siblings = attempt_rows(
        db,
        run,
        "a.step = (SELECT step FROM orca.attempts WHERE id = $2)",
        id,
    )
    var log = read_log(a.log)

    var j = String(
        '{"attempt": {"id": ',
        a.id,
        ', "run": ',
        a.run,
        ', "step": ',
        quote(a.step),
        ', "partition": ',
        quote(a.partition),
        ', "attempt": ',
        a.attempt,
        ', "status": ',
        quote(a.status),
        ', "host": ',
        quote(a.host),
        ', "started": ',
        quote(a.started),
        ', "ended": ',
        quote(a.ended),
        ', "error": ',
        quote(a.error),
        ', "log": ',
        quote(a.log),
        ', "usage": ',
        usage_json(a.usage),
        '}, "text": ',
        quote(log[0]),
        ', "truncated": ',
        "true" if log[1] else "false",
        "}",
    )

    var h = Html()
    h.raw('<p class="crumb"><a href="/runs">Runs</a> · ')
    h.link(String("/runs/", a.run), String("run ", a.run))
    h.raw("</p><h1>")
    h.text(a.step)
    if a.partition:
        h.text(" ", a.partition)
    h.text(", attempt ", a.attempt, " ")
    h.status(a.status)
    # A new run from this step on, once the run has ended, as the run
    # page's "Rerun from" does.
    var ended = db.conn.query(
        (
            "SELECT status NOT IN ('queued', 'running') AS ended FROM orca.runs"
            " WHERE id = $1"
        ),
        Params().int64(Int64(run)),
    )
    if len(ended) > 0 and ended.row(0).bool("ended"):
        act_button(
            h,
            String("/runs/", run, "/rerun"),
            "Rerun from here",
            "rerun from here",
            String(
                "no orca command for this yet: a new run from ",
                a.step,
                " on, the steps before it copied",
            ),
            danger=True,
            step=a.step,
        )
    h.raw("</h1>")
    # What changes while it runs, refreshed until it ends.
    if live_status(a.status):
        h.raw('<div data-poll="/runs/')
        h.text(run, "/attempts/", a.id, "/facts")
        h.raw('" data-poll-every="', LIVE_EVERY, '">')
    else:
        h.raw("<div>")
    _attempt_facts(h, db, run, a, siblings)
    h.raw("</div><h2>Log</h2>")
    if live_status(a.status):
        h.raw('<div data-poll="/runs/')
        h.text(run, "/attempts/", a.id, "/log")
        h.raw('">')
    else:
        h.raw("<div>")
    _log_html(h, a)
    h.raw("</div>")
    var title = String(a.step, " ", a.partition, " attempt ", a.attempt)
    return Page(200, j^, page_html(title, "/runs", h))


def attempt_log(mut db: State, run: Int, id: Int) raises -> Page:
    """An attempt's log, as a fragment; 286 once the attempt has ended."""
    var found = attempt_rows(db, run, "a.id = $2", id)
    if len(found) == 0:
        return not_found(String("no attempt ", id, " in run ", run))
    var h = Html()
    _log_html(h, found[0])
    return Page(
        200 if live_status(found[0].status) else 286,
        String(),
        h^.finish(),
        "text/html; charset=utf-8",
    )
