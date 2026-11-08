"""The Ops pages: Control (`/ops/control`, the machine running the
scheduler, the UI and Postgres; `orca.system`), Fleet (`/ops/fleet`, the
EC2 instances orca pays for; `orca.fleet`), Workers (`/ops/workers`;
`orca.workers`), the watchdog (`/watchdog`; `orca.watchdog`) and
Prometheus' `/metrics`, with the fragments each refreshes from.
"""

from std.os import getenv

from orca.admission import (
    pressure_html,
    pressure_prometheus,
    pressure_rows,
    row_json,
    scheduler_pressure,
)
from orca.fleet import (
    charts_html,
    charts_json,
    fleet_charts,
    fleet_hosts,
    fleet_summary,
    hosts_fragment,
    hosts_json,
    sort_hosts,
    summary_json,
)
from orca.html import Html
from orca.notify import (
    configured as notify_configured,
    notify_html,
    notify_json,
    recent as recent_alerts,
)
from orca.state import State
from orca.system import (
    prometheus,
    range_hours,
    system_charts,
    system_html,
    system_json,
    system_now,
)
from orca.ui.layout import page_html
from orca.watchdog import (
    active_trips,
    banner_html,
    config as watchdog_config,
    prometheus as watchdog_prometheus,
    rule_rows,
    trips_json,
    watchdog_html,
    watchdog_json,
)
from orca.web.page import Page
from orca.workers import worker_rows, workers_html, workers_json


def _fleet_sort(sort: String) -> String:
    """`sort` if it names a column of the hosts table, else the default."""
    for c in [
        "role",
        "type",
        "run",
        "uptime",
        "cpu",
        "busiest",
        "mem",
        "net",
        "disk",
        "state",
    ]:
        if sort == c:
            return sort
    return String()


def fleet_page(
    mut db: State, sort: String, dir: String, range: String
) raises -> Page:
    """The machines orca is paying for, and what they have been doing."""
    var col = _fleet_sort(sort)
    var desc = dir == "desc"
    var span = String("7d") if range == "7d" else String("24h")
    var summary = fleet_summary(db)
    var hosts = fleet_hosts(db)
    sort_hosts(hosts, col, desc)
    var charts = fleet_charts(db, 168 if span == "7d" else 24)
    var trips = active_trips(db)
    var j = String(
        '{"watchdog": ',
        trips_json(trips),
        ', "summary": ',
        summary_json(summary),
        ', "hosts": ',
        hosts_json(hosts),
        ', "charts": ',
        charts_json(charts),
        "}",
    )
    var h = Html()
    h.raw("<h1>Fleet</h1>")
    h.raw(
        '<p class="muted">The EC2 instances orca launches for runs: head and',
        " worker hosts, their uptime, load and network. The one machine that",
        ' runs the scheduler, this UI and Postgres is <a href="/ops/control">',
        "Control</a>.</p>",
    )
    banner_html(h, trips)
    if getenv("ORCA_RUNNER") == "local":
        h.raw(
            '<p class="alert">This scheduler\'s runner is',
            " <code>local</code>: it runs every run on its own machine and",
            " launches no instances, so there are none to list and no",
            ' per-instance cost. The machine is <a href="/ops/control">',
            "Control</a>; its price is the server's fixed price.</p>",
        )
    h.raw(
        '<div data-poll="/ops/fleet/hosts?sort=',
        col,
        "&amp;dir=",
        "desc" if desc else "asc",
        "&amp;range=",
        span,
        '" data-poll-every="10">',
        hosts_fragment(db, col, desc, span),
        "</div>",
    )
    charts_html(h, charts, span)
    return Page(200, j^, page_html("Fleet", "/ops/fleet", h))


def fleet_hosts_page(
    mut db: State, sort: String, dir: String, range: String
) raises -> Page:
    """The live part of the fleet page, as a fragment; it never ends."""
    var span = String("7d") if range == "7d" else String("24h")
    return Page(
        200,
        String(),
        hosts_fragment(db, _fleet_sort(sort), dir == "desc", span),
        "text/html; charset=utf-8",
    )


def _system_range(range: String) -> String:
    return range if range == "7d" or range == "14d" else String("24h")


comptime _OPS_EVERY = 10
"""Seconds between refreshes of the Workers page, as Fleet's hosts."""


comptime _CONTROL_EVERY = 30
"""Seconds between refreshes of the Control page's charts and alerts."""


comptime _WATCHDOG_EVERY = 30
"""Seconds between refreshes of the watchdog's rules and trips."""


def _system_live(mut h: Html, mut db: State, span: String) raises:
    """What the Control page refreshes: the watchdog's banner, the machine's
    health over `span`, and the alerts sent."""
    banner_html(h, active_trips(db))
    system_html(h, system_now(db), system_charts(db, range_hours(span)), span)
    pressure_html(h, scheduler_pressure(db))
    notify_html(h, notify_configured(), recent_alerts(db))


def system_page(mut db: State, range: String) raises -> Page:
    """The control instance's own health: CPU, memory, the disk Postgres is
    on and the network over `range`, and each container's memory."""
    var span = _system_range(range)
    var now = system_now(db)
    var charts = system_charts(db, range_hours(span))
    var h = Html()
    h.raw("<h1>Control</h1>")
    h.raw(
        (
            '<p class="muted">The one machine running the scheduler, this UI,'
            " Postgres and control-placed runs: its CPU, memory, disk and"
            " network. The EC2 instances orca launches for runs are on"
            ' <a href="/ops/fleet">Fleet</a>; <a href="/watchdog">Watchdog</a>'
            " lists the rules that pause pipelines.</p>"
        ),
    )
    h.raw('<div data-poll="/ops/control/live?range=')
    h.text(span)
    h.raw('" data-poll-every="', _CONTROL_EVERY, '">')
    banner_html(h, active_trips(db))
    system_html(h, now, charts, span)
    var pressure = scheduler_pressure(db)
    pressure_html(h, pressure)
    var sent = recent_alerts(db)
    notify_html(h, notify_configured(), sent)
    h.raw("</div>")
    var json = system_json(now, charts)
    var close = json.rfind("}")
    var with_alerts = String(
        json[byte=:close],
        ', "notify": ',
        notify_json(notify_configured(), sent),
        ', "memory_pressure": ',
        row_json(pressure),
        "}",
    )
    return Page(200, with_alerts, page_html("Control", "/ops/control", h))


def metrics_page(mut db: State) raises -> Page:
    """`/metrics`: Prometheus' gauges for the control instance and the
    fleet's counts."""
    return Page(
        200,
        String(),
        String(
            prometheus(system_now(db), fleet_summary(db)),
            pressure_prometheus(pressure_rows(db)),
            watchdog_prometheus(db, watchdog_config()),
        ),
        "text/plain; version=0.0.4; charset=utf-8",
    )


def ops_home() -> Page:
    """`/ops`: Control, the first of Ops' pages."""
    return Page(302, String(), String(), location="/ops/control")


def moved(url: String, path: String) -> Page:
    """`/system`, `/fleet` and `/fleet/hosts`, the Ops pages' old homes:
    moved for good, the query kept."""
    var q = url.find("?")
    var to = String("/ops/control") if path == "/system" else String(
        "/ops", path
    )
    return Page(
        301,
        String(),
        String(),
        location=String(to, String(url[byte=q:]) if q >= 0 else String("")),
    )


def control_live(mut db: State, range: String) raises -> Page:
    """`/ops/control/live`: Control's live part, as the fragment it
    refreshes from."""
    var h = Html()
    _system_live(h, db, _system_range(range))
    return Page(200, String(), h^.finish(), "text/html; charset=utf-8")


def workers_page(mut db: State) raises -> Page:
    """`/ops/workers` (and `/api/workers`): each worker token's pool,
    label, worker, image, last heartbeat, runs, CPUs and memory."""
    var rows = worker_rows(db)
    var h = Html()
    h.raw(
        '<div data-poll="/ops/workers/live" data-poll-every="',
        _OPS_EVERY,
        '">',
    )
    workers_html(h, rows)
    h.raw("</div>")
    return Page(
        200, workers_json(rows), page_html("Workers", "/ops/workers", h)
    )


def workers_live(mut db: State) raises -> Page:
    """`/ops/workers/live`: the workers' table, as the fragment it
    refreshes from."""
    var h = Html()
    workers_html(h, worker_rows(db))
    return Page(200, String(), h^.finish(), "text/html; charset=utf-8")


def watchdog_page(mut db: State) raises -> Page:
    """`/watchdog`: the watchdog's rules with their last values, limits and
    snoozes, and the trips with a pipeline still paused."""
    var h = Html()
    h.raw(
        '<div data-poll="/watchdog/live" data-poll-every="',
        _WATCHDOG_EVERY,
        '">',
    )
    watchdog_html(h, rule_rows(db, watchdog_config()), active_trips(db))
    h.raw("</div>")
    return Page(
        200,
        watchdog_json(db, watchdog_config()),
        page_html("Watchdog", "", h),
    )


def watchdog_live(mut db: State) raises -> Page:
    """`/watchdog/live`: the watchdog's page, as the fragment it refreshes
    from."""
    var h = Html()
    watchdog_html(h, rule_rows(db, watchdog_config()), active_trips(db))
    return Page(200, String(), h^.finish(), "text/html; charset=utf-8")
