"""The Ops pages: Control (`/ops/control`, the machine running the
scheduler, the UI and Postgres; `orca.system`), the watchdog (`/watchdog`;
`orca.watchdog`) and Prometheus' `/metrics`, with the fragments each
refreshes from; Pools (`/ops/pools`) is `orca.ui.pools`. Fleet and
Workers' old paths redirect to Pools (`moved`, `to_pools`).
"""

from std.os import getenv

from orca.admission import (
    pressure_html,
    pressure_prometheus,
    pressure_rows,
    row_json,
    scheduler_pressure,
)
from orca.fleet import fleet_summary
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


def _system_range(range: String) -> String:
    return range if range == "7d" or range == "14d" else String("24h")


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
            " network. The machines runs are placed on elsewhere (EC2,"
            " Kubernetes, self-hosted workers) are on"
            ' <a href="/ops/pools">Pools</a>; <a href="/watchdog">Watchdog</a>'
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
    """`/system`, the Ops page's old home: moved for good, the query
    kept."""
    var q = url.find("?")
    return Page(
        301,
        String(),
        String(),
        location=String(
            "/ops/control", String(url[byte=q:]) if q >= 0 else String("")
        ),
    )


def to_pools(path: String, range: String, var json: String) -> Page:
    """`/ops/fleet`, `/ops/workers`, their fragments and `/fleet`,
    `/fleet/hosts`, the pages Pools replaced: moved for good to Pools,
    filtered to the kind they showed (EC2, self-hosted), Fleet's `?range=`
    kept. `json` is the old page's JSON, which a script asking for JSON
    still gets, with no redirect (`orca.web.page.respond`)."""
    var workers = path.startswith("/ops/workers")
    var live = path.endswith("/hosts") or path.endswith("/live")
    var to = String(
        "/ops/pools/live" if live else "/ops/pools",
        "?kind=self-hosted" if workers else "?kind=ec2",
    )
    if not workers and range == "7d":
        to.write("&range=7d")
    return Page(301, json^, String(), location=to)


def control_live(mut db: State, range: String) raises -> Page:
    """`/ops/control/live`: Control's live part, as the fragment it
    refreshes from."""
    var h = Html()
    _system_live(h, db, _system_range(range))
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
        page_html("Watchdog", "/watchdog", h),
    )


def watchdog_live(mut db: State) raises -> Page:
    """`/watchdog/live`: the watchdog's page, as the fragment it refreshes
    from."""
    var h = Html()
    watchdog_html(h, rule_rows(db, watchdog_config()), active_trips(db))
    return Page(200, String(), h^.finish(), "text/html; charset=utf-8")
