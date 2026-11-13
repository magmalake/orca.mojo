"""The Ops pages' JSON endpoints: `/api/fleet`, `/api/workers`,
`/api/system`, `/api/gantt` and `/api/watchdog` (`/api/pools` is the Pools
page's, `morca._internal.ui.pools`). `/api/fleet` and `/api/workers` are what the
Fleet and Workers pages were, before Pools replaced them.
"""

from morca._internal.fleet import (
    charts_json,
    fleet_charts,
    fleet_hosts,
    fleet_summary,
    hosts_json,
    sort_hosts,
    summary_json,
)
from morca.state import State
from morca._internal.ui.ops import system_page
from morca._internal.ui.viz import gantt_json
from morca.watchdog import (
    active_trips,
    config as watchdog_config,
    trips_json,
    watchdog_json,
)
from morca._internal.web.page import Page
from morca._internal.workers import worker_rows, workers_json


def _fleet_sort(sort: String) -> String:
    """`sort` if it names a column Fleet's hosts sorted by, else the
    default."""
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


def fleet_json(
    mut db: State, sort: String, dir: String, range: String
) raises -> String:
    """The machines morca is paying for, and what they have been doing: the
    watchdog's trips, the summary, the hosts (`?sort=` a column,
    `&dir=desc`) and the charts over `?range=24h` or `7d`."""
    var hosts = fleet_hosts(db)
    sort_hosts(hosts, _fleet_sort(sort), dir == "desc")
    return String(
        '{"watchdog": ',
        trips_json(active_trips(db)),
        ', "summary": ',
        summary_json(fleet_summary(db)),
        ', "hosts": ',
        hosts_json(hosts),
        ', "charts": ',
        charts_json(fleet_charts(db, 168 if range == "7d" else 24)),
        "}",
    )


def fleet_api(mut db: State, range: String) raises -> Page:
    """`/api/fleet`: the fleet's numbers as JSON, whatever is asked for."""
    var j = fleet_json(db, "", "", range)
    return Page(200, j.copy(), j^, "application/json")


def workers_api(mut db: State) raises -> Page:
    """`/api/workers`: each worker token's pool, label, worker, image, last
    heartbeat, runs, CPUs and memory."""
    var j = workers_json(worker_rows(db))
    return Page(200, j.copy(), j^, "application/json")


def system_api(mut db: State, range: String) raises -> Page:
    """`/api/system`: Control as JSON."""
    var page = system_page(db, range)
    return Page(200, String(), page.json.copy(), "application/json")


def watchdog_api(mut db: State) raises -> Page:
    """`/api/watchdog`: the watchdog's page as JSON."""
    var j = watchdog_json(db, watchdog_config())
    return Page(200, j.copy(), j^, "application/json")


def gantt_api(mut db: State, window: String) raises -> Page:
    """`/api/gantt`: the home page's Timeline tab's pipelines and runs over
    `window` (`morca._internal.ui.viz`)."""
    var j = gantt_json(db, window)
    return Page(200, j.copy(), j^, "application/json")
