"""The Ops pages' JSON endpoints: `/api/fleet`, `/api/system`,
`/api/gantt` and `/api/watchdog`, each its page's JSON (`/api/workers` is
the Workers page's, `orca.ui.ops`).
"""

from orca.state import State
from orca.ui.ops import fleet_page, system_page
from orca.ui.viz import gantt_json
from orca.watchdog import config as watchdog_config, watchdog_json
from orca.web.page import Page


def fleet_api(mut db: State, range: String) raises -> Page:
    """`/api/fleet`: the fleet page's numbers as JSON, whatever is asked
    for."""
    var page = fleet_page(db, "", "", range)
    return Page(200, String(), page.json.copy(), "application/json")


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
    `window` (`orca.ui.viz`)."""
    var j = gantt_json(db, window)
    return Page(200, j.copy(), j^, "application/json")
