"""Every path `orca ui` answers, and what answers it: the route table
`orca.web.server` serves (`orca.web.router` for how a path matches).

Each handler here only takes the request's parameters out of the `Call`
and hands them to the page (`orca.ui`) or the action (`orca.api.actions`)
that does the work. A page answers HTML, or its JSON to `Accept:
application/json` or `?format=json` (`/api/...` always JSON). The worker
API (`/api/worker/...`) has a table of its own, `orca.api.worker`, and the
stylesheet is served before any route is looked up.
"""

from orca.api import actions
from orca.state import State
from orca.ui import (
    Page,
    _runs_fragment_query,
    attempt,
    attempt_facts,
    attempt_log,
    calendar_page,
    cluster_log,
    code_page,
    control_live,
    dataset_page,
    datasets_page,
    driver_log,
    fleet_api,
    fleet_hosts_page,
    fleet_page,
    gantt_api,
    home,
    home_pipelines,
    key_page,
    metrics_page,
    moved,
    ops_home,
    pipeline,
    pipeline_progress,
    rebuild_page,
    run,
    run_live,
    run_metrics,
    run_steps,
    runs,
    runs_query,
    runs_table,
    snapshot_page,
    system_api,
    system_page,
    timeline,
    watchdog_api,
    watchdog_live,
    watchdog_page,
    workers_live,
    workers_page,
)
from orca.web.auth import (
    API_LOGIN,
    API_LOGOUT,
    LOGIN,
    LOGOUT,
    api_login,
    api_logout,
    login,
    login_form,
    logout,
)
from orca.web.router import Call, Route, action, auth, page


def routes() -> List[Route]:
    """The table, first match first."""
    return [
        # Home, and the runs.
        page("/", _home),
        page("/home/pipelines", _home_pipelines),
        page("/runs", _runs),
        page("/runs/table", _runs_table),
        page("/runs/{id:int}", _run, "run"),
        page("/runs/{id:int}/live", _run_live, "run"),
        page("/runs/{id:int}/steps", _run_steps, "run"),
        page("/runs/{id:int}/metrics", _run_metrics, "run"),
        page("/runs/{id:int}/timeline.json", _timeline, "run"),
        page("/runs/{id:int}/driver.log", _driver_log, "run"),
        page("/runs/{id:int}/cluster/{name}", _cluster_log, "run"),
        page("/runs/{id:int}/attempts/{attempt:int}", _attempt, "attempt"),
        page(
            "/runs/{id:int}/attempts/{attempt:int}/log", _attempt_log, "attempt"
        ),
        page(
            "/runs/{id:int}/attempts/{attempt:int}/facts",
            _attempt_facts,
            "attempt",
        ),
        page("/code", _code),
        # Pipelines; /api/pipelines/... is the same as JSON.
        page("/pipelines/{name}", _pipeline),
        page("/pipelines/{name}/progress", _progress),
        page("/pipelines/{name}/calendar", _calendar),
        page("/pipelines/{name}/keys/{key}", _key),
        page("/api/pipelines/{name}", _pipeline),
        page("/api/pipelines/{name}/progress", _progress),
        page("/api/pipelines/{name}/calendar", _calendar),
        page("/api/pipelines/{name}/keys/{key}", _key),
        # Datasets and their lineage.
        page("/datasets", _datasets),
        page("/datasets/{name}", _dataset),
        page("/datasets/{name}/snapshots/{id:int}", _snapshot, "snapshot"),
        page("/rebuilds/{id:int}", _rebuild, "rebuild"),
        # Ops: Control, Fleet, Workers, the watchdog.
        page("/ops", _ops),
        page("/system", _moved),
        page("/fleet", _moved),
        page("/fleet/hosts", _moved),
        page("/ops/control", _control),
        page("/ops/control/live", _control_live),
        page("/api/system", _system_api),
        page("/metrics", _metrics),
        page("/ops/fleet", _fleet),
        page("/ops/fleet/hosts", _fleet_hosts),
        page("/api/fleet", _fleet_api),
        page("/ops/workers", _workers),
        page("/ops/workers/live", _workers_live),
        page("/api/workers", _workers),
        page("/watchdog", _watchdog),
        page("/watchdog/live", _watchdog_live),
        page("/api/watchdog", _watchdog_api),
        page("/api/gantt", _gantt),
        # Logging in and out (orca.web.auth).
        page(LOGIN, login_form),
        auth(LOGIN, login),
        auth(LOGOUT, logout),
        auth(API_LOGIN, api_login),
        auth(API_LOGOUT, api_logout),
        # The actions (orca.api.actions).
        action("/pipelines/{name}/runs", actions.start),
        action("/pipelines/{name}/backfill", actions.backfill),
        action("/pipelines/{name}/cancel", actions.cancel_pipeline),
        action("/pipelines/{name}/pause", actions.pause),
        action("/pipelines/{name}/unpause", actions.unpause),
        action("/watchdog/all/ack", actions.acknowledge_all),
        action("/watchdog/{trip:int}/ack", actions.acknowledge_trip, "trip"),
        action("/watchdog/{rule}/snooze", actions.snooze),
        action("/datasets/{name}/rebuild", actions.rebuild),
        action("/runs/{id:int}/resume", actions.resume, "run"),
        action("/runs/{id:int}/rerun", actions.rerun, "run"),
        action("/runs/{id:int}/cancel", actions.cancel, "run"),
    ]


# ── home and runs ────────────────────────────────────────────────────────────


def _home(mut db: State, call: Call) raises -> Page:
    return home(db)


def _home_pipelines(mut db: State, call: Call) raises -> Page:
    var p = home_pipelines(db)
    p.here = "/"
    return p^


def _runs(mut db: State, call: Call) raises -> Page:
    return runs(db, runs_query(call.url, "list"))


def _runs_table(mut db: State, call: Call) raises -> Page:
    var p = runs_table(db, call.url)
    # Its login links go back to the list's page, not the fragment.
    p.here = _runs_fragment_query(call.url).here()
    return p^


def _run(mut db: State, call: Call) raises -> Page:
    return run(db, call.int("id"), call.query("steps"))


def _run_live(mut db: State, call: Call) raises -> Page:
    return run_live(db, call.int("id"))


def _run_steps(mut db: State, call: Call) raises -> Page:
    return run_steps(db, call.int("id"))


def _run_metrics(mut db: State, call: Call) raises -> Page:
    return run_metrics(db, call.int("id"))


def _timeline(mut db: State, call: Call) raises -> Page:
    return timeline(db, call.int("id"))


def _driver_log(mut db: State, call: Call) raises -> Page:
    return driver_log(db, call.int("id"))


def _cluster_log(mut db: State, call: Call) raises -> Page:
    return cluster_log(db, call.int("id"), call.param("name"))


def _attempt(mut db: State, call: Call) raises -> Page:
    return attempt(db, call.int("id"), call.int("attempt"))


def _attempt_log(mut db: State, call: Call) raises -> Page:
    return attempt_log(db, call.int("id"), call.int("attempt"))


def _attempt_facts(mut db: State, call: Call) raises -> Page:
    return attempt_facts(db, call.int("id"), call.int("attempt"))


def _code(mut db: State, call: Call) raises -> Page:
    return code_page(call.query("u"))


# ── pipelines ────────────────────────────────────────────────────────────────


def _pipeline(mut db: State, call: Call) raises -> Page:
    return pipeline(
        db,
        call.param("name"),
        call.query("partitions"),
        call.query("rerun") == "1",
        call.query("preview") == "1",
        call.query("cal"),
        call.query("allow_append") == "1",
        call.query("steps"),
        call.query("tab"),
        call.url,
    )


def _progress(mut db: State, call: Call) raises -> Page:
    return pipeline_progress(db, call.param("name"))


def _calendar(mut db: State, call: Call) raises -> Page:
    return calendar_page(db, call.param("name"), call.query("month"))


def _key(mut db: State, call: Call) raises -> Page:
    return key_page(db, call.param("name"), call.param("key"))


# ── datasets ─────────────────────────────────────────────────────────────────


def _datasets(mut db: State, call: Call) raises -> Page:
    return datasets_page(db)


def _dataset(mut db: State, call: Call) raises -> Page:
    return dataset_page(
        db,
        call.param("name"),
        call.query("partitions"),
        call.query("preview") == "1",
        call.url,
    )


def _snapshot(mut db: State, call: Call) raises -> Page:
    return snapshot_page(db, call.param("name"), Int64(call.int("id")))


def _rebuild(mut db: State, call: Call) raises -> Page:
    return rebuild_page(db, call.int("id"))


# ── ops ──────────────────────────────────────────────────────────────────────


def _ops(mut db: State, call: Call) raises -> Page:
    return ops_home()


def _moved(mut db: State, call: Call) raises -> Page:
    return moved(call.url, call.path)


def _control(mut db: State, call: Call) raises -> Page:
    return system_page(db, call.query("range"))


def _control_live(mut db: State, call: Call) raises -> Page:
    return control_live(db, call.query("range"))


def _system_api(mut db: State, call: Call) raises -> Page:
    return system_api(db, call.query("range"))


def _metrics(mut db: State, call: Call) raises -> Page:
    return metrics_page(db)


def _fleet(mut db: State, call: Call) raises -> Page:
    return fleet_page(
        db, call.query("sort"), call.query("dir"), call.query("range")
    )


def _fleet_hosts(mut db: State, call: Call) raises -> Page:
    return fleet_hosts_page(
        db, call.query("sort"), call.query("dir"), call.query("range")
    )


def _fleet_api(mut db: State, call: Call) raises -> Page:
    return fleet_api(db, call.query("range"))


def _workers(mut db: State, call: Call) raises -> Page:
    return workers_page(db)


def _workers_live(mut db: State, call: Call) raises -> Page:
    return workers_live(db)


def _watchdog(mut db: State, call: Call) raises -> Page:
    return watchdog_page(db)


def _watchdog_live(mut db: State, call: Call) raises -> Page:
    return watchdog_live(db)


def _watchdog_api(mut db: State, call: Call) raises -> Page:
    return watchdog_api(db)


def _gantt(mut db: State, call: Call) raises -> Page:
    return gantt_api(db, call.query("window"))
