"""Every path `orca ui` answers, and what answers it: the route table
`orca.web.server` serves (`orca.web.router` for how a path matches).

Each handler here only takes the request's parameters out of the `Call`
and hands them to the page (`orca.ui`) or the action (`orca.api.actions`)
that does the work. A page answers HTML, or its JSON to `Accept:
application/json` or `?format=json` (`/api/...` always JSON). The worker
API (`/api/worker/...`) has a table of its own, `orca.api.worker`, and the
stylesheet is served before any route is looked up.

| Path | |
|---|---|
| `/` | pipelines, with their schedules, next runs and last runs |
| `/pipelines/<name>` | `?tab=overview` (default), `runs` or `operate`, each its own page; a pipeline's steps, partitions, schedule, next runs and runs; a keyed one's keys, done, live and failed; its fill (`orca.fill`): done of total, rate, ETA, today's estimated spend against its budget and why it's idle; its registration as `orca config show` prints it (`#config`, Overview); `?preview=1&partitions=` (`&rerun=1`): what Start runs would queue, queueing nothing; `?steps=graph` or `table`: the Steps tab shown without scripts |
| `/pipelines/<name>/progress` | the page's Progress card (runs going, fill, how far a sensor is behind what it follows (`orca.follow`), next run), as an HTML fragment the page refreshes every 5 s |
| `/api/pipelines/<name>` | the same as JSON, whatever is asked for |
| `/pipelines/<name>/calendar` | a daily or monthly pipeline's days (months) of `?month=2023-01` (`2023`) with their state, as JSON: done, failed, live, open, outside; the page shows it as coverage (`?cal=` a month or year, `?cov=month` or `year`: a daily one's tab, year by default, both from one read of the year in `orca.ui.coverage`) and as the Start runs form's date picker |
| `/pipelines/<name>/keys/<key>` | one key of a keyed pipeline: its steps in every run it was in |
| `/runs` | runs, newest first, `?limit=` (or `?n=`; 50, at most 500) to a page; `?pipeline=`, `?status=`, `?trigger=`; `?before=<id>`: the page older than that run, `?after=<id>`: the page newer than it (keyset, so pages stay put as runs arrive); the JSON's `next`, `prev` (and `next_url`, `prev_url`) are the cursors either side. A pipeline's Runs tab pages the same, 20 to a page (`runs_page` in its JSON) |
| `/runs/table` | a runs list's page as the fragment it refreshes from: `?view=list` (`/runs`), `pipeline` (a pipeline's Runs tab) or `home` (recent runs), with the list's filters, `limit` and cursor; `X-Poll-Every` says when to ask again (5 s while a run on it is queued or running, 30 s on the newest page otherwise), and an older page with nothing going on answers 286 |
| `/home/pipelines` | the home page's table of pipelines, as the fragment it refreshes from (5 s while a run is queued or running, else 30 s) |
| `/runs/<id>` | a run's cards (Status, Partitions & trigger, Code, Resources, Settings), its steps (`?steps=table` (default), `timeline` or `graph`: the Steps tab shown without scripts), its machines, and the snapshots each step read and wrote |
| `/runs/<id>/attempts/<attempt>` | an attempt's log, with its error; its facts refresh from `/runs/<id>/attempts/<attempt>/facts` while it runs |
| `/code` | `?u=` a link to a file at a commit on GitHub: its lines and each coloured as HTML (`orca.highlight`), as JSON, for the code shown on hover; the UI fetches the file, so a browser needn't reach GitHub. With `&at=` a line, only that `def`'s signature, coloured, and its docstring's first paragraph, for a step's card (`orca.ui.step_cards`) |
| `/runs/<id>/timeline.json` | the run's Dista timeline, which its page draws (`orca.ui.viz`) and Perfetto opens |
| `/api/gantt` | the pipelines in dataset order with their runs over `?window=1h`, `6h`, `24h` (default) or `7d`, and the runs each started: the home page's Timeline tab (`orca.ui.viz`) |
| `/runs/<id>/driver.log` | the run's driver output, as the scheduler kept it |
| `/runs/<id>/metrics` | the run's machines: charts and peaks from its samples (`orca.metrics`) |
| `/ops` | redirects to `/ops/control`; "Ops" in the nav has Control, Fleet and Workers under it |
| `/ops/control` | Control, the one machine running the scheduler, this UI, Postgres and control-placed runs: its own health: CPU, memory, the disk Postgres is on and network over `?range=24h`, `7d` or `14d`, and each container's memory (`orca.system`), and the watchdog's banner, refreshed every 30 s from `/ops/control/live`; `/api/system` is the same as JSON; `/system` redirects here (301) |
| `/metrics` | Prometheus gauges: the control instance's CPU, memory, disk, network and containers' memory, runs running and queued, hosts, and errors in the last hour; open like the other pages (reads need no login) |
| `/ops/fleet` | Fleet, the EC2 instances orca launches for runs, the machines it is paying for: hosts running and what they cost an hour, runs and errors per hour, a table of live hosts (`?sort=` a column, `&dir=desc`) and charts over `?range=24h` or `7d` (`orca.fleet`); `/ops/fleet/hosts` is the live part of it, and `/api/fleet` the same as JSON; `/fleet` and `/fleet/hosts` redirect here (301) |
| `/ops/workers` | Workers, the machines running pipelines placed `worker:<pool>` (`orca.workers`): each worker token's pool, label, worker, image, last heartbeat, runs, CPUs and memory; `/api/workers` the same as JSON; refreshed every 10 s from `/ops/workers/live`. A worker's own requests go to `/api/worker/...` with its token |
| `/watchdog`, `/api/watchdog` | the watchdog's rules with their last values, limits and snoozes, and the trips with a pipeline still paused (`orca.watchdog`); also the red banner on `/`, `/ops/fleet` and `/ops/control`; the page refreshes every 30 s from `/watchdog/live` |
| `/datasets` | datasets, with their writers, readers and latest snapshots, and the graph of pipelines and datasets |
| `/datasets/<name>` | a dataset's place in that graph, the steps that write and read it (linked to their code), what a rebuild of it runs, and its snapshots; `?preview=1&partitions=`: a rebuild's runs by level, queueing nothing |
| `/datasets/<name>?before=<cursor>` | its snapshots 50 to a page, newest first; `?after=` for the newer page, the cursor `<attempt>.<snapshot>` (`snapshots_page` in the JSON) |
| `/datasets/<name>/snapshots/<id>` | a snapshot's lineage (`orca.lineage`): its diagram, Upstream (`back` in the JSON: the snapshot, if a run wrote it, and the snapshots it was written from, with the run and step that wrote each and what that step read) and Downstream (`forward`: the snapshots written from it) |
| `/rebuilds/<id>` | a rebuild's pipelines by level, with their runs (`orca.sensors`) |

The actions, each a `POST`, are `orca.api.actions`'; `/login`, `/logout`,
`/api/login` and `/api/logout` are `orca.web.auth`'s.
"""

from orca.api import actions
from orca.api.ops import fleet_api, gantt_api, system_api, watchdog_api
from orca.api.pipelines import calendar_page, pipeline_json
from orca.api.runs import run_json
from orca.state import State
from orca.ui.attempt import attempt, attempt_facts, attempt_log
from orca.ui.code import code_page
from orca.ui.datasets import (
    dataset_page,
    datasets_page,
    rebuild_page,
    snapshot_page,
)
from orca.ui.home import home, home_pipelines
from orca.ui.layout import not_found
from orca.ui.ops import (
    control_live,
    fleet_hosts_page,
    fleet_page,
    metrics_page,
    moved,
    ops_home,
    system_page,
    watchdog_live,
    watchdog_page,
    workers_live,
    workers_page,
)
from orca.ui.pipeline import key_page, pipeline_html, pipeline_progress
from orca.ui.run import (
    cluster_log,
    driver_log,
    run_html,
    run_live,
    run_metrics,
    run_steps,
    timeline,
)
from orca.ui.runs import jumped, runs, runs_fragment_query, runs_table
from orca.views.pipelines import PipelineAsk, pipeline_view
from orca.views.runs import run_view, runs_query
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
from orca.web.page import Page
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
        action("/runs/bulk", actions.bulk),
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
    return runs(db, runs_query(call.url, "list"), call.url)


def _runs_table(mut db: State, call: Call) raises -> Page:
    var p = runs_table(db, call.url)
    # Its login links go back to the list's page, not the fragment.
    p.here = runs_fragment_query(call.url).here()
    return p^


def _run(mut db: State, call: Call) raises -> Page:
    var id = call.int("id")
    var v = run_view(db, id)
    if not v:
        return not_found(String("no run ", id))
    return Page(
        200, run_json(v.value()), run_html(db, v.value(), call.query("steps"))
    )


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
    return code_page(call.query("u"), call.query("at"))


# ── pipelines ────────────────────────────────────────────────────────────────


def _pipeline(mut db: State, call: Call) raises -> Page:
    var ask = PipelineAsk(
        name=call.param("name"),
        partitions=call.query("partitions"),
        rerun=call.query("rerun") == "1",
        preview=call.query("preview") == "1",
        cal=call.query("cal"),
        allow_append=call.query("allow_append") == "1",
        steps=call.query("steps"),
        tab=call.query("tab"),
        cov=call.query("cov"),
        url=call.url,
    )
    var v = pipeline_view(db, ask)
    if not v:
        return not_found(String("no pipeline ", ask.name))
    if ask.shown_tab() == "runs" and jumped(v.value().runs):
        return Page(
            303,
            pipeline_json(v.value(), ask),
            String(),
            location=String("/runs/", v.value().runs.q.run),
        )
    return Page(
        200,
        pipeline_json(v.value(), ask),
        pipeline_html(db, v.value(), ask),
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
