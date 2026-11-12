"""morca's JSON API: what `morca --remote`, `morca worker` and
`morca.http_store.HttpStore` talk to. A page's JSON is built from the same
read models as its HTML (`morca.views`); these are the writers, and the
endpoints that are JSON only.

| Module | |
|---|---|
| `morca.api.pipelines` | a pipeline, a keyed one's counts, Start runs' preview |
| `morca.api.runs` | a run, a list's page, a code version, a run's machines |
| `morca.api.datasets` | rebuilds and lineage |
| `morca.api.ops` | `/api/fleet`, `/api/workers`, `/api/system`, `/api/gantt`, `/api/watchdog` |
| `morca.api.actions` | the `POST`s that change something |
| `morca.api.worker` | the worker API, `/api/worker/...`, and its own listener |
"""
