"""morca's JSON API: what `morca --remote`, `morca worker` and
`morca._internal.http_store.HttpStore` talk to. A page's JSON is built from the same
read models as its HTML (`morca._internal.views`); these are the writers, and the
endpoints that are JSON only.

| Module | |
|---|---|
| `morca._internal.api.pipelines` | a pipeline, a keyed one's counts, Start runs' preview |
| `morca._internal.api.runs` | a run, a list's page, a code version, a run's machines |
| `morca._internal.api.datasets` | rebuilds and lineage |
| `morca._internal.api.ops` | `/api/fleet`, `/api/workers`, `/api/system`, `/api/gantt`, `/api/watchdog` |
| `morca._internal.api.actions` | the `POST`s that change something |
| `morca._internal.api.worker` | the worker API, `/api/worker/...`, and its own listener |
"""
