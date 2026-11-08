"""orca's JSON API: what `orca --remote`, `orca worker` and
`orca.http_store.HttpStore` talk to. A page's JSON is built from the same
read models as its HTML (`orca.views`); these are the writers, and the
endpoints that are JSON only.

| Module | |
|---|---|
| `orca.api.pipelines` | a pipeline, a keyed one's counts, Start runs' preview |
| `orca.api.runs` | a run, a list's page, a code version, a run's machines |
| `orca.api.datasets` | rebuilds and lineage |
| `orca.api.ops` | `/api/fleet`, `/api/workers`, `/api/system`, `/api/gantt`, `/api/watchdog` |
| `orca.api.actions` | the `POST`s that change something |
| `orca.api.worker` | the worker API, `/api/worker/...`, and its own listener |
"""
