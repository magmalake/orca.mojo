"""What the pages and their JSON show, read from the database once into
typed rows: the query layer `morca.ui` and `morca.api` share.

| Module | |
|---|---|
| `morca.views.pipelines` | `PipelineRow`, its steps, fires, key counts, fill and usage |
| `morca.views.runs` | `RunRow`, its steps, tables, machines, attempts; a runs list's page |
| `morca.views.code` | where each step's code is, at a run's commit |
| `morca.views.steps` | the step cards' data |
| `morca.views.datasets` | a dataset's writers and readers, a rebuild's preview |
| `morca.views.start` | what Start runs would queue |
| `morca.views.commands` | the `morca` command lines pages show beside their buttons |
| `morca.views.pools` | Ops' Pools: a `Machine` per EC2 instance, Kubernetes run and worker |
"""
