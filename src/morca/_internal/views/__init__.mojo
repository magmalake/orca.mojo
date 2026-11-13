"""What the pages and their JSON show, read from the database once into
typed rows: the query layer `morca._internal.ui` and `morca._internal.api` share.

| Module | |
|---|---|
| `morca._internal.views.pipelines` | `PipelineRow`, its steps, fires, key counts, fill and usage |
| `morca._internal.views.runs` | `RunRow`, its steps, tables, machines, attempts; a runs list's page |
| `morca._internal.views.code` | where each step's code is, at a run's commit |
| `morca._internal.views.steps` | the step cards' data |
| `morca._internal.views.datasets` | a dataset's writers and readers, a rebuild's preview |
| `morca._internal.views.start` | what Start runs would queue |
| `morca._internal.views.commands` | the `morca` command lines pages show beside their buttons |
| `morca._internal.views.pools` | Ops' Pools: a `Machine` per EC2 instance, Kubernetes run and worker |
"""
