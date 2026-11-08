"""What the pages and their JSON show, read from the database once into
typed rows: the query layer `orca.ui` and `orca.api` share.

| Module | |
|---|---|
| `orca.views.pipelines` | `PipelineRow`, its steps, fires, key counts, fill and usage |
| `orca.views.runs` | `RunRow`, its steps, tables, machines, attempts; a runs list's page |
| `orca.views.code` | where each step's code is, at a run's commit |
| `orca.views.steps` | the step cards' data |
| `orca.views.datasets` | a dataset's writers and readers, a rebuild's preview |
| `orca.views.start` | what Start runs would queue |
| `orca.views.commands` | the `orca` command lines pages show beside their buttons |
"""
