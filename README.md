# orca

Orchestration for data pipelines written in Mojo: DAGs declared in Mojo and
checked by the compiler, schedules in cron syntax, and a web UI for runs,
reruns and backfills. Steps run on [dista](https://github.com/magmalake/dista.mojo)
clusters over Iceberg tables in object storage.

Nothing is built yet. [VISION.md](VISION.md) says what orca is for and how it
gets there; [DESIGN.md](DESIGN.md) is the plan for building it.

Part of [magmalake](https://magmalake.dev).
