# orca

Orchestration for data pipelines written in Mojo: DAGs declared in Mojo and
checked by the compiler, schedules in cron syntax, and a web UI for runs,
reruns and backfills. Steps run on [dista](https://github.com/magmalake/dista.mojo)
clusters over Iceberg tables in object storage.

Early days: steps and pipelines are declared and checked at compile time, and
runs are recorded in Postgres and resume where they stopped, with Iceberg
reads pinned and commits recognised on retry (`orca.tables`).
[VISION.md](VISION.md) says what orca is for and how it gets there;
[DESIGN.md](DESIGN.md) is the plan for building it; [K8S.md](K8S.md) is how
it will run on Kubernetes and AWS.

```
pixi run test                  # unit, CLI, compile-fail, state and kill tests
pixi run -e data test-tables   # Iceberg; needs the sibling checkouts (see CI)
pixi run toy && ./build/toy run --partitions 2024-01..2024-03
ORCA_DATABASE_URL=postgresql:///orca ./build/toy run --partitions 2024-01..2024-03
./build/toy status
```

Part of [magmalake](https://magmalake.dev).
