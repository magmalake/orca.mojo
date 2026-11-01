# orca

Orchestration for data pipelines written in Mojo: DAGs declared in Mojo and
checked by the compiler, schedules in cron syntax, and a web UI for runs,
reruns and backfills. Steps run on [dista](https://github.com/magmalake/dista.mojo)
clusters over Iceberg tables in object storage.

Early days: steps and pipelines are declared and checked at compile time, and
runs are recorded in Postgres and resume where they stopped.
[VISION.md](VISION.md) says what orca is for and how it gets there;
[DESIGN.md](DESIGN.md) is the plan for building it.

```
pixi run test        # unit, CLI, compile-fail, state and kill tests
pixi run toy && ./build/toy run --partitions 2024-01..2024-03
ORCA_DATABASE_URL=postgresql:///orca ./build/toy run --partitions 2024-01..2024-03
./build/toy status
```

Part of [magmalake](https://magmalake.dev).
