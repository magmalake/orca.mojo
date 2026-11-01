# orca

Orchestration for data pipelines written in Mojo: DAGs declared in Mojo and
checked by the compiler, schedules in cron syntax, and a web UI for runs,
reruns and backfills. Steps run on [dista](https://github.com/magmalake/dista.mojo)
clusters over Iceberg tables in object storage.

Early days: steps and pipelines are declared and checked at compile time and
run in one process; nothing is recorded yet. [VISION.md](VISION.md) says what
orca is for and how it gets there; [DESIGN.md](DESIGN.md) is the plan for
building it.

```
pixi run test                                   # unit, CLI and compile-fail tests
pixi run toy && ./build/toy run --partitions 2024-01..2024-03
```

Part of [magmalake](https://magmalake.dev).
