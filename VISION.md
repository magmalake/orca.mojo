# morca: vision

November 1, 2026

morca orchestrates data pipelines written in Mojo. A pipeline is a DAG of steps, declared in Mojo and checked by the compiler. A schedule says when it runs, in cron syntax. A web UI shows what ran, what's running and what failed, and lets someone rerun or backfill it. The steps' heavy work runs on dista clusters. The data they read and write lives in Iceberg tables in object storage.

## Why

magmalake can now read and write a data lake in Mojo (Iceberg, Parquet, Arrow, object storage), and dista spreads that work across a cluster. What's still missing is the layer that decides *when* work runs, *in what order*, and *what to do when a step fails*. Today that layer would be Dagster or Airflow:

- **They're Python.** A Mojo step runs as a subprocess or a container behind a Python definition. The DAG, its types and its configuration live in a different language from the code they describe, and the compiler checks neither against the other.
- **They don't know the data lake.** They track runs of code. Which Iceberg snapshot a step read, and which one it wrote, is left to each step to log somewhere.
- **They bring a large stack.** A Python service, a metadata database, workers, a UI server and their dependencies, all to schedule work that's otherwise compiled Mojo.

morca is that layer for this stack, built from the same tins.

## Where it fits

```
  schedules · UI · reruns and backfills        morca
  ──────────────────────────────────────────
  steps: Mojo functions, one dista cluster     dista
  per run of a pipeline
  ──────────────────────────────────────────
  Iceberg in S3 (versioned datasets),          iceberg-mojo, parquet,
  Lance (vectors), Postgres (morca's state)     objectstore, postgres
  ──────────────────────────────────────────
  Kubernetes
```

- **One dista cluster per run.** A run of a pipeline gets a dista cluster of its own, sized for it, and gives it back when it ends. Runs don't share workers, so one run's failure or memory use can't reach another's.
- **Datasets are Iceberg tables.** A step reads tables at known snapshots and commits new snapshots. A rerun of a step reads the same snapshots it read the first time, so its output is reproducible. Vectors and embeddings go in Lance.
- **Lineage is recorded where the data is.** Every snapshot a step commits carries, in its summary properties, the run, the step and the snapshots it read. morca also keeps the same edges in a Postgres table, so the UI can walk from any dataset to the steps and inputs that made it, without scanning table metadata.
- **morca's own state is in Postgres**, through postgres-mojo: pipelines, schedules, runs, step attempts and their logs' locations.

## Principles

1. **Pipelines are code, compiled.** A DAG is declared in Mojo. Its steps are typed functions, and an edge that connects mismatched types doesn't compile, as dista's tasks don't. There's no YAML pipeline language and no templating.
2. **Assets, not just tasks.** A step declares the datasets it reads and writes. Dependencies come from the data as much as from the code: "rebuild everything downstream of this table" is a query, not a script. This is Dagster's best idea, kept.
3. **Partitions are first class.** Most data arrives by time. A pipeline can be partitioned by day or month, a schedule fills the next partition, and a backfill is the same pipeline over a range of partitions.
4. **Retries and reruns are cheap and safe.** A step can rerun because its inputs are pinned snapshots and its output is a new snapshot that replaces nothing until it commits. A failed run resumes at the failed step.
5. **Small and explainable.** One scheduler process, one Postgres database, one UI. A core one person can read in an afternoon.
6. **Built from tins.** HTTP and the UI server through flare, state through postgres-mojo, data through iceberg-mojo and objectstore, compute through dista.

## Who it's for

People building data pipelines in Mojo on a data lake, who want them scheduled, observed and rerunnable without a Python orchestrator in front. The first workloads:

- **Dataset builds.** Ingest raw data by partition, clean and join it into Iceberg tables, publish a new version. The NYC taxi load that the dista benchmarks run on is the first example.
- **Multi-stage processing.** Pipelines whose stages are each a distributed job, such as perception and auto-labeling over driving logs: detection, then tracking, then lifting to 3D, each stage reading the previous one's table.
- **Feature and embedding refreshes.** Recompute features or embeddings for new partitions on a schedule, writing vectors to Lance.

## The pieces

- **DAG definitions.** Steps and the datasets between them, declared at compile time and registered in one binary, as dista registers tasks. The registry's hash identifies a version of the pipeline, so a run records exactly which code it ran.
- **The scheduler.** Cron expressions (`0 3 * * *`, with time zones), triggers on a new upstream snapshot, and manual runs. It decides which partitions are due, starts runs, retries failed steps with backoff, and enforces how many runs of a pipeline can run at once.
- **The runner.** For each run, it starts a dista cluster on Kubernetes, runs the steps in dependency order, records each step's attempts, inputs, outputs and logs, and tears the cluster down.
- **The UI.** A web interface served by the scheduler. It shows pipelines and their DAGs, schedules and the next runs due, run history with each step's status, timing and logs, a run's dista timeline, and the lineage graph of datasets. It can start a run, rerun from a failed step, and backfill a range of partitions.
- **An API and a command line.** Everything the UI does, over HTTP, plus a `morca` command that drives it.

## Non-goals

- **Not Dagster-compatible.** morca borrows its ideas (assets, partitions, sensors) but not its API, and it doesn't run Python steps.
- **Not a general workflow engine.** It orchestrates data pipelines over a data lake. Long-running business workflows with human steps and timers belong to a durable-execution system. Restate is the candidate for that, later, and for making morca's own runs durable.
- **Not a compute engine.** Steps run on dista. morca decides when and in what order.
- **Not multi-tenant at first.** One team, trusted code, no per-user quotas.

## Where it goes

Each phase ends at a gate: something that works, not a feature list.

1. **DAGs and runs.** Declare steps and the datasets between them; run a DAG once, locally, in dependency order, with a dista runtime per run; record runs and steps in Postgres. *Gate:* the taxi load runs as a DAG (download, per-month load, compaction), and a run killed halfway resumes at the step that failed.
2. **Schedules and partitions.** Cron schedules, monthly and daily partitions, backfills, retries with backoff. *Gate:* a schedule fills each month's partition as it falls due, and a backfill over 2011–2024 runs with a bounded number at once.
3. **The UI.** Pipelines, runs, steps, logs and the dista timeline, served by flare; start, rerun and backfill from the browser. *Gate:* a failed step is found, its logs read and the run resumed, without a terminal.
4. **Lineage and sensors.** Snapshot summary properties and the lineage table; runs triggered by a new upstream snapshot; "rebuild downstream of this table." *Gate:* committing a new snapshot to a source table rebuilds exactly the tables downstream of it.
5. **On Kubernetes.** The scheduler as a Deployment; a dista cluster per run, created and deleted through the Kubernetes API. *Gate:* the taxi pipeline runs on a schedule on EC2, each run on its own cluster.

## What success looks like

| Target | Proposed bar | Why it matters |
| --- | --- | --- |
| Definition | A pipeline is one Mojo file; a type mismatch between steps doesn't compile | Pipelines are code |
| Recovery | A failed run resumes at the failed step, reading the same snapshots | Reruns are cheap and reproducible |
| Lineage | Any table's snapshot traces back to the run, the code version and the inputs that made it | Trust in the data |
| Footprint | One scheduler process and one Postgres database | Small and explainable |
| Scheduling overhead | A run's first step starts within seconds of its schedule, cluster start excepted | Orchestration stays out of the way |

## Open questions

- **How a DAG is declared.** Compile-time registration, as dista declares tasks, or a builder API that runs at start-up and produces a checked graph. The first keeps the compiler's guarantees; the second is easier to compose.
- **Where step code runs.** In the scheduler's own binary, with dista's workers built from the same code, or as a separate image per pipeline. One binary is simplest, but the scheduler shouldn't need every pipeline's dependencies, which is the same split dista's head and workers are heading for.
- **The UI's stack.** Server-rendered HTML from flare with a little JavaScript, or a small single-page app against the API.
- **Cluster start time.** A dista cluster per run is clean, but a cluster takes time to start. Short, frequent runs may want a warm pool.
- **Restate.** When morca's own runs need to be durable across scheduler restarts, whether that's Postgres alone or Restate underneath.
