# orca: design

November 1, 2026 · targets stable Mojo 1.2.0

This turns [VISION.md](VISION.md) into a buildable plan. It covers phase 1 in detail, sketches phases 2 to 5, and proposes answers to the vision's open questions. Everything marked *verified* was compiled and run on Mojo 1.2.0; every other claim about what Mojo accepts is checked in the PR that first needs it, and this file is corrected when one turns out wrong.

## What orca builds on

| Tin | What orca uses | State today |
|---|---|---|
| dista | `Runtime[*Ts]` in local mode (forked head and workers) per run; `Task`, `ObjectRef`, `Seq`; registry hash; the timeline | Phases 1 and 2 merged, actors in progress |
| iceberg | `FilesystemCatalog`, `Table.scan().use_snapshot(id)`, `AppendFiles`, `prepare_commit` with `extra_summary` and file removals | 0.8.2 |
| parquet, arrow-mlake | Reading TLC files, building `RecordBatch`es | 0.11.x, 0.2.0 |
| objectstore | HTTP GET for downloads, S3 for the warehouse and logs | ABI 4 |
| postgres | orca's state: `Connection`, `Transaction`, prepared statements; `scripts/with-pg-server.sh` for tests | 0.3.2 |
| flare_mlake | The HTTP API and UI (phase 3) | 0.13.0 |

orca adds no FFI of its own.

## Programming model

A step is a top-level `def`. Like a dista task, it's declared once with its input and output types written out, and a pipeline is the list of its steps:

```mojo
from dista import Seq
from orca import Ctx, Step, Mapped, Gather, Pipeline, Month, orca_main

@fieldwise_init
struct RawFile(Movable, Copyable):
    var month: Month
    var path: String

@fieldwise_init
struct Loaded(Movable, Copyable):
    var month: Month
    var snapshot_id: Int64

def download(mut ctx: Ctx, month: Month) raises -> RawFile: ...
def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded: ...
def compact(mut ctx: Ctx, loaded: Seq[Loaded]) raises -> Int64: ...

comptime Download = Step["download", Month, RawFile, download, Mapped]
comptime Load = Step["load", RawFile, Loaded, load, Mapped, after=Download]
comptime Compact = Step["compact", Seq[Loaded], Int64, compact, Gather, after=Load]

comptime Taxi = Pipeline["taxi", Download, Load, Compact]

def main() raises:
    orca_main[Taxi]()
```

- **An edge is a type check.** `after=Download` means `load` takes `Download`'s output. A `Mapped` step after a `Mapped` step takes one partition's output; a `Gather` step takes a `Seq` of all of them. `Step` asserts at compile time that its input type matches what its `after` produces, so `Load` declared with `Int` as input doesn't compile. This is the vision's first principle. *Verified:* `Step` takes `after` as a keyword parameter of a `StepLike` type, and its `check` compares reflected type names in `comptime assert`s, which fail the build with the assert's message. `tests/compile_fail/` holds one pipeline per rule.
- **Partitions from the start, kept simple.** A run is over a list of partition keys (months in phase 1). A `Mapped` step runs once per key; a `Gather` step once per run. The first step of a pipeline takes the key itself. Phase 2 adds partition definitions, schedules and backfills on top of the same two kinds.
- **Steps are listed in run order.** A step comes after the step it follows, so the declared order is a valid order and there's no sort to do. `Pipeline.check` asserts it, along with unique names and that a step's `after` is the pipeline's own step of that name (compared by function, so another step with the same name doesn't pass). *Verified.*
- **One input per step, as in dista.** Several upstreams arrive later as `after=(A, B)` and a struct input; the taxi load doesn't need it.
- **Step outputs are small values**: paths, snapshot IDs, counts. They're stored in Postgres through dista's codec, so a resumed run hands a step the same input the first attempt had. The data itself goes in tables.
- **`Ctx`** is what a step gets besides its input: the run and step IDs, the partition key, a logger, the catalog, and the run's dista runtime. Its table methods are where pinning and lineage happen (below).
- **The registry hash** of a pipeline is a hash over its steps' linkage names, their kinds and edges, and the dista registry hash. A run records it, so the code a run ran is known, and resuming with a different binary is refused.

### Heavy work inside a step

A step runs in the run's driver process and calls dista for anything distributed. dista's `Runtime[*Ts]` is parameterised by its tasks, so the pipeline names them, as a keyword parameter after its steps:

```mojo
comptime TaxiTasks = Tasks[ReadMonth, WriteFile]
comptime Taxi = Pipeline["taxi", Download, Load, Compact, tasks=TaxiTasks]

def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded:
    ref rt = ctx.runtime[TaxiTasks]()
    var r = rt.remote[ReadMonth](raw.month)
```

- **`Ctx` stays one type.** It holds the runtime by address, and `ctx.runtime[T]()` returns it as `T.RT` (`Runtime[*Ts]`) after checking that `T`'s tasks are the run's. The check compares task linkage names, because every `Runtime` reflects to the same type name. *Verified.*
- **One runtime per run**, started before the first step and kept to the end: in local mode it forks a head and `ORCA_WORKERS` workers (default 4); with `DISTA_HEAD` set it joins that cluster. It starts before orca opens a database connection or its heartbeat thread, so the forked processes inherit neither. A pipeline without `tasks=` starts none.
- **The same binary is dista's head and worker.** With `DISTA_ROLE` set, `orca_main` hands the process to dista before reading its arguments, so `DISTA_ROLE=worker DISTA_HEAD=... taxi` is a worker. *Verified* by hand: a head, three workers and a driver, each a separate process of the fanout example.
- **The tasks are part of the DAG**: their names are in `describe` and in the registry hash.

## Processes

**The pipeline binary is the unit.** Every pipeline is its own binary, built from its one Mojo file, and the binary is also the run's dista driver and workers. `orca_main` gives it a command line:

| Command | What it does |
|---|---|
| `taxi describe` | The DAG as JSON: steps, kinds, edges, types, registry hash |
| `taxi run --partitions 2024-01..2024-12` | Starts a run, prints its ID, runs it to the end |
| `taxi resume <run>` | Continues a run at its unfinished steps |
| `taxi status [<run>]` | Runs, or one run's steps and attempts |

Without `ORCA_DATABASE_URL`, `run` runs in memory and nothing is recorded. Exit codes: 0 for a run that succeeded, 1 for one with a step that didn't, 2 for a usage error, 3 for anything else (the database, a run that can't be resumed).

Connection settings come from the environment: `ORCA_DATABASE_URL`, `ORCA_WAREHOUSE`, `ORCA_LOGS` (a directory or `s3://`), and dista's own variables.

**The scheduler, from phase 2, is a separate generic `orca` binary** that depends on orca's core, postgres and flare, and on no pipeline. It knows a pipeline by its `describe` output and starts runs by executing the pipeline binary: a subprocess locally, a Kubernetes Job in phase 5. That answers the vision's "where step code runs": the scheduler never links a pipeline's dependencies, which is the same split dista is making between its generic head and the user's driver. It also means a scheduler restart doesn't kill running runs; they're separate processes writing to Postgres themselves.

## State in Postgres

One schema, `orca`, created and migrated by the binary at start-up (a `schema_version` table, migrations as SQL strings in the source):

| Table | Holds |
|---|---|
| `pipelines` | name, registry hash, `describe` JSON, first and last seen |
| `runs` | id, pipeline, registry hash, partitions, trigger (manual, schedule, backfill, sensor), status, created, started, ended, heartbeat |
| `steps` | run, step, partition key (null for a gather step), status, the encoded input, the encoded output |
| `attempts` | step row, attempt number, status, started, ended, error message, log location, host |
| `reads` | attempt, table, snapshot ID: what each attempt read |
| `commits` | attempt, table, snapshot ID: what each attempt committed |

`reads` and `commits` are the lineage table the vision puts in phase 4; they're written from phase 1 because resuming needs them. Phase 4 adds the queries and the UI over them.

- **Statuses.** A run is `queued`, `running`, `succeeded`, `failed` or `cancelled`. A step is `pending`, `running`, `succeeded`, `failed` or `skipped` (its upstream failed).
- **One writer per run.** A process that runs a run is its `owner` (`host:pid`) and holds it by its heartbeat: a thread with a connection of its own (threads.mojo's `TypedPool`) updates `runs.heartbeat` five times per `ORCA_STALE_SECONDS` (default 30), and `resume` refuses a run whose heartbeat is younger than that. A run whose process died has a stale heartbeat and can be resumed; a run that ended clears its heartbeat, so a failed run can be resumed at once. Every attempt's first transaction also checks that this process still owns the run, so a process that lost its run to a resume (it was paused, or the database was unreachable) stops at its next step instead of racing the new owner. No advisory locks for this, so a pooled connection is fine; migrations take a transaction-scoped one. *Verified* by `tests/kill.sh`.
- **Each state change is one transaction.** Starting an attempt inserts it and marks the step running; finishing it records the output, reads and commits, and marks the step. A kill between the two leaves an attempt `running` with a stale heartbeat, which resume marks `failed` before retrying.

## Snapshots: pinned reads, recognisable commits

This is what makes reruns safe (the vision's fourth principle).

- **`ctx.scan(table)`** returns a `TableScan` pinned to a snapshot. On a step's first attempt that's the table's current snapshot, recorded in `reads`. A retry or resume of the same step gets the snapshot the first attempt read, so it sees the same data whatever was committed in between.
- **`ctx.append(table)`** and **`ctx.commit(table, ...)`** add summary properties to the snapshot they commit: `orca.pipeline`, `orca.run`, `orca.step`, `orca.partition`, `orca.attempt`, `orca.code` (the registry hash) and `orca.reads` (the `table@snapshot` pairs the attempt read). iceberg's `prepare_append` and `prepare_commit` already take `extra_summary`.
- **A commit is recognised on retry.** If an attempt committed and was killed before Postgres heard of it, the retry looks through the table's snapshots for one whose `orca.run`, `orca.step` and `orca.partition` match, and treats it as the step's commit instead of appending again. So a crash between the commit and the record doesn't load a month twice. Snapshots are walked newest first and only back to the run's start.
- **A step that commits to several tables** records each; one recognised commit doesn't imply the others happened, so each is checked. A step that commits to one table more than once numbers its commits (`orca.commit`), and each is recognised by its number.
- **Reads and commits are recorded as they happen**, each by its own insert from the attempt (`Ctx.recorder`), not when the attempt ends, so a process killed between its commit and its end still leaves both on record.
- **An in-memory run** (no database) has run 0: it pins reads across its own retries but recognises no commits, since every in-memory run would share that number.
- **A table read while it had no snapshot** stays empty for the step, even if a commit lands before a retry.
- **Where it lives.** `orca.tables` (`scan`, `append`, `commit`) imports iceberg, so only pipelines with tables build it. Its tests run in the `data` environment, with iceberg and its siblings from source as in dista. *Verified* by `tests/test_tables.mojo`.

## Failure and resume

- **A failed step** fails its run once its retries are used up. Its downstream steps are `skipped`; other partitions' steps that don't depend on it carry on, so one bad month doesn't stop the other eleven.
- **Retries** are per step: `retries=2` in the `Step` declaration, immediate in phase 1, with backoff in phase 2. `ctx.attempt` tells a step which attempt it is. Every attempt is a row in `attempts`, retries and resumes alike.
- **Resume** reruns every step that hasn't `succeeded`, in dependency order, with the recorded inputs and pinned reads. Steps that succeeded aren't run again; their outputs are read back from Postgres. A resume after a code change is refused unless `--allow-new-code`, which records the new hash on the attempts it makes.
- **Order within a run.** Steps run in dependency order; independent partitions of a `Mapped` step run one after another in the driver in phase 1, each free to use dista for parallelism inside. Running several partitions' steps at once moved to phase 2, with backfills: in one driver it would need a thread per partition, each with its own database connection, and dista's `Runtime` isn't safe to share between threads. Phase 2 gets the same effect with one run per partition, started `max_runs` at a time, each in its own process.

## Logs

With `ORCA_LOGS` set to a directory, each attempt writes its log to `$ORCA_LOGS/<pipeline>/<run>/<step>/<partition>/<attempt>.log` (`_run` for a gather step) and records the location in `attempts.log`, which `status <run>` shows. The file always exists: orca writes the attempt's start, its end, and its error if it failed; `ctx.log` adds the step's lines, each with a UTC timestamp, and prints them to stdout too. The step's own stdout and stderr aren't captured in phase 1, since steps run in the driver process. A run of a pipeline with tasks writes its dista timeline (Chrome's trace format) beside its logs as `timeline.json`. Logs in object storage (`s3://`) come with phase 5, where pods don't share a disk. *Verified* by `tests/logs.sh`.

## Testing

- **Unit tests** with `TestSuite`, as in the other tins: the DAG's order and edges, partition ranges, the codec round-trip of step outputs, cron parsing (phase 2).
- **Compile-fail tests**: `tests/compile_fail/*.mojo`, each a pipeline that must not compile, run by a script that checks the compiler rejects each one with the expected message.
- **State tests** run against a real Postgres started by a `with-pg-server.sh` like postgres.mojo's, with `postgresql` from conda-forge.
- **Kill tests**: `tests/kill.sh` starts a run, `kill -9`s it at a random step, resumes it, and checks the tables equal those of an uninterrupted run. Run with several seeds in CI, many locally, as dista's `chaos.sh` does.
- **Fuzzing** with mozz, as dista does (`pixi run -e fuzz fuzz`, 20,000 inputs per harness in CI): `parse_months`; step inputs decoded from arbitrary bytes, as resume reads them back, where accepted bytes must be one canonical encoding; `quote`, whose output must unescape to its input; and `Ctx.made`, against a reference predicate. The last one found that a summary *missing* a key matched an empty value, such as a gather step's partition; `made` now requires every key.
- **Chaos** (`tests/chaos.sh`): per seed, one run driven to the end through random `kill -9`s of the process running it (its dista head and workers go with it), Postgres restarts under it (`pg_ctl -m immediate`), two resumes racing to claim it, and steps failing at random. It must end with every step succeeded by exactly one attempt, none left running, no two attempts by different processes overlapping in time, the same total as an uninterrupted run, and no process left behind. Three seeds in CI, `CHAOS_SEEDS` for more (25 pass locally). `test-chaos-tables`, in the data environment, does the same to a pipeline that appends to Iceberg and fails right after committing, and checks one snapshot per month.
- **Postgres must keep `synchronous_commit` on.** The chaos test's restarts found that with it off, a crashed server loses transactions it had acknowledged, such as a run's creation, and the run vanishes. The test server keeps it on; a deployment must too.
- **The taxi gate** runs by hand: it downloads gigabytes.

## Phase 1: DAGs and runs

The vision's gate: the taxi load runs as a DAG (download, per-month load, compaction), and a run killed halfway resumes at the step that failed.

Stacked PRs of at most about 2,000 lines, each with its own gate:

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **This design.** | Agreed. | — |
| 2 | **Scaffold, steps and pipelines.** `pixi.toml` (stable and nightly envs, CI on ubuntu and macOS, format check), `Step`, `Mapped`, `Gather`, `Pipeline`, compile-time edge checks, the order, `describe`, and an in-memory executor with no state. | A three-step pipeline runs over three partitions in order; the compile-fail tests are rejected. | ~900 |
| 3 | **State in Postgres.** The schema and migrations; runs, steps and attempts; heartbeats; `run`, `resume`, `status`; step outputs through dista's codec. | `tests/kill.sh` on the toy pipeline: killed at a random step, resumed, every step ran to success exactly once. | ~1,200 |
| 4 | **Pinned reads and recognised commits.** `ctx.scan`, `ctx.append`, `ctx.commit`; `reads` and `commits`; snapshot summary properties; finding a commit on retry. | A step that's killed right after its commit, then resumed, leaves one snapshot, not two; a resumed step reads its pinned snapshot after another commit lands. | ~800 |
| 5 | **dista inside steps, and logs.** `tasks=` on the pipeline, the run's `Runtime` in `Ctx`, per-attempt logs and the run's timeline. (`max_parallel` moved to phase 2; see "Failure and resume".) | A step fans 100 dista tasks over 4 workers; each attempt's log is where `status` says. | ~600 |
| 6 | **The taxi pipeline.** `examples/taxi.mojo`: download each month's TLC file over HTTPS, load it (the two schemas cast onto one and out-of-month rows dropped, as taxibench's `load_table.py` does), append it to a month-partitioned table, then compact. | Phase 1's gate: 2023–2024 loads to 79,478,796 rows, the same as taxibench's table, and a run killed during the load resumes at the failed month. | ~700 |

What PR 6 needs from the tins, checked at PR 2:

- **Casting columns** (float64 to int32, the 2023 schema to the 2024 one). arrow-mlake has no cast kernel, so the load step casts the five columns itself, building new arrays from the old.
- **A compaction commit.** iceberg's `prepare_commit` takes the operation, a `FileChanges` of files added and removed, and `extra_summary`, so the compaction step builds a `replace` commit (`OP_REPLACE`) itself and commits it with `Table.commit_metadata`. No change to iceberg.
- **An HTTPS download to a file** through objectstore. A month is about 50 MB, so holding it in memory is fine for phase 1.

## Phase 2: schedules and partitions

Gate: a schedule fills each month's partition as it falls due, and a backfill over 2011–2024 runs with a bounded number at once.

- **Partition definitions** on the pipeline: `partitions=Monthly(start="2011-01")` or `Daily(...)`, so the scheduler knows the set of keys and which are due.
- **Cron**: five-field expressions with ranges, steps and lists, and a time zone. Time zones are read from the system's TZif files (`/usr/share/zoneinfo`), which is a small, documented format. A schedule says which partition a fire fills, e.g. "at 03:00 on the 2nd, the previous month".
- **The `orca` scheduler binary**: registers pipelines from their `describe`, ticks once a second, creates runs that are due, starts them as subprocesses, retries failed steps with exponential backoff and jitter, and enforces `max_runs` per pipeline by counting `running` rows in the same transaction that starts one.
- **Backfills**: one run per partition over a range, queued, started `max_runs` at a time. This is also how partitions run in parallel, in place of phase 1's `max_parallel`.
- **Missed fires** after the scheduler was down are caught up once each, not dropped and not fired many times; a pipeline can say `catchup=False`.

## Phase 3: the UI

Gate: a failed step is found, its logs read and the run resumed, without a terminal.

**Proposed: server-rendered HTML from flare, with a little JavaScript** for refreshing a running run's status. It keeps the footprint at one process with nothing to build, and every page is also the HTTP API's JSON at the same path with `Accept: application/json`, so the command line and the UI can't drift. The DAG is drawn server-side as SVG, laid out by layer from the order. The dista timeline is linked as `timeline.json` for Perfetto rather than drawn by orca.

Pages: pipelines; a pipeline with its DAG, schedule, next fires and recent runs; a run with each step and partition, status, timing and attempts; an attempt's log. Actions: start a run, resume, rerun from a step, backfill a range, cancel. No authentication in phase 3 (one team, trusted network); it binds to localhost unless told otherwise.

## Phase 4: lineage and sensors

Gate: committing a new snapshot to a source table rebuilds exactly the tables downstream of it.

- **Datasets declared on steps**: `reads=("raw.trips",)`, `writes=("nyc.trips",)`, so the graph across pipelines is known before anything runs. `ctx.scan` and `ctx.append` check that a step only touches what it declared.
- **Lineage queries** over `reads` and `commits`: from a table snapshot back to the run, the code and the inputs; from a table forward to everything built from it. In the UI as a graph.
- **Sensors**: the scheduler polls declared source tables' current snapshot (metadata only, a few requests per table) and starts the pipelines that read them when it changes.
- **"Rebuild downstream of this table"**: the set of pipelines and partitions reachable from it, run in dependency order.

## Phase 5: on Kubernetes

Gate: the taxi pipeline runs on a schedule on EC2, each run on its own cluster.

- **The scheduler as a Deployment** with one replica and Postgres beside it (or RDS).
- **A run as a dista cluster**: the scheduler creates a head, a worker set sized by the pipeline, and a Job for the driver, all labelled with the run ID, through the Kubernetes API over HTTPS with the pod's service-account token; and deletes them when the run ends. The run's pipeline image is named in its registration.
- **Logs and timelines in S3**, through `ORCA_LOGS=s3://...`.
- **Cluster start time** is measured here. If it dominates short runs, a warm pool of workers per pipeline image is the next step, not before.

## Answers proposed to the vision's open questions

| Question | Proposed | Why |
|---|---|---|
| How a DAG is declared | Compile-time, `Step` and `Pipeline` types | It's the only way an edge's types are checked by the compiler, and it's what dista does. Composition comes from pipelines sharing step declarations from a module. |
| Where step code runs | In the pipeline's own binary, as the run's dista driver and workers; the scheduler is a separate generic binary | The scheduler links no pipeline's dependencies, and runs survive a scheduler restart. |
| The UI's stack | Server-rendered HTML from flare, a little JavaScript, JSON at the same paths | One process, nothing to build, API and UI from the same handlers. |
| Cluster start time | Local runtime per run until phase 5, then measure; warm pool only if needed | No data yet. |
| Restate | Not needed: runs are processes that record their own progress in Postgres, and the scheduler is restartable from Postgres alone | Revisit if orca grows long-lived workflows. |

## Defaults to confirm

| Question | Default | Alternative |
|---|---|---|
| Lineage edges and summary properties | From phase 1, because resume needs them | Phase 4, as the vision orders it, with resume trusting Postgres alone |
| A failed partition | Fails the run but lets independent partitions finish | Stops the run at the first failure |
| Resume after a code change | Refused unless `--allow-new-code` | Always allowed, recording the new hash |
| Postgres connection | `ORCA_DATABASE_URL`, one connection per process | A pool from postgres.mojo from the start |
