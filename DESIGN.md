# orca: design

November 1, 2026 · targets stable Mojo 1.2.0

This turns [VISION.md](VISION.md) into a buildable plan. It covers phase 1 in detail, sketches phases 2 to 5, and proposes answers to the vision's open questions. Everything marked *verified* was compiled and run on Mojo 1.2.0; every other claim about what Mojo accepts is checked in the PR that first needs it, and this file is corrected when one turns out wrong.

## What orca builds on

| Tin | What orca uses | State today |
|---|---|---|
| dista | `Runtime[*Ts]` in local mode (forked head and workers) per run; `Task`, `ObjectRef`, `Seq`; registry hash; the timeline | Phases 1 to 3 merged: actors, checkpoints, shuffles |
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
| `taxi fires [<count>] [--after <time>]` | From phase 2: the schedule's next fires, each with the partition it fills |

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

`reads` and `commits` are the lineage table the vision puts in phase 4; they're written from phase 1 because resuming needs them. Phase 4 adds dataset names, the queries and the UI over them.

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
- **Retries** are per step: `retries=2` in the `Step` declaration, immediate. Phase 2 retries a failed *run* with backoff, by resuming it. `ctx.attempt` tells a step which attempt it is. Every attempt is a row in `attempts`, retries and resumes alike.
- **What dista recovers, and what's left to orca.** A dista worker that dies has the tasks it was running sent to another worker, up to `DISTA_TASK_RETRIES` times (default 3). dista doesn't rebuild results lost with a worker (Ray's lineage reconstruction is out of scope there), so a step whose dista work needed one fails, with `result <id> lost: its worker went away`, as does a step whose local runtime has no workers left (`no workers left`). Either failure is an ordinary step error: the step's own retries run it again, and past them the run fails and the scheduler retries the run with backoff, as a new process with a fresh runtime. What makes running a step again safe is already here: its reads are pinned to the first attempt's snapshots, and a commit the earlier attempt made is recognised rather than made twice (see "Snapshots"). A step's own side effects outside its tables must be safe to repeat too.
- **A local runtime doesn't replace workers.** One runtime serves a run from its first step to its last, and in local mode a worker that dies stays dead, so a step retried in the same run has fewer workers. A run that loses them all fails its next steps quickly instead of hanging, and its retry, a new process, starts with a full set. On a cluster (`DISTA_HEAD` set) Kubernetes restarts the pods. *Verified* by `tests/lost_workers.sh`: with one of four workers killed mid-step the run succeeds with an undisturbed run's total; with all four killed the step fails, its retry fails at once with `no workers left`, and the run, resumed as a new process, succeeds with the same total.
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
- **The taxi gate** runs by hand: it downloads 1.2 GB. CI runs the pipeline on two real months sliced to about 500 rows each (`tests/fixtures/taxi`, written by `tests/taxi_fixtures.mojo`): a run, and a run whose download fails, resumed.

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
| 6 | **The taxi pipeline.** `examples/taxi/`: download each month's TLC file over HTTPS, load it (the two schemas cast onto one and out-of-month rows dropped, as taxibench's `load_table.py` does), append it to a month-partitioned table, then compact. | Phase 1's gate: 2023–2024 loads to 79,478,796 rows, the same as taxibench's table, and a run killed during the load resumes at the failed month. | ~700 |

What PR 6 needs from the tins, checked at PR 2, and what it turned out to need:

- **Casting columns** (float64 to int32, the 2023 schema to the 2024 one). arrow-mlake has no cast kernel, but none was needed: iceberg's writer casts every column to the table's type as it writes (`align_batch`, through `cast_column`), truncating a double to an int as taxibench's `safe=False` cast does. The load step only renames `Airport_fee` to `airport_fee` and drops the out-of-month rows.
- **A compaction commit.** iceberg's `prepare_commit` takes the operation, a `FileChanges` of files added and removed, and `extra_summary`, so the compaction step builds a `replace` commit (`OP_REPLACE`) through `orca.tables.commit`. No change to iceberg. iceberg has no compaction of its own, so the step reads each month's files, concatenates them and writes one. `orca.tables.find_commit` lets it check for an earlier attempt's commit before writing anything.
- **An HTTPS download to a file** through objectstore's `HttpClient`. A month is about 50 MB, so holding it in memory is fine for phase 1.

## Phase 2: schedules and partitions

Gate: a schedule fills each month's partition as it falls due, and a backfill over 2011–2024 runs with a bounded number at once.

- **Partition definitions** on the pipeline: `partitions=Monthly["2011-01"]` or `Daily["2024-01-01"]`, so the scheduler knows the set of keys and which are due.
- **Cron**: five-field expressions with ranges, steps and lists, and a time zone. Time zones are read from the system's TZif files (`/usr/share/zoneinfo`), which is a small, documented format. A schedule says which partition a fire fills, e.g. "at 03:00 on the 2nd, the previous month".
- **The `orca` scheduler binary**: registers pipelines from their `describe`, ticks once a second, creates runs that are due, starts them as subprocesses, retries failed runs with exponential backoff and jitter, and enforces `max_runs` per pipeline by counting `running` rows in the same transaction that starts one.
- **Backfills**: one run per partition over a range, queued, started `max_runs` at a time. This is also how partitions run in parallel, in place of phase 1's `max_parallel`.
- **Missed fires** after the scheduler was down are caught up once each, not dropped and not fired many times; a pipeline can say `catchup=False`.

### How it fits together

- **A partition key is a type.** `Month` and a new `Day` share a `Key` trait (parse, write, next, compare). `Monthly[start]` and `Daily[start]` are partition sets over them, with the range syntax the command line already takes (`2024-01..2024-03`, comma lists). `Pipeline` takes `partitions=`, and its first step must take that key (a `Mapped` step) or a `Seq` of it (a `Gather` step), checked at compile time like any other edge. A pipeline that leaves it out is `Monthly` with no start, as in phase 1, and can't be scheduled or backfilled from the beginning.
- **A schedule is declared with the pipeline**: `schedule=Cron["0 3 2 * *", "America/New_York"]` and `fills=Previous` (the key before the one the fire falls in) or `Current`. `describe` prints the partitions and the schedule, so the scheduler learns both without linking the pipeline.
- **Registration.** `orca register ./build/taxi` runs `taxi describe` and stores the binary's path with the DAG in `pipelines` (`binary_path`, and `since`, when its schedule starts counting). The scheduler reads the pipelines it schedules from Postgres, and their schedules, partitions and limits from the stored JSON in SQL; there is no configuration file and no JSON parser. Registering a new build moves the pipeline's queued runs to its registry hash.
- **A fire is a row.** `fires` (pipeline, fire time, partition, run) has a unique (pipeline, fire time), and the scheduler inserts the fire and its `queued` run in one transaction, so a scheduler killed and restarted mid-tick doesn't fire twice. Backfills insert queued runs the same way, with trigger `backfill`.
- **Starting a run is `resume`.** A queued run has nothing done and no heartbeat, which is exactly what `taxi resume <run>` already takes. The scheduler forks and executes the binary (`fork` and `execv` through FFI, as dista starts its local processes) and doesn't wait on it beyond reaping it: the run records its own progress, and a run whose process dies is found by its stale heartbeat.
- **`max_runs`**: the scheduler starts a queued run only if, in the same transaction and under a lock on the pipeline's row, fewer than `max_runs` of the pipeline's runs are live. It marks a run `running` with itself as owner and no heartbeat when it starts it, and the process's claim takes it from there. A run is live while its heartbeat is fresh, or, before the claim, for the stale limit after the scheduler started it; after that its process is taken to have died.
- **Retrying a failed run** is resuming it again, up to `run_retries` times (3 by default), after `ORCA_RETRY_SECONDS` (60) doubling with each start, plus up to a tenth more as jitter. A run whose process died is started again at once, within the same budget. Only scheduled and backfilled runs are retried. Step retries inside a run stay immediate.
- **Missed fires**: on each tick the scheduler computes the fires between a schedule's last recorded fire and now. With `catchup=True` (the default) each becomes a queued run, at most `max_runs` running at once; with `catchup=False` only the latest does.
- **Time in tests.** `ORCA_NOW` (`2024-03-10T07:00Z`, or epoch seconds) fixes the clock of `fires` and the scheduler, and `orca tick` runs one tick and exits, so the tests step time forward deterministically.
- **Daylight saving.** A cron time that a spring-forward skips fires at the first minute after the gap; one that a fall-back repeats fires once, at its first occurrence. That's what Vixie cron and systemd timers do.
- **The time zone rules** come from TZif version 2 and later: the 64-bit transition times, then the footer's POSIX `TZ` string (such as `EST5EDT,M3.2.0,M11.1.0`) for times after the last transition, which "slim" TZif files rely on for every year since 2007.

### PRs

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **Partition keys and sets.** The `Key` trait, `Day`, `Monthly`, `Daily`, `partitions=` on `Pipeline`, the first step's input checked against the key, `describe` with partitions, the command line parsing keys of the pipeline's type. | A daily toy pipeline runs over a range of days; a pipeline whose first step takes a `Month` under `Daily` doesn't compile; phase 1's tests pass unchanged. | ~600 |
| 2 | **Cron and time zones.** Cron parsing and next-fire computation in a time zone; a TZif reader with POSIX `TZ` rules; `schedule=`, `fills=` and `catchup=` on `Pipeline`, in `describe`. | Next fires match a table of known ones, including both daylight-saving transitions in `America/New_York` and `Europe/London`; the cron parser and the `TZ` string parser are fuzzed. | ~1,000 |
| 3 | **The scheduler.** The `orca` binary: `register`, `tick`, `scheduler` (tick once a second), `backfill`; `fires`, queued runs, starting runs as subprocesses, `max_runs`, run retries with backoff, missed fires and `catchup`. | With `ORCA_NOW` stepped month by month, the toy pipeline's partitions are filled once each, as they fall due; a scheduler killed mid-tick and restarted fires nothing twice; a backfill of 24 months never has more than `max_runs` running. | ~1,200 |
| 4 | **The taxi backfill.** The taxi pipeline gets `partitions=Monthly["2011-01"]` and a monthly schedule; the load handles the 2011–2022 files' schemas. | Phase 2's gate, by hand: `orca backfill taxi 2011-01..2024-12` with `max_runs=4` loads every month once, and the schedule then fills the next month when it falls due (with `ORCA_NOW`). | ~300 |

### How the gate went

Done by hand on one Mac (10 cores, 24 GB) from the 168 TLC files, 19 GB, already downloaded: `orca register ./build/taxi`, `orca backfill taxi 2011-01..2024-12`, then `orca scheduler` with `ORCA_NOW` fixed at 2025-01-15. All 168 runs succeeded on their first start, never more than 4 running, in 11 minutes, with 336 commits (a load and a compaction per month). The table holds 1,438,321,685 rows for 2011–2024, one file per month; its 2023–2024 months are still phase 1's 79,478,796. A tick at 05:59 on 2 February 2025 fired nothing; at 06:00 it fired once, for 2025-01, and that run downloaded the month from the TLC and loaded its 3,475,204 rows.

What it took:

- **The 2011–2022 schemas.** Every file from 2011 has the same 19 columns; only the types move. Before 2019 `congestion_surcharge` and before 2021 `airport_fee` are null, and a file whose column is all null types it as int32, not double. iceberg's writer already casts to the table's type, and an all-null int32 column stays null, so the load needed no change.
- **Each run compacts only its months.** Phase 1's compaction rewrote every month with more than one file; with four runs loading at once it would rewrite the others' half-loaded months. It now rewrites the months its own loads loaded.
- **Seeding `std.random` in each process.** The first attempt at the gate failed compactions with "none of the files this commit removes is in the current snapshot". iceberg draws snapshot IDs and file UUIDs from `std.random` and never seeds it, so every pipeline process drew the same sequence: concurrent runs committed snapshots with the same IDs and wrote each other's `snap-<id>` manifest lists. `orca_main` now seeds it from the clock and the process ID, and `tests/taxi.sh` commits from two runs at once and checks the snapshot IDs and manifest lists are distinct; without the seed it fails. iceberg should seed it itself.
- **Memory.** A 2011 month is 14 to 16 million rows in one row group. Loading one peaks at about 4 GB and compacting it at about 6 GB, mostly iceberg's writer copying the batch to align it and again per partition; four at once peaked at 15 GB. Streaming the compaction's reads didn't change that, so it's left to iceberg's writer.
- **The schedule** fires at 06:00 UTC on the 2nd for the month before. The TLC publishes a month about two months after it ends, so in real time that fire fails its download and is retried with backoff until its budget is spent; a backfill of the month once it's out loads it. A fill with a lag is left for later.

## Phase 3: the UI

Gate: a failed step is found, its logs read and the run resumed, without a terminal.

**Decided: server-rendered HTML from flare, with a little JavaScript for the live parts.** It keeps the footprint at one process with nothing to build: no Node toolchain, no bundler, no client-side state. Every page is also the API: the same path answers JSON to `Accept: application/json` (or `?format=json`), so the command line, scripts and the UI read one thing and can't drift. A single-page app can be evaluated later, if a view needs more than this gives it (a zoomable DAG, a live Gantt chart); the JSON paths are what it would be built on.

### How it fits together

- **`orca ui`** is a command of the `orca` binary, not a new binary: it needs `ORCA_DATABASE_URL` like the others, reads the tables the scheduler and the runs write, and binds `127.0.0.1:8270` unless `ORCA_UI_ADDR` says otherwise. It holds no state of its own, so it can run beside the scheduler, or several of it at once.
- **flare serves it**: its `Router` for paths, one reactor thread (`num_workers=1`) to start with. A request runs a few short queries against indexed columns, so it doesn't hold the loop for long; idle and write timeouts are set for the work rather than left at flare's web defaults (see flare's silent-reap history). Postgres is reached through one connection per request at first, measured, and pooled only if that shows.
- **HTML is written in Mojo**, through a small `orca.html` writer that escapes every value it's given and only writes raw markup it's told is markup. flare's template engine was looked at and isn't used: its context is strings and lists of strings, and a run page is a table of records.
- **The page skeleton**: one layout with a header (pipelines, runs) and one stylesheet with magmalake.dev's colours and type, light and dark. Nothing is fetched from elsewhere, so the UI works without the internet.
- **Live parts are polled fragments**: a running run's DAG and steps, and a running attempt's log, sit in an element with `data-poll` naming a fragment path that returns only that piece of HTML. Twelve lines of script in the layout replace it every two seconds until the fragment answers 286 (htmx's "stop polling"), which it does once what it shows has ended. htmx was the plan; for this one pattern it would have been 50 KB to embed in the binary. Forms are plain HTML forms, so the actions need no script either. Server-sent events are left for later; flare has them, but polling a few fragments is simpler and plenty for one team.
- **The DAG is drawn server-side as SVG**: steps laid out left to right by their order, mapped steps drawn as a stack, edges from `after`, each box coloured by the status of its partitions in the run being shown. A pipeline is a chain today, so the layout is a row; it's written for a DAG, as phase 4's datasets may branch.
- **The Dista timeline** is served from the run's log directory as `timeline.json`, linked for opening in Perfetto, rather than drawn by orca. The directory is found from the run's attempts' log paths, so the UI needn't share the runs' `ORCA_LOGS`; a relative `ORCA_LOGS` is relative to where the run was started, which the UI must share.
- **Actions go through Postgres, never through the UI's process.** The UI doesn't start pipeline binaries: it writes a row and the scheduler, which already starts and limits runs, acts on it. So `max_runs`, retries and ownership stay in one place, and the UI works the same whether the scheduler is on this machine or not.
  - **Start a run**: a queued run over the partitions entered, trigger `ui`.
  - **Resume** a failed, cancelled or stalled run (`running`, with a heartbeat older than the stale limit): it's queued again (`retry_at` cleared), and the scheduler resumes it as it does a retry. A resume of a run whose code has changed asks for confirmation, which sets `allow_new_code` on the run, and the scheduler passes `--allow-new-code`.
  - **Rerun from a step**: a new run, trigger `rerun`, over the same partitions, with the steps before the chosen one that succeeded copied from the old run (their inputs and outputs, and `copied_from` naming the step each came from, whose attempts, reads and commits stay with it), so `resume` runs the chosen step and what follows. The old run is left as it was. A run whose pipeline's code has changed since can only be rerun from the first step, as the old code's outputs may not be what the new code reads.
  - **Backfill a range**: what `orca backfill` does.
  - **Cancel**: a queued or failed run is `cancelled` at once (a failed one is then not retried). A running one gets `cancel_requested`: the run checks it where it already checks its ownership, before each attempt, and ends `cancelled`; if its process has died, the scheduler ends it `cancelled` instead of starting it again. A step already running finishes its attempt: cancelling doesn't kill it mid-commit.
- **An action answers `303 See Other`** to the page of the run it made or acted on (or, to `Accept: application/json`, that run's ID). One that can't be done (a run still going, a range with a bad key) answers 409 with the reason, on a page with a way back; a resume onto new code answers 409 with the form that confirms it.
- **No authentication in phase 3** (one team, a trusted network). Because the UI is on localhost and its actions change state, every `POST` checks that `Origin` (or, without it, `Referer`) is the UI's own, so a page on another site can't make the browser post to it. Nothing changes state on a `GET`.

### Pages

| Path | What it shows |
|---|---|
| `/` | Pipelines: each with its schedule, next fire, last run and its status, and runs live now against `max_runs`. |
| `/pipelines/<name>` | The DAG, partitions and schedule, the next fires, the latest runs, and the forms to start a run and backfill a range. |
| `/runs` | Runs across pipelines, newest first, filterable by pipeline, status and trigger. |
| `/runs/<id>` | The run: each step and partition with status, timing and attempts, the error of a failed one, its reads and commits, the DAG in this run's colours, and resume, rerun from a step and cancel. |
| `/runs/<id>/attempts/<attempt>` | An attempt's log (its last MiB), followed while it's running, with its error at the top when it failed, and the step's other attempts. |
| `/runs/<id>/timeline.json` | The run's Dista timeline, for Perfetto. |

The actions are `POST`s of forms: `/pipelines/<name>/runs` and `/pipelines/<name>/backfill` (`partitions`), and `/runs/<id>/resume` (`allow_new_code`), `/runs/<id>/rerun` (`step`) and `/runs/<id>/cancel`. `tests/actions.sh` is the gate.

### PRs

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **Read-only pages and their JSON.** `orca ui` on flare; `orca.html`; the layout and stylesheet; `/`, `/pipelines/<name>`, `/runs` and `/runs/<id>` as HTML and JSON. The first commit builds flare into the `orca` binary, which shares `fork`, `waitpid`, `getpid`, `clock_gettime` and `_exit` with it: Mojo refuses two bindings of one symbol with different signatures, so orca's are matched to flare's. | `tests/ui.sh`: with the toy's runs in Postgres, every page answers HTML and JSON; the JSON of a run agrees with `toy status <run>`; every value written into a page is escaped (a pipeline step's error with `<script>` in it comes out as text). | ~1,100 |
| 2 | **Logs, the DAG and live updates.** The attempt page and its followed tail, the SVG DAG on the pipeline and run pages, the fragments a running run polls, `timeline.json` and the Perfetto link. | A running toy run's page, fetched as its fragment while the run goes on, shows the steps move from running to succeeded and stops polling at the end; an attempt's log page shows lines as the step writes them; the SVG of the daily and toy pipelines has a box per step and an edge per `after`. | ~800 |
| 3 | **Actions.** Start, resume, rerun from a step, backfill and cancel, as rows the scheduler acts on; `cancel_requested` and the `cancelled` status (a migration); the `Origin` check. | Phase 3's gate, as a test that only speaks HTTP: a toy run fails a step (`ORCA_TOY_FAIL`), the test finds it on `/runs`, reads the failed attempt's log, posts resume, and with the scheduler running the run succeeds. Also: rerun from `measure` reruns `measure` and `total` and not `days`; a cancelled run ends `cancelled` before its next step; a cross-origin `POST` is refused. | ~900 |

## Phase 4: lineage and sensors

Gate: committing a new snapshot to a source table rebuilds exactly the tables downstream of it.

Phases 1 to 3 already record every table read and commit: `reads` and `commits` in Postgres, and `orca.*` properties in each committed snapshot's summary. What's missing is a name for a table that means the same thing in every pipeline, the graph across pipelines that those names make, queries over it, and runs started by it. A pipeline in phase 4 still runs by itself; what changes is that orca knows which pipelines feed which.

### How it fits together

- **A dataset is a name, `namespace.table`**, such as `nyc.trips`. Today a table is known by its location, which depends on where the warehouse is (a local path, later `s3://`). A name doesn't, so two pipelines that name `nyc.trips` mean the same table in every deployment. `ORCA_WAREHOUSE` is the root of a filesystem catalog (iceberg's `catalog.filesystem`): `nyc.trips` is `<warehouse>/nyc/trips`. Phase 5 points it at S3; a REST catalog can come later behind the same names.
- **A step declares what it reads and writes**: `Step[..., reads=["raw.trips"], writes=["nyc.trips"]]`. `ctx.table("nyc.trips")` loads a declared dataset from the warehouse. `scan` raises for a dataset the step didn't declare in `reads`, and `append` and `commit` for one it didn't declare in `writes`, naming the step and the dataset. So the graph is known before anything runs, and it can't drift from what the steps do. A step may read and write one dataset (the taxi compaction does). A table that isn't in the warehouse, such as a test's temporary table, can still be scanned and appended to by location, as now, and isn't part of the graph.
- **`describe` lists each step's `reads` and `writes`**, so `orca register` knows every pipeline's datasets without linking it. Registration refuses a pipeline that would close a cycle across pipelines: A writes X, B reads X and writes Y, and A reads Y. A pipeline that reads and writes one dataset is not a cycle.
- **`reads` and `commits` gain a `dataset` column** (a migration), null for a table outside the warehouse. Lineage queries join on it; the location stays, as it says where the snapshot is.
- **Lineage is two walks** over `commits` and `reads`, in SQL (recursive CTEs):
  - **Back** from a snapshot: the attempt that committed it, its step, run, partition and code, the snapshots that attempt read, and, for each of those, the attempt that committed it, back to snapshots no orca run made (a source).
  - **Forward** from a snapshot: every attempt that read it, what each committed, and so on.
  
  Both are bounded by a depth (default 20) and return edges, so a deep graph is cut, not slow. They're checked against the snapshots' own `orca.reads` summaries, which carry the same edges in the tables themselves.
- **Partitions across pipelines.** A sensor or a rebuild has to say which partitions of the downstream pipeline to run. A snapshot an orca run committed says which partitions it holds: a mapped step's commit has `orca.partition`, and a gather step's commit gains `orca.partitions`, the run's keys. A snapshot committed by something else carries no keys; its writer can set `orca.partitions` in its summary, and without it the downstream pipeline runs the partition its schedule would fill now (its `fills` partition), or, unscheduled, its latest partition by the clock. Keys move between partition kinds as they contain each other: a day upstream is the month that contains it downstream, and a month upstream is each of its days downstream.
- **Sensors are opt-in per pipeline**: `Pipeline[..., sensor=True]` runs it when a dataset it reads gets a new snapshot. There are two ways the scheduler learns of one:
  - **From Postgres, for snapshots orca made.** A succeeded run's commits are in `commits`, so the scheduler sees them on its next tick, without reading any table.
  - **By polling, for snapshots something else made.** The scheduler links no iceberg, so it runs `<binary> sense`, a new command of every pipeline binary, every `ORCA_SENSE_SECONDS` (default 60). It prints the current snapshot ID of each dataset the pipeline reads: one metadata read per dataset.
  
  `orca.sensed` records, per pipeline and dataset, the last snapshot that started a run, so each new snapshot starts one run once, as a fire does. The run's trigger is `sensor`, and it records the snapshot that started it. A run is started only for a snapshot committed after the pipeline was registered, so registering doesn't rebuild history; a backfill does that.
- **"Rebuild downstream of this table"** queues runs for every pipeline reachable from a dataset in the graph, over the partitions given, in dependency order. Each pipeline is placed at its longest distance from the dataset, so one that reads two rebuilt datasets runs after both. A rebuild is a row (`orca.rebuilds`), and its runs carry its ID. The scheduler queues the pipelines at the next distance once every run at the ones before has succeeded, and stops if one fails, so it can be resumed from the UI as a run is. It's in the UI on a dataset's page and on the command line as `orca rebuild <dataset> KEYS`.
- **In the UI:**
  - **`/datasets`** lists every dataset with its writers, readers and latest snapshot.
  - **`/datasets/<name>`** shows the graph of pipelines and datasets around it (drawn server-side as SVG, like the DAG), its snapshots, newest first, with the run that made each, and the rebuild form.
  - **`/datasets/<name>/snapshots/<id>`** shows the lineage back and forward, as a graph and as a table.
  
  A run's page links the datasets it read and committed.

### PRs

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **Datasets on steps.** `reads=` and `writes=` on `Step`; `ORCA_WAREHOUSE` and `ctx.table`; the checks in `scan`, `append` and `commit`; `dataset` on `reads` and `commits`; `orca.partitions` on a gather step's commits; datasets in `describe`; the cycle check at registration; the taxi pipeline moved to `nyc.trips`. | A step that scans a dataset it didn't declare fails its attempt with an error naming both; `describe` lists each step's datasets; registering a pipeline that closes a cycle is refused; the table tests and the taxi test pass with datasets. | ~700 |
| 2 | **Lineage.** The two walks in SQL; `orca lineage <dataset> [<snapshot>]`; the dataset pages and the dataset graph; run pages linking their datasets. | Three small pipelines over warehouse tables, `ingest` → `clean` → `report`: the lineage back from a `report` snapshot names its run, its code and the `clean` snapshot it read, and that one's `ingest` snapshot, and agrees with the snapshots' `orca.reads` summaries; forward from the `ingest` snapshot reaches the `report` snapshot. | ~900 |
| 3 | **Sensors and rebuild.** `sensor=True`; `<binary> sense`; the scheduler's sensor pass, from `commits` and by polling; `orca.sensed`; partition mapping; `orca.rebuilds` and its levels; `orca rebuild`, and the rebuild form in the UI. | Phase 4's gate: with the scheduler running, a snapshot committed to `raw.events` by a program that isn't an orca pipeline starts `clean`, whose commit starts `report`, and nothing else runs; `orca rebuild raw.events` reruns both, `report` after `clean`; a pipeline reading an unrelated dataset never runs; each snapshot starts one run however many ticks see it. | ~1,000 |

These tests need iceberg, so they run in the `data` environment, as the table tests do.

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
