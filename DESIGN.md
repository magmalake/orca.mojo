# orca: design

November 1, 2026 · targets stable Mojo 1.2.0

This turns [VISION.md](VISION.md) into a buildable plan. It covers phase 1 in detail, sketches phases 2 to 6 and keyed partitions, and proposes answers to the vision's open questions. Everything marked *verified* was compiled and run on Mojo 1.2.0; every other claim about what Mojo accepts is checked in the PR that first needs it, and this file is corrected when one turns out wrong.

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
| `runs` | id, pipeline, registry hash, partitions, trigger (manual, schedule, backfill, sensor), status, created, started, ended, heartbeat, and an error when no step's says why it ended |
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
- **`ctx.append(table)`** and **`ctx.commit(table, ...)`** add summary properties to the snapshot they commit: `orca.pipeline`, `orca.run`, `orca.step`, `orca.partition`, `orca.attempt`, `orca.code` (the registry hash), `orca.partitions` (the keys the commit holds: its partition for a mapped step, the run's for a gather step; from phase 4) and `orca.reads` (the `table@snapshot` pairs the attempt read). iceberg's `prepare_append` and `prepare_commit` already take `extra_summary`.
- **A commit is recognised on retry.** If an attempt committed and was killed before Postgres heard of it, the retry looks through the table's snapshots for one whose `orca.run`, `orca.step` and `orca.partition` match, and treats it as the step's commit instead of appending again. So a crash between the commit and the record doesn't load a month twice. Snapshots are walked newest first and only back to the run's start.
- **A step that commits to several tables** records each; one recognised commit doesn't imply the others happened, so each is checked. A step that commits to one table more than once numbers its commits (`orca.commit`), and each is recognised by its number.
- **Reads and commits are recorded as they happen**, each by its own insert from the attempt (`Ctx.recorder`), not when the attempt ends, so a process killed between its commit and its end still leaves both on record.
- **An in-memory run** (no database) has run 0: it pins reads across its own retries but recognises no commits, since every in-memory run would share that number.
- **A table read while it had no snapshot** stays empty for the step, even if a commit lands before a retry.
- **Writers racing on one table.** Two commits on the same metadata version both back out (iceberg's filesystem catalog deletes both files), so `append` and `commit` reload and retry after a random wait that grows with each try, up to 8 times; retrying at once, writers kept clashing in step. *Verified* by `tests/concurrent_commits.sh`: six processes appending to a new table at once, five times, all commit.
- **Where it lives.** `orca.tables` (`scan`, `append`, `commit`) imports iceberg, so only pipelines with tables build it. Its tests run in the `data` environment, with iceberg and its siblings from source as in dista. *Verified* by `tests/test_tables.mojo`.

## Failure and resume

- **A failed step** fails its run once its retries are used up. Its downstream steps are `skipped`; other partitions' steps that don't depend on it carry on, so one bad month doesn't stop the other eleven.
- **Retries** are per step: `retries=2` in the `Step` declaration, immediate. Phase 2 retries a failed *run* with backoff, by resuming it. `ctx.attempt` tells a step which attempt it is. Every attempt is a row in `attempts`, retries and resumes alike.
- **What dista recovers, and what's left to orca.** A dista worker that dies has the tasks it was running sent to another worker, up to `DISTA_TASK_RETRIES` times (default 3). dista doesn't rebuild results lost with a worker (Ray's lineage reconstruction is out of scope there), so a step whose dista work needed one fails, with `result <id> lost: its worker went away`, as does a step whose local runtime has no workers left (`no workers left`). Either failure is an ordinary step error: the step's own retries run it again, and past them the run fails and the scheduler retries the run with backoff, as a new process with a fresh runtime. What makes running a step again safe is already here: its reads are pinned to the first attempt's snapshots, and a commit the earlier attempt made is recognised rather than made twice (see "Snapshots"). A step's own side effects outside its tables must be safe to repeat too.
- **A blip of Postgres doesn't stop a run.** Every write a run's process makes (starting and finishing an attempt, a skip, a read or commit recorded, the run's end) is retried on a new connection, after a quarter of a second doubling to 4 s, for `ORCA_DB_RETRY_SECONDS` (default the stale limit, past which another process may take the run over anyway). A step that worked for an hour and then met a two-second restart is recorded, not redone. The ownership check is the one failure not retried: a run lost to another process, or cancelled, stops at once. Finishing an attempt sets its rows to what they'd be after one try, so a retry after a lost reply is harmless; one after a lost reply to *starting* an attempt leaves the first try's row `running`, which the next resume marks `interrupted`. A server that stays away longer stops the run as before, to be resumed. *Verified* by `tests/db_blip.sh`: the server restarted twice under a toy run, which succeeds with one attempt per step.
- **A local runtime doesn't replace workers.** One runtime serves a run from its first step to its last, and in local mode a worker that dies stays dead, so a step retried in the same run has fewer workers. A run that loses them all fails its next steps quickly instead of hanging, and its retry, a new process, starts with a full set. On a cluster (`DISTA_HEAD` set) Kubernetes restarts the pods. *Verified* by `tests/lost_workers.sh`: with one of four workers killed mid-step the run succeeds with an undisturbed run's total; with all four killed the step fails, its retry fails at once with `no workers left`, and the run, resumed as a new process, succeeds with the same total.
- **Resume** reruns every step that hasn't `succeeded`, in dependency order, with the recorded inputs and pinned reads. Steps that succeeded aren't run again; their outputs are read back from Postgres. A resume after a code change is refused unless `--allow-new-code`, which records the new hash on the attempts it makes.
- **Order within a run.** Steps run in dependency order; independent partitions of a `Mapped` step run one after another in the driver in phase 1, each free to use dista for parallelism inside. Running several partitions' steps at once moved to phase 2, with backfills: in one driver it would need a thread per partition, each with its own database connection, and dista's `Runtime` isn't safe to share between threads. Phase 2 gets the same effect with one run per partition, started `max_runs` at a time, each in its own process.

## Logs

With `ORCA_LOGS` set to a directory, each attempt writes its log to `$ORCA_LOGS/<pipeline>/<run>/<step>/<partition>/<attempt>.log` (`_run` for a gather step) and records the location in `attempts.log`, which `status <run>` shows. The file always exists: orca writes the attempt's start, its end, and its error if it failed; `ctx.log` adds the step's lines, each with a UTC timestamp, and prints them to stdout too. The step's own stdout and stderr aren't captured in phase 1, since steps run in the driver process. A run of a pipeline with tasks writes its dista timeline (Chrome's trace format) beside its logs as `timeline.json`. Since phase 5, `ORCA_LOGS` can be in object storage (`s3://`), where pods don't share a disk; see phase 5's PR 4. *Verified* by `tests/logs.sh`, and over MinIO by `test-lineage-s3`.

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
- **Registration.** `orca register ./build/taxi` runs `taxi describe` and stores the binary's path with the DAG in `pipelines` (`binary_path`, and `since`, when its schedule starts counting). The scheduler reads the pipelines it schedules from Postgres, and their schedules, partitions and limits from the stored JSON in SQL; there is no configuration file and no JSON parser. Registering a new build moves the pipeline's queued runs to its registry hash, and its failed runs waiting for a retry that have no step done. A retry of a run with steps done by other code would be refused by the binary's `resume`, so the scheduler fails it instead of starting it, with an error on the run (`runs.error`, shown by `<binary> status <run>` and the run's page) saying to resume it with the new code; a local run whose process exits before claiming it fails with the exit code and the last line it printed as its error. Only `orca register` moves a pipeline's code: `<binary> run` records a pipeline that isn't registered yet, but leaves a registered one's hash and `describe` alone, and refuses (exit code 2) a binary whose code isn't the registered one's unless it's given `--allow-new-code`; such a run counts as its own code's, so it makes nothing done for the pipeline. *Verified* by `tests/new_code.sh` and `tests/manual_run.sh`.
- **A fire is a row.** `fires` (pipeline, fire time, partition, run) has a unique (pipeline, fire time), and the scheduler inserts the fire and its `queued` run in one transaction, so a scheduler killed and restarted mid-tick doesn't fire twice. Backfills insert queued runs the same way, with trigger `backfill`.
- **One live run per partition.** Every path that queues a run (a fire, a sensor, a rebuild's level, the UI's start, a backfill) checks for a live run of the same keys, in the transaction that would queue it and under a lock on the pipeline's row, so two runs never run one partition at once and both append. A fire records the live run as its run, a sensor leaves the keys to it (and records it as the snapshot's run if no other key is left), a rebuild's level waits until it has ended, the UI refuses with the run's ID, and a backfill skips the key. A partition that's done is not protected: a later fire, sensor, rebuild or start runs it again, and its steps append again, so `orca register` warns of a schedule that fires more than once for one partition (`0 */6 * * *` on `Daily`). *Verified* by `tests/one_run_per_key.sh` and, for sensors and rebuilds, `tests/sensors.sh`.
- **Starting a run is `resume`.** A queued run has nothing done and no heartbeat, which is exactly what `taxi resume <run>` already takes. The scheduler forks and executes the binary (`fork` and `execv` through FFI, as dista starts its local processes) and doesn't wait on it beyond reaping it: the run records its own progress, and a run whose process dies is found by its stale heartbeat.
- **`max_runs`**: the scheduler starts a queued run only if, in the same transaction and under a lock on the pipeline's row, fewer than `max_runs` of the pipeline's runs are live. It marks a run `running` with itself as owner and no heartbeat when it starts it, and the process's claim takes it from there. A run is live while its heartbeat is fresh, or, before the claim, for the stale limit after the scheduler started it; after that its process is taken to have died.
- **Retrying a failed run** is resuming it again, up to `run_retries` times (3 by default), after `ORCA_RETRY_SECONDS` (60) doubling with each start, plus up to a tenth more as jitter. A run whose process died is started again at once, within the same budget. Only scheduled, backfilled and sensed runs are retried. A manual, UI or rerun run whose process died is marked `failed` (`interrupted: its process stopped`) by the next tick, so it can be resumed or cancelled; a run whose process died never holds its keys from a backfill or a keyed sensor, which queue them again. `orca backfill` prints on stderr the keys it left and why: done, or in live run N. *Verified* by `tests/dead_runs.sh`. Step retries inside a run stay immediate.
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

- **`orca ui`** is a command of the `orca` binary, not a new binary: it needs `ORCA_DATABASE_URL` like the others, reads the tables the scheduler and the runs write, and binds `127.0.0.1:8270` unless `ORCA_UI_ADDR` says otherwise. `ORCA_UI_LINKS` adds a deployment's own links to the nav after Docs, as comma-separated `label=url` entries (`Census ↗=https://census.mlake.dev`); labels and urls are escaped and only http(s) urls are kept. It holds no state of its own, so it can run beside the scheduler, or several of it at once.
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
  - **Backfill a range**: what `orca backfill` does; with its checkbox checked, what `orca backfill --rerun` does.
  - **Cancel**: a queued or failed run is `cancelled` at once (a failed one is then not retried). A running one gets `cancel_requested`: the run checks it where it already checks its ownership, before each attempt, and ends `cancelled`; if its process has died, the scheduler ends it `cancelled` instead of starting it again. A step already running finishes its attempt: cancelling doesn't kill it mid-commit.
  - **Cancel in bulk**: what `orca cancel <pipeline> KEYS|@all` does, from the pipeline page (`partitions`, empty for all): each of the pipeline's runs over any of the keys (a run of several keys whole) that is queued, running, or failed and due a retry is cancelled as above. A failed run with no retries left, and one that ended, are left as they are. Added after a 31-run backfill had to be cancelled in SQL.
  - **Pause**: `orca pause <pipeline>` (or the page's button) sets `pipelines.paused` (migration 16), and the scheduler starts none of the pipeline's runs: fires, sensors, retries and backfills still queue theirs, which wait, and runs already going go on. `orca unpause` lets the scheduler start them on its next tick. A paused pipeline says so on `/` and its page, and in their JSON (`paused`).
- **An action answers `303 See Other`** to the page of the run it made or acted on (or, to `Accept: application/json`, that run's ID). One that can't be done (a run still going, a range with a bad key) answers 409 with the reason, on a page with a way back; a resume onto new code answers 409 with the form that confirms it.
- **No authentication in phase 3** (one team, a trusted network). Because the UI is on localhost and its actions change state, every `POST` checks that `Origin` (or, without it, `Referer`) is the UI's own, so a page on another site can't make the browser post to it. The UI's own origin is `Host` with the scheme the browser used: `X-Forwarded-Proto` behind a proxy that terminates TLS (`ORCA_UI_TRUST_PROXY=1`, as `orca aws up` deploys Caddy), else `https` with `ORCA_UI_SECURE_COOKIES=1`, else `http`; scheme and host compare in any case, and a default port is left out. Nothing changes state on a `GET`.

### Pages

| Path | What it shows |
|---|---|
| `/` | Pipelines: each with its schedule, next fire, last run and its status, and runs live now against `max_runs`. |
| `/pipelines/<name>` | The DAG, partitions and schedule, the next fires, the latest runs, and the forms to start a run, backfill a range, cancel its runs and pause it. |
| `/runs` | Runs across pipelines, newest first, filterable by pipeline, status and trigger. |
| `/runs/<id>` | The run: each step and partition with status, timing and attempts, the error of a failed one, its reads and commits, the DAG in this run's colours, and resume, rerun from a step and cancel. |
| `/runs/<id>/attempts/<attempt>` | An attempt's log (its last MiB), followed while it's running, with its error at the top when it failed, and the step's other attempts. |
| `/runs/<id>/timeline.json` | The run's Dista timeline, for Perfetto. |

The actions are `POST`s of forms: `/pipelines/<name>/runs`, `/pipelines/<name>/backfill` and `/pipelines/<name>/cancel` (`partitions`), `/pipelines/<name>/pause` and `/unpause`, and `/runs/<id>/resume` (`allow_new_code`), `/runs/<id>/rerun` (`step`) and `/runs/<id>/cancel`. `tests/actions.sh` is the gate, and `tests/pause.sh` for cancelling in bulk and pausing.

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
| 1 | **Datasets on steps.** `reads=` and `writes=` on `Step`; `ORCA_WAREHOUSE` and `ctx.table`; the checks in `scan`, `append` and `commit`; `dataset` on `reads` and `commits`; `orca.partitions` on a gather step's commits; datasets in `describe`; the cycle check at registration; the taxi pipeline declaring `taxi.trips`, the name its table already had in the warehouse, so a warehouse loaded before stays valid. | A step that scans a dataset it didn't declare fails its attempt with an error naming both; `describe` lists each step's datasets; registering a pipeline that closes a cycle is refused; the table tests and the taxi test pass with datasets. | ~700 |
| 2 | **Lineage.** The two walks in SQL; `orca lineage <dataset> [<snapshot>]`; the dataset pages and the dataset graph; run pages linking their datasets. | Three small pipelines over warehouse tables, `ingest` → `clean` → `report`: the lineage back from a `report` snapshot names its run, its code and the `clean` snapshot it read, and that one's `ingest` snapshot, and agrees with the snapshots' `orca.reads` summaries; forward from the `ingest` snapshot reaches the `report` snapshot. | ~900 |
| 3 | **Sensors and rebuild.** `sensor=True`; `<binary> sense`; the scheduler's sensor pass, from `commits` and by polling; `orca.sensed`; partition mapping; `orca.rebuilds` and its levels; `orca rebuild`, and the rebuild form in the UI. | Phase 4's gate: with the scheduler running, a snapshot committed to `raw.events` by a program that isn't an orca pipeline starts `clean`, whose commit starts `report`, and nothing else runs; `orca rebuild raw.events` reruns both, `report` after `clean`; a pipeline reading an unrelated dataset never runs; each snapshot starts one run however many ticks see it. | ~1,000 |

These tests need iceberg, so they run in the `data` environment, as the table tests do.

As built (`orca.sensors`, `orca.warehouse`):

- **`sense` reads metadata JSON itself.** A pipeline imports iceberg only if it uses tables, but every binary answers `sense`, so `orca.warehouse` finds the current metadata file as iceberg's filesystem catalog does and reads just the current snapshot's ID and its summary's `orca.run` and `orca.partitions`. It prints `<dataset> <snapshot> <orca|other> <partitions|->`; the scheduler polls it only for snapshots something other than orca made. A dataset the pipeline also writes isn't sensed, so the taxi compaction doesn't start its own pipeline.
- **"Registered" is when it became a sensor.** `orca register` asks a sensor's binary to `sense` and records what its datasets hold then as seen, with no run, and `pipelines.sensing_since` dates the commits it counts from. Registering it again keeps both; one registered as no longer a sensor loses `sensing_since`, so if it becomes one again it starts afresh. A sensor that reads no dataset is refused.
- **One run per tick per sensor.** `orca.sensed` has a row per pipeline and snapshot, inserted in the transaction that queues the run, so the snapshots one tick sees start one run over all their partitions, and none starts a second. A sensor's run is retried like a scheduled one.
- **A snapshot a sensor can't run is seen all the same.** One whose keys aren't partitions of the sensor's kind (a week another writer put in `orca.partitions`, or names reaching a dated sensor) gets its `orca.sensed` row with no run and the reason in `error`, and is said once; the other snapshots the tick saw start their run. Otherwise one such snapshot would roll back the tick's transaction on every tick, and no later snapshot of any of the sensor's datasets would run. A pass that fails (a `sense` or `keys` that exits non-zero) is kept as the pipeline's `sense_error` and printed when it changes, not every second; a pass that works clears it. The pipeline's page in `orca ui` shows both.
- **A rebuild's plan is a table**, `orca.rebuild_pipelines`: each pipeline downstream, its level, its partitions (the keys given, moved to its kind) and its run. Commits made by a rebuild's runs don't start a sensor in a pipeline the rebuild runs itself, so `report` runs once, after `clean`, not also as a sensor. The scheduler doesn't retry a rebuild's runs: one that fails stops the rebuild (`failed`), and resuming it from the UI carries it on. `/rebuilds/<id>` shows the plan and its runs.

## Keyed partitions

Gate: a pipeline partitioned by the 20,000 log IDs of a dataset backfills every log in runs of 100 at most `max_runs` at once, and when rows are added to that dataset, runs start for the new logs only.

Months and days are the only partition keys so far, and the `Key` trait assumes a calendar: `next`, `prev` and `containing(year, month, day)` exist so ranges, `fills` and sensors can move between keys by date. A lot of data isn't partitioned by date. The first consumer is labelrefinery's auto-labeling of the Argoverse 2 LiDAR dataset, which is 20,000 recordings named by UUID: a mapped step per recording, a gather step scoring all of them. Its keys are names, there are too many to write in the source, and more arrive later. This section adds a third kind of partition for that, independent of phase 5 and buildable before it.

### How it fits together

- **The `Key` trait splits in two.** `Key` keeps what every key has: `parse`, `write_to`, equality and an order (`__lt__`, used for sorting and display only). `TimeKey(Key)` adds `next`, `prev` and `containing`; `Month` and `Day` implement it. Everything that does date arithmetic (ranges, `fills_for`, the scheduler's fires, the sensors' day↔month mapping) asks for `TimeKey`, so using it on a key that isn't a date is a compile error rather than a runtime surprise.
- **`Name` is the new key**: a string of 1 to 128 bytes from `[A-Za-z0-9._:/-]`, without two dots in a row, and with a `/` only between other bytes, one at a time (so `owner/name` is a name, and `/a`, `a/` and `a//b` aren't). Since neither a comma nor `..` can appear in one, the command line and the stored `runs.partitions` text stay unambiguous. It orders by bytes. Where a name becomes one segment of a path, an attempt's log directory, it's written with `/` as `%2F` (`orca.name.path_segment`), and the UI's links encode it the same way, so `owner/name`'s page is `/pipelines/<p>/keys/owner%2Fname`. `/` was added after gharchive.example needed repositories as keys.
- **`Keyed[dataset, column]` is the partition set**: `partitions=Keyed["av2.logs", "log_id"]`. Its keys are the distinct values of `column` in the current snapshot of the warehouse dataset `dataset`. Keys live in data rather than in the source because there are thousands of them and they grow, and because "which logs exist" then has snapshots and lineage like anything else. The first step takes a `Name`, or a `Seq[Name]` for a first gather step, checked at compile time as for `Monthly`. `describe` writes `{"kind": "keyed", "dataset": ..., "column": ...}`.
- **Keyed pipelines are refused things that need a calendar**, at compile time: `schedule=` and `fills=`. A keyed pipeline runs by `run`, by `backfill`, or as a sensor of its keys dataset (below). A cron schedule over names has no partition to fill, and guessing at one (all keys? new keys?) is a sensor's job done worse.
- **`<binary> keys`** prints the keys dataset's current snapshot ID and then its keys, one per line, sorted. A value of the column that isn't a name, or is null, isn't a key: it's left out, and `keys` says on stderr how many there were, with the first few, so one odd row doesn't make every key unreadable. Like `sense`, it exists so the scheduler, which links no iceberg, can learn a pipeline's keys. It reads the one column through `orca.tables`, so only binaries with a keyed pipeline import iceberg for it.
- **On the command line**, `--partitions` for a keyed pipeline takes a comma-separated list of names, or `@all` for every current key, or `@file:<path>` for a list one per line, which scripts and the labelrefinery side need for samples. A range (`A..B`) is refused with a message saying keyed partitions take lists. A key that isn't in the current keys snapshot is refused, so a typo doesn't make a run of nothing.
- **Many keys per run.** A run over a list of keys is already what phase 1 does: a mapped step runs once per key, one after another in the driver, each free to use dista inside, and a gather step runs once over all of them. Keyed pipelines are where that matters, since one run per key would mean 20,000 driver processes and as many dista runtimes. So the pipeline gets `keys_per_run: Int = 1`, and for a keyed pipeline the default to recommend is in the tens to hundreds. Parallelism is then `max_runs` runs at once, each using dista inside its steps. Time-partitioned pipelines keep one key per run by default; the parameter works for them too.
- **A key is done** when every mapped step of the pipeline has succeeded for it in some run whose registry hash is the pipeline's current one. That's a query over `steps` and `runs`, not a new table. A key whose run failed partway still has its succeeded steps; resuming the run reruns only what didn't succeed, as now.
- **Backfill picks undone keys and batches them.** `orca backfill <pipeline> @all` (or a list) takes the keys given, drops those that are done or in a live run (queued, or running with its process alive), sorts the rest and queues one run per `keys_per_run` of them, all with trigger `backfill`. This replaces today's duplicate check, which compares a run's whole `partitions` text and so can't see a key inside a batch. Time-partitioned backfills use the same per-key check, so their behaviour doesn't change. `orca backfill --rerun` (the backfill form's checkbox, `rerun=1`) queues the done keys too, for a key whose source has changed since it ran, such as a day that ran out of an API's budget; a key in a queued or running run is still left to that run.
- **Gather steps cover their run's keys.** With batches, a gather step sees its batch, not the whole dataset. A score over all 20,000 keys is a second pipeline, a sensor on the dataset the first one writes, with one gather step and no partitions of its own. That keeps runs bounded and the scoring rerunnable alone. A pipeline that wants one gather over everything can still run `--partitions @all` as one run.
- **Sensors on the keys dataset start runs for new keys.** A keyed pipeline with `sensor=True` is also sensed on its keys dataset, whether or not a step reads it. When that dataset gets a snapshot, the scheduler runs `<binary> keys`, takes the keys that aren't done and aren't in a live run, and queues them in batches of `keys_per_run`. `orca.sensed` records the snapshot as for any sensed dataset, so each snapshot starts its batches once. Keys removed from the dataset aren't unlabeled or deleted; they just stop being offered to backfills.
- **Sensors on other datasets map keys by equality.** A snapshot committed by a mapped step of a keyed pipeline carries `orca.partition` (one name), and one by a gather step carries `orca.partitions` (its run's names). A keyed sensor downstream runs those keys if they're in its own keys dataset, and ignores the rest. Mapping between a keyed and a time-partitioned pipeline is refused at registration, since no rule turns a name into a month. The keys dataset itself needs no mapping, so a time-partitioned pipeline may write it: a daily pipeline that finds new repositories can write the keys a keyed sensor runs, and each of its snapshots offers the keys it holds.
- **In the UI** a keyed pipeline's page shows its keys dataset, how many keys it has, how many are done, how many are in live runs and how many failed, with the failed ones listed and a link to rerun them. The partitions field of the backfill form accepts the same lists as the command line, and `@all`, but not `@file:<path>`: the UI would read the file on its own host, so it answers 400 instead. On the command line, a line of the file that isn't a name is named by its number, not its text. `/pipelines/<name>/keys/<key>` shows one key's steps across the runs it was in.

### PRs

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **Keys that aren't dates.** The `Key`/`TimeKey` split; `Name`; `Keyed[dataset, column]`; the compile-time refusals (`schedule=`, `fills=`, the first step's key type); `<binary> keys`; `--partitions` lists, `@all` and `@file:`, with ranges and unknown keys refused; `keys_per_run`; `describe` with keyed partitions. | A keyed toy pipeline over a warehouse dataset of 50 names runs `--partitions @all` with a mapped and a gather step; `keys` prints the 50 sorted; a keyed pipeline with `schedule=` and one whose first step takes a `Month` don't compile (`tests/compile_fail/`); `a..b` and an unknown name are refused with exit code 2; phases 1 to 4's tests pass unchanged. | ~700 |
| 2 | **Backfills and sensors over keys.** "Done" per key; backfill choosing undone keys and batching them by `keys_per_run`, also for time partitions; the keys dataset sensed for a keyed sensor, and new keys batched into runs; equality mapping across keyed pipelines, and the refusal across kinds at registration; the keyed pipeline page and the per-key page. | The section's gate on a 20,000-key dataset of generated names and a step that sleeps briefly: `orca backfill keyed @all` with `keys_per_run=100` and `max_runs=4` queues 200 runs, never runs more than 4, and leaves every key done once; a backfill run again queues nothing; failing 3 keys and backfilling again queues one run of those 3; appending 500 names to the dataset starts 5 runs of the new names and none of the old; the per-key query is checked against the steps table directly. | ~900 |

These need iceberg for the keys dataset, so like the lineage and sensor tests they run in the `data` environment.

As built in PR 1: `Keyed` is in `orca.keyed`, the one module of it that imports iceberg, and each partition set reads its own `--partitions` (`Partitions.parse`), fills a schedule's partition only if it has a calendar (`Partitions.fill`, which `fills_for` calls), and lists its keys only if it's keyed (`Partitions.keys`). `fills=` is refused as `fills=Current`, since the default can't be told from `fills=Previous` written out. A keyed pipeline's registry hash covers its keys dataset and column; a time-partitioned pipeline's is unchanged. The scheduler takes a keyed backfill as a list of names; `@all` there comes with PR 2, through `<binary> keys`.

As built in PR 2: the scheduler reads a keyed pipeline's keys by running `<binary> keys` (`current_keys`), for `@all`, for checking names, for a keyed sensor and for the UI's counts. "Done" is one SQL query (`DONE_SQL`) over `steps` and `runs`, with an index on a step's partition key (migration 7); a pipeline with no mapped step has a key done when a run of its current code over it succeeded. A backfill takes a lock on the pipeline's row while it chooses and queues keys, so two at once don't both queue one. A keyed sensor's keys dataset is in `sense`'s output, so a snapshot something else made is polled for like any other, and one an orca pipeline committed is found in `commits`. A sensor's snapshot offers every current key (the keys dataset's) or the names it holds (another keyed pipeline's), and the offered keys that aren't done or live are queued in the transaction that records the snapshot as seen. The refusal across kinds applies to sensors: a keyed sensor reading a dataset a time-partitioned pipeline writes, or the other way round, isn't registered. Its own keys dataset is the exception (added after gharchive.example ran into it): a time-partitioned pipeline may write it, since a snapshot of it offers its keys whoever made it, and `tests/keyed_sensor.sh` checks that a daily pipeline's commit there starts runs of the new keys only, with lineage through it.

### Not decided here

- **Running several keys of a mapped step at once inside one run.** One driver runs a batch's keys one after another, as phase 1 does, and any parallelism within a key goes through dista. Dispatching a whole batch's keys as dista tasks would make one run use a cluster better, but a step is a function in the driver, not a dista task, so it's a change to the programming model. It's worth measuring on labelrefinery's first backfill before deciding.
- **Keys from somewhere other than a warehouse dataset**, such as a listing of an S3 prefix. A small pipeline that writes the listing to a dataset does this today, with lineage, so nothing is built for it until that's shown to be too slow.

## Phase 5: on Kubernetes

Gate: the taxi pipeline runs on a schedule on EC2, each run on its own cluster of instances.

- **The scheduler as a Deployment** with one replica and Postgres beside it (or RDS).
- **A run as a dista cluster**: the scheduler creates a head, a worker set sized by the pipeline, and a Job for the driver, all labelled with the run ID, through the Kubernetes API over HTTPS with the pod's service-account token; and deletes them when the run ends. The run's pipeline image is named in its registration.
- **Logs and timelines in S3**, through `ORCA_LOGS=s3://...`.
- **Cluster start time** is measured here. If it dominates short runs, a warm pool of workers per pipeline image is the next step, not before.

[K8S.md](K8S.md) says who creates the pods, how orca talks to Kubernetes, what it needs on AWS and how cost stays bounded. Phase 5 is built and tested on a kind cluster on one machine first, as dista's Kubernetes support was (`deploy/`, `tests/kind.sh` in dista), so only the last PR needs AWS.

### PRs

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **A Kubernetes client, in [k8s.mojo](https://github.com/magmalake/k8s.mojo).** Configuration in-cluster (the service-account token and CA) and from a kubeconfig (kind's client certificates); create, get and delete for Pod, Job and Service; list and watch by label selector, a watch read as JSON lines. HTTPS through the TLS magmalake already has, JSON through json_mlake. | On a kind cluster in CI: create a pod, watch it to `Succeeded`, list it by label, delete everything with the label, and get a 404 for it after; a create the API refuses (a bad spec, or RBAC forbidding it) raises with the API's message. | ~1,300 |
| 2 | **Images and manifests.** A `deploy/Dockerfile` that builds a pipeline binary and the `orca` binary into slim images, as dista's does; manifests for the `orca` namespace (the scheduler as a one-replica Deployment, Postgres as a StatefulSet on a volume with `synchronous_commit` on, `orca ui` as a Service) and the `orca-runs` namespace (its ServiceAccount, the Role of K8S.md, a ResourceQuota). A run still uses a local dista runtime, inside its driver's pod. | `tests/kind.sh`: on kind, the toy pipeline is registered from its image and run by its schedule, and `orca ui` shows the run. | ~500 |
| 3 | **A dista cluster per run.** With `ORCA_RUNNER=kubernetes`, the scheduler starts a run by creating a head Pod and Service, the pipeline's worker Pods and a driver Job (`<binary> resume <run>` with `DISTA_HEAD` set), all labelled `orca.run=<id>`, the head and workers owned by the Job, the Job with `activeDeadlineSeconds` and `ttlSecondsAfterFinished`. `orca register --image <image>` records the pipeline's image, and `workers=` on the pipeline its cluster size. The scheduler sees a run end from its Job, deletes what's labelled with it, and on start-up deletes objects of runs Postgres doesn't list as running. A total vCPU budget (`ORCA_VCPU_BUDGET`) is checked before a cluster is made, and each run records its pods' requested CPU × seconds, which its page shows. | On kind: the fanout pipeline's runs each get their own cluster of 3 workers, and their tasks run on all 3; nothing labelled with a run is left after it ends; a driver pod killed mid-run is resumed on a new cluster and succeeds; objects of a run a killed scheduler left behind are deleted when it starts again; a run that would exceed the budget waits. | ~1,200 |
| 4 | **Object storage.** `ORCA_WAREHOUSE=s3://...` for datasets, `sense` reading metadata through objectstore, and `ORCA_LOGS=s3://...` for attempt logs, driver logs and timelines, which `orca ui` reads back. | The lineage and sensor tests pass with the warehouse and logs on MinIO (conda-forge's `minio-server`), and the UI shows an attempt's log from it. | ~700 |
| 5 | **On AWS, two ways** (below): the `ec2` runner, instances bought per run with nothing else idle; and Kubernetes on EKS, the runner of PR 3 on a cluster that's up. | Phase 5's gate on AWS, below, with each. | ~1,600 and ~600 |

As built in PR 2: one image (`deploy/Dockerfile`) holds `orca` and the pipeline binaries, built with pixi and copied onto Debian with the libraries they link and the ones they dlopen from `$CONDA_PREFIX/lib` (libpq, flare's shims), which the image sets to `/opt/orca`. The UI runs as a second container in the scheduler's pod, so both see the run logs on the pod's volume until logs move to object storage in PR 4. `ORCA_REGISTER` names the image's pipeline binaries to register when the scheduler starts.

As built in PR 3 (`orca.kube`): the scheduler makes a run's driver Job first, then its head Pod and Service and workers owned by it, from the image `orca register --image` recorded; `workers=` on the pipeline (default 2) is the cluster's size, and a pipeline without tasks gets only the Job. Every tick it deletes the objects of runs that aren't running at that start any more, and those of Jobs that finished, which covers what a killed scheduler left behind without a separate start-up pass. A run the scheduler started may go without a heartbeat for `ORCA_START_SECONDS` (default 300 on Kubernetes) before it's taken to have died, as a pod can wait minutes for a node. `ORCA_VCPU_BUDGET` is checked, under an advisory lock, against the CPU of every live run (`runs.cpu_millis`, migration 8). One thing the gate found: a driver started a moment after its head's Service was made could send its first connection before the node routed the Service's address, and that connection, tracked unrouted, then waited minutes, longer than dista's 60 s for reaching a head. dista's connect has no timeout, so a driver or worker given `DISTA_HEAD` first waits for the head with one-second tries of its own (`orca.tasks.wait_for_head`); a connect timeout in dista would make that unnecessary. Logs stay in the pods until PR 4.

As built in PR 4: `ORCA_WAREHOUSE` and `ORCA_LOGS` can be `s3://bucket/prefix`, read and written through iceberg's `FileIO` and objectstore's `FileIOResolver`, configured from the `AWS_*` environment. The filesystem catalog commits on S3 as it does on disk: each version is written under a unique name and the directory listed for a clash, which S3's strongly consistent listing makes as safe as the local case. `sense` lists and reads a table's metadata through objectstore, which every pipeline binary already links through dista. An object can't be appended to, so an attempt writes its log to a staging file (`ORCA_LOG_STAGE`) and publishes it when the attempt ends, and a run's timeline when the run does; a log is in the bucket once complete, not while it's written. The scheduler publishes a local run's `driver.log` when it reaps the process, and on Kubernetes it appends the driver pod's log to `driver.log` before it deletes the run's Job (k8s.mojo's `logs`, which needs `get` on `pods/log`). `orca ui` reads all three back, and shows a run's driver log at `/runs/<id>/driver.log`. The lineage and sensor tests run over MinIO too (`test-lineage-s3`, `test-sensors-s3`, `scripts/with-minio.sh`).

**Login for the UI's actions** (`orca.auth`), added before the UI is on AWS: reading needs no login, and every action does once login is on, which `ORCA_UI_LOGIN=required` (set in `deploy/k8s`) or any user turns on. Users are added with `orca user add`; their passwords are bcrypt hashes made by pgcrypto (migration 9), sessions are 32 random bytes kept as their SHA-256 digest and sent as an `HttpOnly`, `SameSite=Strict` cookie (and `Secure` with `ORCA_UI_SECURE_COOKIES=1`), and every action is a row of `orca.audit`, shown on its run's page. Behind an ALB that asks for a login itself, trusting its signed identity header instead is a later step: it needs the header's ES256 signature checked, which nothing in magmalake does yet.

PRs 1 to 4 need no AWS account: kind runs on colima on one Mac, and in CI. PR 5 creates paid resources, so its plan (the instance types, the cluster, Postgres, and their cost per month) is agreed before any is created, and what it creates is deleted or kept as agreed when the gate has passed.

### PR 5: on AWS, with the `ec2` runner and with EKS

orca runs on AWS both ways, chosen by `ORCA_RUNNER`. **EKS** (Auto Mode, Karpenter on Spot for runs) is the runner of PR 3 on a managed cluster: autoscaling across nodes, pod identity per run, about $150 a month before a run starts for its control plane and an always-on node. **The `ec2` runner** is for a deployment that can't justify an idle cluster: runs here are batch work, minutes to hours a day, so a small always-on instance holds what has to be up and each run buys instances by the second, for about $60 a month before runs. The `ec2` runner comes first, as the cheaper demo; EKS follows on the same bucket, repository and image.

**`ORCA_RUNNER=ec2`** starts each run on instances of its own, in one Availability Zone (us-east-2a):

1. **A head instance**, on-demand: the run's dista head and its driver (`<binary> resume <run>`, with `DISTA_HEAD` on localhost). `RunInstances` gives back its private address at once.
2. **`workers` worker instances**, Spot when the pipeline is registered with `--spot`, started straight after with `DISTA_HEAD` at the head's address; they wait for the head (`orca.tasks.wait_for_head`) and advertise their own private addresses for results.
3. Each instance starts the pipeline's image from user data and is told to terminate on shutdown, which the script ends with: the head when the driver exits, a worker when its head hangs up and dista's reconnect window passes.
4. **Cleanup in layers, as on Kubernetes:** instances terminate themselves; every tick the scheduler terminates instances tagged `orca.run` whose run isn't running at that start, and those older than the run's deadline; and on start-up it does the same for everything tagged.

A pipeline without dista tasks gets only the head instance. The instance types, the count and Spot are a deployment's choice, so they're given at registration rather than in the pipeline's code: `orca register --image <ecr image> --head-type c7g.large --worker-type c7g.4xlarge --workers 4 --spot <binary>`, defaulting to `ORCA_EC2_HEAD_TYPE`, `ORCA_EC2_WORKER_TYPE` and the pipeline's `workers=`. `--workers MIN..MAX` scales a run's workers between the two (MIN may be 0): the run starts MIN, and each tick the scheduler reads the head's demand on port 6390 and applies `dista.scale.decide`, starting workers and draining idle ones with `dista drain`; a drained worker's instance shuts itself down, and `orca.instances` records it as `drained: idle`. `ORCA_VCPU_BUDGET` counts a run's instances' vCPUs before any is started. The image is arm64, so the types are Graviton.

The EC2 API is SigV4-signed HTTPS, as S3 is, so the client is a few calls (`RunInstances`, `DescribeInstances`, `TerminateInstances`, with tags in the call) on objectstore's signing and HTTP; the always-on instance gets its credentials from its instance role (IMDSv2), which objectstore reads already.

**On AWS** (one CloudFormation stack, `deploy/aws/orca.yaml`, so it's reviewed as one file and deleted as one stack):

| Resource | What for | About, per month |
|---|---|---|
| The **control** instance: t4g.large (2 vCPU, 8 GiB), on-demand, 40 GB gp3 | the scheduler, Postgres (a container on that volume, `synchronous_commit` on), `orca ui`, and LabelControl's site | $49 + $3 |
| Its public IPv4 address | updates, ECR, and the tunnel out; no port is open to the internet | $3.60 |
| **Run** instances: a head (c7g.large on-demand, $0.07/h) and workers (c7g.4xlarge Spot, about $0.20/h each) | only while a run runs, by the second | a taxi backfill of a few hours: a few dollars |
| An **S3 bucket**, with a gateway endpoint | the warehouse and the logs | under $1 |
| An **ECR repository**, with a lifecycle rule | the orca image | under $1 |
| **IAM**: the control instance's role may start and terminate only instances tagged for orca, of the allowed types, with the run instances' profile; read and write the bucket; pull from the repository. The run instances' role may read and write the bucket's prefix and pull. | | free |
| **Security groups**: run instances accept the head's port and the workers' result ports from each other only; the control instance accepts nothing from outside | | free |
| A **Budgets** alert at $100 | | free |

About $60 a month always on, plus the runs. The project's spend limit is set high, so the Budgets alert is what warns first.

**Reaching the UIs.** For the demo they're public: `orca ui` reads without a login and acts only with one (`orca.auth`, rate-limited against guessing before it's exposed), over HTTPS so its cookie is `Secure`, either through a Cloudflare Tunnel (outbound only, nothing open on AWS) or Caddy on the control instance with a Let's Encrypt certificate (ports 80 and 443 open). Logs and errors are public with them, so the demo runs nothing whose logs could hold a secret. LabelControl has no login, so it's public read-only (`LABELCONTROL_READONLY=1`), as its README describes.

**LabelControl** needs two things before it runs there: `linux-aarch64` in its `pixi.toml` (it's built for macOS only today), and a decision about its executor, which runs scene generation, the geometry pass and training in its own process: on the small control instance it would be slow and squeeze everything else, so either it runs elsewhere or its stages become orca pipelines on run instances.

As built in 5a (`orca.ec2`, `orca.awsec2`): the head instance's script runs the head in the background and the driver in front, then keeps the driver's output with `<binary> keep-log`, which appends it to the run's `driver.log` wherever `ORCA_LOGS` is; a worker's script exports `DISTA_ADVERTISE` from `hostname -I`. Instances are told `ORCA_EC2_DATABASE_URL` as their `ORCA_DATABASE_URL`, which goes in their user data: anyone in the account allowed `ec2:DescribeInstanceAttribute` can read it, so the demo's database password is for that database alone. `DescribeInstances` is read without paging, which holds up to a thousand instances. `tests/ec2.sh` (`test-ec2`) is 5a's gate, against `tests/fake_ec2.py`.

**The PRs:**

| # | PR | Gate | Size |
|---|---|---|---|
| 5a | **The `ec2` runner.** The EC2 calls; `orca.ec2` (start, sweep, budget); registration's instance options; the user data that starts a head or a worker. | Against a stand-in EC2 API (a small HTTP server that records calls and runs each "instance" as a local process with its user data's environment): a fanout run starts one head and 3 workers with the right types, tags and `DISTA_HEAD`, its tasks reach all 3, everything it started is terminated after, a "Spot interruption" (a worker process killed) is survived, a head killed mid-run is restarted on new instances, and the budget makes a second run wait. | ~1,000 |
| 5b | **On AWS, with `ec2`.** `deploy/aws/orca.yaml`; the image pushed to ECR; the control instance set up from user data; HTTPS in front of the UIs. | Phase 5's gate: the taxi pipeline runs on its monthly schedule, each run on its own head and workers, its start times (to the first task on every worker) and its cost measured and written here; the UIs public over HTTPS, `orca ui`'s actions behind its login. Afterwards, the stack is kept or deleted as agreed. | ~600 |
| 5c | **On AWS, with EKS.** An EKS Auto Mode cluster beside the stack: a NodePool for runs on Spot with a CPU limit, Pod Identity for run pods' access to the bucket, `deploy/k8s` with the ECR image. | The same gate with `ORCA_RUNNER=kubernetes`: taxi's runs on their own pods across nodes, start times and cost measured beside 5b's. | ~600 |


## Phase 6: getting started

Gate: on a clean machine, an agent given the getting-started page's prompt builds a GitHub Archive lake from 2023 on, imports a day of it every day, sizes its dista workers to the work, and serves a flare app showing how Mojo libraries grew and which Mojo versions they moved to; a person following the page's step-by-step section builds the same thing at a smaller size. Both run on AWS, and the orca UI, the app and the lake's tables stay up afterwards.

The page is written last, from what worked. Writing it is how this phase finds what a user is missing, so the list below is what the first draft found and will grow while the agent runs.

### What the example builds

`magmalake/gharchive.example`, a project of its own made with `pixi shelf add orca`:

1. **`gharchive`**, daily-partitioned (`Daily`, `schedule="0 3 * * *"`, `fills=Previous`): each day's 24 hourly files from data.gharchive.org, read with objectstore's HTTP, parsed as JSON lines and written to `gharchive.events` (Iceberg, partitioned by day). Backfilled from 2023-01-01, a few TB of input.
2. **`mojo_repos`**, a sensor on `gharchive.events`: finds Mojo repositories in each day's events and in GitHub's code search (files ending `.mojo` or `.🔥`, and `pixi.toml`/`mojoproject.toml` naming a Mojo package), and adds the new ones to `mojo.repos`. How repositories are found is the example's code, so the page shows it rather than orca hiding it.
3. **`mojo_history`**, keyed by repository (`Keyed["mojo.repos", "repo"]`, `keys_per_run=50`, `sensor=True`): clones each repository as a dista task, reads its history and writes `mojo.commits` and `mojo.migrations`. New repositories start runs by themselves; a daily refresh brings known ones up to date.
4. **The console**, a flare app reading the `mojo.*` tables: libraries by size and age, Mojo versions adopted over time, and a page per repository.

**The lake's schema.** Everything from GitHub is kept, so questions other than the console's can be asked of it:

| Table | One row per | Columns |
|---|---|---|
| `gharchive.events` | GitHub event | `id`, `type`, `created_at`, `repo`, `actor`, `payload` (JSON) |
| `mojo.repos` | repository | `repo`, `created_at` (GitHub's), `first_mojo_commit`, `first_seen` (first event), `found_by`, `stars`, `forks`, `license`, `default_branch`, `archived`, `description`, `refreshed_at` |
| `mojo.commits` | commit on the default branch | `repo`, `sha`, `committed_at`, `author`, `mojo_files`, `mojo_lines`, `lines_added`, `lines_removed`, `tooling`, `manifest`, `mojo_package`, `mojo_constraint`, `mojo_version`, `channel` |
| `mojo.migrations` | change of tooling or Mojo version | `repo`, `sha`, `at`, `kind` (`tooling` or `mojo`), `from`, `to` |
| `mojo.daily` | repository and day it changed | `repo`, `day`, `mojo_lines`, `mojo_version`, `tooling`, `stars`, for the console's charts |

How the history is read, cheaply: `git log --numstat` over the default branch gives every commit's lines added and removed by file, summed into `mojo_lines` without checking anything out. Only commits touching a manifest or lock file are read with `git show <sha>:<file>`, which is where the tooling and the Mojo version change.

- **`tooling`** is what the project was built with at that commit: `none` (early 2023, the `modular` CLI, no project file), `magic` (`mojoproject.toml`, `magic.lock`), or `pixi` (`pixi.toml`, `pixi.lock`, or `[tool.pixi]` in `pyproject.toml`). A repository's move from magic to pixi is a `tooling` row of `mojo.migrations`.
- **`mojo_version`** is the version the lock file resolved (the `max`, `mojo`, `mojo-compiler` or `modular` package, whichever the project used, kept in `mojo_package`), else the manifest's constraint (`mojo_constraint`), else null. `channel` is `stable` or `nightly`, from the version (`.dev` builds) and the channel URL. Versions are compared in one order across the renumbering (24.x, 25.x, 0.25.x and 0.26.x, 1.x), which the example's code defines and tests.

### The data is public

The `mojo.*` and `gharchive.*` tables are written to a bucket of their own (`mlake-public-<account>`) with a bucket policy allowing anyone `s3:GetObject` and `s3:ListBucket`; orca's bucket, with logs and run state, stays private. The project's resource control policy denies S3 to principals of other accounts, so readers read unsigned: `aws s3 ls --no-sign-request`, DuckDB's and pyiceberg's anonymous settings, and the page says how. Downloads are billed to the project at S3's egress price, so the Budgets alert covers the bucket, and CloudFront in front of it is the step if readers turn out to download a lot.

### What users are missing

| | Missing | What it is |
|---|---|---|
| A | **The code each run ran** | `orca register` keeps every version of a pipeline instead of replacing it: its registry hash, its image digest, and the git repository and commit it was built from, which a build passes with `-D ORCA_SOURCE=... -D ORCA_COMMIT=...` and `describe` reports. Each run records its version. The run page links to `github.com/<repo>/tree/<sha>`, and the pipeline page lists its versions with the runs of each. A binary built from a dirty tree says so. |
| B | **dista's logs** | Today a run keeps its driver's output and its attempts' logs; the head's and workers' output goes with their instances. Each instance's script now uploads its output to `<run>/cluster/<role>-<instance>.log`, every 30 s while it runs and once when it exits, since an instance can go without notice. The run page lists the run's instances (role, type, started, ended, why it ended) with their logs. |
| C | **The timeline, in the UI** | The run's timeline is a JSON download for Perfetto today. The run page draws it: one lane per worker, tasks as bars by step, and the number of workers over the run, so scaling shows. An "open in Perfetto" link stays, through Perfetto's `?url=` with CORS allowed for ui.perfetto.dev. |
| D | **Workers sized to the work** | dista's autoscaling (dista's DESIGN.md, "Autoscaling"): a pipeline is registered with `--workers 1..12`, the scheduler reads each running run's demand from its head every tick, starts workers when tasks wait and drains and terminates idle ones, within the range, `ORCA_VCPU_BUDGET` and the instance quota. A backfill and a day's import then use the same registration. |
| E | **Secrets for runs** | `orca register --secret GITHUB_TOKEN=orca/github-token` names a Secrets Manager secret; run instances read it at start with their role (which may read only secrets under `orca/`), and the run's environment gets it. Its value is masked in every log orca writes, since the logs are public. |
| F | **orca from mojoshelf** | `pixi shelf add orca` in a new project; orca and k8s.mojo published like the other tins. |
| G | **Deploying from a user's project** | `orca aws up` creates or updates the stack from the template shipped in the package, `orca aws push` builds the project's image from a template Dockerfile and pushes it to ECR, `orca aws register` registers its pipelines on the control instance, and `orca aws down` deletes the stack. They call the `aws` and `docker` commands a user already has, logged in with `aws login`. |
| H | **Long-running apps** | The stack runs an app container next to the orca UI, behind the same Caddy, on its own hostname: `orca aws app console --image ... --host libs.mlake.dev`. It reads the public tables like any reader. |
| I | **Skills** | In magmalake/skills, beside `mojo-data-performance`: `orca` (pipelines, partitions, keyed partitions, sensors, registering, backfilling, the runners), `dista` (tasks, refs, `remote_many`, actors, local and cluster mode, autoscaling), `magmalake-apps` (a project's layout, a lake on Iceberg over S3, a flare app over it, deploying with `orca aws`), and `mojoshelf` (finding and adding tins) if mojoshelf has none. With the AWS MCP server and its skills, these are what the page tells an agent to install. |

**Instances are on-demand.** The project is on the Paid plan, whose service control policy denies Spot (`DenySpotMarketType`) and Reserved Instances, and keeping the spend limit means staying on it. `--spot` stays for accounts that allow it; the page says so. Autoscaling is what keeps on-demand workers affordable: an idle lake runs no workers, and a backfill's cost is measured on one month before the rest is queued.

### The site

magmalake.dev (mlake.dev is the same site):

- **The home page fits on one screen**: a headline (a mostly-Mojo stack for comptime-checked, scalable, reliable, distributed data pipelines); the layers, bottom to top, each linked: formats (Parquet, Avro, Arrow, compression), storage (objectstore, Iceberg), compute (dista), orchestration (orca), apps (flare), with what isn't Mojo (Postgres) in a footnote; three short points (agent friendly; performance; small images and fast builds, the last two linking to the performance page); and two buttons, Getting started and GitHub. The rest of today's home page moves to pages of its own.
- **Getting started** has two sections over the same example. **Let an agent build it**: install the magmalake, mojoshelf and AWS skills, then one prompt to copy, with what it will do and roughly what it costs. **Step by step**: install pixi and mojoshelf, create the project, `pixi shelf add orca`, the code of each pipeline and of the console (one month of the archive and a few repositories), and deploying it with `orca aws`.

### PRs

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **The stack on AWS** (#38, phase 5's 5b), now with the public bucket and the console's hostname. #38 merged; the public bucket (`mlake-public-<account>`, `ORCA_PUBLIC_WAREHOUSE` for runs) and `ConsoleHostname` (Caddy to a `console` container on port 8080) follow it. | Phase 5's gate. | in review |
| 2 | **Pipeline versions** (A): migration for `pipeline_versions`, `runs.version`; `-D` source and commit in `describe`; the links. | Two registrations of the toy from two commits: each run's page links its own commit, the pipeline page lists both versions. | ~500 |
| 3 | **Cluster logs and the timeline view** (B, C). | `tests/ec2.sh`: a fanout run's head and workers' logs are listed on its page, including the worker terminated mid-run; the page draws 3 lanes. | ~800 |
| 4 | **Autoscaling** (D), after dista's autoscaling PRs. Done: `--workers 0..N` is allowed, and `tests/autoscale.sh` grows a run from 0 to 3 workers and drains them all while it pauses. | `tests/ec2.sh` against the stand-in EC2: with `--workers 1..6`, a run of 200 slow tasks grows to 6 workers and back to 1, never past the budget; a worker drained while holding results loses none. On AWS: the backfill of one month, its workers over time and its cost. | ~700 |
| 5 | **Secrets** (E). Done on the ec2 runner: `tests/secrets.sh`. | A run reads a secret from a stand-in Secrets Manager; its value appears in no log. | ~400 |
| 6 | **orca on mojoshelf, `orca aws`, apps** (F, G, H). F done (orca-mojo 0.1.0, 0.2.0). G and H: `orca aws up/push/register/app/status/down`, the template and a project Dockerfile embedded in `orca.aws_assets`, `orca-refresh` and `orca-app` on the control instance; `tests/aws.sh` against stand-in `aws` and `docker`. A project needs `linux-aarch64` in its platforms. | From a new directory: `pixi init`, `pixi shelf add orca`, a toy pipeline, `orca aws up/push/register` against the running stack, and an app answering on its hostname. | ~900 |
| 7 | **The skills** (I), in magmalake/skills. | `agentskills validate` passes; an agent with only the skills writes a keyed pipeline that compiles. | ~1,500 lines of text |
| 8 | **gharchive.example**, built by an agent from the draft prompt, then fixed by hand where it got stuck, each fix recorded as a missing feature here. | The phase's gate. | the example |
| 9 | **The site**: the one-screen home page and the getting-started page (magmalake.org repo). | Someone following Step by step, and an agent following the prompt, both finish. | site |

The order is 1, 2, 3, dista's autoscaling, 4, 5, 6, 7, 8, 9. Paid resources beyond the stack (the backfill's instances, the console's container) are created once the stack's gate has passed, with their cost estimated first.


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
