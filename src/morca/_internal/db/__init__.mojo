"""morca's SQL: every query on the `orca` schema, behind typed functions.

Each module owns the queries of one table or concern, and each function
takes typed arguments and returns typed rows (structs), counts or ids,
never a result set. A function runs on the connection or the transaction
its caller passes (`mut conn: Connection` or `mut tx: Transaction`), so
the caller keeps deciding what is one transaction and what it locks; a
query run both ways has a function for each, over one SQL text.

| Module | Owns |
|---|---|
| `morca._internal.db.live` | which runs are live, dead or held, as SQL the others build on |
| `morca._internal.db.pipelines` | `orca.pipelines`' paused flag, names and version |
| `morca._internal.db.actions` | the actions' runs: started, resumed, rerun and cancelled |
| `morca._internal.db.audit` | `orca.audit` |
| `morca._internal.db.auth` | `orca.users`, `orca.sessions`, `orca.api_tokens` |
| `morca._internal.db.batches` | `orca.runs.batch` |
| `morca._internal.db.calendar` | the calendar's live and failed keys |
| `morca._internal.db.config_show` | each pipeline's settings as registered, for `morca config` |
| `morca._internal.db.ec2` | the EC2 runner's runs and instances, and its warm pool |
| `morca._internal.db.estimate` | how long a pipeline's recent runs took |
| `morca._internal.db.expiry` | the snapshots snapshot expiry keeps |
| `morca._internal.db.fill` | a pipeline's fill runs, their progress and cost |
| `morca._internal.db.fleet` | `orca.fleet_samples`, `orca.fleet_hourly` and the fleet page's reads |
| `morca._internal.db.handover` | whether a run is outdated |
| `morca._internal.db.health` | whether the database answers |
| `morca._internal.db.kube` | the Kubernetes runner's reads of which runs' objects to keep |
| `morca._internal.db.lineage` | lineage walks over `orca.reads` and `orca.commits`, and declared datasets |
| `morca._internal.db.notify` | `orca.notifications`, `orca.alerts` and the alert rules' reads |
| `morca._internal.db.pool_runs` | runs queued for a worker pool |
| `morca._internal.db.pools` | `orca.pools`, `orca.pool_machines` and the pool autoscaler |
| `morca._internal.db.pressure` | `orca.pressure`, each host's memory pressure |
| `morca._internal.db.queueing` | backfills: the keys done, `orca.keys`, the runs queued |
| `morca._internal.db.rebuilds` | `orca.rebuilds`, `orca.rebuild_pipelines` and the runs they queue |
| `morca._internal.db.recommend` | a pipeline's runs' peaks and its out-of-memory raise |
| `morca._internal.db.registered` | a registered pipeline's row, and its learned request |
| `morca._internal.db.registering` | `morca register`'s row of `orca.pipelines` |
| `morca._internal.db.run_cluster` | a run's cluster over a pool: its head, `orca.run_members` |
| `morca._internal.db.scheduler` | the scheduler's retries, dead runs and cgroup memory |
| `morca._internal.db.schedulers` | `orca.schedulers`, the scheduler instances |
| `morca._internal.db.sensing` | `orca.sensed`, `orca.fires`, the runs they queue, and what a sensor follows |
| `morca._internal.db.starting` | a pass that starts a pipeline's runs |
| `morca._internal.db.system` | `orca.system_minutes`, `orca.system_containers` |
| `morca._internal.db.usage` | what each attempt of a process used |
| `morca._internal.db.watchdog` | `orca.watchdog_rules`, `orca.watchdog_trips` and the rules' readings |
| `morca._internal.db.workers` | `orca.workers`, the worker tokens, and workers' claims |

The run stores (`morca.state`'s `State` and `PgStore`, `morca.sqlite_store`'s
`SqliteStore`), a module per dialect and table, over the same rows:

| Module | Owns |
|---|---|
| `morca._internal.db.run_rows` | the rows both dialects return (`RunInfo`, `StepInfo`, `Usage`, ...) |
| `morca._internal.db.schema` | `State.migrate`'s advisory lock and `orca.schema_version` |
| `morca._internal.db.runs` | a pipeline binary's registration, `orca.runs`: claims, heartbeats, ends, the watchdog's stops |
| `morca._internal.db.steps` | `orca.steps`, `orca.attempts`, `orca.reads` and `orca.commits` |
| `morca._internal.db.sqlite_schema` | the SQLite file's migrations and `schema_version` |
| `morca._internal.db.sqlite_runs` | the SQLite file's runs, steps and attempts: `runs` and `steps`, in its dialect |

The web UI's read models, a module per page (`morca.views`, `morca.ui`):

| Module | Reads for |
|---|---|
| `morca._internal.db.runs_view` | the runs lists: a page of runs through a `RunsFilter`, its counts |
| `morca._internal.db.run_view` | a run's page: steps, attempts, tables, machines, cluster, versions, audit |
| `morca._internal.db.pipelines_view` | the pipelines' pages: registrations, steps, step cards, keys, usage |
| `morca._internal.db.code_view` | the code links: which version a run or pipeline has, its `describe` |
| `morca._internal.db.home_view` | the home page: recent runs by batch, runs per day, the timeline |
| `morca._internal.db.datasets_view` | the datasets' pages: the steps that write and read one |
| `morca._internal.db.machines_view` | Ops' Pools and Control: instances, pods, workers, scaled pools |
| `morca._internal.db.alerts_view` | Ops' Alerts: a page of `orca.alerts` and its counts |

**Layering.** These modules import only the standard library, `postgres`
(`sqlite` for the SQLite ones), `morca.migrations` (the schema, which
imports nothing of morca's) and each other: never a module of morca's that
runs queries through them, so nothing above can import its way back in.
The rows a query returns are declared here, and the modules that used to
declare them import them from here (so `from morca.watchdog import Lease`
and `from morca.store import RunInfo` keep working), and `morca.live` and
`morca.batches` re-export what moved from them.

`tests/sql_in_db.sh` (CI's format job) fails when SQL appears in a string
outside `src/morca/_internal/db/` and `src/morca/migrations.mojo`, but in the files
`tests/sql_allow.txt` still lists; that list only shrinks.
"""
