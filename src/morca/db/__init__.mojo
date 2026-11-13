"""morca's SQL: every query on the `orca` schema, behind typed functions.

Each module owns the queries of one table or concern, and each function
takes typed arguments and returns typed rows (structs), counts or ids,
never a result set. A function runs on the connection or the transaction
its caller passes (`mut conn: Connection` or `mut tx: Transaction`), so
the caller keeps deciding what is one transaction and what it locks; a
query run both ways has a function for each, over one SQL text.

| Module | Owns |
|---|---|
| `morca.db.live` | which runs are live, dead or held, as SQL the others build on |
| `morca.db.pipelines` | `orca.pipelines`' paused flag, names and version |
| `morca.db.audit` | `orca.audit` |
| `morca.db.auth` | `orca.users`, `orca.sessions`, `orca.api_tokens` |
| `morca.db.batches` | `orca.runs.batch` |
| `morca.db.ec2` | the EC2 runner's runs and instances, and its warm pool |
| `morca.db.fleet` | `orca.fleet_samples`, `orca.fleet_hourly` and the fleet page's reads |
| `morca.db.health` | whether the database answers |
| `morca.db.kube` | the Kubernetes runner's reads of which runs' objects to keep |
| `morca.db.notify` | `orca.notifications`, `orca.alerts` and the alert rules' reads |
| `morca.db.pool_runs` | runs queued for a worker pool |
| `morca.db.pools` | `orca.pools`, `orca.pool_machines` and the pool autoscaler |
| `morca.db.queueing` | backfills: the keys done, `orca.keys`, the runs queued |
| `morca.db.registering` | `morca register`'s row of `orca.pipelines` |
| `morca.db.run_cluster` | a run's cluster over a pool: its head, `orca.run_members` |
| `morca.db.scheduler` | the scheduler's retries, dead runs and cgroup memory |
| `morca.db.schedulers` | `orca.schedulers`, the scheduler instances |
| `morca.db.starting` | a pass that starts a pipeline's runs |
| `morca.db.system` | `orca.system_minutes`, `orca.system_containers` |
| `morca.db.watchdog` | `orca.watchdog_rules`, `orca.watchdog_trips` and the rules' readings |
| `morca.db.workers` | `orca.workers`, the worker tokens, and workers' claims |

**Layering.** These modules import only the standard library, `postgres`
and each other: never a module of morca's that runs queries through them,
so nothing above can import its way back in. The rows a query returns are
declared here, and the modules that used to declare them import them from
here (so `from morca.watchdog import Lease` keeps working), and
`morca.live` and `morca.batches` re-export what moved from them.

`tests/sql_in_db.sh` (CI's format job) fails when SQL appears in a string
outside `src/morca/db/` and `src/morca/migrations.mojo`, but in the files
`tests/sql_allow.txt` still lists; that list only shrinks.
"""
