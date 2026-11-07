# Remote workers

A remote worker runs a pool's runs on a machine other than the server, such
as a coding machine with spare CPU, another server or a laptop. The worker
talks only to `orca ui`'s API, over HTTPS. Nothing on it holds database
credentials. The scheduler stays the only brain. It fires, senses, queues,
retries and runs the watchdog, and keeps all state in its Postgres. A worker
only runs what it is given.

```text
server                                         worker (bgent)
──────                                         ──────────────
orca scheduler  ── queues runs, held           orca worker --pool bgent
                   "waiting for a worker"         │  polls POST /api/worker/claim
orca ui  ◀──────── HTTPS (Cloudflare Tunnel) ─────┤  gets a run's spec
  │  /api/worker/claim, /exited, /release         │  starts <binary> resume <run>
  │  /api/worker/store/<call>  ◀── the run's state calls, with the token
  ▼                                               ▼
Postgres                                       ORCA_WAREHOUSE, ORCA_LOGS
                                               (Object Storage, straight from the worker)
```

## Placing a pipeline on a pool

```toml
[pipelines.mojo_releases]
placement = "worker:bgent"
```

Or `orca register --placement worker:bgent`. A pool's name is 1 to 32 of
`a-z`, `0-9`, `-` and `_`. The scheduler never starts these runs. They stay
queued, held "waiting for a worker of pool bgent", and the pipeline's page
says so. `max_runs` and `orca pause` apply as for any pipeline: a worker is
given a run only while its pipeline isn't paused and has fewer than
`max_runs` live runs. Fills (`fills = "all"`) don't run on workers yet.

## Tokens

On the server:

```sh
orca worker-token create --pool bgent --label bgent   # prints the token, once
orca worker-token list                                # id, pool, label, state, worker, last seen
orca worker-token revoke 3
```

A worker token is an API token, `orca_` and 64 hex digits, like `orca
login`'s. The server keeps only its SHA-256, in `orca.workers`. It is scoped
to one pool. It can claim that pool's runs and make the state calls of the
runs it claimed. It can do nothing else, and a user's token gets nothing
from the worker API. It lasts `--days`, by default and at most
`ORCA_WORKER_TOKEN_DAYS` (365). A revoked token stays listed as revoked, and
the worker using it is refused and exits. Each claim is a row of
`orca.audit`, user `worker:<label>`, source `worker`.

## Running a worker

```sh
ORCA_WORKER_TOKEN=orca_... ORCA_IMAGE=ghcr.io/magmalake/orca:0.11.0 \
ORCA_WAREHOUSE=s3://... ORCA_LOGS=s3://... ORCA_SECRET_FETCH=/etc/orca/fetch-secret \
  orca worker --pool bgent --remote https://orca.mlake.dev
```

| Flag | Default | |
|---|---|---|
| `--pool` | (required) | the pool, which must be the token's |
| `--remote` | `ORCA_REMOTE` | the UI; `https`, or `http` to this machine only |
| `--token-file` | `ORCA_WORKER_TOKEN` | the token, the file's first line |
| `--max-runs` | 1 | runs at once on this worker |
| `--poll-seconds` | 2 | how often it asks for a run, and beats |
| `--dir` | `$TMPDIR/orca-worker` | where a run's output is staged |
| `--image` | `ORCA_IMAGE` | the worker's version (below) |
| `--name` | the host name | how Ops and the run's page name it |

A worker with `ORCA_DATABASE_URL` set refuses to start.

A run it is given starts the way the local runner starts one. The pipeline's
binary runs at the path it was registered with, which the worker's image
must have too, as `<binary> resume <run>`, in a process group of its own.
It gets the pipeline's `env`, its secrets read on the worker with the
worker's own `ORCA_SECRET_FETCH` (the spec names them, never their values),
and `ORCA_STATE_URL` and `ORCA_STATE_TOKEN` in place of a database. Data and
logs go straight from the worker to `ORCA_WAREHOUSE` and `ORCA_LOGS`, as
any run's do. When the process ends, `<binary> keep-log` appends its output,
masked, to the run's `driver.log`.

## The run's state over HTTPS

A pipeline binary with `ORCA_STATE_URL` records its run through
`orca.http_store.HttpStore`, the HTTP implementation of the `RunStore`
trait that `PgStore` and `SqliteStore` implement. Each call is a `POST
/api/worker/store/<call>` of JSON. The UI makes the same call on `PgStore`,
so every check is the same code as for a run that writes to Postgres
itself: the run is still this process's, it isn't cancelled, and its
heartbeat is fresh enough to commit. The calls are `run_info`, `claim`,
`version`, `ran_version`, `interrupt`, `prior`, `steps`, `start_attempt`,
`finish_attempt`, `skip`, `pinned_reads`, `record` (reads and commits, so
lineage and `find_commit`'s retry guard work as before), `fence`,
`set_log`, `finish_run`, `defer_run`, `cancel_requested` and `beat`.
Encoded inputs and outputs travel as hex.

A call that can't reach the UI, or gets a 5xx, is retried for
`ORCA_DB_RETRY_SECONDS`, as Postgres is. A call that is refused (401, 403,
or 409 for a lost or cancelled run) is final, and the run stops. A fence
that can't be made within half the stale limit refuses the commit.

## Failures

- **A dead worker.** Each run's heartbeat thread beats over the API. It
  also exits its process once the worker that started it (`ORCA_WORKER_PID`)
  isn't its parent any more. So when a worker is killed, or its machine
  goes to sleep, the run's heartbeat goes stale, and the scheduler queues
  the run again for a worker. That is the dead-run rule every runner has,
  within `run_retries`. As on every runner, a run the scheduler doesn't
  retry (manual, UI or rerun) fails instead, `interrupted: its process
  stopped`, and can be resumed from the UI. A machine that wakes up finds
  its run lost at its next call, and a commit is fenced.
- **A process that never claimed its run** (a missing binary, a `resume`
  that refused) has the run failed, saying why (`/api/worker/exited`).
- **Versions.** A run needs its pipeline's image (`--image`, `image` in
  orca.toml) to be the worker's (`--image`, `ORCA_IMAGE`), both empty
  included, so there's no silent drift. A worker of another image isn't
  given the run, and the run stays queued, held saying why. The worker
  checks the spec again and puts back a run that isn't for it
  (`/api/worker/release`). Pin the same tag in `deploy.sh` and the worker's
  script.

## In the UI

Ops > Workers (`/ops/workers`, and `/api/workers` as JSON) lists each
worker token with its pool, label, worker name, image, last heartbeat,
current runs, CPUs and memory. A run's page says where it ran: the worker's
name, and the token's id and label.

## Network

Outbound HTTPS only, through the server's existing Cloudflare Tunnel. No
firewall change is needed, and Postgres stays reachable only on the
server.
