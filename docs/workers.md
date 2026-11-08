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
`max_runs` live runs.

## Fills on a pool

A pool-placed pipeline with `fills = "all"` fills its range as a local one
does ([orca.toml](orca-toml.md#fills)): the scheduler queues its fill runs
in the same order (`fill_order`), within the same `fill_window`, and never
over a partition that is done or in a queued or running run, or that a fill
run ever took. It doesn't start them: they stay queued, held "waiting for
a worker of pool <pool>", until a worker claims one.

**The budget.** `fill_max_runs` caps the pipeline's live fill runs across
the whole pool: the ones running on any worker and the ones queued for
one. So a fill never has more than `fill_max_runs` runs going, however many
workers the pool has, and the scheduler queues a new one only as one ends.
What one machine takes is that worker's own `--max-runs` and, for a
pipeline with a request, what its host can grant. Fill runs don't count
against `max_runs`, and the pipeline's other runs are claimed first. A
worker's machine runs anyway, so a pool's fill runs cost nothing against
`fill_budget_per_day`. The pipeline page's Fill line and the coverage
calendar count the queued fill runs as live.

A two-machine pool, the server and a Mac, filling gharchive two at a time,
oldest first:

```toml
[pipelines.gharchive]
placement = "worker:gharchive"
fills = "all"
fill_order = "oldest"
fill_max_runs = 2      # across the pool
```

```sh
# On the server, once: a token per machine.
orca worker-token create --pool gharchive --label hetzner
orca worker-token create --pool gharchive --label bgent

# On each machine, its own token in worker.env (below), one run at a time:
docker run -d --name orca-worker --restart always --env-file worker.env \
    -v "$PWD/secrets:/etc/orca/secrets:ro" -v "$PWD/secret-fetch:/etc/orca/secret-fetch:ro" \
    ghcr.io/<owner>/<image>:<tag> worker --pool gharchive --max-runs 1 \
    --name hetzner --remote https://orca.example.com    # --name bgent on the Mac
```

With `fill_max_runs = 2` and `--max-runs 1` on each, each machine runs one
fill run; with `--max-runs 2` on both, still two at once in all, wherever
they land. Give each machine its own token: the scheduler tells which runs
a worker still has by what its token's worker last said.

When a worker dies, its fill runs are queued again for the pool (below),
so the fill goes on on the other machine.

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

A pipeline with a request (`cpus`, `memory_gb`: [orca.toml](orca-toml.md#requests-and-grants))
is admitted against the worker's own host, as the scheduler admits a local
run: each claim says what the worker can grant (its CPUs and memory, the
cgroup's limits if set, less `ORCA_RESERVE_CPUS`, 0, and
`ORCA_RESERVE_MEMORY_MB`, 512, of the worker's environment), and the UI
gives it a run only if the run's minimum fits less what the worker's
running runs were granted. The run gets its grant in `ORCA_GRANT_CPUS` and
`ORCA_GRANT_MEMORY_MB`, recorded on the run. One that doesn't fit stays
queued, held saying which worker had no room, for another worker of the
pool. A worker from before requests says nothing of its room, and its runs
are started as before.

## A worker on another machine

The simplest worker is the server's own image in Docker, at the same tag,
so the pipeline binaries are at the paths they were registered with
(`/opt/orca/pipelines/<name>` in a project image).

On the server, once, a token for the pool:

```sh
orca worker-token create --pool bgent --label bgent
```

On the worker, `worker.env` (mode 600) holds the token, the bucket and
where secrets come from:

```sh
ORCA_WORKER_TOKEN=orca_...
ORCA_WAREHOUSE=s3://<bucket>/warehouse
ORCA_LOGS=s3://<bucket>/logs
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=<location>
AWS_ENDPOINT_URL_S3=https://<location>.your-objectstorage.com   # S3-compatible storage only
ORCA_SECRET_FETCH=sh /etc/orca/secret-fetch
```

Then:

```sh
docker run -d --name orca-worker --restart always --env-file worker.env \
    -v "$PWD/secrets:/etc/orca/secrets:ro" -v "$PWD/secret-fetch:/etc/orca/secret-fetch:ro" \
    ghcr.io/<owner>/<image>:<tag> worker --pool bgent --remote https://orca.example.com
```

- `secret-fetch` can be a copy of the server's
  ([`deploy/hetzner/server/secret-fetch`](../deploy/hetzner/server/secret-fetch)):
  it prints `/etc/orca/secrets/<id>`, so a pipeline's
  `GITHUB_TOKEN = "orca/github-token"` reads `secrets/orca/github-token`.
- Pass `--image` (or `ORCA_IMAGE`) only if the pipelines were registered
  with `--image`, and then the same value: [Versions](#failures).
- The worker needs only outbound HTTPS, to the UI and to the bucket.
- `docker logs orca-worker` shows each run it starts and how it exited;
  Ops > Workers shows its heartbeat.

**Deploying.** The worker runs the code of its own image, so keep it on
the server's tag: deploy the server first (`deploy.sh <tag>`), then
restart the worker on the same tag (`docker rm -f orca-worker`, then the
`docker run` above with the new tag). In between, a worker on the old tag
runs the old code, unless the pipelines are registered with `--image`: then
it is given nothing until it matches.

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
  goes to sleep, the run's heartbeat goes stale. A run whose worker is
  gone is queued again for the pool, whatever its trigger (a manual, UI or
  rerun run too, which on other runners fails `interrupted`): one whose
  heartbeat is stale and whose worker hasn't been seen for half the stale
  limit (`ORCA_STALE_SECONDS`), or has been seen since without it (a
  restarted worker). Its next start resumes it after its last step done.
  A run a worker claimed and never started has its start given back. Each
  is a row of `orca.audit` (user `scheduler`, action
  `/api/worker/requeue`, saying why) and a line of the scheduler's log.
  A run that started counts that start, and past `run_retries` it fails as
  on any runner, so a run that takes its worker down can't loop. A run
  whose process stopped while its worker goes on is the dead-run rule
  every runner has: queued again within `run_retries` if the scheduler
  retries its trigger, else failed `interrupted: its process stopped`,
  resumable from the UI. A machine that wakes up finds its run lost at
  its next call, and a commit is fenced.
- **A process that never claimed its run** (a missing binary, a `resume`
  that refused) has the run failed, saying why (`/api/worker/exited`).
- **What a run uses.** The worker samples each run's process tree every
  poll, as the scheduler does a local run's, and sends the samples with
  `/api/worker/exited`; the UI records each attempt's peak memory and CPU
  from them ([the UI](ui.md#what-a-run-uses)). Beside each run's process
  it also starts `<binary> sample`, as the scheduler does beside a local
  run, which writes the machine's metrics to
  `<ORCA_LOGS>/<pipeline>/<run>/metrics/worker-<name>.jsonl`, so the run's
  page has its Machines card (`ORCA_SAMPLE_SECONDS=0` in the worker's
  environment turns it off).
- **Versions.** A run needs its pipeline's image (`orca register
  --image`; `orca config show <pipeline>` prints it) to be the worker's
  (`--image`, `ORCA_IMAGE`), both empty included, so there's no silent
  drift. A worker of another image isn't
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

### A listener of its own

By default `orca ui` answers the worker API (`/api/worker/...`) on the same
port as its pages, so a page that is slow to render (a large pipeline's
Overview, Control's charts) can hold up a worker's claims and heartbeats,
and its runs' store calls, behind it. Set `ORCA_WORKER_ADDR` (`host:port`)
and `orca ui` also serves the worker API on that address, from a thread of
its own:

```sh
ORCA_UI_ADDR=127.0.0.1:8270 ORCA_WORKER_ADDR=127.0.0.1:8271 orca ui
```

Point `orca worker --remote` at it (through the tunnel, a hostname of its
own for that port); the runs it starts record themselves there too. That
address answers only the worker API: anything else is 404. The UI's own
port keeps answering the worker API as well, so workers can move over one
at a time. Unset, nothing changes.
