# orca

Orchestration for data pipelines written in Mojo: DAGs declared in Mojo and
checked by the compiler, schedules in cron syntax, and a web UI for runs,
reruns and backfills. Steps run on [dista](https://github.com/magmalake/dista.mojo)
clusters over Iceberg tables in object storage.

Early days: steps and pipelines are declared and checked at compile time, and
runs are recorded in Postgres and resume where they stopped, with Iceberg
reads pinned and commits recognised on retry (`orca.tables`), including
data files a step's dista tasks wrote (`append_files`), and steps that
replace their partitions so a rerun doesn't duplicate rows (`replaces=`).
A scheduler
fires cron schedules and backfills, and runs sensor pipelines when a dataset
they read gets a snapshot; `orca ui` shows pipelines, runs, logs and the
lineage of datasets, and starts, resumes, reruns, backfills, cancels and
rebuilds through it.
## Install

orca is the mojoshelf tin `orca-mojo`, built for Mojo 1.2:

```sh
pixi init lake && cd lake
pixi shelf add orca-mojo
```

A pipeline is a Mojo file ending in `orca_main[YourPipeline]()` (see
`examples/`), and the `orca` command (scheduler, UI, registration) is two
lines of your own (not in a file named `orca.mojo`, which would be the
module `orca` itself):

```mojo
# bin/scheduler.mojo
from orca.command import orca_command

def main() raises:
    orca_command()
```

```sh
mojo build pipelines/daily.mojo -o build/daily
mojo build bin/scheduler.mojo -o build/orca
./build/orca locate build/daily --root . && ./build/orca register ./build/daily
```

## Docs

| Page | What it covers |
|---|---|
| [docs/orca-toml.md](docs/orca-toml.md) | `orca.toml`: every key, precedence, `orca config show` and `export`, and [fills](docs/orca-toml.md#fills) |
| [docs/placement.md](docs/placement.md) | where a pipeline's runs run: `default`, `ec2`, `control` and `worker:<pool>`, and the control host's limits |
| [docs/workers.md](docs/workers.md) | remote workers: `orca worker`, worker tokens, a worker on another machine, the run's state over the API |
| [docs/watchdog.md](docs/watchdog.md) | the watchdog's rules and thresholds, what a trip pauses, `orca watchdog status`, `ack` and `snooze` |
| [docs/alerts.md](docs/alerts.md) | alerts to your phone through ntfy: what is sent, `ORCA_NOTIFY_*`, `orca notify` |
| [docs/ui.md](docs/ui.md) | the UI's newer parts: the pipeline page's tabs and cards, a run's timeline, Ops › Control, Pools and Watchdog |
| [docs/hetzner.md](docs/hetzner.md) | the runbook for one Hetzner server: setup, deploying, rescaling, backups, alerts |
| [docs/replace-control.md](docs/replace-control.md) | the runbook for replacing the AWS control instance |
| [docs/future-directions.md](docs/future-directions.md) | where orca's seams are, and which are worth cutting |

The site's [orca docs](https://mlake.dev/s/orca/docs) cover writing a
pipeline, the command line, the scheduler and the UI's pages.
[VISION.md](VISION.md) says what orca is for and how it gets there;
[DESIGN.md](DESIGN.md) is the plan for building it; [K8S.md](K8S.md) is how
it will run on Kubernetes and AWS; [docs/future-directions.md](docs/future-directions.md)
is where its seams are and which are worth cutting.

## Commands at a glance

```
pixi run -e default test       # unit, CLI, compile-fail, state, kill, tasks, logs (plain `pixi run test` is ambiguous)
pixi run -e data test-tables   # Iceberg; needs the sibling checkouts (see CI)
pixi run -e data test-append-files   # files written in dista tasks, committed once by the step
pixi run -e data test-taxi     # examples/taxi on two sliced TLC months
pixi run -e data test-lineage  # lineage over three pipelines' tables
pixi run -e data test-expire   # snapshot expiry: orca expire, orca_maintain, the scheduler's sweep
pixi run -e data test-sensors  # sensors and a rebuild, with the scheduler running
pixi run -e data test-lineage-s3   # lineage, and sensors (test-sensors-s3), over MinIO
pixi run -e data test-keyed    # partitions named by a dataset's column, not dates
pixi run -e data test-keyed-backfills   # 20,000 keys backfilled a hundred to a run, and new ones sensed
pixi run -e data test-keyed-sensor   # a keyed sensor over the keys a daily pipeline writes
pixi run -e data test-keyed-names   # keys such as owner/repo, in logs and the UI
pixi run toy && ./build/toy run --partitions 2024-01..2024-03
pixi run fanout && ./build/fanout run --partitions 2024-01   # steps calling dista
pixi run daily && ./build/daily run --partitions 2024-02-27..2024-03-02   # by day
ORCA_DATABASE_URL=postgresql:///orca ./build/toy run --partitions 2024-01..2024-03   # a server you run; scripts/with-postgres.sh starts a throwaway one
ORCA_DATABASE_URL=sqlite:build/runs.db ./build/toy run --partitions 2024-01..2024-03   # or a file: resumable, no server
./build/toy status
pixi run -e data taxi && pixi run -e data ./build/taxi run --partitions 2023-01..2024-12
pixi run orca && ./build/orca register ./build/toy && ./build/orca scheduler   # needs ORCA_DATABASE_URL
./build/orca register ./build/taxi && ./build/orca backfill taxi 2011-01..2024-12   # 19 GB of TLC files
./build/orca backfill --rerun --allow-append taxi 2024-03   # queue a month again, though it succeeded; taxi's load appends, so say so
./build/orca cancel taxi @all  # cancel its queued, running and retrying runs (or KEYS)
./build/orca pause taxi        # the scheduler starts none of its runs until orca unpause taxi
./build/orca watchdog status   # the watchdog pauses pipelines on a launch storm, runaway cost, idle workers,
                               # failures, stuck runs or a full control host (docs/watchdog.md);
                               # orca watchdog snooze launches 2h silences a rule, orca watchdog ack keeps a trip's pipelines paused
./build/orca notify test        # a push to your phone through ntfy (ORCA_NOTIFY_URL): watchdog trips, failed runs, stalled fills, disk, lost workers (docs/alerts.md)
./build/orca ui                # http://127.0.0.1:8270: pipelines, runs and datasets, also as JSON; start, resume, rerun, backfill, cancel, pause;
                               # Ops > Control is the machine running the scheduler and UI, Ops > Pools every machine runs are placed on
                               # (EC2 instances, Kubernetes pods, remote workers), Ops > Watchdog the rules that pause pipelines
                               # Pipelines > Timeline: each pipeline's runs over 1 h to 7 d, in dataset order, with what started what;
                               # a run's page draws its Dista timeline (zoom, hover) without leaving for Perfetto
./build/orca user add alice    # a login for the UI's actions; the password from stdin
./build/orca status taxi       # its state and latest runs, with how long each ran, as its page shows them
./build/orca login https://orca.example.com   # from a laptop: a token for the deployed UI, kept mode 600
./build/orca --remote orca.example.com backfill taxi 2024-03   # then backfill, pause, cancel, resume, rerun, status there
./build/orca lineage lake.report   # what a dataset's latest snapshot was made from, and what was made from it
./build/orca rebuild raw.events 2024-03   # rerun every pipeline downstream of a dataset, in order
./build/orca expire raw.events --older-than 7d   # drop old snapshots (or --all, by each table's policy), keeping what live runs need
docker build -f deploy/Dockerfile -t orca:dev . && kubectl apply -f deploy/k8s/   # on Kubernetes
tests/kind.sh                  # that, on a kind cluster: each run on a dista cluster of its own
pixi run test-ec2              # ORCA_RUNNER=ec2 against a stand-in EC2: a head and workers per run
./build/orca register --placement control ./build/toy   # a small pipeline's runs on the scheduler's own host, no instance (docs/placement.md)
pixi run test-placement        # that, within the control instance's limits, beside runs on the stand-in EC2
./build/orca register --placement worker:bgent ./build/toy   # its runs on a remote worker of pool bgent (docs/workers.md)
./build/orca worker-token create --pool bgent   # that pool's token; then on the worker, with no database:
ORCA_WORKER_TOKEN=orca_... ./build/orca worker --pool bgent --remote https://orca.example.com
pixi run -e data test-worker   # orca ui, the scheduler and a worker: claims, a killed worker's runs requeued, tokens, versions
./build/orca register ./build/toy   # with ./orca.toml if there is one: how each pipeline runs, checked in (below)
./build/orca config show toy   # its effective settings and where each came from (--json for scripts)
./build/orca config export     # an orca.toml from what's registered, to adopt one without retyping flags
pixi run test-fill             # fills = "all": a pipeline filling its whole range by itself, within its limits
mojo build -D ORCA_SOURCE=$(git remote get-url origin) -D ORCA_COMMIT=$(git rev-parse HEAD) ...   # each run links the commit it ran
```

## orca.toml: how each pipeline runs

What a pipeline does stays in its code; where and how much it runs with
goes in `orca.toml` at the root of the project, which `orca register` and
`orca aws register` read (`orca.config`):

```toml
[defaults]
placement = "ec2"
head_type = "t4g.medium"

[pipelines.gharchive]
worker_type = "c7g.xlarge"
workers = "0..4"             # or a fixed count, 4
worker_procs = 1             # or "auto"
max_runs = 2                 # over the code's Pipeline max_runs
env = { DISTA_WORKER_SLOTS = "4", GHARCHIVE_PARALLEL = "16" }
secrets = { GITHUB_TOKEN = "orca/github-token" }   # Secrets Manager IDs, never values
fills = "all"                # fill the whole range by itself (below)
fill_max_runs = 2            # fill runs at once, beside max_runs
fill_budget_per_day = 5.0    # estimated US dollars a UTC day
fill_window = "22..6"        # UTC hours fill runs start in
```

A pipeline the file names is registered with exactly what `[defaults]`,
its own table and any flags say (flags win, and are recorded as
overrides; a key taken out of the file goes back to its default). One it
doesn't name is registered as before. Its `env` goes to its runs alone, on
every runner, and is part of the EC2 warm pool's key; `ORCA_RUN_ENV` still
copies the scheduler's variables to every run, under the pipeline's own.
The registration keeps the file's SHA-256 and git commit. `--config F`
reads another file, `--no-config` none; `orca config show` prints each
value with its source (code default, orca.toml, flag, environment). Every key, the
precedence and migrating from flags: [docs/orca-toml.md](docs/orca-toml.md).

**Fills.** With `fills = "all"` the scheduler fills the pipeline's whole
range by itself, from its first partition through the last one that has
ended. The default is `"since_registration"`, where a schedule fills from
registration on. Each fill run takes one partition, trigger `fill`, in
`fill_order` (`oldest` or `newest`). A fill never runs a done partition
again; that stays `orca backfill --rerun`. It waits for the pipeline's
queued runs, starts at most `fill_max_runs` runs on top of `max_runs`, and
only in `fill_window`. It stops when the pipeline is paused, by a person or
by the watchdog.

The budget is an estimate from orca.fleet's on-demand price table, and it
fails closed: fill runs on EC2 or Kubernetes don't start without a budget,
nor when a price is unknown. The pipeline page's Fill line and
`/api/pipelines/<name>` show done of total, rate, ETA, today's spend
against the budget, and why the fill is idle. Keyed pipelines and sensors
don't fill. See [Fills](docs/orca-toml.md#fills).

**Operations go through the UI or the remote CLI** (`orca login`, `orca
--remote <url> backfill|pause|unpause|cancel|resume`), which keep audit
rows. Change a fill's limits in `orca.toml` and register again. On AWS,
SSM `sudo docker exec scheduler orca ...` is for debugging only.

## Remote workers

A pipeline placed `worker:<pool>` runs on another machine, such as a coding
machine with spare CPU: `orca worker --pool <pool> --remote <url>` polls
`orca ui` for the pool's runs and runs them there, with its run's state
calls going over HTTPS to the UI (`orca.http_store`), which writes them to
Postgres. Nothing on the worker holds database credentials. Its token,
from `orca worker-token create --pool <pool>`, is scoped to the pool and
revocable. A worker that dies has its runs requeued once their heartbeats
are stale, and a worker of another image than the pipeline's is given
nothing. Ops > Pools lists the workers' machines. See
[docs/workers.md](docs/workers.md).

## Alerts

Problems otherwise show only in the UI. With `ORCA_NOTIFY_URL` set to an
[ntfy](https://ntfy.sh) topic URL, the scheduler pushes them to the phone
app: a watchdog trip, a run that failed after its last retry, a stalled
fill, the control host's disk over 85%, a lost worker, and, from a script,
`orca notify --kind backup-failed ... "message"`. The same alert is sent at
most once per 30 minutes, and a failed send is retried later without
blocking the scheduler. `orca notify test` checks the setup; Ops > Control
shows what was sent. Every setting: [docs/alerts.md](docs/alerts.md).

## The watchdog

Every minute the scheduler checks launches, estimated cost, idle workers,
failing attempts, stuck runs and the control host's memory and disk. A rule
past a threshold pauses its pipelines and shows a banner; it never resumes
anything. Unpause the pipelines to clear the trip, `orca watchdog ack` to
keep them paused, or `orca watchdog snooze <rule> 2h` while you fix the
cause. On the `local` runner the cost rule reads "fixed price". Each rule's
thresholds and variables: [docs/watchdog.md](docs/watchdog.md).

## Running it on one server

[docs/hetzner.md](docs/hetzner.md) is the runbook for a fixed-price setup: one
Hetzner Cloud ARM server, the `local` runner, S3-compatible Object Storage
(`AWS_ENDPOINT_URL_S3`), secrets from files (`ORCA_SECRET_FETCH`), and
Cloudflare Tunnel, with scripts in `deploy/hetzner/`. On the `local` runner
the watchdog's cost rule reads "fixed price" and Ops > Pools says no
instances are launched.

## Releasing and operations

Releasing and operations: https://mlake.dev/getting-started/maintain

## Developing locally

The tests and examples above run from a checkout of this repository, with the
repositories it compiles from next to it (`../dista.mojo` and `../k8s.mojo`
for `pixi run toy` and the default tests, many more for `-e data`), at the
commits `.github/workflows/ci.yml` pins. [Develop
locally](https://mlake.dev/getting-started/develop) has the whole layout, a
loop that clones it from the workflow, the examples, a local Postgres,
scheduler and UI, the test suites and their timings, and how to try a change
inside [mojo-census](https://github.com/magmalake/mojo-census) before it is
released. `pixi run -e default test` is the whole default suite (`pixi run
test` stops with "the task 'build-shared' is ambiguous"), about 12 minutes.

## Reruns that replace a partition

A new run of a key (`orca backfill --rerun`, `orca rerun`, the UI's rerun)
appends again to what its steps append to, beside the rows earlier runs
committed. A step that recomputes a key's rows declares the dataset in
`replaces=` instead of `writes=`:

```mojo
comptime LoadDay = Step[
    "load_day", Day, Int64, load_day, Mapped, replaces=["gharchive.events"],
]
```

Its `append` and `append_files` to that dataset then make the run's first
commit to each partition an `overwrite` that removes the files earlier
runs left there; its later commits append, and a retried attempt still
finds what it committed. Two runs replacing one partition at once don't
both keep their rows: the one that finds the other's files after its own
first commit fails instead of committing. `replace_partitions` and
`replace_partition_files` do the same per call. A rerun of done keys of a
pipeline whose steps append is refused unless `--allow-append` (in the UI,
a confirm button). See `orca.tables` and DESIGN.md.

Switching a step from `writes=` to `replaces=` keeps the pipeline's
registry hash, and so its done keys, as long as the datasets keep their
order.

## When a step's outputs must be made again

The registry hash covers a pipeline's step names, functions, edges and
declared datasets, not what a step's body does. When a change to the code
means its earlier outputs are wrong, raise the step's `version=` (an
integer; none is 0):

```mojo
comptime Refresh = Step["refresh", Day, Int64, refresh, Mapped, replaces=["mojo.repo_stats"], version=2]
```

The new version is a new hash. Once it's registered, the keys done under
the old one are no longer done: coverage counts again from zero, a keyed
sensor queues every key at its next snapshot, and `orca backfill <pipeline>
@all` queues them all. A time-partitioned pipeline with `fills = "all"`
fills its partitions again, within its fill limits (`fill_max_runs`,
`fill_window`, `fill_budget_per_day`), since its fill counts a partition
done only under code with the same steps' versions; any other change of
code fills nothing again. With `since_registration`, regenerate its history
with `orca backfill <pipeline> <range>` or `orca rebuild <dataset> <range>`
(which also reruns what reads it). `orca register` says which applies.

Part of [magmalake](https://magmalake.dev).
