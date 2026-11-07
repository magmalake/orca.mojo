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

[VISION.md](VISION.md) says what orca is for and how it gets there;
[DESIGN.md](DESIGN.md) is the plan for building it; [K8S.md](K8S.md) is how
it will run on Kubernetes and AWS; [docs/future-directions.md](docs/future-directions.md)
is where its seams are and which are worth cutting.

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
                               # failures or stuck runs; orca watchdog snooze launches 2h silences a rule
./build/orca ui                # http://127.0.0.1:8270: pipelines, runs and datasets, also as JSON; start, resume, rerun, backfill, cancel, pause;
                               # Ops > Control is the machine running the scheduler and UI, Ops > Fleet the EC2 instances launched for runs,
                               # Ops > Workers the remote workers
./build/orca user add alice    # a login for the UI's actions; the password from stdin
./build/orca status taxi       # its state and latest runs, as its page shows them
./build/orca login https://orca.example.com   # from a laptop: a token for the deployed UI, kept mode 600
./build/orca --remote orca.example.com backfill taxi 2024-03   # then backfill, pause, cancel, resume, rerun, status there
./build/orca lineage lake.report   # what a dataset's latest snapshot was made from, and what was made from it
./build/orca rebuild raw.events 2024-03   # rerun every pipeline downstream of a dataset, in order
./build/orca expire raw.events --older-than 7d   # drop old snapshots (or --all, by each table's policy), keeping what live runs need
docker build -f deploy/Dockerfile -t orca:dev . && kubectl apply -f deploy/k8s/   # on Kubernetes
tests/kind.sh                  # that, on a kind cluster: each run on a dista cluster of its own
pixi run test-ec2              # ORCA_RUNNER=ec2 against a stand-in EC2: a head and workers per run
./build/orca register --placement control ./build/toy   # a small pipeline's runs on the scheduler's own host, no instance (orca.control)
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
nothing. Ops > Workers lists the workers. See
[docs/workers.md](docs/workers.md).

## Running it on one server

[docs/hetzner.md](docs/hetzner.md) is the runbook for a fixed-price setup: one
Hetzner Cloud ARM server, the `local` runner, S3-compatible Object Storage
(`AWS_ENDPOINT_URL_S3`), secrets from files (`ORCA_SECRET_FETCH`), and
Cloudflare Tunnel, with scripts in `deploy/hetzner/`. On the `local` runner
the watchdog's cost rule reads "fixed price" and Ops > Fleet says no
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

Part of [magmalake](https://magmalake.dev).
