# orca

Orchestration for data pipelines written in Mojo: DAGs declared in Mojo and
checked by the compiler, schedules in cron syntax, and a web UI for runs,
reruns and backfills. Steps run on [dista](https://github.com/magmalake/dista.mojo)
clusters over Iceberg tables in object storage.

Early days: steps and pipelines are declared and checked at compile time, and
runs are recorded in Postgres and resume where they stopped, with Iceberg
reads pinned and commits recognised on retry (`orca.tables`). A scheduler
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
it will run on Kubernetes and AWS.

```
pixi run test                  # unit, CLI, compile-fail, state, kill, tasks, logs
pixi run -e data test-tables   # Iceberg; needs the sibling checkouts (see CI)
pixi run -e data test-taxi     # examples/taxi on two sliced TLC months
pixi run -e data test-lineage  # lineage over three pipelines' tables
pixi run -e data test-sensors  # sensors and a rebuild, with the scheduler running
pixi run -e data test-lineage-s3   # lineage, and sensors (test-sensors-s3), over MinIO
pixi run -e data test-keyed    # partitions named by a dataset's column, not dates
pixi run -e data test-keyed-backfills   # 20,000 keys backfilled a hundred to a run, and new ones sensed
pixi run -e data test-keyed-sensor   # a keyed sensor over the keys a daily pipeline writes
pixi run toy && ./build/toy run --partitions 2024-01..2024-03
pixi run fanout && ./build/fanout run --partitions 2024-01   # steps calling dista
pixi run daily && ./build/daily run --partitions 2024-02-27..2024-03-02   # by day
ORCA_DATABASE_URL=postgresql:///orca ./build/toy run --partitions 2024-01..2024-03
./build/toy status
pixi run -e data taxi && pixi run -e data ./build/taxi run --partitions 2023-01..2024-12
pixi run orca && ./build/orca register ./build/toy && ./build/orca scheduler   # needs ORCA_DATABASE_URL
./build/orca register ./build/taxi && ./build/orca backfill taxi 2011-01..2024-12   # 19 GB of TLC files
./build/orca backfill --rerun taxi 2024-03   # queue a month again, though it succeeded
./build/orca ui                # http://127.0.0.1:8270: pipelines, runs and datasets, also as JSON; start, resume, rerun, backfill, cancel
./build/orca user add alice    # a login for the UI's actions; the password from stdin
./build/orca lineage lake.report   # what a dataset's latest snapshot was made from, and what was made from it
./build/orca rebuild raw.events 2024-03   # rerun every pipeline downstream of a dataset, in order
docker build -f deploy/Dockerfile -t orca:dev . && kubectl apply -f deploy/k8s/   # on Kubernetes
tests/kind.sh                  # that, on a kind cluster: each run on a dista cluster of its own
pixi run test-ec2              # ORCA_RUNNER=ec2 against a stand-in EC2: a head and workers per run
mojo build -D ORCA_SOURCE=$(git remote get-url origin) -D ORCA_COMMIT=$(git rev-parse HEAD) ...   # each run links the commit it ran
```

Part of [magmalake](https://magmalake.dev).
