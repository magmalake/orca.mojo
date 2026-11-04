# Changelog

## Unreleased

- **`ensure_table` adds the columns a table lacks.** Given a schema with
  columns the existing table doesn't have, matched by name at any depth, it
  adds them in one schema commit (iceberg-mojo's `UpdateSchema.union_by_name`)
  instead of returning the table unchanged; old rows read them as null. A
  column the schema marks optional becomes optional, and columns it leaves
  out are kept. It refuses, naming the dataset, a type change, a new
  required column, a required column left out, and a field id that isn't the
  one the table gives that column (batches built from the schema carry its
  ids). A concurrent commit is retried like a data commit. Needs
  iceberg-mojo with `UpdateSchema` (magmalake/iceberg.mojo#64).

- **A dated pipeline may write a keyed sensor's keys.** `orca register` no
  longer refuses a keyed sensor whose keys dataset a daily or monthly
  pipeline writes: a new snapshot of the keys dataset offers the keys it
  holds, and the sensor queues those that aren't done, so no rule from dates
  to names is needed. A keyed sensor reading any other dataset a
  time-partitioned pipeline writes, or the other way round, is still
  refused. `orca lineage` follows a step's declared read of the keys dataset
  back to the pipeline that wrote it.

- **Names can hold a `/`.** A keyed pipeline's keys may now be
  `owner/name`, such as GitHub repositories: a `Name` is 1 to 128 bytes of
  `A-Z a-z 0-9 . _ : - /`, still without `..` or a comma, and with a `/`
  only between other bytes, one at a time. An attempt's log directory
  writes the `/` as `%2F` (`orca.name.path_segment`), and the UI's key
  pages are at `/pipelines/<pipeline>/keys/owner%2Fname`.

- **`orca backfill --rerun`.** A backfill skips keys that are done, so
  there was no way from the command line to run a finished day or month
  again. `orca backfill --rerun <pipeline> KEYS` queues the done keys as
  well; a key already in a queued or running run is still left to it. The
  UI's backfill form has the same as a checkbox (`rerun=1`).

## 0.3.0 (2026-11-03)

A project deploys itself to AWS.

- **`orca aws`.** `up` creates or updates the stack from the template orca
  carries, `push` builds a project's image (its `bin/` and `pipelines/`,
  for linux-aarch64) and pushes it to the stack's repository, `register`
  moves the control instance to it and registers pipelines with
  `orca register`'s flags, `app` runs a project's `bin/<name>` beside the
  UI on its own hostname, `status` prints the stack's outputs and `down
  --yes` deletes it. They call the `aws` and `docker` commands, logged in
  as their user. The control instance gains `orca-refresh` and `orca-app`,
  and the stack's `ImageTag` defaults to `latest`.
- **The stack's warehouse is public.** `ORCA_WAREHOUSE` is the public
  bucket, so a project's tables can be read unsigned; logs and run state
  stay in orca's own bucket. Apps get the region and the warehouse, not
  orca's database. `orca aws up --zone` picks the Availability Zone.
- **Scaling goes through the pipeline's binary** (`<binary> dista-status`,
  `dista-drain`), since dista's sockets can't share orca's binary with
  flare's on Linux; workers advertise their instance's private address.

## 0.2.0 (2026-11-03)

Workers sized to the work, secrets for runs, and the stack's public data.

- **Secrets for runs.** `orca register --secret NAME=ID` (ID under `orca/`)
  gives an EC2 run's instances the Secrets Manager secret as `NAME`, read at
  start with the run role, which the stack lets read `orca/*` only. Every
  log orca writes masks the values of the variables `ORCA_SECRETS` names;
  Postgres keeps only the IDs.

- **Autoscaling on EC2.** `orca register --workers MIN..MAX` (MIN may be 0)
  starts a run with MIN workers; each tick the scheduler reads the run's
  head demand and applies `dista.scale.decide`, starting workers up to MAX
  and draining idle ones. `orca.instances` records each instance's address
  and when it was drained (`drained: idle`). `ORCA_SCALE_UP_AFTER`,
  `ORCA_SCALE_IDLE` and `ORCA_SCALE_STEP` tune it. The stack lets the
  control instance reach run heads on port 6390. Needs dista-mojo 0.2.0.
- **The stack's public data.** `mlake-public-<account>`, which anyone may
  read unsigned, for a lake's public tables; runs get its warehouse as
  `ORCA_PUBLIC_WAREHOUSE`. The console's hostname (`ConsoleHostname`,
  default `libs.mlake.dev`) is served by the control instance's Caddy.

## 0.1.0 (2026-11-03)

orca's first release, as mojoshelf tin `orca-mojo`: phases 1 to 5 of
DESIGN.md, keyed partitions, and the first of phase 6.

- **Pipelines in Mojo.** `Step` and `Pipeline` types whose edges the
  compiler checks; mapped and gather steps; monthly, daily and keyed
  partitions (`Keyed[dataset, column]`, `keys_per_run`); cron schedules in
  any time zone; retries; dista tasks inside steps. Every pipeline binary
  gets orca's command line (`run`, `resume`, `status`, `describe`, `keys`,
  `sense`, `fires`).
- **Runs in Postgres,** resumable after any crash, one writer per run by
  heartbeat; attempts' logs, the run's driver log and its dista timeline,
  on disk or in S3.
- **The scheduler** (`orca`): registration, schedules, backfills, retries,
  `max_runs`, a vCPU budget, sensors on datasets' snapshots, lineage and
  rebuilds over Iceberg tables.
- **Runners:** local processes; a dista cluster per run on Kubernetes
  (through k8s-mojo); or instances per run on EC2, a head and workers of the
  types registered (`--head-type`, `--worker-type`, `--workers`, `--spot`).
- **The UI** (`orca ui`): pipelines, runs, steps, attempts, datasets,
  snapshots and lineage, as HTML and JSON; actions behind a login; runs per
  day; a run's instances and their logs, its timeline drawn in the page and
  in Perfetto; the commit each run ran and its steps' and tasks' code
  linked (`-D ORCA_SOURCE`, `-D ORCA_COMMIT`, `orca locate`).
- **Tables:** pinned Iceberg reads and recognisable commits from steps
  (`orca.tables`, over iceberg-mojo).
