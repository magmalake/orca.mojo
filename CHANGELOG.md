# Changelog

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
