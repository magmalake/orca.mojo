# Changelog

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
