# Changelog

## Unreleased

- **Runs in a SQLite file: `ORCA_DATABASE_URL=sqlite:<path>`** (#82).
  `orca.sqlite_store.SqliteStore`, over sqlite-mojo 0.3.5, is a whole
  `RunStore`. With it, `run`, `resume` and `status` record a pipeline's
  runs in that file, resumable after a kill as on Postgres, with no server
  to operate: for a laptop, one VM or CI. `sqlite:///abs/path` is an
  absolute path. It is WAL mode with one writer at a time (`BEGIN
  IMMEDIATE`, a one-second busy timeout, then retries on the file opened
  again for `ORCA_DB_RETRY_SECONDS`), `synchronous = FULL`, and the file
  must be on a local disk. The scheduler, the UI and orca's other
  commands still need Postgres, and refuse a SQLite URL. `tests/chaos.sh`
  runs over a file as well (`test-chaos-sqlite`), with another writer
  holding the file's lock in place of Postgres restarts. orca-mojo now
  depends on sqlite-mojo, at the revision mojoshelf published (the one
  iceberg-mojo pins), and on libsqlite. `DbRetry`'s waits are
  `orca.store.Backoff`, shared by both stores.
- **`MemoryStore` is a whole `RunStore`** (#82). It keeps pipelines, runs,
  steps, attempts, reads and commits in the process, with the rules
  `PgStore` has (claims, stale heartbeats, losing a run, cancelling), so
  a run in it can be interrupted and resumed within the process.
  `<pipeline> run` without `ORCA_DATABASE_URL` now takes the same path
  as a recorded run: it prints `run 0` first, and its steps as `status`
  lists them (`after N attempts` included). In-memory runs are numbered
  0, -1, -2 and so on, never positive, so their commits are never taken
  for another run's. The executor's store tests (`tests/store_suite.mojo`)
  are written once over the trait: `test-store` runs them with no server,
  `test-state` on Postgres.
- **A pipeline binary depends on the `RunStore` trait alone** (#82).
  `orca.store.RunStore` extends `Store` with what `orca.cli` asked of
  `State` directly: `migrate`, `register`, `scheduled_hash`, `version`,
  `create_run`, `claim`, `ran_version`, `interrupt`, `prior`, `run_info`,
  `runs`, `steps`, and for the run it owns `own`, `start_heartbeat`,
  `stop_heartbeat`, `finish_run` and `cancel_requested`. `PgStore`
  implements it over `State` (`PgStore(url)` opens one), and keeps the
  heartbeat thread itself (`orca.heartbeat.Heartbeat[B]` over a `Beater`,
  `PgBeat` for Postgres), so `orca.cli` imports neither `State` nor
  postgres. `RunResult`, `StepRun`, the statuses, `RunInfo`, `StepInfo`
  and `process_owner` move to `orca.store`; the old import paths still
  work. The scheduler and the UI stay on `State`'s SQL.
- **A step's reads and commits are recorded through its run's `Store`**
  (#82). `Recorder` used to open a Postgres connection of its own;
  it now calls the store it came from (`Store.record`, which `PgStore`
  retries as it does its other writes), so `orca.ctx` no longer imports
  postgres and another store can keep lineage too. `TableSnapshot` and
  `Recorder` live in `orca.store`, and `orca.ctx` still exports both.
- **One live run per partition.** Only backfills checked for a queued or
  running run of the same key; a fire, a time-partitioned sensor, a
  rebuild's level and the UI's start each queued a run regardless, so two
  runs over one day could run at once and both append. Each now checks, in
  the transaction that would queue it and under a lock on the pipeline's
  row: a fire records the live run as its own (`fired ... left to live run
  N`), a sensor leaves those keys to it, a rebuild's level waits for it to
  end, and the UI refuses the start, naming the run. `orca register` warns
  of a schedule that fires more than once for one partition, since a fire
  after the first run has ended runs it again. The liveness SQL moved to
  `orca.live`. (#62)

- **A manual, UI or rerun run whose process died no longer stays
  `running`.** The scheduler retries only scheduled, backfilled and sensed
  runs, and left the others `running` with a stale heartbeat for good;
  since every queued or running run held its keys, `orca backfill` then
  queued nothing for them and a keyed sensor never offered them again. A
  tick now fails such a run with the error `interrupted: its process
  stopped`, so it can be resumed or cancelled, and a running run whose
  process died no longer holds its keys anywhere (`live_sql` replaces
  `LIVE_SQL`). A run whose retries are spent says so too. `orca backfill`
  prints on stderr which keys it left: those done, and those in a live
  run, with the run. (#61)
- **A process that may have lost its run no longer commits.** Liveness is
  a heartbeat, so a run whose process stayed alive while its heartbeat went
  stale (Postgres unreachable for longer than `ORCA_STALE_SECONDS`, a
  paused VM) was started again, and both processes could append the same
  step's rows. `append` and `commit` in `orca.tables` now check, right
  before committing to Iceberg, that this process still owns the run and
  that its heartbeat is younger than half the stale limit (`Ctx.fence`,
  which asks the run's store: `Store.fence`; `MemoryStore` has nothing to
  check); otherwise the attempt fails without committing, and a process that lost
  its run stops at the attempt's end as before. (#60)
- **Cancel stops a running step, and steps can time out** (#68). A
  running run asked to cancel has `ORCA_CANCEL_KILL_SECONDS` (default 60)
  to finish its step; past that its heartbeat thread fails the attempt,
  ends the run `cancelled` and exits the process, so on EC2 and Kubernetes
  the sweep removes its cluster instead of it billing until
  `ORCA_RUN_DEADLINE`. `Step[..., timeout_seconds=N]` stops an attempt
  older than N seconds the same way, failing the run (retried as any
  failed run is) with the timeout as its error. Both write an `orca.audit`
  row. The scheduler kills a local process of its own whose heartbeat
  went stale before it acts on the run. `runs.cancel_at`, migration 19.
- **A pipeline binary that hangs no longer stalls the scheduler or the UI**
  (#64). `capture` takes a timeout and kills the command's process group
  past it. The scheduler runs `sense` and `keys` in children it checks on
  each tick instead of waiting for them, killing one still going after
  `ORCA_BINARY_SECONDS` (default 300) and keeping that as the sensor's
  `sense_error`; `orca tick` still waits. `dista-status` and `dista-drain`
  get 10 s. A keyed sensor's keys are read again only for a newer snapshot
  of its keys dataset, and kept in `orca.keys` (migration 20), which the
  UI's keyed page shows instead of running `keys` on every view.

- **Behind an HTTPS proxy, the UI's forms work** (#57). The `Origin` check
  compared the browser's `Origin` with `http://` + `Host`, so behind Caddy
  terminating TLS (as `orca aws up` deploys it) every `POST`, `/login`
  included, got 403. The UI's own origin now takes its scheme from
  `X-Forwarded-Proto` when `ORCA_UI_TRUST_PROXY=1`, else `https` when
  `ORCA_UI_SECURE_COOKIES=1`; scheme and host compare in any case, and a
  default port is ignored. Another site's `Origin` is still refused, and
  `X-Forwarded-Proto` is ignored without a trusted proxy.
- **The UI refuses `partitions=@file:<path>`** (#58). The start and backfill
  actions passed the field to the CLI's key parser, which read the file on
  the UI's host and echoed its first line that wasn't a name in the 409, so
  a logged-in user could read `/proc/self/environ` or a password file, or
  hang the UI on `/dev/zero`. They now answer 400 without reading anything;
  `@all` and lists of names work as before. On the command line, a line of
  an `@file:` list that isn't a name is named by its number, not its text.
- **Retries after `orca register` of a new build no longer loop.** A failed
  run waiting for its retry kept the old registry hash, so the scheduler
  started it, `resume` refused it (exit 3) before claiming it, and the run
  was started again until its retries were spent, ending `failed` with no
  error (on EC2, launching a head each time). Registration now also moves
  failed runs waiting for a retry that have no step done to the new code.
  One with steps done is failed at once instead of started, with an error
  saying how to resume it with the new code. Runs have an `error` column
  (migration 15), shown by `<binary> status <run>` and the run's page: also
  set when a local run's process exits before claiming it (its exit code
  and last line) and when a run's cluster or instances can't start. (#63)
- **A Postgres blip no longer throws away a finished step** (#67). A run's
  process retries each write to Postgres (an attempt's start and end, a
  skip, a read or commit recorded, the run's end) on a new connection for
  `ORCA_DB_RETRY_SECONDS`, by default the stale limit, with backoff from a
  quarter second to 4 s. Losing the run to another process, or a cancel,
  still stops it at once. Before, any error left the run `running` and
  exited 3, and the step was redone on resume.
- **Cancel runs in bulk, and pause a pipeline.** `orca cancel <pipeline>
  KEYS|@all` cancels the pipeline's runs over those keys that are queued,
  running (before their next step) or failed and due a retry, as cancelling
  one run does; the pipeline page has the same form, empty for all. `orca
  pause <pipeline>` and `orca unpause`, and the page's buttons, stop and
  restart the scheduler starting its runs: fires, sensors, retries and
  backfills still queue theirs while it's paused, and runs already going go
  on. Migration 16 adds `pipelines.paused`; `/` and the pipeline page show
  it, also in JSON.
- **`<binary> run` no longer re-registers the pipeline.** With
  `ORCA_DATABASE_URL` set, a run by hand of a pipeline `orca register` gave
  a binary left its registry hash and `describe` changed to the binary's,
  so a dev build run once made the scheduler queue fires and backfills with
  code the registered binary refuses, and counted no key done. `run` now
  leaves the registration alone, and refuses, with exit code 2, a binary
  whose code isn't the registered one's unless it's given
  `--allow-new-code`; that run is recorded as its own code's version. Only
  `orca register` moves a pipeline's code. (#59)
- **A snapshot a sensor can't run no longer wedges it** (#65). One whose
  `orca.partitions` (or the keys an upstream run committed) aren't
  partitions of the sensor's kind is recorded in `orca.sensed` with no run
  and why (`sensed.error`), and the tick's other snapshots start their run;
  before, the raise rolled the tick back, every tick, for good. A value of a
  keys column that isn't a name is left out of `<binary> keys`, and counted
  on stderr, instead of failing every keyed sensor pass, backfill and page.
  A sensor pass that fails is kept as the pipeline's `sense_error` (printed
  once, not every second) and shown on its page in `orca ui`, with the
  snapshots it saw and didn't run. Migration 17.
- **Retries that wait, and steps that say "not yet"** (#66).
  `Step[..., retries=3, backoff_seconds=30]` waits 30 s before the first
  retry, doubling. `Pipeline[..., retry_seconds=3600,
  max_retry_seconds=3600]` sets a run's retry wait (else
  `ORCA_RETRY_SECONDS`) and caps its doubling, so a run can be retried
  hourly for days. A step raises `defer(seconds, why)` when it can't work
  yet: the attempt ends `deferred` without spending a retry, the run ends
  `deferred`, and the scheduler starts it again at that time without
  counting it against `run_retries` (`runs.deferrals`, migration 18). The
  UI shows "until ..." and can resume or cancel a deferred run. A backfill
  doesn't queue a deferred run's keys again.

## 0.3.2 (2026-11-03)

- **Built against flare_mlake 0.13.2 and k8s-mojo 0.1.2.** flare now declares
  `fcntl` variadic, as dista and orca do, so a Linux program that links
  flare's client (or k8s) beside dista compiles. The CI checkouts and
  `deploy/Dockerfile` pin the same revisions.

## 0.3.1 (2026-11-03)

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

- **Built against the shim-loading fix releases.** dista-mojo 0.2.1,
  k8s-mojo 0.1.1, iceberg-mojo 0.8.6, postgres-mojo 0.3.3,
  objectstore-mojo 0.4.1, flare_mlake 0.13.1 and json_mlake 0.4.3 (and, in
  the data environment, zstd 0.1.5, lz4 0.1.4, brotli 0.1.4 and sqlite
  0.3.5): every library they load is found without `CONDA_PREFIX`, next to
  the executable, in its `../lib` or on the binary's rpath, so a pipeline
  binary built under pixi runs from a plain shell, and a missing one is an
  Error instead of an abort. The CI checkouts and `deploy/Dockerfile` pin
  the same revisions.

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
