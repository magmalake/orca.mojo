# Changelog

## 0.18.0 - 2026-11-09

- **Overlapping schedulers.** With `ORCA_OVERLAP=on` (off by default) a new
  scheduler takes over while the old one finishes its runs (#232).
- **`orca-stack apply` replaces only what changed.** `orca-stack plan`
  previews it; the first apply replaces every container once (#243, #230).
- **`deploy.sh` pauses the pipelines only when the scheduler is replaced**;
  `--all` replaces every container (#243, #230).
- **No downtime when the UI or an app is replaced.** The old container serves
  until the new one is healthy (#244, #231).
- **`orca ui` answers `/healthz`.** It returns 200 once the database answers
  and 503 otherwise, with no login needed (#244, #231).
- **Hetzner deploys that don't pause.** With `orca-stack configure OVERLAP=1`
  (off by default), `deploy.sh` overlaps the schedulers when it can (#232).
- **`orca ui` drains on SIGTERM**, finishing the requests in flight so a
  deploy no longer cuts a page off (#248).
- **The Hetzner tunnel runs a pinned cloudflared**, not `latest`; set
  `CLOUDFLARED_IMAGE` in `hetzner.env` to change it (#249).
- **orca-stack can deploy images from before 0.18**, whose UI has no
  `/healthz`, without leaving the pipelines paused (#251).
- **Pipeline pages: no stray vertical scrollbar beside the tabs** (#252).
- **`/healthz` answers 503 `draining` once `orca ui` starts draining**, so
  proxies stop routing to a UI that's going away (#253).
- **`fill_lag`** in orca.toml (such as `"2h"`) waits that long after a
  partition ends before filling it, for sources that publish late (#254).
- **A dev stack on Hetzner**, described in docs/hetzner.md with a release
  check list; the scripts say which stack they act on (#250).
- **A deploy that fails only on an unhealthy UI or app unpauses the
  pipelines** and names the containers it couldn't replace (#256).
- **`orca-stack quiesce` no longer waits for runs on remote workers**, since a
  scheduler restart doesn't stop them (#256).
- **`deploy.sh` syncs `/etc/orca/settings.env` from `hetzner.env`.** A hand
  edit there is now undone, so put it in `EXTRA_ENV_FILE` (#257).

## 0.17.0 - 2026-11-08

The backfill and rebuild forms show one copyable `orca login ... && orca
--remote ... backfill` line, and Ops › Control's Memory pressure and Runs
cards explain each value (#236).

- **`deploy.sh` registers the pipelines on every deploy**, and `orca-stack
  restore` unpauses what the deploy paused (#237).
- **The home page's pipelines table puts each pipeline's state first**, with
  Coverage, Running and Queued right after the name (#237).
- **Ops › Alerts lists every alert sent**, delivered or not and resolved or
  open; also `/api/alerts` and `orca alerts` (#238).

## 0.16.0 - 2026-11-08

- **Each pipeline shows one coverage number.** The home page and the pipeline
  page show how many of its partitions or keys are done (#227).
- **Finding which keys are done is fast for a pipeline without a mapped
  step**, 10 ms instead of over a second on 30,000 runs (#227).
- **Actions ask in a dialog in the UI's colours, not the browser's**, saying
  what will happen; without scripts the forms post as before (#233).
- **Code on hover on the lists.** Runs tables and the home page show a
  pipeline's or failed step's code on hover (#233).
- **Every code card leads with its docstring's first sentence** (#233).
- **An older orca runs on a newer, additive schema**, so a rollback needs no
  restore, except to a binary from before migration 24 (#234).
- **`deploy.sh` backs up before it deploys, and `--rollback` goes back** to
  the previous tags (docs/hetzner.md) (#234).

## 0.15.0 - 2026-11-08

- **A log or samples file an EC2 instance keeps is never left empty** or half
  written when the instance stops mid-copy (#218).
- **A run's `started` is when the scheduler started it**, not when its tick
  began, so runs that waited no longer look as if they overlapped (#218).
- **The web layer in modules, behind one router.** The `orca.ui_*`,
  `orca.terms` and `orca.step_cards` modules moved under `orca.ui` (#219).
- **`ORCA_WORKER_ADDR`: the worker API on a listener of its own**, so a slow
  page can't hold up remote workers (#219).
- **Fills on worker pools.** A `worker:<pool>` pipeline can use `fills =
  "all"`; `fill_max_runs` caps the whole pool (#221).
- **A run whose worker is gone is queued again** for the pool, whatever its
  trigger, within `run_retries` (#221).
- **Machine metrics on workers.** Runs on `orca worker` now have the Machines
  card (#221).
- **A worker's claim no longer stalls behind one pipeline's queue**, since it
  now looks at each pipeline's oldest queued run (#221).
- **Runs lists filter by status, date, partition and run**, and cancel, retry
  or backfill many runs at once (#223).
- **Ops › Pools: one page for every machine runs are placed on.** It replaces
  Fleet and Workers, whose old URLs redirect to it (#224).
- **Admission by memory pressure.** No new run starts while memory pressure is
  over `ORCA_ADMIT_MEMORY_PRESSURE` (10%) (#225, #202).
- **The pressure, shown** on Ops › Control, `/api/workers`, `/metrics` and
  `orca status` (#225, #202).
- **Runs get `ORCA_MEMORY_PRESSURE_FILE`**, and `orca.memory_pressure()` reads
  it, so a step can lower its parallelism while pressure is high (#225, #202).
- **Runs get a cgroup memory limit, and an out-of-memory kill retries
  bigger.** On Hetzner it needs `orca-stack configure CGROUPS=1` (#201).

## 0.14.0 - 2026-11-08

- **Each run's peak memory and CPU** are measured and shown on its page, its
  steps and attempts, and a pipeline's Resources card (#200, #171).
- **`orca register` with no arguments works**, registering every pipeline in
  `orca.toml` as documented (#204).
- **Runs on the local runner get their secrets.** A run with secrets writes
  its `driver.log` at the end rather than live (#205).
- **A run's page is cards, like a pipeline's**, with its steps as Table,
  Timeline and Graph tabs (#207).
- **Lineage reads Upstream and Downstream**, and its diagram has a legend
  (#207).
- **Graphs are drawn top to bottom**, so `/datasets` and a snapshot's lineage
  fit a phone's width (#207).
- **Runs sized to their share of the machine.** A pipeline can ask for `cpus`
  and `memory_gb` ranges, and a run starts once its minimum fits (#171, #207).
- **Help on every table, and on the values in it**, as a "?" on each column
  and an explanation on hover for fixed values such as statuses (#207).
- **The Configuration table explains itself and links each value to its line
  in orca.toml** (#207).
- **Code on hover is coloured** (#207).
- **A year of coverage.** A daily pipeline's Coverage card has a Year tab with
  twelve months and a strip for its whole range (#211).
- **How far a sensor is behind what it follows** is shown on its Progress card
  and as amber days on Coverage (#211).
- **Lists in pages, and pages that keep up by themselves**: runs and snapshots
  page by Newer and Older, and lists refresh live (#214).
- **A card per step, on hover**, with its signature, docstring, datasets and
  recent timings (#215).

## 0.13.0 - 2026-11-07

- **`orca config check`** validates orca.toml in CI with no database, and the
  pipeline page shows where each setting came from (#181).
- **The watchdog shows every threshold a rule checks, and which one tripped**,
  in `orca watchdog status`, `/watchdog` and `/api/watchdog` (#180).
- **The pipeline page has three tabs**, Overview, Runs and Operate, each its
  own page (#194).
- **The pipeline page's header is three cards**, Progress, Config and Code,
  with a "?" on each fact (#194).
- **Hovering a link to code shows the code** on every page (#194).
- **Steps: Graph and Table tabs** (#194).
- **Relative times and live pages work again in browsers** (#194).
- **`deploy/hetzner/deploy.sh` pauses, waits and resumes by itself**;
  `--force` keeps the old behaviour (#193).
- **`orca status <pipeline>` shows how long each run ran** instead of when it
  was created (#192).
- **Ops sections can be linked to**, such as `/ops/control#cpu` (#189).
- **PostHog analytics in the UI, when asked for**, with `ORCA_POSTHOG_KEY`
  (`POSTHOG_KEY` on Hetzner); unset, nothing is added (#195).

## 0.12.0 - 2026-11-07

- **Alerts to your phone through ntfy.** Set `ORCA_NOTIFY_URL` for failed
  runs, watchdog trips and more; `orca notify test` checks it (#184).
- **Images build for Neoverse N1**, as Hetzner CAX servers need; a project's
  own `Dockerfile.orca` should pass `--target-cpu` too (#178, mojo-census#35).
- **`docs/hetzner.md` corrections from the first real setup**, and
  `FIXED_COST` in `hetzner.env` for the Fleet page's Cost tile (#179).
- **The UI draws a run's timeline and the pipelines over time**, on the run
  page and as a Timeline tab on the home page (#175).
- **Remote workers over the API.** `placement = "worker:<pool>"` leaves a
  pipeline's runs to `orca worker` machines (docs/workers.md) (#173).

## 0.11.0 - 2026-11-07

- **A live run's page refreshes itself, and shows its settings**, its duration
  and links to the previous and next run (#172).
- **Watchdog "Keep paused"** (`orca watchdog ack`), a fixed-cost tile for the
  local runner, and orca's icon in the header (#172).
- **`orca register` with no paths registers the whole `orca.toml`**, and
  `orca-stack` starts cloudflared on HTTP/2 (#170).

## 0.10.0 - 2026-11-06

- **One Hetzner server.** `deploy/hetzner/` runs orca and the census on one
  server with the local runner; docs/hetzner.md is the runbook (#166).
- **Fills.** `fills = "all"` fills a pipeline's whole range within limits; on
  EC2 or Kubernetes it needs `fill_budget_per_day` (#155).
- **UI: an Ops menu with Control and Fleet.** `/system` and `/fleet` moved to
  `/ops/control` and `/ops/fleet` and redirect there (#160).

## 0.9.0 - 2026-11-06

- **`orca.toml` configures each pipeline.** `orca register` reads it; a key
  removed from it goes back to its default (#145, #134).
- **Small pipelines on the control instance** with `--placement control`; to
  read secrets the AWS stack needs updating (#144).
- **`orca aws replace-control`** swaps in a new control instance; on an older
  stack `orca aws up` needs `--yes` and restarts it once (#154).
- **A watchdog** pauses pipelines whose launches, cost, failures or stuck runs
  go past their limits, and never resumes them itself (#151).
- **A run whose tasks wait for workers that never come fails its attempt**
  after `ORCA_NO_WORKERS_SECONDS` (600) and is retried (#148).
- **Steps that replace their partitions.** `Step[..., replaces=[...]]` makes a
  rerun overwrite a partition instead of appending to it (#120).
- **`orca.tables.commit` refuses to remove files not in the current snapshot**
  (mojo-census GAPS #29) (#143).
- **`orca backfill --rerun` and `orca rerun` are refused for steps that
  append**, unless given `--allow-append` (#143).

## 0.8.0 - 2026-11-06

- **Caddy compresses what it serves.** An existing stack's Caddyfile has to be
  edited by hand on the instance; the PR has the steps (#126).
- **The control instance's own health** on a `/system` page, `/metrics` and a
  warning on `/`; a running stack needs the PR's steps (#96).
- **A warm pool for the ec2 runner**: a pipeline's next run reuses its
  instances; `ORCA_EC2_POOL_SECONDS=0` turns it off (#134).
- **A calendar for dated pipelines in the UI**, to pick days to run and see
  which are done (#112).

## 0.7.0 - 2026-11-05

## 0.6.1 - 2026-11-05

### Changed

- **Pins dista-mojo 0.3.0** (#133).

## 0.6.0 - 2026-11-05

- **Several dista worker processes per EC2 worker instance**, with `orca
  register --worker-procs N|auto` (#130).
- **`orca login` and `--remote`** run orca's commands from a laptop against
  the deployed UI, with no SSM or database access (#111).

## 0.5.0 - 2026-11-05

- **A Fleet page** (`/fleet`) shows hosts, cost and runs per hour; an existing
  stack needs its `sampler` started by hand (#127).

## 0.4.0 - 2026-11-05

- **Bounded table history.** Old snapshots and metadata expire by default, and
  `orca expire` does it by hand (mojo-census GAPS #27) (#124).
- **The lake's bucket is private unless asked.** Pass `PublicData=true` to
  `orca aws up` to keep it public as before (#122).

## 0.3.5 (2026-11-05)

- **Built against iceberg-mojo 0.9.1** (iceberg.mojo#70), deflate-mojo 0.2.0,
  avro-mojo 0.4.5 and parquet-mojo 0.12.1 (#118).
- **Data files written on workers, committed by the step** with
  `orca.tables.append_files` (#108).
- **A crash's stack dump is symbolized, and a crashed attempt says where**,
  naming the function, file and line (#117).
- **Errors say what orca was doing**, naming the dataset or EC2 action that
  failed (#117).

## 0.3.4 (2026-11-04)

- **Built against iceberg-mojo 0.9.0** (parquet-mojo 0.12.0), so data files go
  through the streaming Parquet writer (#116).
- **`/runs` filters are dropdowns, and the runs go back further than 100**,
  paging 50 at a time (#113).
- **The pipeline page's Start runs is one form with a preview** of what a
  backfill would queue (#113).
- **Actions sit beside the statuses they change**, such as Pause by each
  pipeline and Cancel or Retry by each run (#113).
- **A dataset's Rebuild section says what it does and what it runs**, with a
  preview (#113).
- **Runs tables say Tries, not Starts**; the JSON field is still `starts`
  (#113).
- **Fixed: `/runs` sorted run ids as text**, so the newest weren't first past
  run 9 (#113).

## 0.3.3 (2026-11-04)

- **A run's page charts its machines' CPU, memory, disk and network** beside
  its steps (#95).
- **Each run records its machines' metrics** with `orca sample`;
  `ORCA_SAMPLE_SECONDS=0` turns it off (#94).
- **Runs in a SQLite file: `ORCA_DATABASE_URL=sqlite:<path>`**, for a laptop,
  one VM or CI; the scheduler and UI still need Postgres (#82).
- **`MemoryStore` is a whole `RunStore`**, so `<pipeline> run` without a
  database behaves like a recorded run (#82).
- **A pipeline binary depends on the `RunStore` trait alone**; the old import
  paths still work (#82).
- **Links to code look like links to code** (#98).
- **A dataset's page links the steps that write and read it to their code**
  (#98).
- **A step's reads and commits are recorded through its run's `Store`** (#82).
- **One live run per partition.** Fires, sensors, rebuilds and the UI no
  longer queue a second run over a partition that has one (#62).
- **A manual, UI or rerun run whose process died no longer stays `running`**;
  it fails as `interrupted` and can be resumed or cancelled (#61).
- **A process that may have lost its run no longer commits** (#60).
- **Cancel stops a running step, and steps can time out** with `Step[...,
  timeout_seconds=N]` (#68).
- **A pipeline binary that hangs no longer stalls the scheduler or the UI**
  (#64).
- **Behind an HTTPS proxy, the UI's forms work** when `ORCA_UI_TRUST_PROXY=1`
  or `ORCA_UI_SECURE_COOKIES=1` is set (#57).
- **The UI refuses `partitions=@file:<path>`**, which let a logged-in user
  read files on the UI's host (#58).
- **Retries after `orca register` of a new build no longer loop**, and runs
  record an `error` (#63).
- **A Postgres blip no longer throws away a finished step**; writes are
  retried for `ORCA_DB_RETRY_SECONDS` (#67).
- **Cancel runs in bulk, and pause a pipeline**, with `orca cancel <pipeline>
  KEYS|@all`, `orca pause` and `orca unpause` (#73).
- **`<binary> run` no longer re-registers the pipeline**, and refuses
  unregistered code without `--allow-new-code` (#59).
- **A snapshot a sensor can't run no longer wedges it** (#65).
- **Retries that wait, and steps that say "not yet"**, with `backoff_seconds`
  and `defer(seconds, why)` (#66).
- **Built against dista-mojo 0.2.2, iceberg-mojo 0.8.7 and objectstore-mojo
  0.4.2**, with the S3 multipart fix (#110).

## 0.3.2 (2026-11-03)

- **Built against flare_mlake 0.13.2 and k8s-mojo 0.1.2**, so a Linux program
  linking flare's client beside dista compiles (#56).

## 0.3.1 (2026-11-03)

- **`ensure_table` adds the columns a table lacks**, and refuses incompatible
  changes; it needs iceberg-mojo with `UpdateSchema` (iceberg.mojo#64) (#54).
- **A dated pipeline may write a keyed sensor's keys** (#51).
- **Names can hold a `/`**, so a keyed pipeline's keys can be `owner/name`
  (#53).
- **`orca backfill --rerun`** queues keys that are already done, too (#52).
- **Built against the shim-loading fix releases**, so a pipeline binary built
  under pixi runs from a plain shell (#55).

## 0.3.0 (2026-11-03)

A project deploys itself to AWS.

- **`orca aws`** creates the stack, pushes the image and registers pipelines;
  the stack's `ImageTag` defaults to `latest` (#49).
- **The stack's warehouse is public**, so a project's tables can be read
  unsigned; logs and run state stay private (#50).
- **Scaling goes through the pipeline's binary** (#50).

## 0.2.0 (2026-11-03)

Workers sized to the work, secrets for runs, and the stack's public data.

- **Secrets for runs.** `orca register --secret NAME=ID` gives a run a Secrets
  Manager secret, masked in every log (#46).
- **Autoscaling on EC2.** `orca register --workers MIN..MAX` starts and drains
  workers as the run needs; it needs dista-mojo 0.2.0 (#44).
- **The stack's public data** bucket, which anyone may read, for a lake's
  public tables (#47).

## 0.1.0 (2026-11-03)

orca's first release, as mojoshelf tin `orca-mojo`: phases 1 to 5 of
DESIGN.md, keyed partitions, and the first of phase 6.

- **Pipelines in Mojo**, with compiler-checked steps, monthly, daily and keyed
  partitions, cron schedules and retries (#42).
- **Runs in Postgres**, resumable after any crash (#42).
- **The scheduler** (`orca`): schedules, backfills, retries, sensors, lineage
  and rebuilds (#42).
- **Runners:** local processes, a dista cluster per run on Kubernetes, or
  instances per run on EC2 (#42).
- **The UI** (`orca ui`): pipelines, runs, datasets and lineage, as HTML and
  JSON (#42).
- **Tables:** pinned Iceberg reads and recognisable commits from steps (#42).
