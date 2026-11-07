# Changelog

## Unreleased

- **Each run's peak memory and CPU** (#200). The scheduler samples each
  local and control-placed run's process tree every tick (one pass over
  `/proc` for all of them: resident bytes, and utime + stime with what the
  tree waited for), and `orca worker` its runs' every poll; `reap` now
  uses `wait4`, whose `ru_maxrss` and CPU time close each process's
  samples. Each attempt the process made gets its share by time:
  `peak_rss_bytes`, `cpu_seconds` and `peak_cpus` on `orca.attempts`
  (migration 35; SQLite files get the columns too), null where nothing
  measured it (EC2, Kubernetes, `<binary> run`, and macOS's peak cores).
  A worker sends its samples with `/api/worker/exited` (`pid`, `max_rss`,
  `marks`). The run's page has Peak memory and CPU used, the Steps table
  and Runs tables a Peak column, an attempt's page its own; the pipeline's
  Overview has a **Resources** card, the p50 and max of its last 20
  measured runs (`usage` in its JSON), what #171's requests will be read
  from. A run, step and attempt have `usage` in the JSON, `orca status
  <pipeline>` prints each run's and `<binary> status <run>` each step's.
  `orca.usage`, `docs/ui.md#what-a-run-uses`, `tests/usage.sh` (a step
  holding 200 MiB beside a child holding 100, each burning a core, on the
  local runner, the control host and a worker).

## 0.13.0 - 2026-11-07

- **orca.toml follow-ups: `orca config check`, the registration on the
  pipeline page** (#181). `orca config check [<file>] [--secrets]` checks a
  file for a project's CI with no database: what `orca register` refuses,
  with its `file:line: [table] key: why`, plus that each `head_type` and
  `worker_type` can be sized (a warning for one not in orca's price table,
  which counts as $0 an hour), and with `--secrets` that each secret can be
  read (`ORCA_SECRET_FETCH` or Secrets Manager; a value is never printed).
  It exits 1 on a problem. The pipeline page has a Configuration section
  (`#config`) with what `orca config show` prints: where it was registered
  from, the flags over the file and each value's source; `/api/pipelines/<name>`
  has it as `config`. A run's Settings link to it. `docs/orca-toml.md`,
  `tests/config.sh`. Still to do: drift warnings, `[stack]` read by
  `orca aws up`, and a register flag for per-pipeline env.

- **The watchdog shows every threshold a rule checks, and which one
  tripped** (#180). `control` printed one limit (85, disk's) while it
  tripped on memory's 90. Each rule now keeps a check per threshold
  (`orca.watchdog.limits`: `control` memory and disk, `cost` the sustained
  rate and the day, `failing` the count and the share, `idle` the CPU and
  the workers, `launches` and `stuck` one each) with its value, limit and
  whether it is past it, on its reading and on each trip (a `checks` jsonb
  column on `orca.watchdog_rules` and `orca.watchdog_trips`, migration 34).
  `orca watchdog status` prints `control   memory 98.30 % (trips over
  90.00, past), disk 10.60 % (trips over 85.00)  TRIPPED on memory`;
  `/watchdog` has a Checks column and says what a trip tripped on;
  `/api/watchdog` adds `checks` and `tripped_on` to rules and trips
  (`value` and `threshold` stay). `tests/watchdog.sh` has a control trip.

- **The pipeline page has three tabs**, each its own page (`?tab=`), so
  links and the back button work without scripts: **Overview** (the
  cards, Steps, keys, snapshots not run, Next runs), **Runs** (its runs
  and Versions) and **Operate** (Coverage beside Start runs and Stop
  runs, since the calendar is where days are picked). The name, Pause and
  the live Progress card are on every tab. `?cal=` and `?preview=` open
  Operate, a link to `#coverage`, `#start` or `#stop` is sent there, and
  a refused start, backfill or cancel goes back to it.

- **The pipeline page's header is three cards.** Progress (runs at once,
  the fill, a failing sensor, the next run, a link to the coverage),
  Config (partitions, schedule, run retries, placement, binary) and Code,
  side by side on a wide screen and stacked on a phone. Progress refreshes
  itself every 5 s from the new `/pipelines/<name>/progress` fragment.
  Each fact's name has a "?" that explains it, on hover or keyboard
  focus, with a link to the docs. The Code card names the pipeline, its
  steps and its dista tasks, each linked to its line (file and line in
  the title), with the code hash in small print under them; the two Code
  lines are gone.
- **Hovering a link to code shows the code.** On every page, a link to a
  line on GitHub shows that function, from its def through its docstring
  (up to 24 lines), fetched from raw.githubusercontent.com once per file;
  for a private repository it says it can't, and the link still opens it.
- **Steps: Graph and Table tabs.** The graph is shown first for a pipeline
  of more than one step, the table for one step; the choice is remembered
  in the browser, and `?steps=graph|table` links work without scripts.
- **Relative times and live pages work again in browsers.** The page
  script built an `Intl.DateTimeFormat` with `dateStyle` and
  `timeZoneName` together, which browsers refuse; the error stopped the
  script, so times stayed UTC and `data-poll` parts never refreshed.

- **`deploy/hetzner/deploy.sh` pauses, waits and resumes by itself.** It
  quiesces (pauses the pipelines that aren't paused, waits for running runs,
  listing them each minute; `--timeout`), deploys, then unpauses what it
  paused, logging each step. It refreshes the server's `orca-stack` first.
  `--force` keeps the old behaviour.

- **`orca status <pipeline>` shows how long each run ran** (`3m 21s`, up to now
  while it runs; `-` before it starts) instead of when it was created.

- **Ops sections can be linked to.** Each section on Ops › Control and Fleet
  has an id and its heading links to it, so `/ops/control#cpu` opens at the
  CPU chart.
- **PostHog analytics in the UI, when asked for.** With `ORCA_POSTHOG_KEY`
  set to a PostHog project key (`phc_...`), every page's head has the same
  snippet as the magmalake.org website: `array.js` and events through `/ph`
  on the UI's own origin (a first-party proxy, such as the `mojourney-ph`
  Worker in magmalake/magmalake.org for orca.mojourney.dev), memory-only
  persistence (no cookies, no `localStorage`), and `site: "orca"` on every
  event. Unset or not a `phc_` key, nothing is added. `deploy/hetzner`
  takes `POSTHOG_KEY` in `hetzner.env` and writes it as `ORCA_POSTHOG_KEY`;
  `orca-stack` passes it to the apps as `POSTHOG_KEY` (the census console's
  analytics). `docs/hetzner.md` has the setup.

## 0.12.0 - 2026-11-07

- **Alerts to your phone through ntfy** (#184). With `ORCA_NOTIFY_URL` (a
  topic URL; the topic itself can come from a secret,
  `ORCA_NOTIFY_SECRET`, read with `ORCA_SECRET_FETCH` and never logged;
  `ORCA_NOTIFY_TOKEN` / `_TOKEN_SECRET` for a protected topic) the scheduler
  POSTs, with ntfy's `Title`, `Priority`, `Tags` and `Click` headers, a
  watchdog trip (high; low when acknowledged or unpaused), a run that
  failed after its last retry, a fill with no fill run finished in
  `ORCA_NOTIFY_FILL_STALL_HOURS` (2), the control disk over
  `ORCA_NOTIFY_DISK_PERCENT` (85), and a worker quiet for
  `ORCA_NOTIFY_WORKER_SECONDS` (300) while it held runs. The same alert is
  sent at most once per `ORCA_NOTIFY_MIN_SECONDS` (1800), kept in the new
  `orca.notifications` table (migration 33) so a restart doesn't resend; a
  send that fails (a 5 s timeout, `ORCA_NOTIFY_TIMEOUT_MS`) is logged
  without the URL and retried on a later tick with a doubling wait, and
  never blocks or fails the tick. `orca notify test` sends a test message;
  `orca notify --kind K [--subject S] ... "message"` is the same for
  scripts (the Hetzner `orca-stack backup` calls it on failure). Ops >
  Control and `/api/system` (`notify`) say whether alerts are on and list
  the last ones. `deploy/hetzner` takes `NTFY_TOPIC_FILE`, `NTFY_TOKEN_FILE`,
  `NTFY_URL` and `UI_URL`; `docs/hetzner.md` has the phone setup.
  `orca.notify`, `orca.alerts`, `tests/notify.sh` (`pixi run -e default
  test-notify`) against `tests/fake_ntfy.py`.

- **The default project image builds for Neoverse N1** (#178). `orca aws
  push`'s image (`deploy/project.Dockerfile`) passes `--target-cpu
  "$MOJO_TARGET_CPU"` (`ARG`, default `neoverse-n1`) in every `mojo build`:
  binaries built on GitHub's arm64 runners (Neoverse N2, SVE2) died with
  SIGILL (exit 132) on Hetzner CAX servers (N1), as mojo-census#35 found.
  `docs/hetzner.md` tells projects with their own `Dockerfile.orca` to do
  the same.

- **`docs/hetzner.md` corrections from the first real setup** (#179).
  Points at the census's `orca.toml` instead of quoting slot counts; image
  tags are `<short-sha>`/`latest` and where to find the sha; `hcloud` with a
  context and `hetzner.env` outside the repository (`HETZNER_ENV`); the
  bucket in the server's location; GHCR's private route as the default;
  Cloudflare's existing A records and DNS-only CNAMEs; the deploy sequence
  with runs running (`orca-stack quiesce`, `deploy.sh`, `orca-stack
  resume`). New `FIXED_COST` in `hetzner.env` becomes `ORCA_FIXED_COST` for
  the Fleet page's Cost tile.

- **The UI draws a run's timeline and the pipelines over time.** A run with
  Dista tasks has a Timeline section that fetches `timeline.json` once it
  scrolls into view and draws it on a canvas: a track per process (driver
  or head, then each worker), a lane per slot, spans coloured by function,
  input waits as thin bars, a time axis, a tooltip with the span's process,
  slot, duration and arguments, wheel or drag to zoom and Reset zoom. Spans
  under a pixel are drawn one pixel per lane, so a trace of ~300,000 spans
  draws in a fraction of a second. Perfetto and the raw file stay linked,
  and the page no longer parses the trace on the server. The home page has
  List and Timeline tabs (remembered; the list is the default and needs no
  JavaScript): a row per pipeline in dataset order, indented by depth, with
  what it reads and what starts it, runs over 1 h, 6 h, 24 h or 7 d coloured
  by status, fill runs thinner, a now line, connectors from a run to the
  sensor or keyed runs its commits started (`orca.sensed` joined to
  `orca.commits`), and dataset edges at the left. The data is
  `GET /api/gantt?window=24h`. New module `orca.ui_viz`; `tests/ui.sh`.

- **Remote workers over the API** (#173). `placement = "worker:<pool>"`
  (orca.toml, `orca register --placement`) leaves a pipeline's runs to
  `orca worker --pool <pool> --remote <url>`, which claims them from
  `orca ui` (`POST /api/worker/claim`) and runs `<binary> resume <run>` on
  its own machine. The run records itself over HTTPS: `HttpStore`
  (`orca.http_store`, `ORCA_STATE_URL` and `ORCA_STATE_TOKEN`) is the
  `RunStore` whose calls `orca ui` makes on `PgStore`
  (`/api/worker/store/<call>`, `orca.workers`), so a worker holds no
  database credentials and refuses to start with `ORCA_DATABASE_URL`.
  The scheduler starts none of these runs; it keeps them queued, held
  "waiting for a worker", and requeues due retries and dead runs, so a
  killed worker's runs (their processes exit once the worker is gone) are
  claimed again. Worker tokens (`orca worker-token create|list|revoke`,
  `orca.workers` table, migration 32) are `orca login`'s kind of token,
  scoped to one pool, revocable, and each claim is audited. A run needs its
  pipeline's image to be the worker's (`ORCA_IMAGE`). Ops > Workers
  (`/ops/workers`, `/api/workers`) lists them; a run's page says where it
  ran. `orca.state.beat_run` is the heartbeat and watchdog both stores
  share. `docs/workers.md`, `tests/worker.sh` (`pixi run -e data
  test-worker`), `tests/test_worker.mojo`.

## 0.11.0 - 2026-11-07

- **A live run's page refreshes itself, and shows its settings.** While a
  run is queued or running, `/runs/<id>` replaces its facts, steps, machines
  and workers every 5 s from the fragment `/runs/<id>/live` (one poller
  instead of two; `/steps` and `/metrics` still answer). The fragment answers
  286 once the run has ended and the page stops asking; a finished run's page
  doesn't poll at all, and without scripts the page is complete as loaded.
  The poller (`data-poll`) now waits while the tab is hidden and asks at once
  when it is shown, backs off when a request fails, and keeps selected text,
  open `<details>` and scrolled boxes as they were. A new Settings section
  shows the environment the run had (what its start recorded, else the
  pipeline's registered one), max runs, placement and worker settings, with
  secrets by name only. The facts start with the run's duration (elapsed
  so far while it goes on, the total after; start and end in its tooltip),
  and the crumb links the pipeline's previous and next run (`rel=prev/next`;
  the left and right arrow keys follow them outside form fields).
  `tests/ui.sh`.

- **Watchdog: "Keep paused", the local runner's cost tile, orca's icon in
  the header.** The trip banner has a Keep paused button, and
  `orca watchdog ack [--all|<trip id>]` does the same: the trip is
  acknowledged (migration 31, `acknowledged_at`/`acknowledged_by`, audited),
  its pipelines stay paused, the banner stops showing it, and
  `orca watchdog status` still lists it as acknowledged by the user. With
  `ORCA_RUNNER=local` the Cost tile reads "Cost", the optional
  `ORCA_FIXED_COST` (free text such as `€8.49/month`; "Fixed" if unset) and
  "fixed price: the local runner launches no instances", with no
  region; `/api/fleet` says `cost_is_estimate: false`. The header's logo is
  the tab's icon (three connected nodes) in the theme's colours.
  `tests/watchdog.sh`, `tests/fleet.sh`.

- **`orca register` with no paths registers the whole `orca.toml`.** Each
  `[pipelines.<name>]` is registered from `<dir>/<name>`, `<dir>` being
  `ORCA_PIPELINES_DIR`, else `pipelines/` next to the file; a table with no
  binary is an error naming it and nothing is registered. A file outside a
  git checkout (the image) records `ORCA_COMMIT` and `ORCA_SOURCE` instead of
  a commit, and git's `fatal: not a git repository` no longer reaches stderr.
  `cloud-init.yaml` creates a 4 GB swapfile if there is none.
  `orca-stack` starts cloudflared on HTTP/2 unless `tunnel.env` sets
  `TUNNEL_TRANSPORT_PROTOCOL`.
  `docs/hetzner.md`: the census's `orca.toml` lives in mojo-census.

## 0.10.0 - 2026-11-06

- **One Hetzner server: scripts, runbook, and the local runner as a
  production setup** (#166). `deploy/hetzner/` (`up.sh`, `deploy.sh`,
  `rescale.sh up|down`, `backup.sh` with a systemd timer on the server,
  `down.sh`, `cloud-init.yaml`, `hetzner.env.example`) runs orca and the
  census on one `hcloud` ARM server with a Volume for Postgres, Object
  Storage, and Cloudflare Tunnel; `--dry-run` prints the commands.
  `docs/hetzner.md` is the runbook (setup, deploy, rescaling for an import,
  backups and restore, teardown, costs, the census `orca.toml`), with
  `AWS_ENDPOINT_URL_S3` for S3-compatible stores and `ORCA_SECRET_FETCH`
  reading files. With `ORCA_RUNNER=local` the watchdog's cost rule reads
  "fixed price" and never trips (the other rules stay), and Ops > Fleet says
  the runner launches no instances. `tests/watchdog.sh`, `tests/fleet.sh`,
  `tests/test_watchdog.mojo`.

- **Fills: a pipeline fills its whole range by itself, within limits**
  (#155, `orca.fill`, migration 30). In `orca.toml` (no register flags):
  `fills = "all"` (the default `"since_registration"` is what orca did
  before), `fill_order` (`"oldest"` or `"newest"`), `fill_max_runs` (fill
  runs at once, on top of `max_runs`), `fill_budget_per_day` (estimated US
  dollars a UTC day) and `fill_window` (UTC hours fill runs start in,
  `"22..6"`). Bad values are refused with the file's line, and the keys
  are shown by `orca config show` and written by `orca config export`.
  Each tick, after the pipeline's other runs, the scheduler starts fill
  runs (trigger `fill`, one partition each) over the partitions of its
  range that no run took: from its first partition through the last that
  has ended, never one done in any of its code, live, or filled before.
  A fill waits while other runs of the pipeline are queued. It doesn't
  start while the pipeline is paused (the watchdog's trips included) or
  outside its window. Fill runs are retried like scheduled ones.
  `orca register` refuses `fills = "all"` for keyed pipelines, sensors and
  pipelines with no first partition.
  - The cost estimate is the fill runs' instances today (`orca.instances`)
    at orca.fleet's on-demand prices × 1.2, plus what the runs going and
    one more may cost (the longest recent fill run, an hour before there's
    one). It fails closed: no fill run starts on a budget with a price
    unknown (a type not in the table, Kubernetes), nor on EC2 or
    Kubernetes without a budget.
  - The pipeline page gets a Fill line (done / total, rate, ETA, today's
    spend against the budget, and why it's idle), and
    `/api/pipelines/<name>` serves the pipeline page's JSON with a `fill`
    object.
  - The README and docs/orca-toml.md say that operations go through the
    UI or the remote CLI, and that SSM `docker exec` is for debugging.
  - `examples/daily.mojo` takes `ORCA_DAILY_SLEEP`. `tests/fill.sh`
    (`pixi run test-fill`) covers it all on the local runner and against
    the stand-in EC2.

- **UI: an Ops menu with Control and Fleet.** "System" and "Fleet" read as
  the same thing, so the nav has one "Ops" item with two pages under it:
  `/ops/control` (was `/system`: the one machine running the scheduler, the
  UI, Postgres and control-placed runs) and `/ops/fleet` (was `/fleet`: the
  EC2 instances orca launches for runs). Each page's first line says what it
  shows and links to the other; Control also shows the watchdog's banner
  and links `/watchdog`. `/ops` redirects to Control. `/system`, `/fleet`
  and `/fleet/hosts` redirect permanently (301, query kept); `/api/system`
  and `/api/fleet` are unchanged. The menu is plain links (hover and
  keyboard focus open it; on a phone it is always shown inline), so it
  needs no JavaScript. `tests/ui.sh`, `tests/fleet.sh`, `tests/system.sh`.

## 0.9.0 - 2026-11-06

- **`orca.toml`: one checked-in file for how each pipeline runs** (#145,
  `orca.config`, `orca.toml`, migration 29). `[defaults]` and
  `[pipelines.<name>]` take `placement`, `head_type`, `worker_type`,
  `workers` (`N` or `"MIN..MAX"`), `worker_procs`, `max_runs` (over the
  code's), `secrets` (`NAME = "orca/<id>"`, references only) and `env`.
  `orca register` reads `./orca.toml` (or `--config F`, `ORCA_CONFIG`;
  `--no-config` for none); `orca aws register` checks the project's file
  and sends it whole, with its git commit, to the control instance's
  `orca register`. A pipeline the file names is registered with exactly
  `[defaults]`, its table and the flags (which win, recorded as
  overrides): a key taken out of the file goes back to its default. One it
  doesn't name keeps today's behaviour. Unknown keys and bad values are
  refused with the file's line. A pipeline's `env` goes to its runs alone
  (local, control, Kubernetes and EC2 runners), over the same names from
  `ORCA_RUN_ENV`, which still applies to every run, and on EC2 it's part of
  the warm pool's key (#134) and of a run's kept spec. `orca register
  --max-runs N` is new. The registration keeps the file's SHA-256, commit
  and whether it had uncommitted changes, with where each value came from
  (`orca.pipelines.config`; also `env`, `max_runs`). `orca config show
  [<pipeline>] [--json]` prints the effective configuration with each
  value's source: code default, orca.toml, flag or environment. `orca
  config export [--out F|-] [--force]` writes an `orca.toml` from what's
  registered. orca has its own TOML subset parser (mojoshelf's `mojo-toml`
  is built for Mojo before 1.0); `fuzz/fuzz_toml.mojo`. `tests/config.sh`.

- **Placement: small pipelines on the control instance** (#144).
  `orca register --placement control|ec2|default` (forwarded by
  `orca aws register`; stored with the registration, migration 26):
  `control` runs a pipeline's runs as children of the scheduler on its own
  host, with no instance to boot, at most `ORCA_CONTROL_MAX_RUNS` (2) at
  once, each at `nice` `ORCA_CONTROL_NICE` (10), killed over
  `ORCA_CONTROL_MEMORY_MB` (1024) resident, with `ORCA_CONTROL_WORKERS` (1)
  local dista workers, and none started while `/system`'s newest minute has
  memory or a disk over 85% (the run is held, saying why). Secrets are read
  by the scheduler (`ORCA_SECRET_FETCH`, else Secrets Manager's API) for that
  run alone; the driver's output is kept with `keep-log`, masked. Shown on
  the pipeline and run pages, `/fleet`'s control host and the JSON
  (`placement`, `held`). `deploy/aws/orca.yaml` lets the control role read
  `orca/*` secrets (stack update needed). `tests/placement.sh`.

- **`orca aws replace-control`** (#154, `orca.aws_replace`): a blue/green
  replacement of the control instance from a clean boot of the current
  template. It refuses unless every pipeline is paused and no run is running
  (`--wait` waits), stops the old scheduler and UI, backs up orca's database
  (`pg_dump -Fc`, verified by a restore into a scratch database) with the
  image tag, Caddy's data and the apps to `s3://<bucket>/backups/<stack>/`,
  boots a new instance in the stack's other slot that restores it, checks it
  (the restore, counts, schema version, the UI and apps, `/system` samples,
  `orca status`, the old settings and hostnames, `--smoke P=KEY`), moves the
  Elastic IP and keeps the old instance stopped for `--keep-old` (24h);
  `--resume`, `--rollback` and `--plan`. `deploy/aws/orca.yaml` now has two
  control slots (`BlueSlot`, `GreenSlot`, `ActiveControl`), each with its
  AMI pinned (`BlueAmi`, `GreenAmi`), the boot script in a launch template
  (`ControlBoot`) so a change to it stops no running instance, and
  `ControlEnv` for `/etc/orca.env` settings (`orca aws up --env K=V`);
  `orca-app` records the apps it starts. `orca aws up` on a stack from
  before the slots needs `--yes` (its instance restarts once).
  `docs/replace-control.md` is the runbook. `tests/replace_control.sh`.

- **A watchdog** (#151, `orca.watchdog`, migration 26): every
  `ORCA_WATCHDOG_SECONDS` (60; 0 turns it off) the scheduler checks, from
  Postgres only, instance launches (more than 200 an hour), estimated cost
  (over $3/h for 15 minutes, or $25 in 24 hours), idle workers (4 or more
  averaging under 15% CPU for 10 minutes), failing attempts (more than 20
  an hour, or over half of at least 10, per pipeline), stuck runs (nothing
  moved for 30 minutes on sampled instances) and the control instance
  (memory over 90%, disk over 85%), each overridable
  (`ORCA_WATCHDOG_*`). A rule over its limit pauses its pipelines as the
  Pause button does, audited as user `watchdog`, shows a red banner on `/`
  and `/fleet`, `/watchdog`, `/api/watchdog` and
  `orca_watchdog_{tripped,value,threshold}{rule}` on `/metrics`, and never
  resumes anything. `orca watchdog status` and `orca watchdog snooze <rule>
  <duration>` (also `--remote`, and the form on `/watchdog`) read and
  silence it. Email through SES is not part of this change.

- **A run whose tasks wait for workers that never come fails its attempt**
  (#148): on the ec2 runner, a run whose head has had tasks waiting and no
  worker for `ORCA_NO_WORKERS_SECONDS` (default 600; 0 turns it off) has its
  running attempts fail with `no workers joined in 10 min (tasks waiting:
  N)` and its instances terminated, and is started again as a run whose
  process died is, while its retries last. The run's page shows "Waiting for
  workers since ...". A live run's workers are now started from the
  registration its start was given (kept in `orca.runs.spec`, migration 26),
  not the pipeline's current one, so registering a pipeline again mid-run
  no longer strands the run without workers. `tests/no_workers.sh`.

- **Steps that replace their partitions** (#120): `Step[..., replaces=[...]]`
  declares datasets whose partitions a new run replaces. `append` and
  `append_files` to them, and the new `replace_partitions` /
  `replace_partition_files` to any table, make a run's first commit to a
  partition an `overwrite` removing the files earlier runs left there; later
  commits of the run append, and retries still recognise their commits. Two
  runs replacing one partition at once don't both keep rows: a commit that
  finds another writer's files in the partition after its run's first
  commit raises. Summaries carry `orca.replaced-files` and
  `orca.replaced-records`; `describe` lists `replaces`.
- `orca.tables.commit` refuses a removal of files that aren't all in the
  current snapshot, on its first try and on retries (mojo-census GAPS #29),
  instead of committing a partly stale overwrite.
- `orca backfill --rerun` of done keys, and `orca rerun`, are refused for a
  pipeline whose steps append to a dataset, naming the steps, unless
  `--allow-append` (`allow_append=1` in the UI and API, with a confirm
  button on the refusal).

## 0.8.0 - 2026-11-06

- **Caddy compresses what it serves.** Every site block of the control
  instance's Caddyfile in `deploy/aws/orca.yaml` (the UI's, the console's
  and those `orca-app` adds) has `encode zstd gzip`. An existing stack's
  Caddyfile is hand-edited on the instance; a template update doesn't change
  it without replacing the control instance, so see the PR for the live
  steps.

- **The control instance's own health** (#96): a `/system` page (24 hours,
  7 or 14 days of its CPU, memory, disk and network, and each container's
  memory), Prometheus gauges on `/metrics`, and a warning on `/` when its
  disk is over 85% or memory over 90%. The control sampler gets
  `--cgroup` / `--names` to read containers' memory from the host's cgroup
  files, read-only; the scheduler keeps a row a minute for 14 days
  (migration 24) and a file a day under `<ORCA_LOGS>/control/`.
  `deploy/aws/orca.yaml` mounts the cgroup files into the sampler and
  expires `logs/control/` after 14 days (new stacks; the PR has the steps for
  a running one). `tests/system.sh`.

- **A warm pool for the ec2 runner**: consecutive runs of a pipeline reuse
  its instances instead of booting new ones. An instance whose run ended
  waits `ORCA_EC2_POOL_SECONDS` (default 150; 0 turns the pool off, as
  before) for a run of the same pipeline started as the same instances
  (type, market, registry hash, image, secrets, environment, worker
  processes: its `pool_key`), which takes it, then launches the rest. Its
  script loops over runs: a new dista head process per run, worker
  processes killed when their run ends (`<binary> pool-wait`), secrets read
  again, each run's logs and samples its own; it asks for its next run
  over Postgres (`<binary> pool-next`). Hand-over locks rows `FOR UPDATE
  SKIP LOCKED` and checks that the instance beat lately
  (`ORCA_EC2_POOL_LIVE_SECONDS`, 5) and that EC2 says it runs; one that
  doesn't take its run up in `ORCA_EC2_POOL_JOIN_SECONDS` (30) is replaced.
  Idle instances count against `ORCA_VCPU_BUDGET` and show on `/fleet` as
  `idle (pooled)`, with their idle time (`idle_seconds` in `/api/fleet`).
  Migration 24 keys `orca.instances` by instance, run and start, and adds
  `pool_key`, `cpu_millis`, `idle`, `polled`, `pooled` and `joined`.
  `tests/pool.sh` (`test-pool`).

- **A calendar for dated pipelines in the UI** (#112). A daily pipeline's page
  has a month grid (a monthly one a year of months) on the Start runs form:
  click a first and a last day to fill the range field, ctrl- or cmd-click to
  add another range, arrow keys to move. Each day is coloured and marked by
  its state, from Postgres only: done, failed, running or queued, not run, or
  outside the pipeline's range. The same grid is the page's read-only
  Coverage, which works without scripts (`?cal=2023-01`), and
  `/pipelines/<name>/calendar?month=2023-01` serves it as JSON. Without
  scripts the text field is the form as before; keyed pipelines keep it.
  Hourly partitions don't exist yet, so there is no hourly grid.

## 0.7.0 - 2026-11-05

## 0.6.1 - 2026-11-05

### Changed

- Pins dista-mojo 0.3.0.

## 0.6.0 - 2026-11-05

- **Several dista worker processes per EC2 worker instance** (#130):
  `orca register --worker-procs N|auto` (`auto`: one per vCPU of the worker
  type; default 1; `orca aws register` passes it on; EC2 only). Each
  worker instance's script starts N supervised `DISTA_ROLE=worker`
  processes against the run's head (each takes a port of its own and files
  named by its process), notes one that exits, forwards SIGTERM to all, and
  shuts the instance down when the last has gone. Scaling still counts
  instances (`--workers MIN..MAX`, the budget, the cost): the policy runs
  over processes, N per instance, and an instance is `drained` once all its
  processes are. Migration 23 adds `pipelines.worker_procs` and
  `instances.procs` / `procs_drained`; the run page and its JSON show the
  processes of each instance. `tests/worker_procs.sh`.
- **`orca login` and `--remote`** (#111): run orca's commands from a laptop
  against the deployed `orca ui`, without SSM or the database.
  `orca login https://<host>` (user and password, the password without
  echo) stores an API token in `~/.config/orca/credentials` (mode 600,
  `ORCA_CREDENTIALS`); `orca --remote <host>` (or `ORCA_REMOTE`) then sends
  `backfill` (and `--preview`), `start`, `cancel`, `pause`, `unpause`,
  `resume`, `rerun` and `rebuild` to the UI's actions, as that user, and
  reads `status` and `lineage` from its JSON; `orca logout` revokes the
  token. Plain http is refused but to localhost.
  - The UI: `POST /api/login` (JSON; the same limit on failed logins) and
    `/api/logout`, and `Authorization: Bearer <token>` on its actions,
    which take JSON as well as forms. Tokens are 256 random bits kept as
    their SHA-256 in `orca.api_tokens` (migration 22), expiring after
    `ORCA_API_TOKEN_DAYS` (30). `orca.audit` gets a `source`, `ui` or
    `cli`, shown on the run's page.
  - `orca user tokens <name>` and `orca user revoke <name> <id>|@all`; a
    new password or a removed user revokes the user's tokens too.
  - New locally as well: `orca status [<pipeline>] [--json]`, `orca start`,
    `orca resume [--allow-new-code] <run>`, `orca rerun <run> <step>`,
    `orca cancel <run>` and `orca backfill --preview`.

## 0.5.0 - 2026-11-05

- **A Fleet page** (`/fleet`, in the nav; `/api/fleet`; a compact strip on
  `/`). Hosts running now (heads and workers by instance type, and the
  control instance), an estimate of what they cost an hour, runs running and
  queued, runs and errors per hour (failed step attempts and failed runs,
  with 24-hour sparklines), a sortable table of live hosts (CPU with a
  10-minute sparkline, busiest core, memory, network, disk, state) that
  refreshes every 10 s, and charts over `?range=24h|7d`: hosts over time by
  role, runs per hour with failures overlaid, fleet CPU and network.
  The scheduler keeps each live instance's last 30 minutes of samples in
  Postgres (`orca.fleet_samples`) and an hourly total for 8 days
  (`orca.fleet_hourly`; migration 21), so a page reads no files. The control
  instance is sampled too: `orca sample --keep N` keeps a file to its last
  lines, and the stack's user data starts a `sampler` container (an existing
  stack needs it started by hand, see the PR). `data-poll-every` sets a fragment's refresh seconds.

## 0.4.0 - 2026-11-05

- **Bounded table history** (magmalake/mojo-census GAPS #27). Commits to
  `gharchive.events` went from 486 ms to 1,207 ms as it reached ~889
  snapshots and metadata files; now:
  - `ensure_table(..., properties=)` takes Iceberg table properties, and
    every table it creates, or finds without them, gets orca's defaults
    (`orca.tables.table_defaults()`): old metadata files deleted after
    each commit (`write.metadata.delete-after-commit.enabled`), a hundred
    kept (`write.metadata.previous-versions-max`), and thirty days of
    snapshots (`history.expire.max-snapshot-age-ms`).
  - **`orca expire <dataset>|--all [--older-than 30d] [--keep N]
    [--dry-run] [--report]`** expires snapshots through the project's
    `orca_maintain` binary (`bin/orca_maintain.mojo` calling
    `orca.maintain.maintain_main`; found as `$ORCA_MAINTAIN`, beside
    `orca` or on `PATH`). It keeps, whatever the policy, the snapshots a
    run that can still resume read or committed, and every snapshot of the
    last run that committed to the dataset (`orca.expiry.retained`), so
    retries, `resume` and `find_commit` keep working.
  - The scheduler expires every dataset orca commits to by its own policy
    every `ORCA_EXPIRE_HOURS` (24; 0 turns it off), the first time
    `ORCA_EXPIRE_DELAY_SECONDS` (300) after it starts, one dataset at a
    time in the background. A table without
    `history.expire.max-snapshot-age-ms` (one orca didn't create) is left
    alone.
  - `orca aws push`'s default image builds `orca_maintain` for a project
    whose pipelines use `orca.tables` and that has none of its own.
  - Lineage (`orca lineage`, the UI's dataset pages) reads Postgres, so it
    still shows an expired snapshot and what it was made from; the snapshot
    can no longer be read from the table.
  - Built against iceberg-mojo with linear expiry, bounded metadata logs,
    commits that don't list the metadata directory, and merge-append.

- **The lake's bucket is private unless asked** (`orca aws up`). A new stack
  parameter, `PublicData` (default `false`), decides whether
  `mlake-public-<account>` gets the bucket policy that lets anyone read it
  unsigned, with Block Public Access on otherwise. Anyone could download
  the whole lake before, billed to the project at S3's egress price. The
  instances read and write it with their roles either way. Pass
  `PublicData=true` to share it as before.

## 0.3.5 (2026-11-05)

- Built against iceberg-mojo 0.9.1 (with the DataFile wire codec, #70),
  deflate-mojo 0.2.0, avro-mojo 0.4.5 and parquet-mojo 0.12.1.

- **Data files written on workers, committed by the step** (#108).
  `orca.tables.append_files(ctx, table, files)` commits data files already
  written, here or in a dista task, as one `append` snapshot, with
  `append`'s `writes=` check, summary and recognition: a retry that finds
  its commit returns it and commits nothing. On the worker,
  `write_files(load_dataset(name), batches)` (or a `FileWriter`, a batch at
  a time) writes the files uncommitted and returns `DataFiles`, a dista
  `Codec` over iceberg's `DataFile.encode`, so a task's result can carry
  them, alone or in a struct beside plain fields. Needs iceberg-mojo with
  magmalake/iceberg.mojo#70.
- **A crash's stack dump is symbolized, and a crashed attempt says where**.
  The project image (`deploy/project.Dockerfile`, which `orca aws push`
  builds) and `deploy/Dockerfile` build every binary with line tables
  (`mojo build -g1`, then compressed with `objcopy`: mojo-census's
  gharchive goes from 4.2 MB to 6.5 MB, 25 MB uncompressed, and parses and
  writes an hour of GH Archive as fast as before) and carry
  the pixi environment's `llvm-symbolizer` at
  `/opt/orca/libexec/llvm-symbolizer`, with `LLVM_SYMBOLIZER_PATH` set,
  so a segfault's or `abort`'s dump in `driver.log` names each frame's
  function, file and line. The runner that sees a crashed process end
  (the scheduler reaping a local run, `<binary> keep-log` on an EC2 head,
  given the run as `ORCA_RUN`, and the Kubernetes runner keeping the
  driver pod's log) fails the attempts it left `running` with the dump's
  summary, `crashed: <abort's message>; at <function> <file:line>, ...`
  (`orca.crash`), instead of leaving them for `resume` to call
  interrupted. A local run's `driver.log` marks each start with a
  `--- start <n>` line, as the other runners' do. `Store.interrupt` takes
  the error to record (`why`). `pixi run test-crash`.
- **Errors say what orca was doing**. A table orca can't load, create,
  reload, write data files for or commit to (`orca.tables`) names its
  dataset and, for a commit, how many tries it made; a dataset whose
  snapshot `sense` can't read is named; and an EC2 call that never got an
  answer names its action.

## 0.3.4 (2026-11-04)

- Built against iceberg-mojo 0.9.0 (parquet-mojo 0.12.0): data files go
  through the streaming Parquet writer.

- **`/runs` filters are dropdowns, and the runs go back further than 100**
  (#113). Pipeline, status and trigger are `<select>`s: the registered
  pipelines; queued, running, deferred, succeeded, failed and cancelled;
  and the triggers orca writes plus any others in `orca.runs`; the current
  choice stays selected, and the page filters as a choice changes (the
  Filter button does the same without JavaScript). The page shows 50 runs
  (`?n=`, at most 500) and pages by id: `?before=<id>` is the runs older
  than that one, and Newer and Older links keep the filters, beside
  "51–100 of 1,234". The JSON has `next_before` (null on the last page),
  `newer`, `total`, `n`, `before` and `filters`.
- **The pipeline page's Start runs is one form with a preview** (#113).
  One range, a primary "Backfill: one run per day" (month, key; "runs of
  up to N" with `keys_per_run`) and "Start one run over the whole range",
  each with a line on what it queues; "Include days that already
  succeeded (rerun them)" is `rerun=1`, as `orca backfill --rerun`.
  Preview (`?preview=1&partitions=...`, a GET) says what the backfill
  would queue, "This queues 30 runs: 2025-01-02 … 2025-01-31. 1 already
  done is skipped.", and whether one run would be refused, with the same
  logic as the backfill in a transaction it rolls back; the pipeline's
  JSON has it as `preview`. Below, the same as `orca backfill` and the
  binary's `run --partitions`, quoted for a shell, and a line of script
  updates both and the preview as you type.
- **Actions sit beside the statuses they change** (#113). A Pause (or,
  paused, Resume) button by each pipeline on the home page and in its
  page's header; Cancel by a queued or running run and Retry by a failed
  one, in every runs table and in the run's header, where a succeeded run
  has Rerun; Rerun from here on an ended run's attempt. Each posts to the
  existing action, with its Origin and login checks; its title is the
  command line that does the same. Cancel and the reruns ask first where
  there's JavaScript. Logged out with login on, each is a "Log in to
  pause" (and so on) link that comes back to the page.
- **A dataset's Rebuild section says what it does and what it runs**
  (#113). A plain line on what a rebuild is for; the chain of pipelines
  it runs, by level, each linked and with what it reads that the rebuild
  changes ("nothing reads this dataset" when none does; the JSON's
  `downstream`); a Preview (`?preview=1&partitions=...`) of the runs each
  level would queue, from the rebuild's own plan (`plan_rebuild`), which
  writes nothing; and `orca rebuild <dataset> <range>` beside the button,
  which asks first and, logged out, is a link to log in.
- **Runs tables say Tries, not Starts** (#113), with a tooltip saying what
  it counts, and show it only when a run was started more than once. The
  JSON's field is still `starts`.
- Fixed: `/runs` sorted run ids as text, so past run 9 the newest weren't
  first (run 9 came before run 64).

## 0.3.3 (2026-11-04)

- **A run's page charts its machines** (#95). Under the timeline, a
  server-rendered SVG lines up each instance's CPU, memory, disk and
  network samples (`orca sample`) with a lane of the run's steps, marks
  where memory was over 90%, a disk over 90% full or CPU steal over 20%,
  and a table gives each instance's peaks, which the run's JSON has as
  `instances[].peaks`. A running run's charts follow it, polling
  `/runs/<id>/metrics`. A run without samples renders as before.
- **Each run records its machines' metrics** (#94). `orca sample` (and
  `<binary> sample`) appends a JSON line every `ORCA_SAMPLE_SECONDS`
  (default 5) with CPU (user, system, iowait, steal, busiest core), memory
  (used, available, cache, swap, the run's resident set), disk (used and
  free per file system the run writes to, read and write rates) and network
  rates, read from `/proc` on Linux and from sysctls and Mach calls on
  macOS. Every runner starts it beside a run's work: the local runner
  beside each run's process, the ec2 runner from each instance's user data
  next to dista, the kube runner in each pod. The lines go to
  `<ORCA_LOGS>/<pipeline>/<run>/metrics/<instance>.jsonl`, uploaded with an
  instance's logs and at its end, so a short run keeps them; on Kubernetes
  the scheduler moves them out of the pod's log. A sampler that can't write
  never fails a run, and `ORCA_SAMPLE_SECONDS=0` turns it off. Its overhead
  is well under 0.1% of one core. On EC2, a head whose run has ended is
  left `ORCA_EC2_HEAD_GRACE` seconds (60) to keep its logs and samples and
  shut itself down before the sweep terminates it.
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
- **Links to code look like links to code.** A link to a pipeline's source
  on its host (the pipeline's declaration, a step's function, a dista task,
  the commit and the repository, on the pipeline, run and attempt pages)
  was styled like a link inside orca. Every one is now drawn by one helper
  (`Html.code`): a small `</>` icon, the place in monospace and muted
  (`pipelines/gharchive.mojo:26`), then `↗`, opening in a new tab, titled
  `View source on GitHub at <commit>`. A DAG box that links to code says
  `↗` and `(view source)`. Links inside orca, and the JSON, are unchanged.
- **A dataset's page links the steps that write and read it to their
  code.** Under *Written by* and *Read by*, each pipeline lists its steps
  that declare the dataset in `writes=` / `reads=` or committed to (read)
  it, each linked to its line at the commit of the newest run that did, or
  of the version registered now if none has; each snapshot's step is linked
  at the commit of the run that committed it. A step without a recorded
  source has no link. An attempt's page links its step's code too.

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

- **Built against dista-mojo 0.2.2, iceberg-mojo 0.8.7 and
  objectstore-mojo 0.4.2** (objectstore-shim 0.2.1, the S3 multipart fix).
  The stable, nightly and data environments, the CI checkouts and
  `deploy/Dockerfile` (`DISTA_REV`) pin the same revisions.

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
