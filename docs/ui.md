# The UI

`orca ui` serves pipelines, runs and datasets as web pages on
`127.0.0.1:8270` (`ORCA_UI_ADDR`). Every page is also JSON, to
`Accept: application/json` or `?format=json`. Reading needs no login; the
actions need one once a user exists (`orca user add <name>`). The full list
of paths and actions is the docstring of `src/orca/ui.mojo`; the site's
[UI page](https://mlake.dev/s/orca/docs/ui) has the older pages. This page is
the parts that changed most recently.

```sh
ORCA_DATABASE_URL=postgresql:///orca ./build/orca ui
pixi run playground          # or the playground: example pipelines and runs, demo / demo-password
```

## Home: List and Timeline

`/` has two tabs, remembered in the browser. **List** (the default, no
JavaScript needed) is the pipelines with their schedules and last runs.
**Timeline** draws each pipeline's runs over 1 h, 6 h, 24 h or 7 d, a row
per pipeline in dataset order, indented by depth, coloured by status, fill
runs thinner, with a line from a run to the sensor or keyed runs its
commits started. Its data is `GET /api/gantt?window=24h` (`1h`, `6h`,
`24h`, `7d`).

## Lists of runs, in pages

`/runs`, a pipeline's Runs tab and a dataset's snapshots come a page at a
time, newest first, with **« Newest**, **← Newer** and **Older →** under
the table (the left and right arrow keys follow Newer and Older). Pages
are keyed by run id, not counted from the top, so a page stays the same
while new runs arrive and each one is a single index range, however far
back it is:

| Parameter | |
|---|---|
| `?limit=` | runs to a page: 50 on `/runs`, 20 on a pipeline's tab, at most 500 (`?n=` still works) |
| `?before=<id>` | the page of runs older than that one |
| `?after=<id>` | the page of runs just newer than it (the newest page once fewer than a page are left) |

The filters (`?pipeline=`, `?status=`, `?trigger=`) and the page size are
kept in every link. `/runs` says where the page is, `1–50 of 1,234`, from
an indexed count (`runs_pipeline_id`, migration 37, for a pipeline's runs).
The JSON keeps its fields (`runs`, `n`, `before`, `total`, `newer`,
`next_before`) and adds `limit`, `after`, `older`, and the cursors either
side: `next` (the `before` of the older page) and `prev` (the `after` of
the newer one), null at the ends, with `next_url` and `prev_url`. With no
parameters it's the same first page as before. A pipeline's page has the
same as `runs_page` in its JSON (`/api/pipelines/<name>?before=...`).

A dataset's snapshots are 50 to a page the same way, by the attempt that
first committed each snapshot: `?before=<attempt>.<snapshot>`, `?after=`,
and `snapshots_page` in the JSON. `<pipeline> status --before <run>`
pages a store's runs on the command line, in Postgres, a SQLite file or
memory alike.

## Live pages

A page with something going on refreshes that part by itself, from a
fragment, only while the tab is visible. A refresh keeps open
`<details>`, scrolled tables, the chosen tab, a field being typed in and
the row at the top of the window, even when new rows arrive above it.

| Page | What refreshes | How often |
|---|---|---|
| `/` | the pipelines table (`/home/pipelines`) and Recent runs | 5 s while a run is queued or running, else 30 s |
| `/runs`, a pipeline's Runs tab | the table and its pager (`/runs/table`) | 5 s while a run on the page is queued or running; the newest page every 30 s otherwise, so new runs appear at its top; an older page stops once nothing on it is going on |
| a pipeline's page, every tab | the Progress card | 5 s |
| `/runs/<id>` | its facts, steps, DAG and machines (`/runs/<id>/live`) | 5 s, until the run ends |
| `/runs/<id>/attempts/<a>` | its facts (`.../facts`) and its log (`.../log`) | 5 s and 2 s, until the attempt ends |
| Ops: Control | its charts, containers and alerts (`/ops/control/live`) | 30 s |
| Ops: Fleet | its hosts (`/ops/fleet/hosts`) | 10 s |
| Ops: Workers | the workers (`/ops/workers/live`) | 10 s |
| `/watchdog` | the rules and trips (`/watchdog/live`) | 30 s |

A fragment answers 286 once it has nothing more to show, and the page
stops asking; a list's fragment sends `X-Poll-Every` with the seconds to
the next ask.

## A pipeline's page

Three tabs, each its own page, so links and the back button work:

| Tab | Link | What it has |
|---|---|---|
| Overview | `/pipelines/<name>` | the cards below, Steps, keys, snapshots not run, Next runs |
| Runs | `?tab=runs` | its runs and versions |
| Operate | `?tab=operate` | Coverage (the calendar of done, failed and open days), Start runs, Stop runs |

The name, Pause and the Progress card are on every tab. Links to
`#coverage`, `#start` or `#stop` open Operate.

The header is four cards, side by side on a wide screen:

- **Progress**: runs going, the [fill](orca-toml.md#fills) (done of total,
  rate, ETA, today's spend against its budget, why it's idle), a failing
  sensor, the next run. It refreshes itself every 5 s.
- **Config**: partitions, schedule, run retries,
  [placement](placement.md), binary.
- **Code**: the pipeline, its steps and dista tasks, each linked to its
  line on GitHub, and the code hash.
- **Resources**: what its last 20 measured runs used, the median (p50)
  and the most of each run's peak memory, CPU time and peak cores: what
  to ask for in a request (#171). It's `usage` in the page's JSON. See
  [What a run uses](#what-a-run-uses). A pipeline with a request
  (`cpus`, `memory_gb` in [orca.toml](orca-toml.md#requests-and-grants))
  has it here too, as **Requested**, and `request` in the JSON.

A sensor that runs when another pipeline's dataset gets a snapshot (the
census's mojo_repos and mojo_activity follow gharchive) has **Following**
on its Progress card: the pipelines and datasets it follows, how many of
their days (months) in its range it has done, and the rest broken down
into running or queued, failed and not queued yet, the first few of each
linking to their month on Coverage (`toy (up.days): 3 of 9 days done; 6
behind: 3 running or queued, 1 failed and 2 not queued yet`). What they
have is every partition orca knows of: those of each snapshot another
pipeline's succeeded attempt committed (`orca.commits`) and those of the
runs the sensor's own sensed snapshots started (`orca.sensed`), mapped to
its kind. The page's JSON (and `/api/pipelines/<name>`) has it as
`follow`: `datasets`, `upstream`, `unit`, `available`, `done`, `behind`,
and `live`, `failed` and `waiting`, each a `count` and its `first` eight
keys; null for a pipeline that isn't a dated sensor. It's one query for
the keys and the calendar's three for their states.

While its oldest queued run waits for room on its host, the Progress card
says why under **Waiting** (`waiting: needs 2 GB, 1.3 GB free of grants`),
and the JSON has it as `waiting` (null otherwise).

### Coverage

Operate's **Coverage** card is a dated pipeline's partitions on a
calendar, each coloured by its state: done, failed, running or queued, not
run, or outside the pipeline's range (before its start, or after today).
Hover a day for its date and state.

A daily pipeline's card has two tabs, **Month** and **Year**, remembered
in the browser like the Steps tabs; Year is the default. Year is twelve
small months (Monday to Sunday, like Month), each named with how many of
its days in the range are done (`28/31 done, 1 failed`) and linking to
that month; `‹ 2026 ›` moves a year within the range. Under it, a strip
has every month of the range (the last ten years at most), darker the
more of its days are done, each linking to its year. The tabs and arrows
are links (`?cal=2026` or `?cal=2026-03`, `&cov=year` or `month`), so all
of it works without scripts. A monthly pipeline's card is a year of
months, a keyed pipeline has none.

For a sensor (`orca.sensors`), days the datasets it follows have but no run
of it has done, failed on or is running are **behind**, in amber, with a
line under the legend saying what that means.

The card reads one year for both tabs: which days are done (of the year
and the strip's range together), the live runs, and each day's last run,
three queries whatever the view.

Each fact's name has a "?" that explains it, with a link to the docs.
Steps shows a Graph and a Table tab (`?steps=graph|table`). Hovering any
link to code on GitHub shows that function, up to 24 lines (not for a
private repository).

## A run's page

`/runs/<id>` refreshes itself while the run is queued or running, and
stops once it has ended. The crumb links the pipeline's previous and next
run (the left and right arrow keys follow them). The header is five
cards, side by side on a wide screen and stacked on a phone, each fact's
name with a "?" that explains it:

| Card | Anchor | What it has |
|---|---|---|
| Status | `#status` | duration, created, started, ended, tries, why it waits, is held or failed; Resume, Rerun from and Cancel |
| Partitions & trigger | `#trigger` | its partitions, and what started it: the rebuild, or the snapshots a sensor saw |
| Code | `#code` | the pipeline, its steps and dista tasks on one line, each a name linked to its line (hover for the source), the commit and image, the driver log, and the code hash |
| Resources | `#resources` | what it requested and was granted, the run's own Peak memory and CPU used (see [What a run uses](#what-a-run-uses)), and on Kubernetes what its pods asked for |
| Settings | `#settings` | where it was placed and ran (a [remote worker](workers.md) by name), max runs, the EC2 settings, its environment, and its secrets by name only |

**Steps** (`#steps`) is one card with three tabs, remembered in the
browser: **Table** (the default: each step and partition, status,
attempts, times, peak memory and CPU, log, error), **Timeline** (the
attempts on the clock, a row per step) and **Graph** (the DAG in the
run's colours). `?steps=table|timeline|graph` links work without scripts.

**Machines** (`#machines`) is the machines' samples, when the runner
recorded them, and its subtitle says whose:

| Runner | Machines shows |
|---|---|
| local | `local`: the whole machine the scheduler runs on. Other runs, the scheduler and anything else on it count too; when other local or control-placed runs overlapped this one, the card says how many shared it |
| control placement | `control`: the whole control host, with Postgres, the scheduler and the UI on it; shared the same way |
| EC2 | `head-<id>` and `worker-<id>`: each of the run's own instances, a whole machine |
| Kubernetes | each pod's samples: CPU, memory and disk io are the node's, which other pods share; network is the pod's own |
| remote worker | nothing: a worker measures only the run's processes, which are in Resources |

So Machines is never the run's own use; Resources is. Below it come
Instances (`#instances`, EC2), the Dista timeline (`#timeline`), Actions
(`#audit`, who did what from the UI or `orca`) and **Snapshots**
(`#tables`): the table snapshots each step read and wrote, linked to
their lineage.

## A snapshot's lineage

`/datasets/<name>/snapshots/<id>` draws the snapshot's lineage: each box
a snapshot, each arrow a step that read the snapshot it starts at and
wrote the one it points to, so data flows down. The legend under
the heading names the boxes: this snapshot (a thick border), one written
by an orca run (its pipeline and partition on the second line; hover for
the run and step), a source (dashed), written by something else, and
another input (dotted) of a step downstream, whose own lineage its page
has. A box nothing points to sits just above what read it. The diagram, like
`/datasets`' graph of pipelines and datasets, is drawn top to bottom, at
most two boxes to a row, so it fits a phone and grows downward.
Below it, **Upstream** is the snapshot and everything it was written
from, with the run and step that wrote each and what that step read
(`back` in the JSON), and **Downstream** is everything written from it
(`forward`), up to 20 steps away either way.

### What a run uses

The scheduler measures the runs it starts here (local and control-placed),
and `orca worker` the runs it starts: each tick (every second; a worker's
every poll) it reads the run's process tree in `/proc` (the run's process
and what it started, dista's local workers too), and when the process
ends `wait4` gives the largest single process and the CPU time of those
that ended between samples. Each attempt of that process gets its share,
by time: its **peak memory** (the most the tree held at once), its **CPU
used** (user and system seconds), and its **peak cores** (the most cores
kept busy over one sample's interval). They are `peak_rss_bytes`,
`cpu_seconds` and `peak_cpus` on `orca.attempts`.

The run's page has the run's Peak memory (its attempts' largest) and CPU
used (their sum, with the most cores at once), and the Steps table each
step's latest attempt's; an attempt's page has its own. Runs tables have
a Peak column. In the JSON, a run, each step and each attempt have
`usage`, with `null` for what wasn't measured. `orca status <pipeline>`
prints each run's, and `<binary> status <run>` each step's.

A run of a pipeline with a request also has **Requested** (what its
latest start asked for) and **Granted** (what it was given of its host, in
`ORCA_GRANT_CPUS` and `ORCA_GRANT_MEMORY_MB`), beside its peak: a peak
well under the grant means the request can come down, one at the grant
that it may need more. The run's JSON has `request` (`cpus`, `memory_gb`)
and `grant` (`cpus`, `memory_mb`), both null for a pipeline without one.
A queued run held for room shows why under **Held**, and `held` in the
JSON.

Not measured: runs on EC2 or Kubernetes (their machines' samples are under
Machines), runs no scheduler or worker started (`<binary> run`), and on
macOS, which has no `/proc`, the peak cores, and an attempt that shares
its process with others (a retry, a second step): there only a process
of one attempt gets `wait4`'s numbers.

A run with dista tasks has a **Timeline**: a track per process (driver or
head, then each worker), a lane per slot, spans coloured by function, input
waits as thin bars. Hover for a span's process, slot, duration and
arguments; wheel or drag to zoom, Reset zoom to go back. Perfetto and the
raw `timeline.json` stay linked.

## Help on every page

Each fact's name and each table's column header has a "?" that explains
it, on hover, keyboard focus or a tap. A value from a fixed set (a status
such as `deferred`, a step's kind `mapped`, a trigger, a placement, where
a setting came from) is dotted underlined instead, and says what it means
the same ways, so a table doesn't get a "?" in every cell. The texts are
in one place, `src/orca/terms.mojo`: a header's by its label (or its
table's, for a word that means different things, as From does in the
Configuration and Actions tables), a value's by its kind and value, and a
setting's by its key. The UI adds a header's "?" to every `<th>` it has a
text for as it answers, so a new table gets them by using the same words.

The Configuration table (a pipeline's Overview) has each key's "?", and
its From column links to where the value is set, where orca knows it:

| From | Links to |
|---|---|
| orca.toml | the line at the commit the file was registered from, on the host of the pipeline's source (`config.sources.<key>.line` and `config.repo_path`, recorded by `orca register` from a git checkout; not for a file with uncommitted changes, one registered before orca kept lines, or one outside a checkout) |
| code default, from the pipeline (max_runs, workers) | the pipeline's `Pipeline[...]` line, from `orca locate` |
| code default, orca's own (placement, instance types, worker_procs, fills) | nothing: it's orca's, not the pipeline's |
| flag, environment | nothing: a flag of `orca register`, or the scheduler's environment, has no line |

## Code on hover

Hovering a link to a line on GitHub shows that function, coloured:
keywords, names a `def` or `struct` defines, strings (triple-quoted too),
comments, numbers and decorators, from the theme's colours. The UI fetches
the file (`/code?u=`, only a GitHub file at a commit, at most 1 MiB) and
colours it (`orca.highlight`, a small tokenizer, no library), so the
browser needn't reach GitHub; a private repository's file it can't fetch
says so, and the link still opens it.

## Ops

"Ops" in the nav has three pages, each linking the others:

| Page | What it shows |
|---|---|
| Ops › Control (`/ops/control`, `/api/system`) | the one machine running the scheduler, the UI, Postgres and control-placed runs: CPU, memory, disk, network and each container's memory over `?range=24h`, `7d` or `14d`; whether [alerts](alerts.md) are on and the last ones; the [watchdog](watchdog.md)'s banner |
| Ops › Fleet (`/ops/fleet`, `/api/fleet`) | the EC2 instances orca launches for runs: hosts up and their cost an hour, runs and errors per hour, live hosts, charts. On the local runner its Cost tile says "fixed price" and shows `ORCA_FIXED_COST` |
| Ops › Workers (`/ops/workers`, `/api/workers`) | each [worker token](workers.md#tokens): pool, label, worker, image, last heartbeat, runs, CPUs and memory |

Each section's heading links to it, so a link can open at one chart:

- Control: `#cpu`, `#memory`, `#disk`, `#network`, `#containers`,
  `#container-memory`
- Fleet: `#hosts`, `#hours`, `#runs`, `#fleet-cpu`, `#fleet-network`

`/system`, `/fleet` and `/fleet/hosts` redirect to the new paths.

## The watchdog page

`/watchdog` lists each rule with each of its thresholds, the last value and
whether it is past it, the snooze form, and the trips with what each
tripped on. See [watchdog.md](watchdog.md).
