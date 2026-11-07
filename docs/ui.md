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

The card reads one year for both tabs: which days are done (of the year
and the strip's range together), the live runs, and each day's last run,
three queries whatever the view.

Each fact's name has a "?" that explains it, with a link to the docs.
Steps shows a Graph and a Table tab (`?steps=graph|table`). Hovering any
link to code on GitHub shows that function, up to 24 lines (not for a
private repository).

## A run's page

`/runs/<id>` refreshes itself while the run is queued or running, and
stops once it has ended. It starts with the run's duration, and the crumb
links the pipeline's previous and next run (the left and right arrow keys
follow them). Settings shows the environment the run had, max runs,
placement and worker settings, with secrets by name only. A run on a
[remote worker](workers.md) says which worker ran it.

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
