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
  [What a run uses](#what-a-run-uses).

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
| Resources | `#resources` | the run's own Peak memory and CPU used (see [What a run uses](#what-a-run-uses)), and on Kubernetes what its pods asked for |
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
