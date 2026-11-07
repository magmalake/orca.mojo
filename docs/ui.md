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

The header is three cards, side by side on a wide screen:

- **Progress**: runs going, the [fill](orca-toml.md#fills) (done of total,
  rate, ETA, today's spend against its budget, why it's idle), a failing
  sensor, the next run. It refreshes itself every 5 s.
- **Config**: partitions, schedule, run retries,
  [placement](placement.md), binary.
- **Code**: the pipeline, its steps and dista tasks, each linked to its
  line on GitHub, and the code hash.

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
