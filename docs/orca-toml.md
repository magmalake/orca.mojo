# orca.toml

`orca.toml` sits at the root of a project's repository and says how each
pipeline runs. The pipeline's code still says what it does (steps, partitions,
schedule, reads and writes); the file says where it runs and how much it uses.
`orca register` and `orca aws register` read it. The code that reads it is in
`src/orca/config.mojo`, and the TOML parser is in `src/orca/toml.mojo`.

```toml
[defaults]
placement = "ec2"
head_type = "t4g.medium"

[pipelines.gharchive]
worker_type = "c7g.xlarge"
workers = "0..4"
worker_procs = 1
max_runs = 2
env = { DISTA_WORKER_SLOTS = "4", GHARCHIVE_PARALLEL = "16" }
fills = "all"                # fill 2023-01-01 to yesterday by itself
fill_order = "oldest"
fill_max_runs = 2
fill_budget_per_day = 5.0    # estimated US dollars a UTC day
fill_window = "22..6"        # UTC hours fill runs start in

[pipelines.mojo_repos]
placement = "control"
secrets = { GITHUB_TOKEN = "orca/github-token" }

[stack]
hostname = "orca.mlake.dev"
```

## Tables

| Table | What it is |
|---|---|
| `[defaults]` | Values for every pipeline the file names. It doesn't apply to pipelines the file leaves out. |
| `[pipelines.<name>]` | One pipeline's own values, by the name its `describe` gives. An empty table is allowed: the pipeline is then "in the file" and gets `[defaults]`. |
| `[stack]` | Reserved for `orca aws up`, which doesn't read it yet. It is accepted and not checked. |

Any other table, and any key at the top level, is refused.

## Keys

Each key can go in `[defaults]` or in `[pipelines.<name>]`.

| Key | Type | Default when absent | Replaces the flag | Notes |
|---|---|---|---|---|
| `placement` | string: `"control"`, `"ec2"`, `"worker:<pool>"` or `"default"` | `default`, the scheduler's runner (`ORCA_RUNNER`) | `--placement` | `control` runs on the scheduler's host (`orca.control`). `worker:<pool>` runs on a remote worker of that pool (`orca worker`), never the scheduler: [workers.md](workers.md). |
| `head_type` | string, an instance type | `ORCA_EC2_HEAD_TYPE`, else `c7g.large` | `--head-type` | Only used on EC2. |
| `worker_type` | string, an instance type | `ORCA_EC2_WORKER_TYPE`, else `c7g.xlarge` | `--worker-type` | Only used on EC2. |
| `workers` | integer `N` (at least 1), or string `"MIN..MAX"` | the code's `workers`, else 2; 0 for a pipeline without tasks | `--workers` | With `MIN..MAX`, the run scales between the two. |
| `worker_procs` | integer from 1 to 256, or `"auto"` | 1 | `--worker-procs` | `auto` means one dista process per vCPU. |
| `max_runs` | integer from 1 to 10000 | the code's `Pipeline[..., max_runs=]`, else 1 | `--max-runs` (new) | Overrides the code's value. |
| `secrets` | table of `NAME = "orca/<id>"` | none | `--secret NAME=ID` | Secrets Manager IDs only, never values. A `--secret` flag replaces the whole set. |
| `env` | table of `NAME = "value"` (numbers and booleans become text) | none | none (it replaces `ORCA_RUN_ENV` for pipeline settings) | See [env](#env-orca_run_env-and-the-warm-pool) below. |
| `fills` | string: `"all"` or `"since_registration"` | `since_registration` | none | `all`: the scheduler fills the pipeline's whole range by itself. See [Fills](#fills). |
| `fill_order` | string: `"oldest"` or `"newest"` | `oldest` | none | Which end of the range fills first. |
| `fill_max_runs` | integer from 1 to 10000 | 1 | none | Fill runs at once, on top of `max_runs`. |
| `fill_budget_per_day` | number from 0 to 100000, US dollars | none | none | Estimated spend of fill runs a UTC day. Fill runs on EC2 or Kubernetes don't start without it. |
| `fill_window` | string `"A..B"`, UTC hours: A from 0 to 23, B from 1 to 24 | `"0..24"`, all day | none | Fill runs start from hour A up to hour B. `"22..6"` wraps past midnight. |

`image` (`--image`) and `--spot` stay flags only. The `fill*` keys are
the file's only: no flag sets them.

### What is refused

- Unknown keys, such as `workrs`.
- A value of the wrong type.
- A `workers` range like `4..2`.
- A secret ID outside `orca/`.
- A `fill_window` like `"6..6"` or `"1..30"`, a negative `fill_budget_per_day`, or `fill_max_runs = 0`.
- `fills = "all"` for a keyed pipeline, a sensor, or one with no first partition. `orca register` refuses it with the file and line, because it has to see the pipeline first; see [Fills](#fills).
- An `env` name that isn't a valid variable name.
- An `env` name that looks like a secret (`*TOKEN*`, `*SECRET*`, `*PASSWORD*`, `*DATABASE_URL*`). Put those in `secrets` instead.
- An `env` variable that orca sets itself: `ORCA_DATABASE_URL`, `ORCA_IMAGE`, `ORCA_SECRETS`, `ORCA_WORKERS`, `DISTA_ROLE`, `DISTA_HEAD`, `DISTA_ADVERTISE`.

Each refusal names the file and line, and nothing is registered:

```text
orca.toml:7: [pipelines.gharchive] has no key workrs (it has placement, head_type, worker_type, workers, worker_procs, max_runs, fills, fill_order, fill_max_runs, fill_budget_per_day, fill_window, secrets and env)
orca.toml:3: [pipelines.toy] workers: not a count or a range MIN..MAX: 4..2
```

### TOML that orca reads

orca reads this part of TOML 1.0:
- comments
- headers, with dotted and quoted keys
- basic strings (with every escape) and literal strings
- decimal integers, floats and booleans
- arrays of these, over several lines, with a trailing comma allowed
- inline tables, nested

Defining a key or table twice is refused, as TOML refuses it. These are left out, and each is refused with a message that says so: `[[arrays of tables]]`, multi-line strings, dates and times, hexadecimal, octal and binary integers, `inf`, `nan`.

## Precedence

Each value comes from the first of these that sets it, highest first:

1. **flag**: a flag given to `orca register`. It is recorded as an override.
2. **orca.toml `[pipelines.<name>]`**
3. **orca.toml `[defaults]`**
4. **environment**: what the runner uses when nothing is registered. For instance types that is `ORCA_EC2_HEAD_TYPE` and `ORCA_EC2_WORKER_TYPE`; for `env`, the variables named in `ORCA_RUN_ENV`.
5. **code default**: what the pipeline declares (`max_runs`, `workers`), else orca's built-in value.

`env` and `secrets` are merged one name at a time: a pipeline's `env.X` beats `[defaults]`'s `env.X`, and other names from `[defaults]` are kept.

**A pipeline the file names** is registered with exactly what the file and the flags say:
- A key you remove from the file goes back to its default at the next registration.
- A flag applies to that registration only. The next `orca register` without the flag goes back to the file's value.

**A pipeline the file doesn't name** keeps the behaviour from before orca.toml: a flag left out keeps what was registered. `orca register` prints a warning that the pipeline isn't in the file.

### Which file is read

| How | File |
|---|---|
| `orca register --config F` | `F` |
| `ORCA_CONFIG=F` | `F` |
| neither | `./orca.toml`, if it exists |
| `--no-config` | none |

### Registering without paths

`orca register` with no pipeline binaries registers every `[pipelines.<name>]` the file names, each as `<dir>/<name>`. `<dir>` is `ORCA_PIPELINES_DIR` if it is set, else the directory `pipelines/` next to the file if it exists; with neither, `orca register` refuses and says so. A table whose binary doesn't exist is an error naming it, and nothing is registered. Binaries given on the command line work as before (and a pipeline in the file that you leave out isn't touched).

A project's image sets `ORCA_CONFIG`, `ORCA_PIPELINES_DIR`, `ORCA_COMMIT` and `ORCA_SOURCE`, so `orca register` alone registers the whole file.

`orca aws register` takes the same `--config` and `--no-config`. It checks the file on your machine, then sends the file whole (base64) to the control instance's `orca register`, with its git commit. A bad file is refused before anything is sent.

### What a registration records

`orca.pipelines.config` (migration 29) holds:
- the file's name
- its SHA-256
- its git commit, and whether the file had uncommitted changes; for a file outside a git checkout (an image), the commit in `ORCA_COMMIT`, with `ORCA_SOURCE` as the commit's source, when they are set
- the flags that overrode it
- the source of every value

`orca.pipelines.env` and `orca.pipelines.max_runs` hold the pipeline's env and its `max_runs` override.

## env, ORCA_RUN_ENV and the warm pool

- **A pipeline's `env` goes only to its own runs**, on every runner:

  | Runner | How the env is passed |
  |---|---|
  | local | the run's process environment |
  | control | the run's process environment |
  | worker:&lt;pool&gt; | the run's process environment, on the worker |
  | Kubernetes | each pod's `env` |
  | EC2 | exported in each instance's script, and passed to its containers |

- **`ORCA_RUN_ENV` keeps working as a fallback for the whole stack.** The scheduler's variables named in it are copied to every run, as before. If a pipeline's `env` sets one of those names, the pipeline's value wins and the scheduler's isn't copied.
- **On EC2 the env is part of the warm pool's key** (#134). An instance from the pool is only handed to a run of the same pipeline started with the same env. Changing a value and registering again launches new instances, and the old ones shut down after `ORCA_EC2_POOL_SECONDS`. The env is also kept in the run's spec (`orca.runs.spec`), so workers started later in a live run get the env its start had.
- **Settings for the whole stack stay in `/etc/orca.env`**: the database URL, warehouse, logs, runner, `ORCA_VCPU_BUDGET` and so on. Pipeline settings, such as `DISTA_WORKER_SLOTS` for one pipeline, belong in orca.toml.

## Secrets without Secrets Manager

`secrets` ids are read with `ORCA_SECRET_FETCH` when it is set: the
scheduler runs `<ORCA_SECRET_FETCH> <id>` through `sh -c` and uses what it
prints (trailing newlines dropped). To keep them in files, as on a single
server (see [hetzner.md](hetzner.md)), set it to a command that reads one:

```sh
ORCA_SECRET_FETCH='sh /etc/orca/secret-fetch'   # prints /etc/orca/secrets/<id>, mode 600, root only
```

The ids still start with `orca/`; the script must refuse `..`. For an
S3-compatible store instead of S3, set `AWS_ENDPOINT_URL_S3` (and
`AWS_REGION`, the keys) in the scheduler's environment file, not in `env`.

## orca config show

```text
$ orca config show gharchive
gharchive  registered from orca.toml @ 3f2a9c1d0b7e, sha256 b0e8af31ea3e
  overrides: --workers 0..8
  placement               ec2         orca.toml [defaults]
  head_type               t4g.medium  orca.toml [defaults]
  worker_type             c7g.xlarge  orca.toml [pipelines.gharchive]
  workers                 0..8        flag --workers
  worker_procs            1           orca.toml [pipelines.gharchive]
  max_runs                2           orca.toml [pipelines.gharchive]
  image                   …/orca:3f2a9c1  flag --image
  env.GHARCHIVE_PARALLEL  16          orca.toml [pipelines.gharchive]
  env.DISTA_WORKER_SLOTS  4           orca.toml [pipelines.gharchive]
  env.ORCA_PUBLIC_WAREHOUSE  …        environment ORCA_RUN_ENV
```

Without a pipeline name it prints every registered pipeline. Values that come from the environment are read from the environment of the process running the command. On AWS, run it as `sudo docker exec scheduler orca config show` to see the scheduler's environment.

`--json` prints the same for scripts. With a pipeline name it prints one object; without one it prints `{"pipelines": [...]}`:

```json
{"pipeline": "gharchive", "file": "orca.toml", "hash": "b0e8…", "commit": "3f2a…", "dirty": false,
 "overrides": {"workers": "0..8"},
 "settings": [{"key": "placement", "value": "ec2", "source": "orca.toml", "detail": "[defaults]"}, …]}
```

`source` is one of `code default`, `orca.toml`, `flag` or `environment`.

The UI's pipeline page has the same under **Configuration** (`/pipelines/<name>#config`, and `config` in `/api/pipelines/<name>`): where it was registered from, the flags that overrode the file, and each value with its source. A run's **Settings** link to it. The values from the environment are the UI's environment, which on AWS is the scheduler's.

## orca config check

```text
$ orca config check                    # ./orca.toml, or $ORCA_CONFIG
orca.toml: ok, 3 pipelines
$ orca config check ci/orca.toml --secrets
ci/orca.toml:9: [pipelines.mojo_repos] secrets: GITHUB_TOKEN: can't read secret orca/github-token: exit 254
orca: ci/orca.toml has 1 problem(s)
```

It checks a file for a project's CI and needs no database. It refuses what `orca register` would refuse, with the same `file:line: [table] key: why`. It also checks that orca can size each `head_type` and `worker_type` (the vCPUs `worker_procs = "auto"` and the fleet use; `ORCA_EC2_VCPUS` adds types), and warns about a type that isn't in orca's price table, which Ops > Fleet and the watchdog's cost rule count as $0 an hour. `--secrets` also reads each secret the way the scheduler does (`ORCA_SECRET_FETCH`, else Secrets Manager with the `AWS_*` credentials or the instance role) and never prints a value. It exits 1 if anything is wrong.

## orca config export

```text
$ orca config export            # writes ./orca.toml; refuses to replace one without --force
$ orca config export --out -    # prints it instead
# orca.toml, written by `orca config export` from the registrations
# in orca's database. `orca register` reads it (orca.config).

[pipelines.gharchive]
placement = "ec2"
head_type = "t4g.medium"
worker_type = "c7g.xlarge"
workers = "0..4"
worker_procs = 1
env = { DISTA_WORKER_SLOTS = "4", GHARCHIVE_PARALLEL = "16" }
```

The export has a table for every registered pipeline, holding what is registered for it beyond the defaults. Registering from the exported file gives the same registration back. It writes no `[defaults]`: move values shared by several pipelines there by hand.

## Migrating from register flags

1. Run `orca config export` against the deployed database. On AWS: `sudo docker exec scheduler orca config export --out -`, and copy the output into the project's `orca.toml`.
2. Move pipeline variables from `/etc/orca.env` into each pipeline's `env`. These are the variables `ORCA_RUN_ENV` names for a single pipeline, for example `DISTA_WORKER_SLOTS`. Keep stack settings in `/etc/orca.env`. Once no pipeline relies on a name, drop it from `ORCA_RUN_ENV`.
3. Optionally move shared values into `[defaults]`.
4. Commit the file, then register as usual without the flags: `orca aws register gharchive` or `orca register ./build/gharchive`.
5. Check with `orca config show <pipeline>`. Every value should say `orca.toml`, and the header should name the commit.

From then on a flag is a one-off, such as `--workers 0..8` for a backfill. The next registration without it goes back to the file.

## Fills

With `fills = "all"` a pipeline fills its whole range by itself, within limits (#155, `src/orca/fill.mojo`). People change the limits; nobody has to drive a long backfill by hand.

The default, `fills = "since_registration"`, is what orca did before. A schedule fills from the pipeline's registration on, and `orca backfill` fills the rest by hand.

### What the scheduler does

On every tick, after a pipeline's other runs have started, the scheduler queues and starts fill runs for it:

- **The range** is the pipeline's first partition through the last one that has ended on its schedule's wall clock (UTC without a schedule): yesterday for a daily pipeline, last month for a monthly one. The partition still going is the schedule's.
- **One partition per fill run**, in `fill_order`. The run's trigger is `fill` (`orca.runs.trigger`), so the runs page can filter for them.
- **A partition is taken** when it's done in a run of any of the pipeline's code, when a live run has it, or when a fill run of it was ever queued. A fill never runs a partition twice. A fill run that fails is retried like a scheduled run, up to `run_retries`. A partition whose fill run failed past its retries, or was cancelled, is left to a person: `orca backfill`, or resume in the UI. Running done partitions again stays `orca backfill --rerun`.
- **Fill runs don't count against `max_runs`.** At most `fill_max_runs` fill runs are going, beside up to `max_runs` other runs.
- **Fill runs never starve scheduled runs.** While any of the pipeline's scheduled, backfill or other runs are queued, no fill run starts. Fill runs still take room in the stack-wide limits (`ORCA_VCPU_BUDGET`, the control instance's slots) once they're going.

### What stops a fill

The pipeline page and `/api/pipelines/<name>` say which of these stopped it, in this order:

| Idle because | |
|---|---|
| `complete` | Every partition of the range is done, or the last ones are running. |
| `paused` | `orca pause`, the UI's Pause button, or the watchdog (#153) tripping: it pauses the pipeline, and a fill stops like everything else. The reason names the watchdog's rule when the watchdog paused it. |
| `outside its window` | The UTC hour is outside `fill_window`. A fill run that has started goes on past the window's end. |
| `no budget` | The runs cost money (EC2 or Kubernetes) and `fill_budget_per_day` isn't set. |
| `price unknown` | Neither orca.fleet's price table nor a Kubernetes cluster can price the run. The [cost estimate](#the-cost-estimate) fails closed. |
| `budget` | Today's estimated spend plus what the next run may cost would go over `fill_budget_per_day`. It starts again at 00:00 UTC. |
| `waiting` | Other runs of the pipeline are queued, and they start first. |
| `at fill_max_runs` | As many fill runs are going as `fill_max_runs` allows. |

### The cost estimate

The estimate is conservative and fails closed. The AWS project's spend limit pauses the whole project, so a fill must stop well before it.

- **Price.** A run's price is an hour of its head instance plus its most workers, at on-demand prices from orca.fleet's table (`hourly_price` in `src/orca/fleet.mojo`). That's the us-east-2 on-demand price for the types orca uses. The estimate is multiplied by 1.2 for what the table leaves out: EBS, transfer and requests.
- **Spend today.** It sums each fill run's instances (`orca.instances`) since 00:00 UTC, at their own types' prices. An instance still up counts to now, including its idle time in the warm pool.
- **Before starting a run**, orca adds what the fill runs going and the new one may still cost. The expected time of a run is the longest of the pipeline's last 20 ended fill runs, or an hour before any has ended. It is at least a minute (EC2's minimum), plus `ORCA_EC2_POOL_SECONDS` of idle time in the pool. A run already going is charged that time less what it has run. The run starts only if the spend plus that is within `fill_budget_per_day`.
- **Unknown prices.** No fill run starts on a budget when the price isn't known: an instance type that isn't in the table (today's runs' or the next run's), or a run on Kubernetes. The reason says which type.
- **Free runs.** A run on the control host (`placement = "control"`) or a local process costs nothing beyond the machine already running. Its fill needs no budget.

The budget is per pipeline, so a `fill_budget_per_day` in `[defaults]` gives each pipeline in the file that much. The estimate can't see a run that lasts longer than every earlier one until it has. The watchdog's `cost` rule (`ORCA_WATCHDOG_COST_DAY`, $25) is the stack-wide backstop: it pauses everything.

### Progress

The pipeline page has a **Fill** line: partitions done of the range's total, fill runs going, partitions in live runs, waiting for a retry and failed. It also gives the rate (partitions an hour over the fill runs that ended in the last six hours), the ETA at that rate, and today's estimated spend against the budget. When the fill is idle, the line says why. `/api/pipelines/<name>` is the page's JSON, with a `fill` object:

```json
{"on": true, "total": 1405, "done": 380, "live": 2, "failed": 0, "retrying": 0, "left": 1023,
 "running": 2, "rate_per_hour": 41.5, "eta_seconds": 88770, "spent_today": 1.734,
 "budget_per_day": 5.0, "price_per_hour": 0.6134, "expected_run_seconds": 210,
 "cost_is_estimate": true, "idle": null}
```

`orca config show` lists the `fill*` keys, with their defaults, for a pipeline that fills.

### Keyed pipelines and sensors don't fill

`orca register` refuses `fills = "all"` for them, with the file and line:

- **Keyed pipelines.** Their keys come from a dataset, so there's no range to fill. Their sensor or `orca backfill <p> @all` queues the keys that aren't done.
- **Sensors.** Their runs follow their inputs' snapshots. Filling their range by itself would run partitions whose input may not be there.
- **No first partition.** A pipeline whose partitions have no first one has no range.

If `fills = "all"` is in `[defaults]`, set `fills = "since_registration"` in such a pipeline's own table.

### Operating a fill

Operations go through the UI or the remote CLI (`orca login` and `orca --remote`, #128), which write audit rows: pause, unpause, cancel, backfill and resume. To change a limit, edit `orca.toml`, commit it, and register again (`orca aws register <pipeline>`). SSM `sudo docker exec scheduler ...` is for debugging only.

## Not yet

- Drift warnings on the pipeline page (registered from orca.toml @ X, which has since changed): the control instance has no checkout to compare with.
- `[stack]` read by `orca aws up`.
