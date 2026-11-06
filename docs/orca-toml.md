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
| `placement` | string: `"control"`, `"ec2"` or `"default"` | `default`, the scheduler's runner (`ORCA_RUNNER`) | `--placement` | `control` runs on the scheduler's host (`orca.control`). |
| `head_type` | string, an instance type | `ORCA_EC2_HEAD_TYPE`, else `c7g.large` | `--head-type` | Only used on EC2. |
| `worker_type` | string, an instance type | `ORCA_EC2_WORKER_TYPE`, else `c7g.xlarge` | `--worker-type` | Only used on EC2. |
| `workers` | integer `N` (at least 1), or string `"MIN..MAX"` | the code's `workers`, else 2; 0 for a pipeline without tasks | `--workers` | With `MIN..MAX`, the run scales between the two. |
| `worker_procs` | integer from 1 to 256, or `"auto"` | 1 | `--worker-procs` | `auto` means one dista process per vCPU. |
| `max_runs` | integer from 1 to 10000 | the code's `Pipeline[..., max_runs=]`, else 1 | `--max-runs` (new) | Overrides the code's value. |
| `secrets` | table of `NAME = "orca/<id>"` | none | `--secret NAME=ID` | Secrets Manager IDs only, never values. A `--secret` flag replaces the whole set. |
| `env` | table of `NAME = "value"` (numbers and booleans become text) | none | none (it replaces `ORCA_RUN_ENV` for pipeline settings) | See [env](#env-orca_run_env-and-the-warm-pool) below. |

`image` (`--image`) and `--spot` stay flags only.

### What is refused

- Unknown keys, such as `workrs`.
- A value of the wrong type.
- A `workers` range like `4..2`.
- A secret ID outside `orca/`.
- An `env` name that isn't a valid variable name.
- An `env` name that looks like a secret (`*TOKEN*`, `*SECRET*`, `*PASSWORD*`, `*DATABASE_URL*`). Put those in `secrets` instead.
- An `env` variable that orca sets itself: `ORCA_DATABASE_URL`, `ORCA_IMAGE`, `ORCA_SECRETS`, `ORCA_WORKERS`, `DISTA_ROLE`, `DISTA_HEAD`, `DISTA_ADVERTISE`.

Each refusal names the file and line, and nothing is registered:

```text
orca.toml:7: [pipelines.gharchive] has no key workrs (it has placement, head_type, worker_type, workers, worker_procs, max_runs, secrets and env)
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

`orca aws register` takes the same `--config` and `--no-config`. It checks the file on your machine, then sends the file whole (base64) to the control instance's `orca register`, with its git commit. A bad file is refused before anything is sent.

### What a registration records

`orca.pipelines.config` (migration 29) holds:
- the file's name
- its SHA-256
- its git commit, and whether the file had uncommitted changes
- the flags that overrode it
- the source of every value

`orca.pipelines.env` and `orca.pipelines.max_runs` hold the pipeline's env and its `max_runs` override.

## env, ORCA_RUN_ENV and the warm pool

- **A pipeline's `env` goes only to its own runs**, on every runner:

  | Runner | How the env is passed |
  |---|---|
  | local | the run's process environment |
  | control | the run's process environment |
  | Kubernetes | each pod's `env` |
  | EC2 | exported in each instance's script, and passed to its containers |

- **`ORCA_RUN_ENV` keeps working as a fallback for the whole stack.** The scheduler's variables named in it are copied to every run, as before. If a pipeline's `env` sets one of those names, the pipeline's value wins and the scheduler's isn't copied.
- **On EC2 the env is part of the warm pool's key** (#134). An instance from the pool is only handed to a run of the same pipeline started with the same env. Changing a value and registering again launches new instances, and the old ones shut down after `ORCA_EC2_POOL_SECONDS`. The env is also kept in the run's spec (`orca.runs.spec`), so workers started later in a live run get the env its start had.
- **Settings for the whole stack stay in `/etc/orca.env`**: the database URL, warehouse, logs, runner, `ORCA_VCPU_BUDGET` and so on. Pipeline settings, such as `DISTA_WORKER_SLOTS` for one pipeline, belong in orca.toml.

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

## Reserved for #155

The self-driving fills in #155 plan to add these per-pipeline keys. They are **planned, not implemented**: today orca refuses them as unknown keys.

| Key | Planned meaning |
|---|---|
| `fills` | which keys a pipeline fills by itself |
| `fill_order` | the order it fills them in |
| `fill_max_runs` | how many fill runs at once |
| `fill_budget_per_day` | how much fills may spend a day |
| `fill_window` | when fills may run |

Adding a key takes a name in `SCALAR_KEYS` and its check in `_canonical` (`src/orca/config.mojo`). Arrays and floats already parse. After that the key is merged, overridden, recorded, shown and exported like the others.

## Not yet

- The UI's pipeline page showing the registered configuration, with drift warnings.
- `orca config check` for a project's CI.
- `[stack]` read by `orca aws up`.
