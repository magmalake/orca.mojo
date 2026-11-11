# orca on one Hetzner Cloud server

One ARM server, a fixed monthly price, nothing launched on demand. orca runs
its runs on the server itself (`ORCA_RUNNER=local`), the warehouse and the
logs are in Hetzner Object Storage, Postgres keeps its data on a Volume, and
Cloudflare Tunnel brings the UI and the console in, so the firewall opens SSH
(from your address) and nothing else. The scripts are in
[`deploy/hetzner/`](../deploy/hetzner/); nothing has to be typed over SSH by
hand. It is the same stack as [`deploy/aws/orca.yaml`](../deploy/aws/orca.yaml)
(same images, env names and `pg_dump` format) without the instances, Caddy,
Secrets Manager and ECR.

```
laptop: hcloud + ssh + deploy/hetzner/*.sh
   |
   v                          Hetzner Cloud server (CAX21, Ubuntu)
firewall: tcp/22 from you     +-- docker network "orca" ---------------------------+
                              | postgres  (data on the Volume, /mnt/orca-data)     |
Cloudflare ---- tunnel ------>| cloudflared -> ui:8270, console:8080               |
 orca.mojourney.dev               | scheduler (local runner), ui, sampler, console     |
 mojourney.dev             +----------------------------------------------------+
                                        |  S3 API (outbound)
                                        v
                              Hetzner Object Storage: warehouse/, logs/, backups/pg/
```

## What you do first (consoles, once)

1. **Hetzner Cloud**: a project, and an API token with read & write
   (Security > API tokens). On the laptop: `hcloud context create orca` and
   paste the token (it stays in hcloud's config, not in this repo).
2. **Object Storage** (same console): a bucket, in the **same location as
   the server** (`LOCATION` in `hetzner.env`). A bucket in another location
   works (it was tried with the bucket in nbg1 and the server in fsn1), but
   every byte of the warehouse and the logs then crosses locations. And
   and S3 credentials (access key and secret). Put them in a file of two
   lines, `AWS_ACCESS_KEY_ID=...` and `AWS_SECRET_ACCESS_KEY=...`, mode 600.
3. **Cloudflare**: Zero Trust > Networks > Tunnels > create a tunnel
   ("Cloudflared"), and copy its token into a file as one line,
   `TUNNEL_TOKEN=...`, mode 600. Add two public hostnames to the tunnel:
   `orca.mojourney.dev` to service `http://ui:8270`, and `mojourney.dev` to
   `http://console:8080`. (If those names are on the AWS stack, Cloudflare
   replaces their DNS records with the tunnel's CNAME; if it can't create the
   CNAME, delete the existing A (and AAAA) records for the names first: a
   name that already has an A record cannot also get the tunnel's CNAME, and
   Cloudflare says so only when you save the hostname.) The CNAMEs must
   be **Proxied** (orange cloud): a DNS-only CNAME resolves to a private
   `fd10::` address and the site is unreachable. The tunnel runs over HTTP/2
   (`TUNNEL_TRANSPORT_PROTOCOL=http2`, set by `orca-stack` unless
   `tunnel.env` sets it), because QUIC over UDP 7844 kept timing out on
   Hetzner ("timeout: no recent network activity").
4. **A GitHub token** for the census's `mojo_repos`, in a file (read-only
   access to public repositories is enough).
5. **Your address** for the firewall: `curl -4 ifconfig.me`, plus `/32`.
6. **Tools** on the machine you deploy from: `hcloud` **with a context**
   (`hcloud context create orca`, step 1; `up.sh` and `deploy.sh` both call
   it), `ssh`, `jq`, POSIX `sh`. Keep `hetzner.env` outside the repository,
   where no commit can pick it up, and point the scripts at it:
   `mkdir -p ~/.config/orca-hetzner && cp hetzner.env.example ~/.config/orca-hetzner/hetzner.env`
   (from the scripts' directory, [below](#first-setup))
   and `export HETZNER_ENV=~/.config/orca-hetzner/hetzner.env` (or put the
   export in your shell's profile).
7. **Images**: the scheduler's image is the project's (for the census,
   the one `orca aws push` builds from its `Dockerfile.orca`), at
   `ghcr.io/magmalake/orca:<tag>`; see [Images](#images).

## First setup

The scripts don't need a checkout of orca.mojo: each release has them as
`orca-hetzner-<version>.tar.gz`, which unpacks to `orca-hetzner-<version>/`
(its `VERSION` file says which, and the scripts print it). The newest:

```sh
curl -fsSL https://github.com/magmalake/orca.mojo/releases/latest/download/orca-hetzner.tar.gz | tar -xz
```

Then, in that directory (or `deploy/hetzner` of a checkout):

```sh
cd orca-hetzner-<version>
cp hetzner.env.example hetzner.env     # fill in every line: the file says what each is
./up.sh --dry-run                      # prints every hcloud and ssh command; runs none
./up.sh
```

`up.sh` creates (or leaves alone, if it exists) the SSH key, the firewall,
the Volume (formatted once, delete-protected), and the server with
`cloud-init.yaml` (Docker, and the Volume mounted before Docker starts so that
Postgres can never start on the root disk). Then, over SSH, it puts the
settings and secrets on the server (`/etc/orca`, mode 600, sent on standard
input and never printed), installs `orca-stack` and the backup timer, and
starts the containers. Run it again to fix drift in the Hetzner resources,
starting again only the containers whose settings changed (see [Deploying
code](#deploying-code)). It does not restart a scheduler that has runs
running unless you add `--force` (a restart kills them). A setting changed
in `hetzner.env` later doesn't need `up.sh`: every `deploy.sh` brings the
server's settings in line with it first ([Settings](#settings-from-hetznerenv)).

Then, once per new database:

```sh
ssh root@<ip>                          # up.sh prints the address
docker exec -it scheduler orca user add <you>      # reads a password
```

and register the pipelines (below). Open `https://orca.mojourney.dev`.

Secrets without Secrets Manager: `ORCA_SECRET_FETCH` is
`sh /etc/orca/secret-fetch`, which prints `/etc/orca/secrets/<id>`, so the
pipeline's `secrets = { GITHUB_TOKEN = "orca/github-token" }` reads
`/etc/orca/secrets/orca/github-token`, which `up.sh` writes from
`GITHUB_TOKEN_FILE`. More secrets: put the file there and use its id.

### Secrets on your laptop: SOPS

The secret files `hetzner.env` names (`S3_CREDENTIALS_FILE`,
`TUNNEL_TOKEN_FILE`, `GITHUB_TOKEN_FILE`, `NTFY_TOPIC_FILE`,
`NTFY_TOKEN_FILE`, `GHCR_TOKEN_FILE`) can be encrypted with
[SOPS](https://github.com/getsops/sops) and [age](https://age-encryption.org),
so the laptop keeps one private key instead of plain tokens. A file named
`*.sops` or `*.sops.<ext>` (`s3.sops.env`, `github-token.sops`) is sent as
`sops -d` decrypts it, through a shell variable and ssh's standard input,
never written to a file on the laptop; the server still gets its plain
`/etc/orca` files, mode 600. `--dry-run` prints `+ sops -d <file> | ssh ...`.

```sh
brew install sops age
mkdir -p ~/Library/Application\ Support/sops/age    # macOS; ~/.config/sops/age on Linux
age-keygen -o ~/Library/Application\ Support/sops/age/keys.txt   # prints the public key, age1...
```

Back the key file up offline (more than one key can be in it, one per
line). SOPS looks there by default; elsewhere, set `SOPS_AGE_KEY_FILE`.

A private repository for the encrypted files, one directory per stack and
a key each, so a laptop with only the dev key can't read production's:

```yaml
# .sops.yaml
creation_rules:
  - path_regex: ^prod/
    age: age1<production's public key>
  - path_regex: ^dev/
    age: age1<dev's public key>
```

From the repository's top (`.sops.yaml` picks the key by the name given in
`--filename-override`, not the plain file's path):

```sh
sops -e --filename-override prod/s3.sops.env ~/.config/orca-hetzner/s3.env > prod/s3.sops.env
sops -e --input-type binary --filename-override prod/github-token.sops \
    ~/.config/orca-hetzner/github_token > prod/github-token.sops
sops edit prod/s3.sops.env                    # to change one later
```

A file that isn't `KEY=value` lines (a bare token) must be encrypted with
`--input-type binary`: as an env file SOPS rejects it, and its error message
quotes the offending line, the secret itself.

then in `hetzner.env` `S3_CREDENTIALS_FILE=$HOME/dev/ops-secrets/prod/s3.sops.env`
and so on (or relative, [below](#one-hetznerenv-for-every-laptop)), and delete the plain files. SOPS reads the extension: `.env` is
an env file (the keys stay readable, the values are encrypted), no known
extension is binary (the whole file). Losing the age key loses the files,
so keep its backup off the laptop; if it leaks, change the secrets
themselves, not only the key, since git keeps every old encrypted version.

#### One file for every secret (recommended)

Each `*.sops*` file is its own `sops -d`, so a key that asks before each
use (an [age-plugin-se](https://github.com/remko/age-plugin-se) Secure
Enclave key: Touch ID) asks once per file. `SECRETS_FILE` in `hetzner.env`
names one SOPS YAML file with every secret instead, decrypted once per
`up.sh` or `deploy.sh` (one Touch ID) into a shell variable, never a file.
Its keys are the server's file names under `/etc/orca`, each value that
file's content; a key it has replaces the `*_FILE` setting (setting both is
an error), and an unknown key is refused:

```yaml
s3.env: |
  AWS_ACCESS_KEY_ID=...
  AWS_SECRET_ACCESS_KEY=...
tunnel.env: TUNNEL_TOKEN=...
secrets/orca/github-token: ...   # GITHUB_TOKEN_FILE's
secrets/orca/ntfy-topic: ...     # NTFY_TOPIC_FILE's
secrets/orca/ntfy-token: ...     # NTFY_TOKEN_FILE's
ghcr-token: ...                  # GHCR_TOKEN_FILE's
```

Write it with `sops edit prod/secrets.sops.yaml`, or, from the repository's
top as above, from the plain files through a pipe (never a plain temporary
file):

```sh
p=~/.config/orca-hetzner
{ echo 's3.env: |'; sed 's/^/  /' $p/s3.env
  echo 'tunnel.env: |'; sed 's/^/  /' $p/tunnel.env
  echo 'secrets/orca/github-token: |'; sed 's/^/  /' $p/github_token
} | sops -e --filename-override prod/secrets.sops.yaml /dev/stdin > prod/secrets.sops.yaml
```

Each value reaches the server with one trailing newline, as a `*.sops*`
file's does. A Secure Enclave key: `age-plugin-se keygen -o se-key.txt`
(Touch ID or the login password; `--access-control any-biometry` for Touch
ID only), then `age-plugin-se recipients -i se-key.txt` prints its public
key, `age1se1...`, for `.sops.yaml`; add the identity's line to `keys.txt`
or point `SOPS_AGE_KEY_FILE` at `se-key.txt`.
`--dry-run` doesn't decrypt: it prints `+ sops -d <file> (once)` and a
`| ssh ... orca-stack sync <name>` line for each secret no `*_FILE` setting
gives, assuming the file has them.

#### One `hetzner.env` for every laptop

`hetzner.env` holds no secret, so it can live in the secrets repository
too (`prod/hetzner.env`), one copy for every laptop. Two things make that
work:

- **Relative file names.** A relative `SECRETS_FILE`, `*_FILE` or
  `EXTRA_ENV_FILE` is read from the folder `hetzner.env` is in, so
  `SECRETS_FILE=secrets.sops.yaml` works wherever the repository is cloned.
- **A file of each laptop's own.** `ADMIN_IP` (the firewall's ssh source)
  and `SSH_PUBLIC_KEY_FILE` belong to a laptop, not the stack. Put them in
  `~/.config/orca-hetzner/<NAME>.local.env` (`orca.local.env`; or the file
  `HETZNER_LOCAL_ENV` names, which must then exist). It's read after the
  shared file, so whatever it sets wins, and its own relative names are read
  from its folder. It can't set `NAME`, which picks it.

```sh
# ~/.config/orca-hetzner/orca.local.env
ADMIN_IP=203.0.113.7/32
SSH_PUBLIC_KEY_FILE=$HOME/.ssh/id_ed25519.pub
```

The scripts' first line names both files: `deploy.sh: stack orca, settings
.../ops-secrets/prod/hetzner.env and ~/.config/orca-hetzner/orca.local.env`.
Two laptops share the stack's firewall, which lets in one `ADMIN_IP`: the
last `up.sh` sets it.

## The census: `orca.toml`

The census's `orca.toml` lives in the mojo-census repository, next to the
code it configures, and the image has it (`ORCA_CONFIG=/opt/orca/orca.toml`,
the pipelines at `ORCA_PIPELINES_DIR=/opt/orca/pipelines`, and the build's
`ORCA_COMMIT` and `ORCA_SOURCE`, which `orca config show` reports as the
file's commit). Read the file there, not here: each setting has a comment,
and the numbers move as the census does, so this page quotes none.
`DISTA_WORKER_SLOTS` is how many dista workers run in one process on the
server, and gharchive's `GHARCHIVE_PARALLEL` is how many hours are in flight
at once. Their defaults are sized for four AWS instances; each hour in
flight takes about 1 GB (processes of 2 to 4.4 GB), and 3 and 3 on the
CAX21 swapped hard, so the census's `orca.toml` sets both lower for it
(mojo-census#37). Raise both on a bigger server (during an import on the
CAX41, register again with more, and set them back after); too many took
the CAX21's memory to 99.6% and then OOM (the watchdog paused everything). `cloud-init.yaml` also
creates a 4 GB `/swapfile` (in `/etc/fstab`) if the server has no swap, so a
burst is slow instead of fatal; a server made before that can run
`fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile`
and add `/swapfile none swap sw 0 0` to `/etc/fstab`.

Instead of numbers that are wrong after a rescale, a pipeline can ask for a
range with `cpus` and `memory_gb` and set `DISTA_WORKER_SLOTS = "auto"`: the
scheduler then starts its run once the minimum fits beside the others' and
grants it up to the maximum of what's left, so the next run after
`rescale.sh up` is simply given more ([orca.toml](orca-toml.md#requests-and-grants)).
A pipeline's own `"auto"`, like `GHARCHIVE_PARALLEL`, needs its code to read
the grant (`orca.auto_count`).

Register (the binaries are in the image; `up.sh` doesn't do this because
what to register is the project's):

```sh
ssh root@<ip> docker exec scheduler orca register
```

To change a setting: edit `orca.toml` in mojo-census, merge it, deploy the
new image (`./deploy.sh <tag>`), and register again. (For a quick try,
`docker exec` can still register another file with `--config` and explicit
binaries; the next registration from the image takes it back.) This needs
an orca with `orca register` reading the file's tables (after 0.10.0);
before that, name the binaries:
`docker exec scheduler sh -c 'cd /opt/orca/pipelines && orca register gharchive gharchive_index mojo_repos mojo_history mojo_activity mojo_stats mojo_versions'`.

`orca config show` says what was registered. A pipeline with `placement`
left out runs on the scheduler's runner, here the local one. Ops > Pools
says the runner launches no instances, its Cost tile shows
`ORCA_FIXED_COST` (`FIXED_COST` in `hetzner.env`, free text such as
`€8.49/month`; "Fixed" if unset), and the watchdog's cost
rule reads "fixed price" and never trips; its launch, idle, failing, stuck
and control rules stay on.

## Memory pressure

Grants are estimates, and on one server everything shares the memory:
Postgres, the UI, the page cache and every run. So the scheduler also
stops starting runs while the machine stalls on memory
([orca.toml](orca-toml.md#memory-pressure)): while Linux's pressure stall
information reads `some avg10` over `ORCA_ADMIT_MEMORY_PRESSURE` (10%, in
`orca.env`; 0 turns it off), new runs wait, saying `waiting: memory
pressure 23% (limit 10%)`, and the runs already going go on. Ops › Control
has the current reading, and `/metrics` and `orca status` too.

What it reads, here: the scheduler runs in a container and its runs are
child processes in that container. Inside a container
`/proc/pressure/memory` is the host's (PSI isn't namespaced), while the
container's own cgroup has `/sys/fs/cgroup/memory.pressure`. The stack
starts the scheduler with no `--memory` limit, so its cgroup can't run
short by itself, and the scheduler reads the host's file: the right signal
when Postgres and the runs compete for the server's memory. Give the
scheduler's container a `--memory` limit and it reads its own cgroup's
instead, which stalls at that limit even with the host at ease. The log's
first lines say which (`orca: memory pressure from /proc/pressure/memory
(the host's) ...`). Ubuntu's kernel has PSI on; on a kernel without it (or
booted with `psi=0`) nothing is held and the log says so once.

Each run gets `ORCA_MEMORY_PRESSURE_FILE`, the same file, to poll if it
wants to lower its own parallelism (`orca.memory_pressure()`). The
watchdog's memory rule stays the last line of defence: it pauses pipelines
once memory is nearly full, which pressure should keep from happening.

## Deploying code

```sh
./deploy.sh a1b2c3d                    # pulls ghcr.io/magmalake/orca:a1b2c3d, starts again what changed
./deploy.sh a1b2c3d console=latest     # one app on another tag
./deploy.sh --all a1b2c3d              # every container again, changed or not
./deploy.sh --rollback                 # back to the tags deployed before (see Rolling back)
```

The tag is the project's image tag: see [Images](#images). This replaces
`orca aws app` and `orca aws up` for code changes, and needs `hcloud` with a
context like `up.sh`.

### Settings from `hetzner.env`

Before it plans, `deploy.sh` (`--rollback` too) writes the server's
settings as `up.sh` does, from `hetzner.env` as it is now (`sync_settings`
in `lib.sh`, which both run):

- `/etc/orca/settings.env` (`AWS_REGION`, `ORCA_WAREHOUSE` and the rest,
  the ntfy ids, `ORCA_FIXED_COST`, `ORCA_POSTHOG_KEY`, then
  `EXTRA_ENV_FILE`'s lines), and the secret files (`s3.env`, `tunnel.env`,
  `ghcr-token`, `secrets/orca/*`): each only if it differs, mode 600, sent
  on ssh's standard input (`orca-stack sync`);
- the stack settings `ORCA_IMAGE`, `CLOUDFLARED_IMAGE`, `BUCKET`,
  `BACKUP_KEEP` and `GHCR_USER` (`orca-stack configure`), and the apps
  (`orca-stack app`). Not the tags: those are `deploy.sh`'s, and
  `ORCA_TAG` in `hetzner.env` is only the first `up.sh`'s. `CGROUPS`,
  `OVERLAP` and `HEALTH_TIMEOUT`, set on the server by hand, aren't in
  `hetzner.env` and are left alone.

It prints what changed, never a value: for `settings.env` the keys added,
removed or changed, for a secret file only that it changed, for a stack
setting old -> new (none is a secret):

```
deploy.sh 10:02:11: the server's settings from hetzner.env (names only, never values):
  orca-stack: settings.env: added ORCA_POSTHOG_KEY
  orca-stack: BACKUP_KEEP 14 -> 30
deploy.sh 10:02:12: the plan:
  scheduler replace settings: its env file changed
  ui replace settings: its env file changed
  sampler keep unchanged
  console replace settings: its env file changed
```

The plan then replaces the containers whose env file changed, and a
changed scheduler quiesces (or overlaps) as a new tag does. A setting
emptied in `hetzner.env` (`POSTHOG_KEY=`, an `EXTRA_ENV_FILE` line taken
out) is removed from `settings.env`, as `up.sh` writes it; `GHCR_USER=`
removes it from `stack.conf`. So `hetzner.env` is where settings change:
an edit made by hand to `/etc/orca/settings.env` is undone by the next
deploy (put the line in `EXTRA_ENV_FILE`). A secret file `hetzner.env` no
longer names (`NTFY_TOKEN_FILE=`) stays on the server, unused once its id
is out of `settings.env`. `--dry-run` prints the `orca-stack sync`
commands and the keys `settings.env` would have, and reads and writes
nothing on the server.

A rollback syncs too: it puts back the tags deployed before, not the
settings, which the server keeps no history of. To undo a setting, change
it back in `hetzner.env` and deploy (or roll back): the next one applies it.

**Only what changed starts again.** `orca-stack apply` compares what each
container would be started with against what it runs, and leaves it
running when nothing differs: the image (the tag, and the image's id, so a
tag pushed again counts), the env file it reads (`orca.env`, `app.env`,
`tunnel.env`, and `secret-fetch`), and its flags and mounts (`CGROUPS`'s
among them). A hash of those is the container's `orca.stack.spec` label.
It prints a line for each:

```
scheduler: unchanged
ui: replaced (image ac0c766 -> b2b9558)
sampler: replaced (image ac0c766 -> b2b9558)
console: unchanged
```

The scheduler, UI and sampler run the orca tag, so a new orca tag replaces
the three; an app on its own tag (the census console) is replaced only when
its tag changes. Postgres is never restarted. A container started by an
`orca-stack` from before the label has none, so the first apply with this
one replaces every container once. `orca-stack plan` with the same
arguments says what an apply would do (`ui replace image ac0c766 ->
b2b9558`) and changes nothing; `apply --all` replaces every container, as
`apply` did before, and `apply --only console` that one alone.

**The UI and the apps without downtime.** The tunnel reaches them by name
on the `orca` network (`http://ui:8270`, `http://console:8080`), so
`apply` replaces them side by side instead of stopping them first:

1. it starts the new container as `ui-next` (or `console-next`) on the
   `orca` network, where nothing resolves `ui` to it yet;
2. waits until it's healthy: for the UI, 200 on `/healthz` (answered once
   the database does, without a login) and, if `ORCA_WORKER_ADDR` is set in
   `settings.env`, an answer on the worker API's port; for an app, 200 on
   its check path, `/` unless `orca-stack app NAME PORT CHECK` says
   otherwise (the census console answers 503 while it warms up, so 200 on
   `/` means it has read the lake);
3. gives it the alias `ui` (a reconnect to the network, as Docker only
   gives aliases on connecting) and checks it again; Docker's DNS now
   answers `ui` with both containers;
4. stops the old one with `docker stop -t 10` (ten seconds for the requests
   in flight; the UI's `/healthz` answers 503 `draining` from the
   moment it gets the signal, docs/ui.md#stopping), removes it, and renames
   `ui-next` to `ui`.

cloudflared resolves the name for each new connection, so traffic moves
over without it restarting, and the worker API's listener, in the same
container, moves with the UI. The check is made inside the container with
bash's `/dev/tcp` (the images are Debian without curl). A new container
that isn't healthy within `HEALTH_TIMEOUT` seconds (`orca-stack configure
HEALTH_TIMEOUT=600`; default 300; `apply --health-timeout` for one apply),
or that exits, is removed and the old one keeps serving; `apply` goes on
with the other containers, then exits non-zero saying which one and why,
with the end of its log, and an `apply` again tries only that one. A
container that isn't running is simply started. The scheduler is not
replaced this way: it's the single writer of scheduling decisions and its
runs are its children, so two at once would start runs twice ([#232](https://github.com/magmalake/orca.mojo/issues/232)). The
sampler and cloudflared are stopped and started as before.

Checked against real Docker (colima): with a loop of requests to `ui` and
`console` from a container on the network, a new connection each 50 ms,
an apply that replaced both (the console warming up for 8 s) and one whose
new containers never became healthy saw no failed request.

Runs on remote workers ([docs/workers.md](workers.md)) talk only to the
UI's API, through the tunnel, so the UI's swap is what they see of a
deploy: its polls and its runs' store calls and heartbeats go on to the
old UI, then to the new one. A request cut as the old one stops (after
its 10 s for requests in flight) is retried: the worker polls again, and a
run's store calls retry for `ORCA_DB_RETRY_SECONDS`. What would hurt is
the UI unreachable for longer than half the stale limit (15 s by
default): the scheduler then takes the worker for lost and queues its
runs again. A UI that's down for minutes does that, which is why
`rescale.sh` waits for the workers' runs too (below).

**The scheduler.** Restarting it kills the runs its processes hold, so
`apply` refuses, while any of those is running, only when the scheduler is
to be replaced (`--force` kills them). Runs on remote workers (placement
`worker:<pool>`) don't count: they're the worker's processes, which talk to
the UI and run on through the scheduler's restart. `deploy.sh` asks for the plan first, and does the safe
sequence itself, logging each step:

1. copies this checkout's `orca-stack` to the server, and prints `orca-stack
   plan`'s lines;
2. if the plan replaces the scheduler, `orca-stack quiesce`: pauses every
   pipeline that isn't paused, then waits for the scheduler's running runs
   to end, listing them each minute (`--timeout SECONDS`, default 3600). It
   doesn't wait for runs on remote workers: it lists those as `left running
   on workers`, so you know they go on. If the plan doesn't replace the
   scheduler, nothing is paused and running runs go on;
3. if the plan replaces anything, `orca-stack backup-now`: a `pg_dump` to
   the bucket, as the nightly one (below), and says where: `previous tag
   ac0c766, new tag b2b9558; backup
   s3://<bucket>/backups/pg/20261108T101500Z.dump`. Beside the dump it
   writes `<time>.paused`, which pipelines were paused by hand (or the
   watchdog) and which by this deploy, and prints both. If the backup fails
   it unpauses what it paused and stops without deploying; `--no-backup`
   deploys without one;
4. `orca-stack apply --tag <tag>`: pulls, replaces what changed, and keeps
   the tags it replaced (see [Rolling back](#rolling-back));
5. if the scheduler was replaced, `orca-stack register`: registers the
   pipelines from the new image (`docker exec scheduler orca register`, its
   `orca.toml`), so queued runs move to the new code before any starts.
   The pipelines are in the scheduler's image, so when it wasn't replaced
   there is nothing new to register;
6. if it paused them, `orca-stack resume`: unpauses the pipelines it
   paused, and only those (not ones the watchdog or you paused).

So a deploy of a new orca tag pauses, waits, backs up, replaces the
scheduler, UI, sampler and the apps that follow the orca tag, registers and
unpauses. A census-only deploy (`./deploy.sh <the orca tag that runs>
console=<sha>`) backs up and replaces the console, and nothing is paused,
killed or registered. A deploy of what already runs does nothing: no
backup, and every container is kept.

If only a new UI or app isn't healthy, the apply fails after the rest is
applied, and the deploy finishes all the same: a scheduler it replaced is
registered, the pipelines it paused are unpaused, and it prints `orca-stack
status`, then exits non-zero naming the containers that weren't replaced
(the old ones keep serving). Once the cause is fixed, `ssh root@<ip>
orca-stack apply` replaces only those. A rollback does the same. On a
timeout, a scheduler that didn't start, a failed registration or a failed
apply otherwise, it stops with the pipelines still paused if it paused
them, and says so (after a failed registration: fix it, then `orca-stack
register` and `orca-stack resume`, or `./deploy.sh --rollback`); `ssh
root@<ip> orca-stack resume` undoes the pause. `--force` skips the pause and the wait and kills the running runs if
the scheduler is replaced. `orca-stack status` shows the running runs (the
scheduler's and those on remote workers) and what quiesce paused.

**Overlapping schedulers** (off by default; [docs/overlap.md](overlap.md)).
With `orca-stack configure OVERLAP=1` the containers get `ORCA_OVERLAP=on`,
and a deploy that replaces the scheduler doesn't have to pause. Step 2
first asks `orca-stack can-overlap --tag <tag>`, which runs the new
image's `orca scheduler can-overlap`. It says yes when the running
scheduler leads with overlap on and the new binary's migrations don't
raise the schema's floor past it. On yes, nothing is paused or killed:

- the apply starts the new scheduler as `scheduler-next` beside the old
  one, sets the old one's restart policy to `no` (once it has drained it
  must not come back as the newest instance), renames it `scheduler-old`
  and the new one `scheduler`, and waits until the new one leads (`orca
  scheduler wait-lead`, `HEALTH_TIMEOUT`). The old one drains as soon as it
  sees the new one: it starts nothing, and its runs go on, under its image;
- the pipelines are registered from the new scheduler, which now alone
  starts runs;
- `orca-stack drain-old --timeout <--timeout>` asks the old one to drain,
  waits for it to exit once its runs have ended, and removes it. After the
  timeout it is stopped instead: its runs are killed, and the new
  scheduler queues them again.

On no (overlap off, a migration that raises the floor, no scheduler going,
or a `scheduler-old` still draining from a deploy before), the deploy
pauses and waits as above, and says why. So does the deploy that turns
`OVERLAP` on: the scheduler it replaces has overlap off. A rollback,
`--force` and a dry run don't overlap. `orca-stack status` lists the
scheduler instances (`orca scheduler status`) when `OVERLAP=1`.

### The check after each deploy

Every `deploy.sh` (a rollback too) ends with `orca-stack check`, a smoke
check that prints a line per item, `ok` or `FAIL` with why, and fails the
deploy when one fails. It undoes nothing; `./deploy.sh --rollback` is the
way back. It checks:

- Postgres answers, and each container (scheduler, ui, sampler, the apps,
  cloudflared) is running the image `stack.conf` says, the UI and the apps
  healthy as apply probes them;
- one scheduler: no `scheduler-next` or `scheduler-old` is left, and with
  `OVERLAP=1` exactly one instance leads;
- the pipelines: at least one registered, and each one's binary is in the
  scheduler's image (`orca-stack register` fixes a stale one); the paused
  ones are listed;
- canary runs, if `hetzner.env` has `CANARY="PIPELINE=KEY ..."`: a run each
  (`orca start`), one at a time, waited for up to `CANARY_TIMEOUT` seconds
  (default 1800), each of which must succeed. For a dev stack, not
  production: they are real runs.

`up.sh` runs it too, but only reports: a new stack has nothing registered
yet. Run it by hand with `ssh root@<ip> orca-stack check [--canary
PIPELINE=KEY]...`; `orca-stack checked` prints the last result, `ok` or
`failed`, its time, `plain` or `canary`, and the tags it checked, which is
what [promote.sh](#a-dev-stack) reads.

## Rolling back

**What is kept.** Each apply that changes a tag records the tags it
replaced on the server: `/etc/orca/deployed` has what runs now (the orca tag
and each app's), `/etc/orca/deployed.prev` what ran before, and
`ORCA_PREV_TAG` in `/etc/orca/stack.conf` that orca tag. Applying the same
tags again keeps the earlier ones. `ssh root@<ip> orca-stack status` shows
both (`deployed: b2b9558   before: ac0c766`), `orca-stack tags [--previous]`
prints one. `deploy.sh` also backs the database up just before each apply
and prints the dump's key, with a `<time>.paused` beside it saying which
pipelines were paused by hand and which by the deploy. The tags are kept from the first deploy with this
`orca-stack` on; before that, the stack's conf is taken as what ran.

**The whole stack.** From the laptop:

```sh
./deploy.sh --rollback                 # quiesce, backup, orca-stack rollback, resume
./deploy.sh --rollback --force         # without waiting: kills the running runs
```

It does what a deploy does (copies `orca-stack`, asks for the plan, pauses
and waits if the scheduler is replaced, backs up), then `orca-stack
rollback`: applies the previous orca tag and the previous app tags (an app
that ran the orca tag follows it again, and one no longer set up is left
out), replacing the containers whose tags differ, and, if it replaced the
scheduler, registers the pipelines again (`docker exec scheduler orca
register`, from the rolled-back image's `orca.toml`), before it unpauses
anything. The older binaries have other registry hashes and maybe
another `orca.toml`, so the registration is what makes the scheduler start
runs with the code that's deployed. On the server, `orca-stack rollback
[--force]` does the same without the pause and the backup. A second rollback
goes forward again: the tags it replaced are now the previous ones.

**Only an app.** The census console is an app with its own tag; rolling it
back leaves the scheduler alone, so no run is killed and nothing needs
registering, with runs running or not:

```sh
ssh root@<ip> orca-stack apply --app console=<sha>
```

The console is replaced without downtime (above), as on a deploy, so a
rollback of it is invisible to readers too. `./deploy.sh <the orca tag that
runs> console=<sha>` does the same with a backup first, and so does `./deploy.sh --rollback` when the deploy before
changed only the console: its plan keeps the scheduler, so nothing is
paused, and `orca-stack rollback` registers the pipelines only when it
replaced the scheduler.

**Running runs.** As on a deploy: when the scheduler is replaced, without
`--force` it pauses the pipelines and waits up to `--timeout` for the
scheduler's running runs to end, and `orca-stack rollback` itself refuses
while any of those is running (runs on remote workers go on). With `--force` the
scheduler's restart kills them. The registration then does what it does
after any new build: queued runs, and failed ones waiting for a retry with
no step done, move to the rolled-back code and start once the pipelines are
unpaused; a run with steps done by the newer code fails, saying how to
resume it (`--allow-new-code`, or Resume with the new code in the UI); and a
manual, UI or rerun run whose process died is failed with an error.

**The schema.** Postgres is not rolled back: the older orca runs on the
newer schema. That works while every migration it doesn't know is
additive (new tables, indexes, and columns that are nullable or have a
default); the scheduler and UI then log `orca: the database is at schema
version 41, ahead of this binary's 40; its migrations since are additive,
so running on it` and go on. A migration that renames, drops or retypes
something, or changes what a value means, raises the floor (AGENTS.md's
Migrations; the list is `_floor_raisers` in `src/orca/migrations.mojo`), and an
orca from before it refuses to start: `orca: the database is at schema
version N, newer than this binary's M, and a migration since needs a binary
that knows at least K`. This applies from orca 0.16.0 (the first that
records floors) on: a rollback to an image with an older orca still stops
at any newer schema, additive or not, and needs the restore below.

**Crossing a floor-raising migration.** Then the database has to go back
too, to the backup taken before the upgrade:

```sh
ssh root@<ip> orca-stack list-backups      # the dumps, by time
ssh root@<ip> orca-stack restore --force backups/pg/20261108T101500Z.dump
ssh root@<ip> orca-stack rollback --force  # the old images on it, registered again
```

`deploy.sh`'s line before the upgrade's apply named the dump (`backup
s3://<bucket>/backups/pg/...`). `orca-stack restore` stops the scheduler
and the UI first, and the rollback starts them again. If a `deploy.sh
--rollback` already stopped at registering, with the scheduler's log
refusing the schema, the old images are deployed already (and `rollback`
would now go forward): restore, then `orca-stack apply` and `orca-stack
register` instead of the last line.

That dump was taken while the deploy had the pipelines paused. `orca-stack
restore` reads the dump's `<time>.paused` and unpauses the pipelines the
deploy paused, so the stack comes back as it was before the deploy; the
ones paused by hand or by the watchdog stay paused, and it prints both.
`list-backups` says which dumps have one (`paused state recorded`). A dump
without one (taken before orca-stack wrote them) leaves every pipeline that
was paused in it paused, and says so: unpause the ones the deploy's log
lists (`orca-stack: paused <name>`) with `docker exec scheduler orca
unpause <name>`. What happened in orca's database since the
dump is lost: runs and their steps, attempts and logs' records, sensor
progress, registrations, users and tokens added, watchdog trips. The
warehouse isn't in the dump: snapshots committed since stay in the tables,
and the runs that made them are gone from orca's history, so a rebuild or
backfill of those keys runs them again. The new runs are given the lost
runs' numbers, and commit all the same: a snapshot carries its run's token
as well as its number. A lost run of an orca from before run tokens (#292)
left snapshots without one, which a new run with its number, pipeline, step
and partition still takes for its own commits and doesn't write again.

**Finding a sha.** Images are tagged by the project's short commit sha on
GHCR, never by version: `orca-stack tags --previous` has the one before,
the project's `git log --oneline` the others (an image exists for each
commit its image job pushed, from the census's `dev` branch; the log ends
with `<short-sha>: digest: sha256:...`), and the package's page on GitHub
lists them.

## Rescaling for an import

```sh
./rescale.sh up                        # CAX21 -> CAX41, keeping the disk
# register the census with DISTA_WORKER_SLOTS = "14" and GHARCHIVE_PARALLEL = "14" if you want the 16 vCPUs used
# ... the fill runs; watch the pipeline page's Fill line ...
./rescale.sh down                      # CAX41 -> CAX21
```

Each does: pause every pipeline and wait for the running runs to end,
those on remote workers too, since the UI they talk to goes down with the
server (`orca-stack quiesce --workers`; `--timeout SECONDS`, default 3600; on a timeout it stops and leaves them
paused: `ssh root@<ip> orca-stack resume` undoes it), a backup, shut the
server down, `hcloud server change-type --keep-disk`, power on, wait for the
scheduler, and unpause what it paused (not pipelines the watchdog paused). It
is down for a few minutes. `--keep-disk` keeps the 80 GB root disk of the
small type, which is what allows coming back down; the data is on the Volume
anyway. `rescale.sh down` waits for the fill to be over or paused: to stop
earlier, cancel the runs in the UI.

## A cgroup per run

Every run on the server shares one machine. Where orca can make cgroups, each
run gets one with a memory limit (`memory.high`) at its grant, and the kernel
kills only that run's processes if they go 25% past it
(`ORCA_RUN_MEMORY_MARGIN`), or orca does once it has been held, throttled, at
the limit for two minutes (`ORCA_RUN_MEMORY_HELD_SECONDS`); the attempt fails `out of memory at <limit>`, and
the retry and the pipeline's later runs ask for 1.5 times as much
(`ORCA_RUN_MEMORY_GROWTH`), up to `memory_gb`'s maximum. Without it, a run
that runs away can push the machine into the kernel's OOM killer, which picks
the run's processes first (`oom_score_adj`) but may take others too.

The scheduler runs in a container, whose `/sys/fs/cgroup` docker mounts
read-only. To turn it on:

```sh
ssh root@<ip> orca-stack configure CGROUPS=1
ssh root@<ip> orca-stack apply           # replaces the scheduler only; pause first, or --force
ssh root@<ip> docker logs scheduler 2>&1 | grep 'run cgroups'
# orca: run cgroups on: runs' cgroups go in /sys/fs/cgroup
```

What `CGROUPS=1` changes in the scheduler's `docker run` (nothing else's):

- `--cgroupns=private`: the container sees its own cgroup as
  `/sys/fs/cgroup` (docker's default on cgroup v2, said explicitly).
- `--cap-add SYS_ADMIN --security-opt apparmor=unconfined` and an entrypoint
  of `mount -o remount,rw /sys/fs/cgroup && exec setpriv --bounding-set
  -sys_admin --inh-caps -sys_admin orca scheduler`: the remount needs the
  capability and AppArmor's leave, and the capability is dropped from the
  bounding set before orca starts, so neither orca nor any run it starts has
  it. (`--privileged` would work too, and gives far more.)

orca then moves itself into `/sys/fs/cgroup/orca` (a cgroup that gives its
children controllers can hold no processes), turns on the memory controller
for the container's cgroup's children, and makes `orca-run-<run>-<start>-<pid>`
beside it for each run. The container's own limits and the host's other
cgroups are out of its reach: under a private namespace with `nsdelegate`, the
container's root cgroup's `memory.max` isn't writable from inside, and nothing
above it is visible. `docker exec scheduler ...` (orca-stack's quiesce,
`orca pause`) still works: runc puts it in `orca`.

**Safe to turn on, but not on by default.** For the machine, it only adds a
boundary; the capability is gone before orca runs, so the one thing given up
for good is docker's AppArmor profile on the scheduler's container. What
changes is what runs may use: a run is throttled past its grant and killed 25%
above it, where today it may take what's free. Before turning it on, check on
each pipeline's page (Resources: the max of its peaks, #203) that its peak
fits under `memory_gb`'s maximum, and raise the maximum where it doesn't; a
pipeline without a request is limited only to what the host can grant (its
memory less `ORCA_RESERVE_MEMORY_MB`). Off again: `orca-stack configure
CGROUPS=0` and `apply`.

Where orca can't make cgroups (this option off, macOS, cgroup v1), the
scheduler says why once (`orca: run cgroups off: ...`) and runs as before:
`first_to_go`, and control-placed runs killed over `ORCA_CONTROL_MEMORY_MB`.
`ORCA_CGROUPS=0` in `settings.env` (a line of `EXTRA_ENV_FILE`) turns it off from orca's side. Ops >
Control lists each run going with its memory against its limit.

## Backups and restore

The server runs `orca-stack backup` every night at 02:30 UTC
(`orca-backup.timer`; `systemctl list-timers` and `journalctl -u
orca-backup` on the server). It writes `pg_dump -n orca -Fc` (the format
[replace-control.md](replace-control.md) uses), checks that it reads back,
uploads it to `s3://<bucket>/backups/pg/<time>.dump`, and keeps the newest 14.
Beside each dump is `<time>.paused`: the pipelines paused when it was taken,
as `paused_by_hand` and `paused_by_deploy` (the ones a deploy's quiesce
paused), which a restore uses to unpause the latter
([Rolling back](#rolling-back)).

```sh
./backup.sh            # now, e.g. before a risky step
./backup.sh list
```

To restore onto a new server and an empty Volume (after `down.sh --all`, or a
lost server):

```sh
./up.sh --restore latest                  # or --restore backups/pg/20261107T023000Z.dump
```

It starts Postgres, restores before the scheduler first starts, then starts
everything. On an existing database, `ssh root@<ip> orca-stack restore --force
<key>` drops the `orca` schema first, then `orca-stack apply`. The warehouse
is in the bucket and needs no restore; only orca's own state (runs, pipelines,
users) is in the dump. Test a restore before you need one.

## Alerts on your phone (ntfy)

The UI is where problems show, so orca can also push the important ones to
your phone through [ntfy](https://ntfy.sh): a watchdog trip, a run that
failed after its last retry, a fill that stalled, a failed nightly backup, the
disk over 85%, a lost worker, each with a link to the UI. Once:

1. Install the ntfy app (iOS, Android) and subscribe to a topic with a long
   random name, e.g. the output of `openssl rand -hex 16`. Anyone who knows
   the topic can read what is sent to it, so treat the name as a secret.
2. Put the topic in a file on the laptop (mode 600), and its path in
   `hetzner.env` as `NTFY_TOPIC_FILE`; `up.sh` writes it to
   `/etc/orca/secrets/orca/ntfy-topic` and sets `ORCA_NOTIFY_SECRET=orca/ntfy-topic`
   (read through `ORCA_SECRET_FETCH` like the other secrets, never logged).
   `NTFY_URL` is the server (default `https://ntfy.sh`; a self-hosted one
   works), `NTFY_TOKEN_FILE` an access token for a protected topic, and
   `UI_URL` (`https://orca.mojourney.dev`) is what the notifications link to
   (`ORCA_UI_URL`).
3. `./up.sh`, then `ssh root@<ip> docker exec scheduler orca notify test`:
   the phone should show "orca: test alert".

Ops > Control lists whether alerts are on and the last ones sent. The same
alert is sent at most once every 30 minutes (`ORCA_NOTIFY_MIN_SECONDS`), and
one that could not be sent (ntfy down) waits in Postgres and goes later. The
nightly backup runs `orca notify --kind backup-failed` on failure.

## Analytics (PostHog)

The UI and the census console can load PostHog the way the magmalake.org
website does: `array.js` and the events go through `/ph` on the page's own
origin, memory-only persistence (no cookies, no `localStorage`, so no consent
banner), and a `site` property on every event (`orca` or `mojourney`) so one
PostHog project can hold both. Once:

1. Deploy the `/ph` proxy, a Cloudflare Worker on the mojourney.dev zone's
   `mojourney.dev/ph/*` and `orca.mojourney.dev/ph/*` routes. It lives in
   magmalake/magmalake.org: `npx wrangler deploy -c worker/mojourney-ph/wrangler.jsonc`.
   The tunnel serves everything else; nothing on the server serves `/ph`.
2. Put the project key (`phc_...`, public, the website's `POSTHOG_KEY` in its
   `src/data/site.ts`) in `hetzner.env` as `POSTHOG_KEY`. `up.sh` writes it
   to `settings.env` as `ORCA_POSTHOG_KEY`, which the UI reads, and
   `orca-stack apply` passes it to the apps as `POSTHOG_KEY`, which the
   census console reads. On a server set up before, the next `./deploy.sh`
   (the tag that runs will do) writes it and replaces the UI and the
   console ([Settings](#settings-from-hetznerenv)).
3. Open https://orca.mojourney.dev and https://mojourney.dev: each page's
   head has `/ph/static/array.js`, and PostHog's Activity shows `$pageview`
   events with `site` set.

Leave `POSTHOG_KEY` empty and nothing is added to either site's pages.

## Teardown

```sh
./down.sh          # deletes the server; the Volume and the bucket stay; up.sh brings it back
./down.sh --all    # also the Volume (the database), firewall and ssh key; asks for the Volume's name
```

Both ask first (`--yes` to skip). The bucket is never touched: delete it in the
Hetzner console. The Volume has delete protection, which `--all` lifts.

## Images

The scripts pull `ghcr.io/magmalake/orca:<tag>` (`ORCA_IMAGE` in
`hetzner.env`). The orca repository's CI doesn't push images anywhere (ECR
is only pushed by `orca aws push`, from a project's own build), so there is
nothing to add behind a CI build here. A project publishes to GHCR from its
own CI, tagged `<short-sha>` of the commit; there are no orca version tags
(`ORCA_TAG=0.10.0` does not exist). The census builds and pushes only from
its `dev` branch (also tagged `dev`; main builds no image, and `latest` is
no longer moved), so a release reaches `dev` first, then the dev stack, then
production. To find a sha, read the `dev` branch's image job in the
project's Actions: its log ends with `<short-sha>: digest: sha256:...` for
the push. `ORCA_TAG` in `hetzner.env` is the tag the first `up.sh` starts
(`dev` will do; `deploy.sh <short-sha>` moves it afterwards).

The tunnel runs `cloudflare/cloudflared` at a release tag
(`CLOUDFLARED_IMAGE`, `2026.10.7` unless `hetzner.env` says otherwise), not
`latest`: `orca-stack apply` pulls the image every time, so with `latest` an
upstream release would change the tunnel's version, and restart it, on
whichever deploy came next. To bump it, pick a release (`gh release list -R
cloudflare/cloudflared`; the image has the same tag), set
`CLOUDFLARED_IMAGE=cloudflare/cloudflared:<tag>` in `hetzner.env` and
deploy (the next `deploy.sh` sets it on the server and replaces the
tunnel). A `orca-stack configure CLOUDFLARED_IMAGE=...` on the server by
hand lasts only until the next deploy, which sets `hetzner.env`'s again.
The tunnel restarts once, which drops traffic for a few seconds. The
defaults in `lib.sh` and `orca-stack` only apply when `hetzner.env` doesn't
set it; bump them along with the example's.

For example:

```yaml
permissions: { contents: read, packages: write }
steps:
  - uses: docker/login-action@v3
    with: { registry: ghcr.io, username: "${{ github.actor }}", password: "${{ secrets.GITHUB_TOKEN }}" }
  - run: docker buildx build --platform linux/arm64 -f Dockerfile.orca -t ghcr.io/magmalake/orca:${{ github.ref_name }} --push .
```

The workflow's `GITHUB_TOKEN` can push to a package only if the package lets
the repository write to it (the package's settings > Manage Actions access).
The server needs ARM images (`linux/arm64`). A new GHCR package is private,
and an organization may forbid public packages, so the private route is the
default: set `GHCR_USER` and `GHCR_TOKEN_FILE` in `hetzner.env` (a token with
`read:packages`, in a file of its own). Making the package public is the
alternative only where the organization allows it. ECR stays as it is for
AWS.

**Build for the server's CPU.** The server is Neoverse N1 (Ampere Altra);
GitHub's arm64 runners are Neoverse N2, with SVE2. `mojo build` targets the
CPU it runs on, so a binary built on a runner dies on the server with
SIGILL (exit 132). The image `orca aws push` builds from
[`deploy/project.Dockerfile`](../deploy/project.Dockerfile) passes
`--target-cpu "$MOJO_TARGET_CPU"` (default `neoverse-n1`; N1 code runs on
N2, V1, V2 and Graviton). A project with its own `Dockerfile.orca` must do
the same in every `mojo build` of it:

```dockerfile
ARG MOJO_TARGET_CPU=neoverse-n1
RUN pixi run sh -c 'for f in bin/*.mojo; do mojo build --target-cpu "$MOJO_TARGET_CPU" "$f" -o "/out/bin/$(basename "$f" .mojo)" || exit 1; done'
```

## S3-compatible storage

orca and the census read `AWS_ENDPOINT_URL_S3` (else `AWS_ENDPOINT_URL`),
`AWS_REGION` and `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
(objectstore.mojo). With an endpoint set it uses path-style addressing
(`AWS_S3_FORCE_PATH_STYLE=1` forces it otherwise) and never asks for an
instance role. For Hetzner: `AWS_ENDPOINT_URL_S3=https://<location>.your-objectstorage.com`
and `AWS_REGION=<location>`. The same settings were tried locally against
MinIO.

## Costs

Prices change; check hetzner.com for the current ones before you start.
Roughly, at the time of writing: the CAX21 (4 ARM vCPU, 8 GB, 80 GB) is a
few euros a month, the CAX41 (16 vCPU, 32 GB) about four times that; both are
billed by the hour up to the monthly price, so a few days of CAX41 for the
import cost about five euros and the rest of the month is the small
server's price. Add the Volume (per GB-month, 50 GB by default) and Object
Storage (a base monthly price covering some storage and traffic, more beyond
that). Cloudflare Tunnel is free. A stopped server is still billed; delete it
(`down.sh`) to stop the charge. Nothing here launches instances, so the bill
cannot grow by itself except for storage and traffic.

## A dev stack

A second, smaller stack to try a release, a deploy or a deploy-script change
before production sees it (#250). It is a second settings file: `NAME`
prefixes every Hetzner resource (server, firewall, Volume `NAME-data`, ssh
key) and the scripts look them up by that name, so `NAME=orca-dev` never
touches `orca`'s. Everything else is on the server itself (`/etc/orca`,
Postgres on its own Volume) or names its own bucket and tunnel. Every script
starts by saying which stack it acts on (`deploy.sh: stack orca-dev,
settings ...`). The two stacks share `known_hosts`
(`~/.local/state/orca-hetzner/known_hosts`), which is keyed by address.

It can be in the production project or a Hetzner project of its own. Its
own project keeps its API token and Object Storage keys apart from
production's (Object Storage keys reach every bucket of their project); set
`HCLOUD_CONTEXT` in its settings file to that project's hcloud context. In
the same project, Hetzner takes a public key only once, so `up.sh` uses the
existing key with the same public key (production's `orca`) instead of
making `orca-dev`; `down.sh --all` then leaves that key alone.

**Settings**, e.g. `~/.config/orca-hetzner/dev.env` (from
`hetzner.env.example`; what's not listed is as there or empty):

```sh
NAME=orca-dev
#HCLOUD_CONTEXT=orca-dev                # if it's in a project of its own
LOCATION=fsn1
SERVER_TYPE_SMALL=cax11                 # 2 vCPU, 4 GB: enough without the backfill
VOLUME_SIZE_GB=10                       # the smallest
SSH_PUBLIC_KEY_FILE=$HOME/.ssh/id_ed25519.pub
ADMIN_IP=203.0.113.7/32
BUCKET=orca-dev-warehouse               # its own bucket
S3_ENDPOINT=https://fsn1.your-objectstorage.com
S3_REGION=fsn1
S3_CREDENTIALS_FILE=$HOME/.config/orca-hetzner/dev-s3.env
TUNNEL_TOKEN_FILE=$HOME/.config/orca-hetzner/dev-tunnel.env   # its own tunnel
ORCA_TAG=<short sha>                    # the tag the first up.sh starts
GHCR_USER=...                           # as production's, for the private package
GHCR_TOKEN_FILE=...
GITHUB_TOKEN_FILE=$HOME/.config/orca-hetzner/github-token     # or mojo_repos's and mojo_stats's runs fail
EXTRA_ENV_FILE=$HOME/.config/orca-hetzner/dev-extra.env
# No NTFY_TOPIC_FILE: no alerts. Or a topic of its own, never production's.
# No POSTHOG_KEY: the dev pages stay out of the analytics.
```

with `dev-extra.env` holding

```sh
ORCA_CONFIG=/opt/orca/orca.dev.toml
```

The scheduler and the UI get it (`settings.env`, then `orca.env`, ahead of
the image's own `ORCA_CONFIG`), so `orca register` there reads the census's
`orca.dev.toml`: the same pipelines as `orca.toml`, without fills and
without a worker pool's placement, so a dev stack never backfills from
2023. orca has no "fill the last N days" setting. For some history, backfill
a recent range by hand once, e.g. `docker exec scheduler orca backfill
gharchive 2026-10-09..2026-11-07`. Only census images built with
`orca.dev.toml` have the file (mojo-census#87 on): `orca register` fails on an
older one, so don't roll the dev stack back past it.

The tunnel's public hostnames, Proxied as for production: the census's dev
stack has `orca-beta.mojourney.dev` to `http://ui:8270` and
`beta.mojourney.dev` to `http://console:8080`, with `UI_URL` set to the
first. One level below the zone, because Cloudflare's free Universal SSL
certificate covers `mojourney.dev` and `*.mojourney.dev` only:
`orca.beta.mojourney.dev` would need an Advanced Certificate.

**Images.** The census's CI pushes `ghcr.io/magmalake/orca:<short sha>` from
its `dev` branch (and `dev`); main builds none, so both stacks deploy dev's
images. To try an orca.mojo main commit, pin it on the census's `dev` branch
(`orca-mojo = { git = ..., rev = "<sha>" }` in `pixi.toml`, then `pixi lock`),
push, and deploy the short sha of that census commit.

**Making it and deploying**, from `deploy/hetzner`:

```sh
export HETZNER_ENV=~/.config/orca-hetzner/dev.env   # in this shell only
./up.sh --dry-run                       # every command names orca-dev
./up.sh
ssh root@<ip> docker exec -it scheduler orca user add <you>
ssh root@<ip> orca-stack register       # orca.dev.toml's pipelines
./deploy.sh <tag>                       # later deploys
./down.sh                               # when it's not needed: the Volume stays, up.sh brings it back
```

**Release check list on dev**, before the same tag goes to production.
Set `CANARY` in the dev stack's settings (e.g. `CANARY="mojo_repos=2026-11-01
gharchive=2026-11-08"`) and steps 2, 3 and 5, and step 4 inside the server (not the public URLs), are
[the check deploy.sh ends with](#the-check-after-each-deploy):

1. `./deploy.sh <tag>`: it ends with `check: all ok` and `done`, and says
   what it replaced.
2. `ssh root@<ip> orca-stack status`: every container up, `deployed: <tag>`
   and `before:` the tag before.
3. The pipelines registered: `docker exec scheduler orca status` lists
   each of `orca.dev.toml`'s, from `<tag>`, and the ones paused before are
   still paused.
4. `curl -s -o /dev/null -w '%{http_code}\n' https://orca-beta.mojourney.dev/healthz`
   says 200, and the console (`https://beta.mojourney.dev`) answers.
5. One manual run per pipeline (`orca start <pipeline> <partition>`, or the
   UI's run button), each succeeding.
6. A deploy with a run in flight: start a long run, `./deploy.sh <tag>
   --all`, and see it pause, wait and resume. Then with overlapping
   schedulers: `ssh root@<ip> orca-stack configure OVERLAP=1`, deploy once
   to turn it on, start a run, deploy again: nothing paused, the run ends
   under `scheduler-old`, which is then removed.
7. `./deploy.sh --rollback` (back to the tag before, registered again), then
   `./deploy.sh --rollback` again to go forward; `orca-stack status` shows
   `<tag>` deployed.

Then promote what dev checked, from production's settings:

```sh
export HETZNER_ENV=~/.config/orca-hetzner/hetzner.env   # production's
./promote.sh --from ~/.config/orca-hetzner/dev.env      # --dry-run first, if you like
```

`promote.sh` asks the dev stack for its last check (`orca-stack checked`)
and refuses unless it passed on the tags dev runs now, so the tags that
ship are the ones that were tested, never rebuilt. It then runs
`deploy.sh` with them (`<tag>`, plus `console=<tag>` for an app on its own
tag) and any of deploy.sh's flags given (`--no-backup`, `--timeout S`,
`--yes`, ...). It warns when dev's check ran no canary.

**Cost**: a CAX11, its IPv4 address, a 10 GB Volume and its bucket's
storage: a few euros a month (see [Costs](#costs)). In a project of its own,
Object Storage's base monthly price is charged again. `down.sh` stops the
server's charge between releases.

## Not verified

This was written without a Hetzner account: no server was created, and the
scripts were checked with `shellcheck`, `--dry-run`, cloud-init's schema
checker, and the container set against MinIO on a workstation. First real
run: do `up.sh --dry-run`, then `up.sh`, and read its output. Unverified
against the real service: Object Storage's endpoint name and path-style
addressing, the Volume's `/dev/disk/by-id/scsi-0HC_Volume_<id>` name in
`cloud-init.yaml`, and the `hcloud ... -o json` field names the scripts read
with `jq` (`.id`, `.status`, `.server_type.name`, `.applied_to`).
