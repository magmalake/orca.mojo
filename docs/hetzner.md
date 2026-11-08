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
   `mkdir -p ~/.config/orca-hetzner && cp deploy/hetzner/hetzner.env.example ~/.config/orca-hetzner/hetzner.env`
   and `export HETZNER_ENV=~/.config/orca-hetzner/hetzner.env` (or put the
   export in your shell's profile).
7. **Images**: the scheduler's image is the project's (for the census,
   the one `orca aws push` builds from its `Dockerfile.orca`), at
   `ghcr.io/magmalake/orca:<tag>`; see [Images](#images).

## First setup

```sh
cd deploy/hetzner
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
starts the containers. Run it again whenever `hetzner.env` changes: it
fixes drift. It does not restart a scheduler that has runs running unless you
add `--force` (a restart kills them).

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

## Deploying code

```sh
./deploy.sh a1b2c3d                    # pulls ghcr.io/magmalake/orca:a1b2c3d, restarts scheduler, ui, sampler, apps, tunnel
./deploy.sh a1b2c3d console=latest     # one app on another tag
```

The tag is the project's image tag: see [Images](#images). This replaces
`orca aws app` and `orca aws up` for code changes, and needs `hcloud` with a
context like `up.sh`. Postgres is not restarted. Restarting the scheduler kills
running runs, so `deploy.sh` does the safe sequence itself, logging each step:

1. copies this checkout's `orca-stack` to the server;
2. `orca-stack quiesce`: pauses every pipeline that isn't paused, then waits
   for the running runs to end, listing them each minute (`--timeout
   SECONDS`, default 3600);
3. `orca-stack apply --tag <tag>`: pulls and restarts;
4. `orca-stack resume`: unpauses the pipelines it paused, and only those
   (not ones the watchdog or you paused).

On a timeout or a failed apply it stops with the pipelines still paused and
says so; `ssh root@<ip> orca-stack resume` undoes the pause. `--force` skips
the pause and the wait and kills the running runs. `orca-stack status` shows
the running runs and what quiesce paused.

## Rescaling for an import

```sh
./rescale.sh up                        # CAX21 -> CAX41, keeping the disk
# register the census with DISTA_WORKER_SLOTS = "14" and GHARCHIVE_PARALLEL = "14" if you want the 16 vCPUs used
# ... the fill runs; watch the pipeline page's Fill line ...
./rescale.sh down                      # CAX41 -> CAX21
```

Each does: pause every pipeline and wait for the running runs to end
(`--timeout SECONDS`, default 3600; on a timeout it stops and leaves them
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
(`ORCA_RUN_MEMORY_MARGIN`); the attempt fails `out of memory at <limit>`, and
the retry and the pipeline's later runs ask for 1.5 times as much
(`ORCA_RUN_MEMORY_GROWTH`), up to `memory_gb`'s maximum. Without it, a run
that runs away can push the machine into the kernel's OOM killer, which picks
the run's processes first (`oom_score_adj`) but may take others too.

The scheduler runs in a container, whose `/sys/fs/cgroup` docker mounts
read-only. To turn it on:

```sh
ssh root@<ip> orca-stack configure CGROUPS=1
ssh root@<ip> orca-stack apply           # restarts the scheduler; pause first, or --force
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
`ORCA_CGROUPS=0` in `settings.env` turns it off from orca's side. Ops >
Control lists each run going with its memory against its limit.

## Backups and restore

The server runs `orca-stack backup` every night at 02:30 UTC
(`orca-backup.timer`; `systemctl list-timers` and `journalctl -u
orca-backup` on the server). It writes `pg_dump -n orca -Fc` (the format
[replace-control.md](replace-control.md) uses), checks that it reads back,
uploads it to `s3://<bucket>/backups/pg/<time>.dump`, and keeps the newest 14.

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
   census console reads. Without `up.sh`, add
   `ORCA_POSTHOG_KEY=phc_...` to `/etc/orca/settings.env` on the server and
   `orca-stack apply`.
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
own CI, tagged `<short-sha>` of the commit and `latest`; there are no orca
version tags (`ORCA_TAG=0.10.0` does not exist). To find a sha, read main's
image job in the project's Actions: its log ends with
`<short-sha>: digest: sha256:...` for the push. `ORCA_TAG` in `hetzner.env`
is the tag the first `up.sh` starts (`latest` will do; `deploy.sh <short-sha>`
moves it afterwards).

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

## Not verified

This was written without a Hetzner account: no server was created, and the
scripts were checked with `shellcheck`, `--dry-run`, cloud-init's schema
checker, and the container set against MinIO on a workstation. First real
run: do `up.sh --dry-run`, then `up.sh`, and read its output. Unverified
against the real service: Object Storage's endpoint name and path-style
addressing, the Volume's `/dev/disk/by-id/scsi-0HC_Volume_<id>` name in
`cloud-init.yaml`, the `hcloud ... -o json` field names the scripts read with
`jq` (`.id`, `.status`, `.server_type.name`, `.applied_to`), and
`cloudflare/cloudflared:latest` (pin a tag in `hetzner.env`).
