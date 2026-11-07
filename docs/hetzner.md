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
 orca.mlake.dev               | scheduler (local runner), ui, sampler, console     |
 census.mlake.dev             +----------------------------------------------------+
                                        |  S3 API (outbound)
                                        v
                              Hetzner Object Storage: warehouse/, logs/, backups/pg/
```

## What you do first (consoles, once)

1. **Hetzner Cloud**: a project, and an API token with read & write
   (Security > API tokens). On the laptop: `hcloud context create orca` and
   paste the token (it stays in hcloud's config, not in this repo).
2. **Object Storage** (same console, same location as the server): a bucket,
   and S3 credentials (access key and secret). Put them in a file of two
   lines, `AWS_ACCESS_KEY_ID=...` and `AWS_SECRET_ACCESS_KEY=...`, mode 600.
3. **Cloudflare**: Zero Trust > Networks > Tunnels > create a tunnel
   ("Cloudflared"), and copy its token into a file as one line,
   `TUNNEL_TOKEN=...`, mode 600. Add two public hostnames to the tunnel:
   `orca.mlake.dev` to service `http://ui:8270`, and `census.mlake.dev` to
   `http://console:8080`. (If those names are on the AWS stack, Cloudflare
   replaces their DNS records with the tunnel's CNAME.)
4. **A GitHub token** for the census's `mojo_repos`, in a file (read-only
   access to public repositories is enough).
5. **Your address** for the firewall: `curl -4 ifconfig.me`, plus `/32`.
6. **Tools** on the laptop: `hcloud`, `ssh`, `jq`, POSIX `sh`.
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

and register the pipelines (below). Open `https://orca.mlake.dev`.

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
file's commit). Read the file there; each setting has a comment. In short:
`max_runs = 1` and `fill_max_runs = 1` with `fills = "all"` is the setting
until iceberg-mojo 0.11.1 is pinned everywhere (issue 164), and
`DISTA_WORKER_SLOTS` is how many dista workers run in one process on the
server: 4 on the CAX21; during an import on the CAX41 register it again with
14, and set it back to 4 after.

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
left out runs on the scheduler's runner, here the local one. The Fleet page
(Ops > Fleet) says the runner launches no instances, and the watchdog's cost
rule reads "fixed price" and never trips; its launch, idle, failing, stuck
and control rules stay on.

## Deploying code

```sh
./deploy.sh 0.10.1                     # pulls ghcr.io/magmalake/orca:0.10.1, restarts scheduler, ui, sampler, apps, tunnel
./deploy.sh 0.10.1 console=0.10.0      # one app on another tag
```

This replaces `orca aws app` and `orca aws up` for code changes. Postgres is
not restarted. It refuses while runs are running (restarting the scheduler
kills them): pause the pipelines in the UI and wait, or use `--force`.

## Rescaling for an import

```sh
./rescale.sh up                        # CAX21 -> CAX41, keeping the disk
# register the census with DISTA_WORKER_SLOTS = "14" if you want the 16 vCPUs used
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
own CI:

```yaml
permissions: { contents: read, packages: write }
steps:
  - uses: docker/login-action@v3
    with: { registry: ghcr.io, username: "${{ github.actor }}", password: "${{ secrets.GITHUB_TOKEN }}" }
  - run: docker buildx build --platform linux/arm64 -f Dockerfile.orca -t ghcr.io/magmalake/orca:${{ github.ref_name }} --push .
```

The workflow's `GITHUB_TOKEN` can push to a package only if the package lets
the repository write to it (the package's settings > Manage Actions access).
The server needs ARM images (`linux/arm64`). A new GHCR package is private:
either make it public, or give the server a token that can read packages
(`GHCR_USER` and `GHCR_TOKEN_FILE` in `hetzner.env`). ECR stays as it is for
AWS.

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
