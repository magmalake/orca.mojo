# Replacing the control instance

The control instance (Postgres, the scheduler, the UI, Caddy and the apps)
is replaced, not edited: a new one boots from the current template beside
it, gets its database, and the Elastic IP moves to it once it's healthy.
`morca aws replace-control` does this; this page is the same steps done by
hand, for when it can't be used, and what to check either way. DESIGN.md,
"Replacing the control instance", says why it's built this way.

## Before you start

- **Rehearse on a dev stack first**: `morca aws up --stack morca-dev --email
  ...`, `morca aws push --stack morca-dev`, `morca aws register --stack morca-dev
  <a pipeline>`, run something, then `morca aws replace-control --stack
  morca-dev` (and `--rollback`, and once more to the end).
- **What it costs**: a second control instance (t4g.large, about $0.07 an
  hour) for the rollback window, 24 hours by default, stopped for all but
  the first hour of it (a stopped instance pays for its 40 GB volume only,
  about $0.10 a day); the backups in the morca bucket, under `backups/`.
- **Downtime**: the UI and the console are down from the freeze (step 2) to
  the switch (step 7), typically 10 to 15 minutes, most of it the new
  instance's boot. Runs are paused throughout: pause every pipeline first,
  unpause them after.
- **The settings**: everything the instance needs is in the stack's
  parameters. Anything set by hand on the old instance has to be in them
  before the new one boots, or the check (step 6) fails, naming it:
  - the UI's and the console's hostnames: `Hostname`, `ConsoleHostname`
    (`--hostname`, `--console-hostname`);
  - extra `/etc/orca.env` settings: `ControlEnv`, `KEY=VALUE` entries
    separated by `;` (`--env KEY=VALUE`, repeated; all of them at once).
    Never a secret: the parameters are plain text. A run's secrets are
    `morca register --secret`.

For the census, that is `--hostname orca.mlake.dev --console-hostname
census.mlake.dev --env 'MORCA_UI_LINKS=Census ↗=https://census.mlake.dev'
--env DISTA_WORKER_SLOTS=4 --env MORCA_RUN_ENV=MORCA_PUBLIC_WAREHOUSE,DISTA_WORKER_SLOTS`.

## With the command

```sh
morca aws replace-control --stack morca --plan          # the steps, nothing done
morca --remote https://orca.mlake.dev pause <pipeline> # each one
morca aws replace-control --stack morca --wait [--smoke <pipeline>=<key>] [--hostname ...] [--env ...]
# ... check the UI, then unpause the pipelines
morca aws replace-control --stack morca --resume        # after 24h: terminates the old instance
morca aws replace-control --stack morca --rollback      # instead, to go back
```

It writes `build/morca-aws/replace-<stack>.state` and `.log` in the
directory it runs in: run `--resume` and `--rollback` from the same one.

## By hand

Below, `S` is the stack, `B` its bucket (`morca aws status`: `Bucket`), the
old instance is in slot blue and the new one goes in green (swap them if
`ActiveControl` is green). On an instance, run commands with `aws ssm
start-session --target <id>` and `sudo -i`; `q` is
`docker exec postgres psql -U orca -d orca -Atc`.

1. **Quiet.** Pause every pipeline (`morca --remote <ui> pause <p>`), then
   on the old instance both of these must print nothing:
   `q "SELECT name FROM orca.pipelines WHERE NOT paused"` and
   `q "SELECT id FROM orca.runs WHERE status = 'running'"`.
2. **Freeze.** `docker update --restart=no scheduler ui && docker stop
   scheduler ui`, then check again that no run is running.
3. **Back up.** With `P=s3://B/backups/S/$(date -u +%Y%m%dT%H%M%SZ)`:

   ```sh
   b=/var/lib/orca/backup; mkdir -p $b
   docker exec postgres pg_dump -U orca -d orca -n orca -Fc > $b/b.dump
   docker exec postgres createdb -U orca morca_verify
   docker exec -i postgres pg_restore -U orca -d morca_verify --no-owner --exit-on-error < $b/b.dump
   # the same four numbers from -d orca and -d morca_verify, then drop morca_verify:
   #   pipelines, runs, max(runs.id), max(schema_version.version)
   docker exec postgres dropdb -U orca morca_verify
   cp /var/lib/orca/tag $b/b.tag
   tar -czf $b/b.caddy.tgz -C /var/lib/orca/caddy .
   cp /var/lib/orca/apps $b/b.apps   # or write it: "console <host> 8080", one line per app
   grep -vE '^(MORCA_DATABASE_URL|MORCA_EC2_DATABASE_URL|MORCA_EC2_AMI)=' /etc/orca.env | grep -viE 'PASSWORD|SECRET|TOKEN' | sort > $b/b.env
   for e in dump tag caddy.tgz apps env; do aws s3 cp $b/b.$e $P.$e; done
   echo $P | aws s3 cp - s3://B/backups/S/restore-green
   rm -rf $b
   ```

   The new instance's boot restores from what `restore-green` points at.
4. **Launch.** From a checkout of morca (or `build/morca-aws/morca.yaml`,
   which `morca aws up` writes), with the old instance's AMI and the latest
   one:

   ```sh
   old_ami=$(aws ec2 describe-instances --instance-ids <old> --query 'Reservations[0].Instances[0].ImageId' --output text)
   new_ami=$(aws ssm get-parameter --name /aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id --query Parameter.Value --output text)
   aws cloudformation deploy --stack-name S --template-file deploy/aws/morca.yaml --capabilities CAPABILITY_IAM \
     --parameter-overrides GreenSlot=true BlueSlot=true ActiveControl=blue GreenAmi=$new_ami BlueAmi=$old_ami [Hostname=... ConsoleHostname=... ControlEnv=...]
   ```

   Parameters not given keep their values. A stack from before the slots
   (no `ActiveControl` output) has its instance restarted once by this
   update, as its user data changes; it's frozen, so that's harmless.
5. **Boot.** `GreenInstance` (`morca aws status`) is the new instance; it's
   booted once `/var/lib/orca/booted` exists (its log:
   `/var/log/cloud-init-output.log`).
6. **Check**, on the new instance: `/var/lib/orca/restored` is `$P`; the four
   numbers of step 3 match (the schema version may be higher); every
   pipeline is paused; `docker ps` has postgres, scheduler, ui, caddy,
   sampler and the apps; `docker exec caddy wget -qO- http://ui:8270/` and
   each app's `http://<name>:<port>/` answer; `q "SELECT count(*) FROM
   orca.system_minutes WHERE minute > now() - interval '10 minutes'"` is
   above 0; `docker exec scheduler morca status` works; every line of
   `$P.env` is in the new `/etc/orca.env`; and Caddy's
   `/var/lib/orca/Caddyfile` has every hostname the old one had. A smoke
   run: unpause a small pipeline, `docker exec scheduler morca start <p>
   <key>`, wait for it to succeed, pause it again.
7. **Switch.** `aws cloudformation deploy ... --parameter-overrides
   ActiveControl=green BlueAmi=$old_ami GreenAmi=$new_ami`; `aws ec2
   describe-addresses --allocation-ids <AddressAllocation>` shows the new
   instance; `aws ec2 stop-instances --instance-ids <old>`; `aws s3 rm
   s3://B/backups/S/restore-green`. The UI answers at its hostname (DNS
   is unchanged; Caddy has the old certificates). Unpause the pipelines.
8. **Retire**, after the rollback window: `aws cloudformation deploy ...
   --parameter-overrides BlueSlot=false GreenAmi=$new_ami`, which
   terminates the old instance.

**Rolling back.** Before the switch: `GreenSlot=false` (with the AMIs
pinned) and, on the old instance, `docker update --restart=always scheduler
ui && docker start scheduler ui`. After it, the new instance has the newer
database: pause and freeze it (steps 1 and 2 on it), back it up (step 3,
without the pointer), `aws ec2 start-instances` the old one, and on it
`dropdb --force` and `createdb` morca (with `--maintenance-db=postgres`),
`pg_restore` the new dump, copy its `.tag` to `/var/lib/orca/tag` and run
`orca-refresh`; then `ActiveControl=blue`, then `GreenSlot=false`.

**A slot left on** by a replacement that has no state file: deploy its
`<Slot>Slot=false` with the other slot's AMI pinned, as in step 8.

## Not covered

- Postgres stays on the root volume, so the database moves by dump and
  restore; a separate data volume that moves from one instance to the other
  would make the copy instant (future work).
- A rollback after the switch carries the new instance's database and image
  tag back to the old one; an app started only on the new instance, or a
  setting changed only there, is not carried.
- The Let's Encrypt certificates come with Caddy's data; a hostname that's
  new with the replacement gets its certificate once the Elastic IP has
  moved, so it answers a minute or so later.
- Run instances started by the old scheduler: none, as the replacement
  starts only when no run is running.
- Apps not started with `morca aws app` (and not the console) are listed as
  not carried over; start them again with `morca aws app`.
