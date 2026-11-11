#!/bin/sh
# deploy/hetzner's deploys and rollbacks, against a fake server: ssh,
# hcloud and docker are tests/hetzner's fakes, and orca-stack runs here in
# its trial mode (ORCA_STACK_TEST=1) on directories of its own.
#
# - each apply that changes a tag keeps the tags before it (orca-stack tags
#   --previous, ORCA_PREV_TAG), including on a stack applied before it kept
#   them; applying the same tags again keeps the earlier ones;
# - deploy.sh backs up before it applies, says the previous and the new tag
#   and where the backup is, and stops before applying (unpausing what it
#   paused) when the backup fails, unless --no-backup;
# - deploy.sh --rollback (orca-stack rollback) applies the tags before
#   again, an app that followed the orca tag following it again, registers
#   the pipelines, and refuses with runs running unless --force; a second
#   rollback goes forward again;
# - a deploy registers the pipelines after the apply and before it unpauses,
#   and leaves them paused when that fails;
# - each backup writes <time>.paused beside the dump: the pipelines paused
#   by hand (manual1) and the ones the deploy paused (toy); list-backups
#   says which dumps have one, and a restore unpauses only the ones the
#   deploy paused (a dump without one leaves them all paused);
# - apply starts again only the containers whose spec (image, env file,
#   flags, CGROUPS) changed, and says why; it refuses over running runs
#   only when the scheduler would be replaced; --only and --all; plan
#   changes nothing; Postgres is started once and never again;
# - deploy.sh (and --rollback) first brings the server's settings in line
#   with hetzner.env: a key added there reaches settings.env, one emptied is
#   removed, a secret file is rewritten (mode 600), a stack setting set,
#   each named and never its value; the plan then replaces the containers
#   whose env file changed and quiesces for the scheduler; the tags stay;
#   a dry run writes nothing;
# - deploy.sh pauses, waits and registers only when the plan replaces the
#   scheduler: a console-only deploy backs up and replaces the console and
#   nothing else, a deploy of what runs does nothing; an app-only rollback
#   goes on over running runs and registers nothing;
# - the UI and the apps are replaced without downtime: the new one starts as
#   NAME-next, is probed until healthy (the UI's /healthz, its worker
#   listener if ORCA_WORKER_ADDR is set, an app's check path), only then
#   gets the alias NAME and is probed again, and only then is the old one
#   stopped, removed and the new one renamed; a new one that isn't healthy (or exits) is removed and the
#   old one kept, apply goes on with the others and exits non-zero, and an
#   apply again replaces only it; deploy.sh still registers a scheduler it
#   replaced when the UI's replacement fails, then unpauses what it paused
#   and fails naming the UI (a rollback too); a scheduler that fails to
#   start leaves the pipelines paused;
# - quiesce, apply's refusal and status count only the runs the scheduler
#   holds: runs on remote workers aren't waited for, and are listed as left
#   running on the workers (quiesce --workers, rescale.sh's, waits for them);
# - with OVERLAP=1 and can-overlap saying yes, a deploy that replaces the
#   scheduler pauses nothing: the new scheduler starts beside the old one,
#   the old one is set not to restart and renamed scheduler-old, the new one
#   leads and registers, then the old one is asked to drain, waited for
#   (stopped after --timeout) and removed; can-overlap says no with OVERLAP
#   off, or with a scheduler-old still draining, and the deploy that turns
#   OVERLAP on pauses and waits as before;
# - package.sh's release tarball unpacks to orca-hetzner-<version>/ with
#   every script, server/ and the example settings, and its up.sh and
#   deploy.sh run from there (hetzner.env next to them), printing the
#   version and sending the tarball's own server files;
# - a shared hetzner.env (a secrets repository's) names its files relative
#   to its folder, and a laptop's <NAME>.local.env (or HETZNER_LOCAL_ENV)
#   is read after it, its relative names from its own folder; the first
#   line names both; a missing HETZNER_LOCAL_ENV, or one that sets NAME,
#   is refused;
# - orca-stack check: ok on a healthy stack, and FAIL naming what's wrong (a
#   stopped container, an unhealthy UI, a scheduler-old left, a pipeline
#   whose binary isn't in the image, none registered, a canary run that
#   failed or didn't end in time); checked keeps the result, kind and tags;
#   deploy.sh ends with it (hetzner.env's CANARY as --canary) and fails
#   when it fails;
# - promote.sh deploys the tags the dev stack checked, with the flags
#   given, and refuses when its last check failed, was of other tags, or
#   when --from is the target's own settings.
set -eu
cd "$(dirname "$0")/.."
FAKE=$(cd "$(mktemp -d "${TMPDIR:-/tmp}/orca-hetzner.XXXXXX")" && pwd)
export FAKE
trap 'rm -rf "$FAKE"' EXIT INT TERM
export PATH="$PWD/tests/hetzner:$PATH"
export ORCA_STACK_TEST=1 ORCA_STACK_REGISTER_TRIES=1 ORCA_CONF="$FAKE/conf" ORCA_DATA="$FAKE/data" ORCA_STATE="$FAKE/state"
mkdir -p "$ORCA_CONF"
# The server's settings as up.sh writes them from hetzner.env (deploy.sh
# brings them in line with it; no tunnel: an empty tunnel.env).
cat > "$ORCA_CONF/settings.env" << 'EOS'
AWS_REGION=fsn1
AWS_ENDPOINT_URL_S3=https://example.invalid
ORCA_WAREHOUSE=s3://bkt/warehouse
ORCA_PUBLIC_WAREHOUSE=s3://bkt/warehouse
ORCA_LOGS=s3://bkt/logs
ORCA_RUN_ENV=ORCA_PUBLIC_WAREHOUSE
EOS
printf 'AWS_ACCESS_KEY_ID=x\nAWS_SECRET_ACCESS_KEY=y\n' > "$FAKE/s3.env"
cp "$FAKE/s3.env" "$ORCA_CONF/s3.env"
: > "$FAKE/tunnel.env"
cat > "$FAKE/hetzner.env" << EOS
NAME=orca
BUCKET=bkt
S3_ENDPOINT=https://example.invalid
S3_REGION=fsn1
S3_CREDENTIALS_FILE=$FAKE/s3.env
TUNNEL_TOKEN_FILE=$FAKE/tunnel.env
ORCA_IMAGE=img
EXTRA_ENV_FILE=$FAKE/extra.env
SSH_PRIVATE_KEY_FILE=
EOS
: > "$FAKE/extra.env"
export HETZNER_ENV="$FAKE/hetzner.env"
XDG_STATE_HOME="$FAKE/xdg"
# No laptop's own settings (~/.config/orca-hetzner/<NAME>.local.env) but the tests'.
XDG_CONFIG_HOME="$FAKE/config"
export XDG_STATE_HOME XDG_CONFIG_HOME

fail() {
    echo "FAIL: $*"
    [ ! -f "$FAKE/out" ] || tail -20 "$FAKE/out"
    exit 1
}
stack() { sh deploy/hetzner/server/orca-stack "$@"; }
deploy() { sh deploy/hetzner/deploy.sh "$@" > "$FAKE/out" 2>&1; }
has() { grep -q -- "$1" "$FAKE/out" || fail "no '$1' in the output"; }
# add_setting KEY=VALUE: a line of hetzner.env's EXTRA_ENV_FILE, and the
# server's settings.env as a deploy would write it (for orca-stack apply).
add_setting() {
    echo "$1" >> "$FAKE/extra.env"
    echo "$1" >> "$ORCA_CONF/settings.env"
}

# A stack that ran before tags were kept: its conf says what runs.
stack configure ORCA_IMAGE=img ORCA_TAG=aaa1111 BUCKET=bkt
stack app console 8080
stack apply > /dev/null
rm "$ORCA_CONF/deployed"
[ "$(stack tags)" = aaa1111 ] || fail "tags from the conf: $(stack tags)"
[ -z "$(stack tags --previous)" ] || fail "a previous deploy before any"
stack rollback > "$FAKE/out" 2>&1 && fail "a rollback with nothing before"
has "nothing to roll back to"

# A deploy: backup first, then apply; the tags before are kept.
: > "$FAKE/docker.log"
echo manual1 > "$FAKE/paused"
deploy bbb2222 || fail "deploy.sh bbb2222"
has "previous tag aaa1111, new tag bbb2222; backup s3://bkt/backups/pg/2"
grep -q 'pg_dump' "$FAKE/docker.log" || fail "no pg_dump"
[ "$(grep -n 'pg_dump\|run -d --name scheduler' "$FAKE/docker.log" | head -1 | grep -c pg_dump)" = 1 ] \
    || fail "the backup wasn't before the apply"
[ "$(stack tags)" = bbb2222 ] || fail "tags after the deploy: $(stack tags)"
[ "$(stack tags --previous)" = aaa1111 ] || fail "previous: $(stack tags --previous)"
grep -q '^ORCA_PREV_TAG=aaa1111$' "$ORCA_CONF/stack.conf" || fail "no ORCA_PREV_TAG"
grep -q 'unpause toy' "$FAKE/docker.log" || fail "the pipelines weren't unpaused"
grep -q 'unpause manual1' "$FAKE/docker.log" && fail "unpaused a pipeline paused by hand"
order=$(grep -o 'run -d --name scheduler\|exec scheduler orca register\|orca unpause toy' "$FAKE/docker.log" | tr '\n' ,)
[ "$order" = "run -d --name scheduler,exec scheduler orca register,orca unpause toy," ] \
    || fail "not apply, register, unpause: $order"
has "registering the pipelines from bbb2222"
# Its backup's paused state: manual1 by hand, toy by the deploy.
side=$(ls "$FAKE/bucket" | grep '\.paused$' | tail -n 1)
[ -n "$side" ] || fail "no .paused beside the dump: $(ls "$FAKE/bucket")"
[ -f "$FAKE/bucket/${side%.paused}.dump" ] || fail "$side has no dump"
grep -qx 'paused_by_hand manual1' "$FAKE/bucket/$side" || fail "by hand: $(cat "$FAKE/bucket/$side")"
grep -qx 'paused_by_deploy toy' "$FAKE/bucket/$side" || fail "by the deploy: $(cat "$FAKE/bucket/$side")"
has "paused then (in ${side}): by hand: manual1; by a deploy: toy"
dump=${side%.paused}.dump
stack list-backups > "$FAKE/out"
has "$dump   paused state recorded"

# Restoring that dump (whose toy is paused, as quiesce left it) unpauses toy
# only.
printf 'manual1\ntoy\n' > "$FAKE/paused"
stack restore --force "backups/pg/$dump" > "$FAKE/out" 2>&1 || fail "restore"
has "unpaused toy (a deploy had paused it)"
has "paused by hand or the watchdog, so still paused: manual1"
[ "$(cat "$FAKE/paused")" = manual1 ] || fail "paused after the restore: $(cat "$FAKE/paused")"
# Without its .paused, all stay paused.
rm "$FAKE/bucket/$side"
printf 'manual1\ntoy\n' > "$FAKE/paused"
stack restore --force latest > "$FAKE/out" 2>&1 || fail "restore latest"
has "has no ${side}: every pipeline paused in it stays paused"
[ "$(sort "$FAKE/paused" | tr '\n' ' ')" = "manual1 toy " ] || fail "unpaused without a .paused"
stack list-backups > "$FAKE/out"
has "$dump   no paused state"
echo manual1 > "$FAKE/paused"

# A deploy whose registration fails stops with the pipelines paused.
touch "$FAKE/register-fails"
deploy bbb2222 && fail "a deploy whose register failed"
has "bbb2222 is deployed, but registering failed; the pipelines stay paused"
grep -qx toy "$FAKE/paused" || fail "toy unpaused after a failed register"
rm "$FAKE/register-fails"
stack register > /dev/null || fail "orca-stack register"
stack resume > /dev/null || fail "orca-stack resume"
[ "$(cat "$FAKE/paused")" = manual1 ] || fail "paused after resume: $(cat "$FAKE/paused")"

# The same tags again keep the ones before.
stack apply --tag bbb2222 > /dev/null
[ "$(stack tags --previous)" = aaa1111 ] || fail "a re-apply moved previous: $(stack tags --previous)"

# A failed backup stops the deploy and unpauses; --no-backup goes on.
: > "$FAKE/docker.log"
touch "$FAKE/backup-fails"
deploy ccc3333 && fail "deployed after a failed backup"
has "the backup failed, so nothing was deployed"
grep -q 'run -d --name scheduler' "$FAKE/docker.log" && fail "applied after a failed backup"
grep -q 'unpause toy' "$FAKE/docker.log" || fail "left paused after a failed backup"
[ "$(stack tags)" = bbb2222 ] || fail "tags after a failed backup: $(stack tags)"
deploy --no-backup ccc3333 console=ddd4444 || fail "deploy.sh --no-backup"
has "no-backup: not backing up; previous tag bbb2222, new tag orca ccc3333, console ddd4444"
rm "$FAKE/backup-fails"
[ "$(stack tags)" = "orca ccc3333, console ddd4444" ] || fail "tags: $(stack tags)"

# Rolling back: refused with runs running, then the tags before, registered.
echo 2 > "$FAKE/running"
stack rollback > "$FAKE/out" 2>&1 && fail "rolled back over running runs"
has "2 run(s) running"
[ "$(stack tags)" = "orca ccc3333, console ddd4444" ] || fail "a refused rollback changed the tags"
echo 0 > "$FAKE/running"
: > "$FAKE/docker.log"
deploy --rollback || fail "deploy.sh --rollback"
has "previous tag orca ccc3333, console ddd4444, new tag bbb2222; backup s3://bkt/backups/pg/"
has "orca-stack: registered toy"
[ "$(stack tags)" = bbb2222 ] || fail "tags after the rollback: $(stack tags)"
grep -q '^console 8080 - /$' "$ORCA_CONF/apps" || fail "the console doesn't follow the orca tag: $(cat "$ORCA_CONF/apps")"
grep -q 'exec scheduler orca register' "$FAKE/docker.log" || fail "not registered"
[ "$(grep -n 'exec scheduler orca register\|unpause toy' "$FAKE/docker.log" | head -1 | grep -c register)" = 1 ] \
    || fail "unpaused before registering"

# Again goes forward; --force over running runs.
echo 1 > "$FAKE/running"
stack rollback --force > "$FAKE/out" 2>&1 || fail "orca-stack rollback --force"
[ "$(stack tags)" = "orca ccc3333, console ddd4444" ] || fail "tags after rolling forward: $(stack tags)"
echo 0 > "$FAKE/running"

# A failed registration fails the rollback, which says so.
touch "$FAKE/register-fails"
deploy --rollback && fail "a rollback whose register failed"
has "the rollback failed; the pipelines stay paused"
rm "$FAKE/register-fails"

# A dry run runs nothing on the server.
: > "$FAKE/docker.log"
sh deploy/hetzner/deploy.sh --dry-run --rollback > "$FAKE/out" 2>&1 || fail "a dry run"
has "+ ssh root@<server-ip> orca-stack rollback"
has "+ ssh root@<server-ip> orca-stack sync --keys settings.env"
[ ! -s "$FAKE/docker.log" ] || fail "a dry run ran docker"

# Only what changed starts again: each container has its spec as a label,
# and an apply that changes nothing replaces nothing.
stack resume > /dev/null
stack apply --tag eee5555 --app console=- > /dev/null || fail "apply eee5555"
grep -q '^orca.stack.spec=' "$FAKE/containers/scheduler/labels" || fail "no spec label"
: > "$FAKE/docker.log"
stack apply > "$FAKE/out" 2>&1 || fail "an apply of what runs"
for c in scheduler ui sampler console; do has "$c: unchanged"; done
grep -q 'run -d\|rm -f\|stop ' "$FAKE/docker.log" && fail "an unchanged apply started or stopped a container"

# Runs running: an apply that keeps the scheduler goes on; one that would
# replace it is refused and changes nothing.
echo 2 > "$FAKE/running"
: > "$FAKE/docker.log"
stack apply --app console=fff6666 > "$FAKE/out" 2>&1 || fail "a console-only apply with runs running"
has "console: replaced (image eee5555 -> fff6666)"
has "scheduler: unchanged"
grep -q 'run -d --name \(scheduler\|ui\|sampler\)' "$FAKE/docker.log" && fail "a console-only apply replaced more"
stack apply --tag ggg7777 > "$FAKE/out" 2>&1 && fail "replaced the scheduler over running runs"
has "2 run(s) running; restarting the scheduler (image eee5555 -> ggg7777) kills them"
[ "$(stack tags)" = "orca eee5555, console fff6666" ] || fail "a refused apply changed the tags: $(stack tags)"
grep -q '^ORCA_TAG=eee5555$' "$ORCA_CONF/stack.conf" || fail "a refused apply changed ORCA_TAG"
echo 0 > "$FAKE/running"

# plan says what apply would do, and changes nothing.
: > "$FAKE/docker.log"
stack plan --tag ggg7777 > "$FAKE/out" 2>&1 || fail "orca-stack plan"
has "^scheduler replace image eee5555 -> ggg7777$"
has "^console keep unchanged$"
grep -q 'run -d\|rm -f' "$FAKE/docker.log" && fail "plan started or removed a container"
[ "$(stack tags)" = "orca eee5555, console fff6666" ] || fail "plan changed the tags"

# A census-only deploy: no pause, a backup, the console replaced and
# nothing else, nothing registered.
: > "$FAKE/docker.log"
deploy eee5555 console=hhh8888 || fail "a console-only deploy"
has "  console replace image fff6666 -> hhh8888"
has "the scheduler keeps running: not pausing anything"
grep -q 'orca pause' "$FAKE/docker.log" && fail "a console-only deploy paused the pipelines"
grep -q pg_dump "$FAKE/docker.log" || fail "a console-only deploy didn't back up"
[ "$(grep -n 'pg_dump\|run -d --name console' "$FAKE/docker.log" | head -1 | grep -c pg_dump)" = 1 ] \
    || fail "the backup wasn't before the console's start"
grep -q 'run -d --name console' "$FAKE/docker.log" || fail "the console wasn't replaced"
grep -q 'run -d --name \(scheduler\|ui\|sampler\)' "$FAKE/docker.log" && fail "a console-only deploy replaced more"
grep -q 'orca register' "$FAKE/docker.log" && fail "a console-only deploy registered"
has "the scheduler wasn't replaced: the pipelines stay registered as they are"
[ "$(stack tags)" = "orca eee5555, console hhh8888" ] || fail "tags after a console deploy: $(stack tags)"

# A deploy of what runs: nothing replaced, no backup, no pause.
: > "$FAKE/docker.log"
deploy eee5555 console=hhh8888 || fail "a deploy of what runs"
has "nothing to replace: not backing up"
grep -q 'pg_dump\|run -d\|orca pause' "$FAKE/docker.log" && fail "a deploy of what runs did something"

# deploy.sh brings the server's settings in line with hetzner.env before
# it plans: a key added there (POSTHOG_KEY) reaches settings.env, by name
# only in the output, the containers whose env file it changes are
# replaced, and the scheduler's replacement quiesces.
mode() { ls -l "$1" | cut -c1-10; }
echo POSTHOG_KEY=phc_notavalue42 >> "$FAKE/hetzner.env"
: > "$FAKE/docker.log"
deploy eee5555 console=hhh8888 || fail "a deploy that adds POSTHOG_KEY"
has "  orca-stack: settings.env: added ORCA_POSTHOG_KEY$"
grep -q phc_notavalue42 "$FAKE/out" && fail "a setting's value was printed"
grep -qx 'ORCA_POSTHOG_KEY=phc_notavalue42' "$ORCA_CONF/settings.env" || fail "no ORCA_POSTHOG_KEY in settings.env"
[ "$(mode "$ORCA_CONF/settings.env")" = -rw------- ] || fail "settings.env is $(mode "$ORCA_CONF/settings.env")"
grep -qx 'POSTHOG_KEY=phc_notavalue42' "$ORCA_CONF/app.env" || fail "no POSTHOG_KEY in app.env"
has "  scheduler replace settings: its env file changed"
has "  ui replace settings: its env file changed"
has "  console replace settings: its env file changed"
has "  sampler keep unchanged"
has "the scheduler is replaced: pausing the pipelines"
grep -q 'exec scheduler orca pause' "$FAKE/docker.log" || fail "a settings change to the scheduler didn't quiesce"
grep -q 'exec scheduler orca register' "$FAKE/docker.log" || fail "not registered after a settings change"
grep -q 'unpause toy' "$FAKE/docker.log" || fail "not unpaused after a settings change"
[ "$(stack tags)" = "orca eee5555, console hhh8888" ] || fail "a settings change moved the tags: $(stack tags)"
# Nothing differs: nothing changes, nothing is replaced.
deploy eee5555 console=hhh8888 || fail "a deploy with the settings in line"
has "names only, never values):"
has "^  unchanged$"
has "nothing to replace: not backing up"
# A dry run shows the sync and the keys, and writes nothing.
sed -i.bak '/^POSTHOG_KEY=/d' "$FAKE/hetzner.env"
printf 'POSTHOG_KEY=\nFIXED_COST=8/month\nBACKUP_KEEP=7\nS3_REGION=nbg1\n' >> "$FAKE/hetzner.env"
printf 'AWS_ACCESS_KEY_ID=x2\nAWS_SECRET_ACCESS_KEY=notasecret99\n' > "$FAKE/s3.env"
before=$(cat "$ORCA_CONF/settings.env" "$ORCA_CONF/s3.env" "$ORCA_CONF/stack.conf" | cksum)
: > "$FAKE/docker.log"
sh deploy/hetzner/deploy.sh --dry-run eee5555 > "$FAKE/out" 2>&1 || fail "a dry run with settings changed"
has "+ ssh root@<server-ip> orca-stack sync --keys settings.env < "
has "+ ssh root@<server-ip> orca-stack sync s3.env < $FAKE/s3.env"
has "+ ssh root@<server-ip> orca-stack configure ORCA_IMAGE=img .*BACKUP_KEEP=7 GHCR_USER=$"
has "# settings.env from $FAKE/hetzner.env: AWS_REGION .*ORCA_FIXED_COST"
grep -q 'ORCA_POSTHOG_KEY\|notasecret99\|nbg1 ' "$FAKE/out" && fail "the dry run: $(grep 'POSTHOG\|notasecret99' "$FAKE/out")"
[ "$(cat "$ORCA_CONF/settings.env" "$ORCA_CONF/s3.env" "$ORCA_CONF/stack.conf" | cksum)" = "$before" ] \
    || fail "a dry run changed the server's settings"
[ ! -s "$FAKE/docker.log" ] || fail "a dry run ran docker"
# A key emptied in hetzner.env is removed; a changed one and a stack
# setting are named; a secret file only said to have changed.
deploy eee5555 console=hhh8888 || fail "a deploy that removes POSTHOG_KEY"
has "  orca-stack: settings.env: added ORCA_FIXED_COST; removed ORCA_POSTHOG_KEY; changed AWS_REGION$"
has "  orca-stack: s3.env: changed$"
has "  orca-stack: BACKUP_KEEP 14 -> 7$"
grep -q 'notasecret99\|8/month\|phc_' "$FAKE/out" && fail "a value was printed"
grep -q ORCA_POSTHOG_KEY "$ORCA_CONF/settings.env" && fail "ORCA_POSTHOG_KEY wasn't removed"
grep -q POSTHOG_KEY "$ORCA_CONF/app.env" && fail "POSTHOG_KEY is still in app.env"
grep -qx 'AWS_REGION=nbg1' "$ORCA_CONF/settings.env" || fail "AWS_REGION wasn't changed"
grep -qx 'AWS_SECRET_ACCESS_KEY=notasecret99' "$ORCA_CONF/s3.env" || fail "s3.env wasn't written"
[ "$(mode "$ORCA_CONF/s3.env")" = -rw------- ] || fail "s3.env is $(mode "$ORCA_CONF/s3.env")"
grep -qx 'BACKUP_KEEP=7' "$ORCA_CONF/stack.conf" || fail "BACKUP_KEEP wasn't configured"
grep -qx 'ORCA_TAG=eee5555' "$ORCA_CONF/stack.conf" || fail "the sync touched ORCA_TAG"
has "  scheduler replace settings: its env file changed"
has "  console replace settings: its env file changed"
stack resume > /dev/null
# A SOPS-encrypted secret file (tests/hetzner/sops: "decrypts" by dropping
# ENC: from each line): sent decrypted, said to need sops in a dry run.
printf 'ENC:AWS_ACCESS_KEY_ID=x3\nENC:AWS_SECRET_ACCESS_KEY=sopssecret77\n' > "$FAKE/s3.sops.env"
sed -i.bak "s|^S3_CREDENTIALS_FILE=.*|S3_CREDENTIALS_FILE=$FAKE/s3.sops.env|" "$FAKE/hetzner.env"
sh deploy/hetzner/deploy.sh --dry-run eee5555 > "$FAKE/out" 2>&1 || fail "a dry run with a SOPS file"
has "+ sops -d $FAKE/s3.sops.env | ssh root@<server-ip> orca-stack sync s3.env$"
deploy eee5555 console=hhh8888 || fail "a deploy with a SOPS file"
has "  orca-stack: s3.env: changed$"
grep -q 'sopssecret77' "$FAKE/out" && fail "a SOPS value was printed"
grep -qx 'AWS_SECRET_ACCESS_KEY=sopssecret77' "$ORCA_CONF/s3.env" || fail "s3.env wasn't decrypted: $(sed 's/=.*//' "$ORCA_CONF/s3.env")"
printf 'garbage\n' > "$FAKE/s3.sops.env"
deploy eee5555 console=hhh8888 && fail "a deploy with a SOPS file that doesn't decrypt"
has "sops couldn't decrypt $FAKE/s3.sops.env"
grep -qx 'AWS_SECRET_ACCESS_KEY=sopssecret77' "$ORCA_CONF/s3.env" || fail "a failed decrypt changed s3.env"
sed -i.bak "s|^S3_CREDENTIALS_FILE=.*|S3_CREDENTIALS_FILE=$FAKE/s3.env|" "$FAKE/hetzner.env"
deploy eee5555 console=hhh8888 || fail "a deploy back on the plain s3.env"
stack resume > /dev/null

# SECRETS_FILE: every secret in one SOPS file (here JSON lines behind ENC:),
# decrypted once per deploy (tests/hetzner/sops counts its calls), each value
# sent as a *.sops file is: one trailing newline. No value is printed.
secrets_file() { # JSON: SECRETS_FILE's content
    printf '%s\n' "$1" | sed 's/^/ENC:/' > "$FAKE/secrets.sops.yaml"
}
all_secrets='{"s3.env": "AWS_ACCESS_KEY_ID=x4\nAWS_SECRET_ACCESS_KEY=onefile55\n",
 "tunnel.env": "TUNNEL_TOKEN=tunnel66",
 "secrets/orca/github-token": "ghp_gh77\n\n",
 "secrets/orca/ntfy-topic": "topic88",
 "secrets/orca/ntfy-token": "tk_ntfy99",
 "ghcr-token": "ghcr11",
 "secrets/orca/hcloud-token": "hc_pool12"}'
secrets_file "$all_secrets"
sed -i.bak '/^S3_CREDENTIALS_FILE=/d; /^TUNNEL_TOKEN_FILE=/d' "$FAKE/hetzner.env"
echo "SECRETS_FILE=$FAKE/secrets.sops.yaml" >> "$FAKE/hetzner.env"
secret_files() { cat "$ORCA_CONF/settings.env" "$ORCA_CONF/s3.env" "$ORCA_CONF/tunnel.env" 2> /dev/null | cksum; }
values='onefile55\|tunnel66\|ghp_gh77\|topic88\|tk_ntfy99\|ghcr11\|hc_pool12'
: > "$FAKE/sops.calls"
before=$(secret_files)
sh deploy/hetzner/deploy.sh --dry-run eee5555 > "$FAKE/out" 2>&1 || fail "a dry run with SECRETS_FILE"
has "# a dry run doesn't decrypt $FAKE/secrets.sops.yaml: it assumes it has s3.env tunnel.env secrets/orca/github-token secrets/orca/ntfy-topic secrets/orca/ntfy-token ghcr-token secrets/orca/hcloud-token$"
has "^  + sops -d $FAKE/secrets.sops.yaml (once)$"
for n in s3.env tunnel.env secrets/orca/github-token secrets/orca/ntfy-topic secrets/orca/ntfy-token ghcr-token secrets/orca/hcloud-token; do
    has "^  | ssh root@<server-ip> orca-stack sync $n$"
done
has "# settings.env from .*ORCA_NOTIFY_SECRET ORCA_NOTIFY_TOKEN_SECRET"
[ ! -s "$FAKE/sops.calls" ] || fail "a dry run ran sops: $(cat "$FAKE/sops.calls")"
[ "$(secret_files)" = "$before" ] || fail "a dry run with SECRETS_FILE changed the server's files"
deploy eee5555 console=hhh8888 || fail "a deploy with SECRETS_FILE"
[ "$(cat "$FAKE/sops.calls")" = "-d --output-type json $FAKE/secrets.sops.yaml" ] \
    || fail "not one decrypt per deploy: $(cat "$FAKE/sops.calls")"
grep -q "$values" "$FAKE/out" && fail "a SECRETS_FILE value was printed"
has "  orca-stack: s3.env: changed$"
has "  orca-stack: secrets/orca/ntfy-topic: new$"
has "  orca-stack: ghcr-token: new$"
has "  orca-stack: secrets/orca/hcloud-token: new$"
# Worker pools (orca #267): the Hetzner token's id, the stack's firewall and
# key, and the registry login's id, in settings.env; the worker image in
# orca.env; ghcr-token among the secrets for orca.
grep -qx "ORCA_HCLOUD_TOKEN_SECRET=orca/hcloud-token" "$ORCA_CONF/settings.env" || fail "no ORCA_HCLOUD_TOKEN_SECRET"
grep -q "^ORCA_HCLOUD_FIREWALL=." "$ORCA_CONF/settings.env" || fail "no ORCA_HCLOUD_FIREWALL"
grep -q "^ORCA_HCLOUD_SSH_KEYS=." "$ORCA_CONF/settings.env" || fail "no ORCA_HCLOUD_SSH_KEYS"
same() { # NAME CONTENT
    printf '%s\n' "$2" | cmp -s - "$ORCA_CONF/$1" || fail "/etc/orca/$1 isn't SECRETS_FILE's value"
    [ "$(mode "$ORCA_CONF/$1")" = -rw------- ] || fail "$1 is $(mode "$ORCA_CONF/$1")"
}
same s3.env "$(printf 'AWS_ACCESS_KEY_ID=x4\nAWS_SECRET_ACCESS_KEY=onefile55')"
same tunnel.env TUNNEL_TOKEN=tunnel66
same secrets/orca/github-token ghp_gh77
same secrets/orca/ntfy-topic topic88
same secrets/orca/ntfy-token tk_ntfy99
same ghcr-token ghcr11
grep -qx 'ORCA_NOTIFY_SECRET=orca/ntfy-topic' "$ORCA_CONF/settings.env" || fail "no ORCA_NOTIFY_SECRET from SECRETS_FILE"
grep -qx 'ORCA_NOTIFY_TOKEN_SECRET=orca/ntfy-token' "$ORCA_CONF/settings.env" || fail "no ORCA_NOTIFY_TOKEN_SECRET from SECRETS_FILE"
stack resume > /dev/null
# Again: unchanged, still one decrypt.
: > "$FAKE/sops.calls"
deploy eee5555 console=hhh8888 || fail "a deploy with SECRETS_FILE in line"
has "^  unchanged$"
[ "$(wc -l < "$FAKE/sops.calls")" -eq 1 ] || fail "$(wc -l < "$FAKE/sops.calls") decrypts in a deploy"
# What's refused, before anything is changed on the server.
refused() { # WHY MESSAGE
    before=$(secret_files)
    deploy eee5555 console=hhh8888 && fail "a deploy with $1"
    has "$2"
    grep -q "$values" "$FAKE/out" && fail "a value was printed ($1)"
    grep -q 'orca-stack' "$FAKE/out" && fail "the server was changed ($1)"
    [ "$(secret_files)" = "$before" ] || fail "$1 changed the server's files"
}
secrets_file '{"s3.env": "AWS_SECRET_ACCESS_KEY=onefile55", "../s3.env": "x", "notes.txt": "x", "/etc/passwd": "x", "ghcr-token": 11}'
refused "bad keys" '"\.\./s3\.env": a name has no leading / and no \.\.'
has '"notes.txt" is not one of: s3.env tunnel.env'
has '"/etc/passwd": a name has no leading / and no \.\.'
has '"ghcr-token": the value is a number, not a string'
secrets_file "$all_secrets"
echo "S3_CREDENTIALS_FILE=$FAKE/s3.env" >> "$FAKE/hetzner.env"
refused "SECRETS_FILE and S3_CREDENTIALS_FILE" "S3_CREDENTIALS_FILE and SECRETS_FILE both give s3.env: set only one"
sed -i.bak '/^S3_CREDENTIALS_FILE=/d' "$FAKE/hetzner.env"
secrets_file '{"s3.env": "a=b", "tunnel.env": ""}'
echo GHCR_USER=me >> "$FAKE/hetzner.env"
refused "GHCR_USER and no ghcr-token" "GHCR_USER needs GHCR_TOKEN_FILE or a ghcr-token key in SECRETS_FILE"
sed -i.bak '/^GHCR_USER=/d' "$FAKE/hetzner.env"
printf 'garbage\n' > "$FAKE/secrets.sops.yaml"
refused "a SECRETS_FILE that doesn't decrypt" "sops couldn't decrypt $FAKE/secrets.sops.yaml; nothing was changed on the server"
# Back on the plain files.
sed -i.bak '/^SECRETS_FILE=/d' "$FAKE/hetzner.env"
printf 'S3_CREDENTIALS_FILE=%s\nTUNNEL_TOKEN_FILE=%s\n' "$FAKE/s3.env" "$FAKE/tunnel.env" >> "$FAKE/hetzner.env"
rm -f "$ORCA_CONF/secrets/orca/ntfy-topic" "$ORCA_CONF/secrets/orca/ntfy-token"
deploy eee5555 console=hhh8888 || fail "a deploy back on the plain files"
grep -q ORCA_NOTIFY "$ORCA_CONF/settings.env" && fail "ORCA_NOTIFY_SECRET without SECRETS_FILE"
stack resume > /dev/null

# A setting only orca.env has: the scheduler and the UI; CGROUPS: the
# scheduler only; a tag pushed again: what runs it.
add_setting ORCA_TEST_SETTING=1
stack apply > "$FAKE/out" 2>&1 || fail "apply after a setting"
has "scheduler: replaced (settings: its env file changed)"
has "ui: replaced (settings: its env file changed)"
has "console: unchanged"
has "sampler: unchanged"
stack configure CGROUPS=1
stack apply > "$FAKE/out" 2>&1 || fail "apply with CGROUPS=1"
has "scheduler: replaced (options: its flags, mounts or CGROUPS changed)"
has "ui: unchanged"
echo sha256:0123456789abcdef0123 > "$FAKE/images/img_eee5555"
stack apply > "$FAKE/out" 2>&1 || fail "apply after a push"
has "scheduler: replaced (image eee5555 pushed again ("
has "sampler: replaced (image eee5555 pushed again ("
has "console: unchanged"

# --all and --only.
stack apply --all > "$FAKE/out" 2>&1 || fail "apply --all"
for c in scheduler ui sampler console; do has "$c: replaced (--all)"; done
: > "$FAKE/docker.log"
stack apply --only console --app console=iii9999 > "$FAKE/out" 2>&1 || fail "apply --only console"
has "console: replaced (image hhh8888 -> iii9999)"
has "scheduler: left as it is (--only console)"
grep -q 'run -d --name \(scheduler\|ui\|sampler\)' "$FAKE/docker.log" && fail "--only console replaced more"
stack apply --only ui --tag jjj0000 > "$FAKE/out" 2>&1 && fail "--only with a new --tag"
has "would leave the other containers on the old one"
stack apply --only ui --app console=jjj0000 > "$FAKE/out" 2>&1 && fail "--only ui with --app console"
[ "$(stack tags)" = "orca eee5555, console iii9999" ] || fail "tags after --only: $(stack tags)"

# Rolling back an app only goes on over running runs and registers nothing.
echo 1 > "$FAKE/running"
: > "$FAKE/docker.log"
deploy --rollback || fail "an app-only deploy.sh --rollback"
has "  console replace image iii9999 -> hhh8888"
has "the scheduler keeps running: not pausing anything"
has "console: replaced (image iii9999 -> hhh8888)"
has "the scheduler kept running, so the pipelines stay registered as they are"
grep -q 'orca register\|orca pause\|run -d --name scheduler' "$FAKE/docker.log" && fail "an app-only rollback touched the scheduler"
[ "$(stack tags)" = "orca eee5555, console hhh8888" ] || fail "tags after an app-only rollback: $(stack tags)"
echo 0 > "$FAKE/running"

# Without downtime: the UI's and the apps' new container starts beside the
# old one, as NAME-next; once it's healthy it gets the alias NAME (a
# reconnect) and is checked again, and only then is the old one stopped.
: > "$FAKE/docker.log"
deploy kkk1111 || fail "deploy.sh kkk1111"
has "ui: replaced (image eee5555 -> kkk1111), without downtime"
has "scheduler: replaced (image eee5555 -> kkk1111)"
has "console: unchanged"
order=$(grep -oE 'run -d --name ui-next|probe ui-next|network disconnect orca ui-next|network connect --alias ui orca ui-next|stop -t 10 ui$|rm -f ui$|rename ui-next ui$' "$FAKE/docker.log" | tr '\n' ,)
[ "$order" = "run -d --name ui-next,probe ui-next,network disconnect orca ui-next,network connect --alias ui orca ui-next,probe ui-next,stop -t 10 ui,rm -f ui,rename ui-next ui," ] \
    || fail "the UI's replacement order: $order"
grep -q 'run -d --name ui-next.*--network-alias' "$FAKE/docker.log" && fail "ui-next had the alias ui before it was healthy"
grep -q '^probe ui-next 8270 /healthz$' "$FAKE/docker.log" || fail "the UI's probe isn't /healthz on 8270"
grep -q 'stop -t 10 scheduler\|run -d --name scheduler-next' "$FAKE/docker.log" && fail "the scheduler was replaced side by side"
[ ! -d "$FAKE/containers/ui-next" ] || fail "ui-next is left"
[ "$(cat "$FAKE/containers/ui/image")" = img:kkk1111 ] || fail "ui runs $(cat "$FAKE/containers/ui/image")"
grep -qw ui "$FAKE/containers/ui/aliases" || fail "the new ui has no alias ui"
# An app, by its check path.
stack app console 8080 /ready
stack app console 8080
grep -q '^console 8080 [^ ]* /ready$' "$ORCA_CONF/apps" || fail "app without a CHECK reset it: $(cat "$ORCA_CONF/apps")"
: > "$FAKE/docker.log"
stack apply --app console=lll2222 > "$FAKE/out" 2>&1 || fail "the console's apply"
has "console: replaced (image hhh8888 -> lll2222), without downtime"
grep -q '^probe console-next 8080 /ready$' "$FAKE/docker.log" || fail "the console's probe isn't /ready on 8080"
[ "$(grep -n 'exec console-next bash\|stop -t 10 console$' "$FAKE/docker.log" | head -1 | grep -c exec)" = 1 ] \
    || fail "the old console was stopped before the new one was healthy"

# A new console that answers 503 (warming up, for good): removed, the old
# one keeps serving, the others are applied, and apply fails saying why.
mkdir -p "$FAKE/health" "$FAKE/exits"
echo 503 > "$FAKE/health/console"
add_setting ORCA_TEST_SETTING=2
: > "$FAKE/docker.log"
stack apply --app console=mmm3333 --health-timeout 0 > "$FAKE/out" 2>&1 && fail "an apply whose console wasn't healthy"
has "console: not replaced: the new container answered 503 on :8080/ready (wants 200) after 0s; the old one keeps serving"
has "fake log of console-next"
has "not replaced: console, whose new container wasn't healthy"
has "ui: replaced (settings: its env file changed), without downtime"
[ ! -d "$FAKE/containers/console-next" ] || fail "console-next is left"
[ "$(cat "$FAKE/containers/console/image")" = img:lll2222 ] || fail "the old console was replaced: $(cat "$FAKE/containers/console/image")"
[ "$(cat "$FAKE/containers/console/status")" = running ] || fail "the old console was stopped"
grep -q 'stop -t 10 console$' "$FAKE/docker.log" && fail "the old console was stopped"
# Healthy again: an apply again replaces only the console.
rm "$FAKE/health/console"
stack apply > "$FAKE/out" 2>&1 || fail "an apply again"
has "console: replaced (image lll2222 -> mmm3333), without downtime"
for c in scheduler ui sampler; do has "$c: unchanged"; done
# One that exits at once fails without waiting out the timeout.
touch "$FAKE/exits/console"
stack apply --app console=nnn4444 > "$FAKE/out" 2>&1 && fail "an apply whose console exited"
has "console: not replaced: the new container stopped (exited, exit code 1)"
[ "$(cat "$FAKE/containers/console/image")" = img:mmm3333 ] || fail "the console was replaced by one that exited"
rm "$FAKE/exits/console"
stack apply --app console=mmm3333 > /dev/null 2>&1 || fail "back to mmm3333"

# The worker API's listener (ORCA_WORKER_ADDR) moves with the UI and is
# probed too.
add_setting ORCA_WORKER_ADDR=0.0.0.0:8271
: > "$FAKE/docker.log"
stack apply > "$FAKE/out" 2>&1 || fail "apply with ORCA_WORKER_ADDR"
grep -q '^probe ui-next 8271 /$' "$FAKE/docker.log" || fail "the worker listener wasn't probed"
has "ui: replaced (settings: its env file changed), without downtime"

# deploy.sh: a UI that isn't healthy after the scheduler was replaced: the
# pipelines are registered from the new image, then unpaused (the
# scheduler is deployed), the status shown, and it fails naming the UI and
# how to retry.
echo 503 > "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=0
: > "$FAKE/docker.log"
deploy ooo5555 && fail "a deploy whose UI wasn't healthy"
rm "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=300
has "registering the pipelines from ooo5555"
grep -q 'exec scheduler orca register' "$FAKE/docker.log" || fail "a replaced scheduler wasn't registered"
has "unpausing the pipelines it paused"
grep -qx toy "$FAKE/paused" && fail "toy left paused after only the UI failed"
grep -qx manual1 "$FAKE/paused" || fail "unpaused manual1, paused by hand"
order=$(grep -o 'exec scheduler orca register\|orca unpause toy' "$FAKE/docker.log" | tr '\n' ,)
[ "$order" = "exec scheduler orca register,orca unpause toy," ] || fail "not register, then unpause: $order"
has "running runs: "
has "not replaced: ui, whose new container wasn't healthy (above): the old one keeps serving"
has "ssh root@192.0.2.1 orca-stack apply"
grep -q 'stay paused' "$FAKE/out" && fail "said the pipelines stay paused"
[ "$(cat "$FAKE/containers/ui/image")" = img:kkk1111 ] || fail "the old UI wasn't kept: $(cat "$FAKE/containers/ui/image")"
[ "$(cat "$FAKE/containers/scheduler/image")" = img:ooo5555 ] || fail "the scheduler wasn't replaced"
stack apply > "$FAKE/out" 2>&1 || fail "the UI's retry"
has "ui: replaced (image kkk1111 -> ooo5555), without downtime"
has "scheduler: unchanged"
[ ! -f "$ORCA_STATE/paused-by-quiesce" ] || fail "quiesce's list is left"

# The same through a rollback: registered, unpaused, failing on the UI.
echo 503 > "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=0
deploy --rollback && fail "a rollback whose UI wasn't healthy"
rm "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=300
has "orca-stack: registered toy"
has "not replaced: ui, whose new container wasn't healthy"
grep -qx toy "$FAKE/paused" && fail "toy left paused after only the UI failed a rollback"
stack apply > /dev/null 2>&1 || fail "the UI's retry after the rollback"
deploy ooo5555 console=mmm3333 > /dev/null 2>&1 || fail "back to ooo5555"

# A scheduler that fails to start stops the deploy with the pipelines
# paused, as a failed registration does; the UI isn't let off either.
mkdir -p "$FAKE/run-fails"
touch "$FAKE/run-fails/scheduler"
: > "$FAKE/docker.log"
deploy ooo6666 && fail "a deploy whose scheduler didn't start"
rm "$FAKE/run-fails/scheduler"
has "the deploy failed; the pipelines stay paused"
grep -q 'orca register' "$FAKE/docker.log" && fail "registered a scheduler that didn't start"
grep -q 'unpause' "$FAKE/docker.log" && fail "unpaused after the scheduler failed"
grep -qx toy "$FAKE/paused" || fail "toy unpaused after the scheduler failed"
stack apply > /dev/null 2>&1 || fail "the scheduler's retry"
stack resume > /dev/null

# The quiesce waits only for the scheduler's own runs: runs on remote
# workers go on through its restart, and are listed as left running there.
# (--timeout 0: a wait for them would fail the deploy.)
echo 2 > "$FAKE/worker-running"
: > "$FAKE/docker.log"
deploy --timeout 0 ooo7777 || fail "a deploy with runs only on workers"
has "pausing the pipelines"
has "2 run(s) left running on workers"
has "  run 91 mojo_history 2026-11-07 on bgent"
has "orca-stack: quiet; paused: toy"
grep -q 'unpause toy' "$FAKE/docker.log" || fail "not unpaused after a deploy over worker runs"
has "running runs: 0 here, 2 on remote workers"
# Runs of its own are waited for, and the worker runs listed beside them.
echo 1 > "$FAKE/running"
deploy --timeout 0 ooo8888 && fail "a deploy over a run of the scheduler's own"
has "waiting for 1 running run(s)"
has "2 run(s) left running on workers"
has "1 run(s) still running after 0s"
echo 0 > "$FAKE/running"
stack resume > /dev/null
# apply's refusal doesn't count them either.
stack apply --tag ooo8888 > "$FAKE/out" 2>&1 || fail "apply refused over runs on workers"
has "scheduler: replaced (image ooo7777 -> ooo8888)"
# rescale.sh's quiesce --workers (the UI goes down too) waits for them.
stack quiesce --workers --timeout 0 > "$FAKE/out" 2>&1 && fail "quiesce --workers over runs on workers"
has "and for 2 on remote workers (--workers)"
has "  run 92 mojo_history"
has "2 run(s) still running after 0s"
grep -q -- 'orca-stack quiesce --workers' deploy/hetzner/rescale.sh || fail "rescale.sh doesn't wait for the workers' runs"
stack resume > /dev/null
rm "$FAKE/worker-running"

# An image before orca 0.18 has no /healthz: its UI's 404 there counts as up.
echo 404 > "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=0
stack apply --tag ppp6666 > "$FAKE/out" 2>&1 || { cat "$FAKE/out"; fail "a UI answering 404 on /healthz wasn't taken as up"; }
rm "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=300
[ "$(cat "$FAKE/containers/ui/image")" = img:ppp6666 ] || fail "the older UI wasn't swapped in: $(cat "$FAKE/containers/ui/image")"

# Overlapping schedulers (OVERLAP=1, docs/overlap.md). Off: orca.env has
# no ORCA_OVERLAP and can-overlap says no without asking the image.
grep -q ORCA_OVERLAP "$ORCA_CONF/orca.env" && fail "ORCA_OVERLAP with OVERLAP unset"
stack can-overlap > "$FAKE/out" 2>&1 && fail "can-overlap with OVERLAP off"
has "no: OVERLAP isn't 1 in stack.conf"
# Turning it on changes the scheduler's env file: that deploy can't overlap
# (the running one has it off), so it pauses and waits as before.
stack configure OVERLAP=1
: > "$FAKE/docker.log"
deploy ppp6666 || fail "the deploy that turns OVERLAP on"
grep -q '^ORCA_OVERLAP=on$' "$ORCA_CONF/orca.env" || fail "no ORCA_OVERLAP=on in orca.env"
has "not overlapping the schedulers: no live scheduler instance leads"
has "pausing the pipelines"
grep -q 'run -d --name scheduler-next' "$FAKE/docker.log" && fail "overlapped when can-overlap said no"
# can-overlap yes: the new scheduler beside the old one, nothing paused, the
# old one told not to restart, renamed, asked to drain, and removed once it
# has exited; registered from the new one, after it leads.
echo yes > "$FAKE/can-overlap"
echo 4 > "$FAKE/running"
echo 7 > "$FAKE/instances"
: > "$FAKE/docker.log"
deploy qqq7777 || fail "an overlapping deploy"
has "the new scheduler starts beside the old one, which drains its runs: not pausing anything"
has "scheduler: replaced (image ppp6666 -> qqq7777), overlapping: scheduler-old drains its runs"
has "asked scheduler instance 7 (scheduler-old) to drain"
has "scheduler-old drained and removed"
grep -q 'exec scheduler orca pause' "$FAKE/docker.log" && fail "an overlapping deploy paused a pipeline"
order=$(grep -oE 'run -d --name scheduler-next|update --restart no scheduler$|rename scheduler scheduler-old|rename scheduler-next scheduler|orca scheduler wait-lead|exec scheduler orca register|orca scheduler drain 7|rm -f scheduler-old' "$FAKE/docker.log" | tr '\n' ,)
[ "$order" = "run -d --name scheduler-next,update --restart no scheduler,rename scheduler scheduler-old,rename scheduler-next scheduler,orca scheduler wait-lead,exec scheduler orca register,orca scheduler drain 7,rm -f scheduler-old," ] \
    || fail "the overlap's order: $order"
grep -q 'stop .*scheduler' "$FAKE/docker.log" && fail "the old scheduler was stopped"
[ "$(cat "$FAKE/containers/scheduler/image")" = img:qqq7777 ] || fail "scheduler runs $(cat "$FAKE/containers/scheduler/image")"
[ ! -d "$FAKE/containers/scheduler-old" ] || fail "scheduler-old is left"
# A drain that runs out: the old one is stopped, then removed.
touch "$FAKE/old-stays"
: > "$FAKE/docker.log"
deploy --timeout 0 rrr8888 || fail "an overlapping deploy whose drain runs out"
has "scheduler-old still runs after 0s: stopping it"
grep -q 'stop -t 30 scheduler-old' "$FAKE/docker.log" || fail "scheduler-old wasn't stopped"
[ ! -d "$FAKE/containers/scheduler-old" ] || fail "scheduler-old is left"
rm "$FAKE/old-stays"
# One still draining from before: no overlap.
mkdir "$FAKE/containers/scheduler-old"
echo running > "$FAKE/containers/scheduler-old/status"
stack can-overlap > "$FAKE/out" 2>&1 && fail "can-overlap with scheduler-old still there"
has "no: scheduler-old is still draining"
rm -r "$FAKE/containers/scheduler-old"
# A new scheduler that doesn't lead: apply fails, and deploy.sh says so.
touch "$FAKE/no-lead"
stack apply --tag sss9999 --overlap > "$FAKE/out" 2>&1 && fail "an overlap whose new scheduler doesn't lead"
has "doesn't lead after"
rm "$FAKE/no-lead"
stack drain-old > /dev/null 2>&1 || fail "drain-old after the failed lead"
stack configure OVERLAP=0
rm "$FAKE/can-overlap" "$FAKE/instances"
echo 0 > "$FAKE/running"
stack apply > /dev/null 2>&1 || fail "back to OVERLAP=0"
stack resume > /dev/null

# Postgres: started once, by the first apply, and never stopped or removed.
[ "$(grep -c 'run -d --name postgres' "$FAKE/docker.all")" = 1 ] || fail "Postgres was started more than once"
grep -q '\(stop\|rm\).* postgres$' "$FAKE/docker.all" && fail "Postgres was stopped or removed"

# One hetzner.env for every laptop: in a secrets repository, its file names
# relative to its folder, and each laptop's ADMIN_IP and ssh key in
# <XDG_CONFIG_HOME>/orca-hetzner/<NAME>.local.env, read after it (its own
# relative names from its folder). The first line names both files.
shared=$FAKE/ops-secrets/prod
mkdir -p "$shared/creds" "$XDG_CONFIG_HOME/orca-hetzner/keys"
cp "$FAKE/s3.env" "$shared/creds/s3.env"
: > "$shared/tunnel.env"
: > "$XDG_CONFIG_HOME/orca-hetzner/keys/laptop.pub"
cat > "$shared/hetzner.env" << 'EOS'
NAME=orca
BUCKET=bkt
S3_ENDPOINT=https://example.invalid
S3_REGION=fsn1
S3_CREDENTIALS_FILE=creds/s3.env
TUNNEL_TOKEN_FILE=./tunnel.env
ORCA_IMAGE=img
ADMIN_IP=192.0.2.1/32
SSH_PUBLIC_KEY_FILE=/nowhere/id.pub
EOS
local_env=$XDG_CONFIG_HOME/orca-hetzner/orca.local.env
printf 'ADMIN_IP=198.51.100.9/32\nSSH_PUBLIC_KEY_FILE=keys/laptop.pub\n' > "$local_env"
(cd "$FAKE" && HETZNER_ENV=ops-secrets/prod/hetzner.env sh "$OLDPWD/deploy/hetzner/up.sh" --dry-run) > "$FAKE/out" 2>&1 \
    || fail "up.sh --dry-run with a shared hetzner.env"
real=$(cd "$shared" && pwd) # HETZNER_ENV was relative: from the folder it was given in
has "up.sh: stack orca, settings $real/hetzner.env and $local_env"
has "orca-stack sync s3.env < $real/creds/s3.env"
has "orca-stack sync tunnel.env < $real/./tunnel.env"
has "public-key-from-file $XDG_CONFIG_HOME/orca-hetzner/keys/laptop.pub"
# Without the local file, the shared file's own settings.
rm "$local_env"
HETZNER_ENV=$shared/hetzner.env sh deploy/hetzner/deploy.sh --dry-run ttt0000 > "$FAKE/out" 2>&1 \
    || fail "deploy.sh --dry-run with no local file"
has "deploy.sh: stack orca, settings $shared/hetzner.env$"
# HETZNER_LOCAL_ENV names another; one that's missing, or that sets NAME, is refused.
echo "ADMIN_IP=198.51.100.10/32" > "$FAKE/other.local.env"
HETZNER_ENV=$shared/hetzner.env HETZNER_LOCAL_ENV=$FAKE/other.local.env sh deploy/hetzner/deploy.sh --dry-run ttt0000 > "$FAKE/out" 2>&1 \
    || fail "deploy.sh --dry-run with HETZNER_LOCAL_ENV"
has "settings $shared/hetzner.env and $FAKE/other.local.env"
HETZNER_ENV=$shared/hetzner.env HETZNER_LOCAL_ENV=$FAKE/none.env sh deploy/hetzner/deploy.sh --dry-run ttt0000 > "$FAKE/out" 2>&1 \
    && fail "a missing HETZNER_LOCAL_ENV"
has "no $FAKE/none.env (HETZNER_LOCAL_ENV)"
echo NAME=orca-dev >> "$FAKE/other.local.env"
HETZNER_ENV=$shared/hetzner.env HETZNER_LOCAL_ENV=$FAKE/other.local.env sh deploy/hetzner/deploy.sh --dry-run ttt0000 > "$FAKE/out" 2>&1 \
    && fail "a local file that sets NAME"
has "sets NAME: the stack's name belongs in $shared/hetzner.env"
# orca-stack check: the smoke check deploy.sh ends with.
export ORCA_STACK_CHECK_SECONDS=0
tags=$(stack tags)
stack check > "$FAKE/out" 2>&1 || fail "check on a healthy stack"
has "check: ok    postgres: ready"
has "check: ok    scheduler: img:"
has "check: ok    ui: img:.*, healthy"
has "check: ok    pipelines: 2 registered, each binary in the image; paused: manual1"
has "check: all ok"
stack checked | grep -q "^ok [0-9T:-]*Z plain $tags\$" || fail "checked: $(stack checked)"
check_fails() { # WHY: check fails, saying WHY, and checked says failed
    stack check "$@" > "$FAKE/out" 2>&1 && fail "check passed, though: $why_"
    has "$why_"
    has "check: 1 failed"
    stack checked | grep -q '^failed ' || fail "checked after a failed check: $(stack checked)"
}
echo exited > "$FAKE/containers/sampler/status"
why_="FAIL  sampler: exited"
check_fails
echo running > "$FAKE/containers/sampler/status"
mkdir -p "$FAKE/health"
echo 500 > "$FAKE/health/ui"
why_="FAIL  ui: answered 500 on :8270/healthz"
check_fails
rm "$FAKE/health/ui"
mkdir "$FAKE/containers/scheduler-old"
echo running > "$FAKE/containers/scheduler-old/status"
why_="FAIL  scheduler-old is still there"
check_fails
rm -r "$FAKE/containers/scheduler-old"
echo /opt/orca/pipelines/toy > "$FAKE/no-binary"
why_="FAIL  pipelines: not in the scheduler's image: toy (/opt/orca/pipelines/toy)"
check_fails
rm "$FAKE/no-binary"
touch "$FAKE/unregistered"
why_="FAIL  pipelines: none registered"
check_fails
rm "$FAKE/unregistered"
# Canary runs: each started and waited for.
: > "$FAKE/canary.log"
stack check --canary toy=2024-01 --canary manual1=x > "$FAKE/out" 2>&1 || fail "check with canaries"
has "check: ok    canary toy 2024-01: run 701 succeeded"
has "check: ok    canary manual1 x: run 702 succeeded"
[ "$(cat "$FAKE/canary.log")" = "toy 2024-01
manual1 x" ] || fail "the canaries started: $(cat "$FAKE/canary.log")"
stack checked | grep -q "^ok .* canary $tags\$" || fail "checked after canaries: $(stack checked)"
mkdir -p "$FAKE/canary-status"
echo failed > "$FAKE/canary-status/toy"
why_="FAIL  canary toy 2024-01: run 703 failed"
check_fails --canary toy=2024-01
echo running > "$FAKE/canary-status/toy"
why_="FAIL  canary toy 2024-01: run 704 still running after 0s"
check_fails --canary toy=2024-01 --timeout 0
rm -r "$FAKE/canary-status"
stack check --canary toy > "$FAKE/out" 2>&1 && fail "a --canary without a key"
has "needs PIPELINE=KEY"
# deploy.sh ends with it, CANARY's runs as --canary; it fails when the check does.
echo 'CANARY="toy=2024-02 manual1=y"' >> "$FAKE/hetzner.env"
echo CANARY_TIMEOUT=60 >> "$FAKE/hetzner.env"
sh deploy/hetzner/deploy.sh --dry-run uuu1111 > "$FAKE/out" 2>&1 || fail "a dry run with CANARY"
has "+ ssh root@<server-ip> orca-stack check --canary toy=2024-02 --canary manual1=y --timeout 60"
deploy uuu1111 || fail "a deploy with canaries"
has "checking the stack (canary runs: toy=2024-02 manual1=y)"
has "check: ok    canary manual1 y"
has "check: all ok"
echo /opt/orca/pipelines/manual1 > "$FAKE/no-binary"
deploy --all uuu1111 && fail "a deploy whose check fails"
has "uuu1111 is deployed, but the check failed (above)"
rm "$FAKE/no-binary"
sed -i.bak '/^CANARY/d' "$FAKE/hetzner.env"
stack check --canary toy=2024-03 > /dev/null 2>&1 || fail "a check with a canary"

# promote.sh: the tags the dev stack checked go to production (the fake
# server is both stacks' here).
sed 's/^NAME=orca$/NAME=orca-dev/' "$FAKE/hetzner.env" > "$FAKE/dev.env"
promote() { sh deploy/hetzner/promote.sh "$@" > "$FAKE/out" 2>&1; }
promote --from "$FAKE/dev.env" --dry-run || fail "promote --dry-run"
has "deploy.sh --dry-run <dev-tag>"
promote --from "$FAKE/dev.env" --no-backup || fail "promote"
has "promote.sh: stack orca-dev"
has "promote.sh: dev checked orca uuu1111, console mmm3333 at .* (canary)"
has "promote.sh: deploy.sh --no-backup uuu1111 console=mmm3333 (settings $FAKE/hetzner.env)"
has "deploy.sh: stack orca, settings $FAKE/hetzner.env"
has "deploy.sh .*done: orca uuu1111, console mmm3333"
# An app on its own tag goes along as app=tag.
deploy uuu1111 console=vvv2222 || fail "a deploy of the console's own tag"
promote --from "$FAKE/dev.env" || fail "promote with an app's own tag"
has "deploy.sh uuu1111 console=vvv2222"
# Refused: a failed check, a check of other tags, the target's own settings.
echo /opt/orca/pipelines/toy > "$FAKE/no-binary"
stack check > /dev/null 2>&1 || true
rm "$FAKE/no-binary"
promote --from "$FAKE/dev.env" && fail "promote after a failed check"
has "the dev stack's last check failed"
stack check > /dev/null 2>&1 || fail "check again"
stack apply --tag www3333 > /dev/null 2>&1 || fail "an apply after the check"
promote --from "$FAKE/dev.env" && fail "promote of tags dev hasn't checked"
has "the dev stack runs orca www3333, console vvv2222, but its last check was of orca uuu1111, console vvv2222"
promote --from "$FAKE/hetzner.env" && fail "promote from the target itself"
has "promote goes from dev to another stack"
promote --from "$FAKE/dev.env" uuu1111 && fail "promote with a tag"
has "promote takes no tags"
stack register > /dev/null 2>&1
deploy uuu1111 > /dev/null 2>&1 || fail "back to uuu1111"
unset ORCA_STACK_CHECK_SECONDS

# Upgrade steps (orca#323): from the new image's upgrade.toml (orca upgrade).
# A before step that isn't confirmed stops a --yes deploy before anything
# changes; with --confirmed it goes on, and the after steps run (a command)
# or are printed (an instruction).
printf 'step\tb1\tbefore\tmanual\tAllow egress to example.org.\nstep\ta1\tafter\trun\torca backfill --rerun toy @all\nstep\ta2\tafter\tmanual\tCheck /live.\n' > "$FAKE/steps"
: > "$FAKE/docker.log"
deploy --yes 1.1.0 && fail "a deploy with an unconfirmed before step"
has "\[b1\] Allow egress to example.org."
has "\[b1\] needs doing before this deploy"
has "nothing was deployed"
grep -q "step exec" "$FAKE/docker.log" && fail "an after step ran though the deploy stopped"
deploy --yes --confirmed b1 1.1.0 || fail "deploy.sh --confirmed b1 1.1.0"
has "\[b1\] confirmed"
has "\[a2\] to do: Check /live."
grep -q "step exec scheduler orca backfill --rerun toy @all" "$FAKE/docker.log" || fail "the after step a1 didn't run"
grep -q "upgrade run --rm img:1.1.0 upgrade --from .* --to 1.1.0 --when before" "$FAKE/docker.log" \
    || fail "the before steps weren't read from the new image"
# The same orca tag again: no steps.
: > "$FAKE/docker.log"
deploy --yes --all 1.1.0 || fail "deploy.sh --all 1.1.0"
grep -q "^upgrade " "$FAKE/docker.log" && fail "steps read for the tag already deployed"
# From 1.1.0: the image is asked for the steps after it.
touch "$FAKE/step-fails"
deploy --yes --confirmed b1 1.2.0 && fail "a deploy whose after step failed"
has "upgrade steps failed: a1"
has "1.2.0 is deployed and checked"
grep -q "upgrade run --rm img:1.2.0 upgrade --from 1.1.0 --to 1.2.0 --when after" "$FAKE/docker.log" \
    || fail "the steps weren't asked for from 1.1.0"
rm "$FAKE/step-fails"
# A rollback runs none.
: > "$FAKE/docker.log"
deploy --yes --rollback || fail "deploy.sh --rollback"
grep -q "^upgrade \|^step " "$FAKE/docker.log" && fail "a rollback read or ran upgrade steps"
# An image whose orca has no `orca upgrade`: no steps.
touch "$FAKE/no-upgrade"
deploy --yes 1.3.0 || fail "deploy.sh 1.3.0 of an image without orca upgrade"
rm "$FAKE/no-upgrade" "$FAKE/steps"
# A dry run lists nothing and needs no confirmation.
deploy --dry-run 1.4.0 || fail "deploy.sh --dry-run 1.4.0"
has "orca-stack upgrade-steps --tag 1.4.0 --from <deployed> --when before"
has "orca-stack upgrade-steps --tag 1.4.0 --from <deployed> --when after"
# orca-stack upgrade-run refuses what isn't an orca command.
stack upgrade-run rm -rf / > "$FAKE/out" 2>&1 && fail "upgrade-run ran rm"
has "orca commands only"
stack upgrade-run orca backfill 'a;b' > "$FAKE/out" 2>&1 && fail "upgrade-run took a ;"
has "refusing the argument"

# The release tarball (deploy/hetzner/package.sh), run from where it's unpacked.
sh deploy/hetzner/package.sh 9.8.7 "$FAKE/pkg" > "$FAKE/out" 2>&1 || fail "package.sh"
cmp -s "$FAKE/pkg/orca-hetzner-9.8.7.tar.gz" "$FAKE/pkg/orca-hetzner.tar.gz" || fail "the two tarballs differ"
mkdir "$FAKE/unpacked"
(cd "$FAKE/unpacked" && gzip -dc "$FAKE/pkg/orca-hetzner.tar.gz" | tar -xf -) || fail "unpacking"
pkg=$FAKE/unpacked/orca-hetzner-9.8.7
for f in up.sh deploy.sh promote.sh down.sh rescale.sh backup.sh lib.sh cloud-init.yaml hetzner.env.example \
    README.md VERSION server/orca-stack server/secret-fetch server/orca-backup.service server/orca-backup.timer; do
    [ -f "$pkg/$f" ] || fail "no $f in the tarball"
done
[ "$(ls "$FAKE/unpacked")" = orca-hetzner-9.8.7 ] || fail "the tarball unpacks to $(ls "$FAKE/unpacked")"
[ "$(cat "$pkg/VERSION")" = 9.8.7 ] || fail "VERSION is $(cat "$pkg/VERSION")"
[ -x "$pkg/up.sh" ] && [ -x "$pkg/server/orca-stack" ] || fail "the scripts aren't executable"
{
    cat "$FAKE/hetzner.env"
    echo ADMIN_IP=203.0.113.7/32
    echo "SSH_PUBLIC_KEY_FILE=$FAKE/k.pub"
} > "$pkg/hetzner.env"
: > "$FAKE/k.pub"
(cd "$pkg" && unset HETZNER_ENV && ./up.sh --dry-run) > "$FAKE/out" 2>&1 || fail "the tarball's up.sh --dry-run"
has "up.sh: stack orca, settings $pkg/hetzner.env, scripts 9.8.7"
has "install -D -m 755 /dev/stdin /usr/local/sbin/orca-stack < $pkg/server/orca-stack"
has "install -D -m 644 /dev/stdin /etc/systemd/system/orca-backup.timer < $pkg/server/orca-backup.timer"
has "hcloud server create"
(cd "$pkg" && unset HETZNER_ENV && ./deploy.sh --dry-run ttt0000) > "$FAKE/out" 2>&1 || fail "the tarball's deploy.sh --dry-run"
has "deploy.sh: stack orca, settings $pkg/hetzner.env, scripts 9.8.7"
has "/usr/local/sbin/orca-stack < $pkg/server/orca-stack"

echo "ok   hetzner"
