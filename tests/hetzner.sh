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
#   deploy paused (a dump without one leaves them all paused).
set -eu
cd "$(dirname "$0")/.."
FAKE=$(mktemp -d "${TMPDIR:-/tmp}/orca-hetzner.XXXXXX")
export FAKE
trap 'rm -rf "$FAKE"' EXIT INT TERM
export PATH="$PWD/tests/hetzner:$PATH"
export ORCA_STACK_TEST=1 ORCA_STACK_REGISTER_TRIES=1 ORCA_CONF="$FAKE/conf" ORCA_DATA="$FAKE/data" ORCA_STATE="$FAKE/state"
mkdir -p "$ORCA_CONF"
printf 'AWS_REGION=fsn1\nAWS_ENDPOINT_URL_S3=https://example.invalid\n' > "$ORCA_CONF/settings.env"
printf 'AWS_ACCESS_KEY_ID=x\nAWS_SECRET_ACCESS_KEY=y\n' > "$ORCA_CONF/s3.env"
printf 'NAME=orca\nBUCKET=bkt\nSSH_PRIVATE_KEY_FILE=\n' > "$FAKE/hetzner.env"
export HETZNER_ENV="$FAKE/hetzner.env"
XDG_STATE_HOME="$FAKE/xdg"
export XDG_STATE_HOME

fail() {
    echo "FAIL: $*"
    [ ! -f "$FAKE/out" ] || tail -20 "$FAKE/out"
    exit 1
}
stack() { sh deploy/hetzner/server/orca-stack "$@"; }
deploy() { sh deploy/hetzner/deploy.sh "$@" > "$FAKE/out" 2>&1; }
has() { grep -q -- "$1" "$FAKE/out" || fail "no '$1' in the output"; }

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
grep -q '^console 8080 -$' "$ORCA_CONF/apps" || fail "the console doesn't follow the orca tag: $(cat "$ORCA_CONF/apps")"
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
[ ! -s "$FAKE/docker.log" ] || fail "a dry run ran docker"

echo "ok   hetzner"
