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
#   replaced when the UI's replacement fails;
# - deploy.sh (and --rollback) first brings the server's settings in line
#   with hetzner.env: a key added there reaches settings.env, one emptied is
#   removed, a secret file is rewritten (mode 600), a stack setting set,
#   each named and never its value; the plan then replaces the containers
#   whose env file changed and quiesces for the scheduler; the tags stay;
#   a dry run writes nothing;
# - with OVERLAP=1 and can-overlap saying yes, a deploy that replaces the
#   scheduler pauses nothing: the new scheduler starts beside the old one,
#   the old one is set not to restart and renamed scheduler-old, the new one
#   leads and registers, then the old one is asked to drain, waited for
#   (stopped after --timeout) and removed; can-overlap says no with OVERLAP
#   off, or with a scheduler-old still draining, and the deploy that turns
#   OVERLAP on pauses and waits as before.
set -eu
cd "$(dirname "$0")/.."
FAKE=$(mktemp -d "${TMPDIR:-/tmp}/orca-hetzner.XXXXXX")
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
export XDG_STATE_HOME

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
# pipelines are registered from the new image, stay paused, and it says so.
echo 503 > "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=0
: > "$FAKE/docker.log"
deploy ooo5555 && fail "a deploy whose UI wasn't healthy"
rm "$FAKE/health/ui"
stack configure HEALTH_TIMEOUT=300
has "not replaced: ui"
has "registering the pipelines from ooo5555"
grep -q 'exec scheduler orca register' "$FAKE/docker.log" || fail "a replaced scheduler wasn't registered"
has "the deploy failed; the pipelines stay paused"
grep -qx toy "$FAKE/paused" || fail "toy unpaused after a failed UI"
[ "$(cat "$FAKE/containers/ui/image")" = img:kkk1111 ] || fail "the old UI wasn't kept: $(cat "$FAKE/containers/ui/image")"
stack apply > "$FAKE/out" 2>&1 || fail "the UI's retry"
has "ui: replaced (image kkk1111 -> ooo5555), without downtime"
has "scheduler: unchanged"
stack resume > /dev/null

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

echo "ok   hetzner"
