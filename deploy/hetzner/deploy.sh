#!/bin/sh
# deploy.sh [--dry-run] [--force] [--all] [--no-backup] [--timeout SECONDS] [--yes] <orca-tag> [<app>=<tag> ...]
# deploy.sh [--dry-run] [--force] [--no-backup] [--timeout SECONDS] [--yes] --rollback
#
# Code changes: pulls the images at the tags and starts again the containers
# whose image or settings changed (the scheduler, ui, sampler, apps and
# tunnel; the others keep running), as `orca aws app` and `orca aws up` did
# on AWS. The orca tag also becomes the default for the apps; an
# <app>=<tag> argument gives one app another. Postgres is not restarted.
# --all starts every container again, changed or not.
#
# First it brings the server's settings in line with hetzner.env, as up.sh
# writes them (sync_settings in lib.sh): settings.env, the secret files,
# the stack settings and the apps; not the tags, which are this script's.
# It prints what changed, settings.env's keys by name (added, removed,
# changed), never a value; a stack setting old -> new. A container whose
# env file this changes is then replaced ("replace settings: ..." in the
# plan), the scheduler's included, which quiesces as below. --rollback
# does it too: it rolls the tags back, not the settings, which are
# hetzner.env's (to undo a setting, change it there).
#
# It first asks the server what the apply would replace (orca-stack plan,
# one line per container) and goes on from that:
#
# - The scheduler is replaced (a new orca tag, or its settings changed):
#   restarting it kills running runs, so it quiesces first: pauses every
#   pipeline that isn't paused, waits for the running runs to end (up to
#   --timeout, default 3600 s), deploys, registers the pipelines from the
#   new image (orca-stack register) and unpauses the pipelines it paused.
#   Pipelines that were already paused (by hand or by the watchdog) stay
#   paused. On a timeout, a failed deploy or a failed registration the
#   pipelines stay paused, and the message says how to undo it. --force
#   skips the pause and kills the running runs.
# - The scheduler is replaced and the stack overlaps schedulers (OVERLAP=1
#   in stack.conf, docs/overlap.md), and orca-stack can-overlap says the new
#   image's scheduler may start beside the old one (no migration of it
#   raises the floor past the old binary): nothing is paused and runs go
#   on. The apply starts the new scheduler beside the old one and waits
#   until it leads; it registers the pipelines from the new image, then
#   waits (up to --timeout) for the old scheduler to drain its runs and
#   exit, and removes it (orca-stack drain-old). A rollback, --force and a
#   dry run don't overlap.
# - The scheduler keeps running (an app's tag only, say the census
#   console's): nothing is paused and runs go on; it backs up and applies,
#   and nothing needs registering (the pipelines are in the scheduler's
#   image, which didn't change).
# - Nothing is replaced: no backup, and the apply keeps every container.
#
# The UI and the apps are replaced without downtime (orca-stack starts the
# new one beside the old and waits until it's healthy). One that never is
# fails the deploy after the rest is applied, the old one still serving: a
# scheduler it replaced is registered first, the pipelines it paused are
# unpaused (the scheduler is deployed: nothing waits on the UI), and it
# exits non-zero naming what wasn't replaced; orca-stack apply on the
# server tries those again. The pipelines stay paused only when the
# scheduler itself, the registration or the backup failed.
#
# The quiesce waits only for the runs the scheduler's own processes hold: a
# run on a remote worker (placement worker:<pool>) goes on through the
# scheduler's restart, and is listed as left running on the workers.
#
# Before it applies anything that replaces a container it backs the
# database up (orca-stack backup-now, to s3://BUCKET/backups/pg/<time>.dump,
# with <time>.paused beside it: the pipelines paused by hand and the ones
# this deploy paused, so a restore of the dump unpauses only the latter) and
# stops if that fails, unless --no-backup. The server keeps the tags it ran
# before (orca-stack tags --previous); --rollback applies those again, the
# same way (docs/hetzner.md#rolling-back).
#
# It ends with orca-stack check (docs/hetzner.md#the-check-after-each-deploy):
# the containers, one scheduler, the pipelines' binaries, and a canary run
# for each of hetzner.env's CANARY; when the check fails the deploy fails,
# with nothing undone.
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

force=
timeout=3600
backup=1
dump=
rollback=0
all=
parse_common "$@"
# shellcheck disable=SC2086
set -- $REST
args=
apps=
tag=
while [ $# -gt 0 ]; do
    case "$1" in
        --force) force=--force ;;
        --no-backup) backup=0 ;;
        --all) all=--all ;;
        --rollback) rollback=1 ;;
        --timeout)
            [ $# -ge 2 ] || die "--timeout needs a number of seconds"
            timeout=$2
            shift
            ;;
        -*) die "unknown flag $1" ;;
        *=*)
            args="$args --app $1"
            apps="$apps, ${1%%=*} ${1#*=}"
            ;;
        *)
            [ -z "$tag" ] || die "one orca tag, then app=tag arguments"
            tag=$1
            ;;
    esac
    shift
done
usage="usage: deploy.sh [--dry-run] [--force] [--all] [--no-backup] [--timeout SECONDS] <orca-tag> [<app>=<tag> ...] | --rollback"
if [ "$rollback" = 1 ]; then
    [ -z "$tag" ] && [ -z "$args" ] || die "--rollback takes no tags: it applies the ones deployed before"
    [ -z "$all" ] || die "--rollback replaces what differs from the tags before; --all isn't for it"
else
    [ -n "$tag" ] || die "$usage"
fi

load_env
need_settings
need_tool hcloud ssh
IP=$(server_ip)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/orca-hetzner.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
log() { echo "deploy.sh $(date -u +%H:%M:%S): $*"; }

# remote_out PLACEHOLDER ARGS...: the remote command's output, or in a dry run
# the command printed (to stderr) and the placeholder.
remote_out() {
    p=$1
    shift
    if [ "$DRY_RUN" = 1 ]; then
        echo "+ ssh root@$IP $*" >&2
        echo "$p"
    else
        remote "$@"
    fi
}

# The server's orca-stack from this checkout, so quiesce and resume match it.
put 755 /usr/local/sbin/orca-stack "$HERE/server/orca-stack"

# The server's settings as hetzner.env says, before the plan (which then
# replaces what they change).
log "the server's settings from $HETZNER_ENV (names only, never values):"
if sync_settings "$tmp" > "$tmp/synced" 2>&1; then synced=1; else synced=0; fi
if [ -s "$tmp/synced" ]; then sed 's/^/  /' "$tmp/synced"; else echo "  unchanged"; fi
[ "$synced" = 1 ] || die "setting the server's settings failed (above); nothing was deployed"
was=$(remote_out "<deployed>" orca-stack tags)
was=${was:-unknown}
if [ "$rollback" = 1 ]; then
    to=$(remote_out "<deployed before>" orca-stack tags --previous)
    [ -n "$to" ] || die "nothing to roll back to: the server has no earlier deploy recorded (one is kept from this deploy.sh on)"
else
    to=$tag
    [ -z "$apps" ] || to="orca $tag$apps"
fi
log "previous tag $was, new tag $to"

# What the apply would replace. A dry run doesn't ask, and shows the
# longest way: the scheduler replaced.
if [ "$rollback" = 1 ]; then
    plan=$(remote_out "scheduler replace (a dry run assumes it)" orca-stack plan --rollback) \
        || die "orca-stack plan --rollback failed; nothing was deployed"
else
    # shellcheck disable=SC2086
    plan=$(remote_out "scheduler replace (a dry run assumes it)" orca-stack plan --tag "$tag" $args $all) \
        || die "orca-stack plan failed; nothing was deployed"
fi
log "the plan:"
echo "$plan" | sed 's/^/  /'
replaces=0
scheduler=0
! echo "$plan" | grep -q '^[^ ]* replace' || replaces=1
! echo "$plan" | grep -q '^scheduler replace' || scheduler=1

quiesced=0
overlap=0
if [ "$scheduler" = 1 ] && [ "$rollback" = 0 ] && [ -z "$force" ] && [ "$DRY_RUN" = 0 ]; then
    if why=$(remote orca-stack can-overlap --tag "$tag"); then
        overlap=1
    else
        log "not overlapping the schedulers: ${why#no: }"
    fi
fi
if [ "$scheduler" = 0 ]; then
    log "the scheduler keeps running: not pausing anything, runs go on"
elif [ "$overlap" = 1 ]; then
    log "the new scheduler starts beside the old one, which drains its runs: not pausing anything"
elif [ -n "$force" ]; then
    log "--force: not pausing; the scheduler is replaced, so running runs are killed"
else
    log "the scheduler is replaced: pausing the pipelines and waiting for running runs (up to ${timeout}s)"
    remote orca-stack quiesce --timeout "$timeout" \
        || die "not deployed; the pipelines it paused stay paused (ssh root@$IP orca-stack resume)"
    quiesced=1
fi
# What a failure leaves paused.
if [ "$quiesced" = 1 ]; then
    left="the pipelines stay paused (fix it and rerun, or ssh root@$IP orca-stack resume)"
else
    left="nothing was paused"
fi

# unhealthy_of OUTPUT: the containers apply's (or rollback's) closing
# "orca-stack: not replaced: NAME..." line names, the ones whose new
# container wasn't healthy, the old ones still serving; empty if none.
unhealthy_of() {
    echo "$1" | sed -n 's/^orca-stack: not replaced: *\(.*\), whose new container.*/\1/p' | tail -n 1
}

# finish_failed OUTPUT WHAT: the end of a failed apply or rollback, after the
# registration. Only a UI or an app not replaced: what this deploy paused
# runs again (the scheduler is deployed and registered), and it fails
# naming them. Anything else (the scheduler too): stays as it is.
finish_failed() {
    bad=$(unhealthy_of "$1")
    case " $bad " in
        "  " | *" scheduler "*) die "$2; $left" ;;
    esac
    if [ "$quiesced" = 1 ]; then
        log "only $bad wasn't replaced and the scheduler is deployed: unpausing the pipelines it paused"
        remote orca-stack resume \
            || die "not replaced: $bad; and unpausing failed, so some pipelines stay paused (ssh root@$IP orca-stack resume)"
    fi
    remote orca-stack status || true
    die "not replaced: $bad, whose new container wasn't healthy (above): the old one keeps serving, and the rest of $to is deployed${dump:+ (backup before it: $dump)}$([ "$quiesced" = 0 ] || echo ', the pipelines it paused running again'). Fix it, then ssh root@$IP orca-stack apply, which replaces only those"
}

if [ "$replaces" = 0 ]; then
    log "nothing to replace: not backing up"
elif [ "$backup" = 1 ]; then
    log "backing up the database"
    if ! out=$(remote_out "orca-stack: backed up to s3://<bucket>/backups/pg/<time>.dump" orca-stack backup-now 2>&1); then
        echo "$out" >&2
        [ "$quiesced" = 0 ] || remote orca-stack resume || true
        die "the backup failed, so nothing was deployed (--no-backup deploys without one)"
    fi
    dump=$(echo "$out" | sed -n 's/^orca-stack: backed up to //p')
    log "previous tag $was, new tag $to; backup ${dump:-$out}"
    echo "$out" | grep '^orca-stack: paused then' || true
else
    log "--no-backup: not backing up; previous tag $was, new tag $to"
fi

if [ "$rollback" = 1 ]; then
    log "rolling back to $to"
    # A UI or app that isn't healthy fails it after the registration.
    failed=0
    rolled=$(remote orca-stack rollback ${force:+"$force"} 2>&1) || failed=1
    echo "$rolled"
    [ "$failed" = 0 ] || finish_failed "$rolled" "the rollback failed"
else
    log "deploying $tag"
    # A new UI or app that isn't healthy fails the apply after the others
    # are applied: a scheduler it replaced is registered all the same, and
    # then finish_failed unpauses.
    failed=0
    # shellcheck disable=SC2086
    ov=
    [ "$overlap" = 0 ] || ov=--overlap
    # shellcheck disable=SC2086
    applied=$(remote orca-stack apply --tag "$tag" $args $all ${force:+"$force"} $ov 2>&1) || failed=1
    echo "$applied"
    if [ "$failed" = 1 ] && ! echo "$applied" | grep -q '^scheduler: replaced'; then
        finish_failed "$applied" "the deploy failed"
    fi
    # The pipelines are in the scheduler's image: registered again when it
    # was replaced.
    if echo "$applied" | grep -q '^scheduler: replaced' || { [ "$DRY_RUN" = 1 ] && [ "$scheduler" = 1 ]; }; then
        log "registering the pipelines from $tag"
        if [ "$quiesced" = 0 ]; then
            remote orca-stack register \
                || die "$tag is deployed, but registering failed (fix it, then ssh root@$IP orca-stack register)"
        else
            remote orca-stack register \
                || die "$tag is deployed, but registering failed; the pipelines stay paused (fix it, then ssh root@$IP orca-stack register and orca-stack resume; or ./deploy.sh --rollback)"
        fi
    else
        log "the scheduler wasn't replaced: the pipelines stay registered as they are"
    fi
    if [ "$overlap" = 1 ] && echo "$applied" | grep -q '^scheduler: replaced'; then
        log "waiting for the old scheduler to drain its runs (up to ${timeout}s)"
        remote orca-stack drain-old --timeout "$timeout" \
            || die "$tag is deployed and registered, but the old scheduler didn't go (ssh root@$IP orca-stack drain-old)"
    fi
    [ "$failed" = 0 ] || finish_failed "$applied" "the deploy failed"
fi
if [ "$quiesced" = 1 ]; then
    log "unpausing the pipelines it paused"
    remote orca-stack resume
fi
remote orca-stack status
# The smoke check (orca-stack check): containers, one scheduler, the
# pipelines' binaries, and hetzner.env's CANARY runs. Nothing is undone when
# it fails: it says what's wrong, and --rollback is a command away.
log "checking the stack${CANARY:+ (canary runs: $CANARY)}"
# shellcheck disable=SC2046
remote orca-stack check $(check_args) \
    || die "$to is deployed, but the check failed (above)${dump:+; backup before it: $dump}; ./deploy.sh --rollback goes back"
log "done: $to${dump:+ (backup before it: $dump)}"
