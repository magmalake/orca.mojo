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
# - The scheduler keeps running (an app's tag only, say the census
#   console's): nothing is paused and runs go on; it backs up and applies,
#   and nothing needs registering (the pipelines are in the scheduler's
#   image, which didn't change).
# - Nothing is replaced: no backup, and the apply keeps every container.
#
# Before it applies anything that replaces a container it backs the
# database up (orca-stack backup-now, to s3://BUCKET/backups/pg/<time>.dump,
# with <time>.paused beside it: the pipelines paused by hand and the ones
# this deploy paused, so a restore of the dump unpauses only the latter) and
# stops if that fails, unless --no-backup. The server keeps the tags it ran
# before (orca-stack tags --previous); --rollback applies those again, the
# same way (docs/hetzner.md#rolling-back).
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
need_tool hcloud ssh
IP=$(server_ip)
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
if [ "$scheduler" = 0 ]; then
    log "the scheduler keeps running: not pausing anything, runs go on"
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
    remote orca-stack rollback ${force:+"$force"} \
        || die "the rollback failed; $left"
else
    log "deploying $tag"
    # shellcheck disable=SC2086
    applied=$(remote orca-stack apply --tag "$tag" $args $all ${force:+"$force"} 2>&1) || {
        echo "$applied"
        die "the deploy failed; $left"
    }
    echo "$applied"
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
fi
if [ "$quiesced" = 1 ]; then
    log "unpausing the pipelines it paused"
    remote orca-stack resume
fi
remote orca-stack status
log "done: $to${dump:+ (backup before it: $dump)}"
