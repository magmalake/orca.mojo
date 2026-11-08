#!/bin/sh
# deploy.sh [--dry-run] [--force] [--no-backup] [--timeout SECONDS] [--yes] <orca-tag> [<app>=<tag> ...]
# deploy.sh [--dry-run] [--force] [--no-backup] [--timeout SECONDS] [--yes] --rollback
#
# Code changes: pulls the images at the tags and restarts the scheduler, ui,
# sampler, apps and tunnel from them (what `orca aws app` and `orca aws up`
# did on AWS). The orca tag also becomes the default for the apps; an
# <app>=<tag> argument gives one app another. Postgres is not restarted.
#
# Restarting the scheduler kills running runs, so it first quiesces: pauses
# every pipeline that isn't paused, waits for the running runs to end (up to
# --timeout, default 3600 s), deploys, then unpauses the pipelines it paused.
# Pipelines that were already paused (by hand or by the watchdog) stay paused.
# On a timeout or a failed deploy the pipelines stay paused, and the message
# says how to undo it. --force skips all that and kills the running runs.
#
# Before it applies anything it backs the database up (orca-stack backup-now,
# to s3://BUCKET/backups/pg/<time>.dump) and stops if that fails, unless
# --no-backup. The server keeps the tags it ran before (orca-stack tags
# --previous); --rollback applies those again, the same way, and registers
# the pipelines again (docs/hetzner.md#rolling-back).
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

force=
timeout=3600
backup=1
dump=
rollback=0
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
usage="usage: deploy.sh [--dry-run] [--force] [--no-backup] [--timeout SECONDS] <orca-tag> [<app>=<tag> ...] | --rollback"
if [ "$rollback" = 1 ]; then
    [ -z "$tag" ] && [ -z "$args" ] || die "--rollback takes no tags: it applies the ones deployed before"
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

if [ -n "$force" ]; then
    log "--force: not pausing; running runs are killed"
else
    log "pausing the pipelines and waiting for running runs (up to ${timeout}s)"
    remote orca-stack quiesce --timeout "$timeout" \
        || die "not deployed; the pipelines it paused stay paused (ssh root@$IP orca-stack resume)"
fi

if [ "$backup" = 1 ]; then
    log "backing up the database"
    if ! out=$(remote_out "orca-stack: backed up to s3://<bucket>/backups/pg/<time>.dump" orca-stack backup-now 2>&1); then
        echo "$out" >&2
        [ -n "$force" ] || remote orca-stack resume || true
        die "the backup failed, so nothing was deployed (--no-backup deploys without one)"
    fi
    dump=$(echo "$out" | sed -n 's/^orca-stack: backed up to //p')
    log "previous tag $was, new tag $to; backup ${dump:-$out}"
else
    log "--no-backup: not backing up; previous tag $was, new tag $to"
fi

if [ "$rollback" = 1 ]; then
    log "rolling back to $to"
    remote orca-stack rollback ${force:+"$force"} \
        || die "the rollback failed; the pipelines stay paused (fix it and rerun, or ssh root@$IP orca-stack resume)"
else
    log "deploying $tag"
    # shellcheck disable=SC2086
    remote orca-stack apply --tag "$tag" $args ${force:+"$force"} \
        || die "the deploy failed; the pipelines stay paused (fix it and rerun, or ssh root@$IP orca-stack resume)"
fi
if [ -z "$force" ]; then
    log "unpausing the pipelines it paused"
    remote orca-stack resume
fi
remote orca-stack status
log "done: $to${dump:+ (backup before it: $dump)}"
