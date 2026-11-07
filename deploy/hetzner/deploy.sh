#!/bin/sh
# deploy.sh [--dry-run] [--force] [--timeout SECONDS] [--yes] <orca-tag> [<app>=<tag> ...]
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
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

force=
timeout=3600
parse_common "$@"
# shellcheck disable=SC2086
set -- $REST
args=
tag=
while [ $# -gt 0 ]; do
    case "$1" in
        --force) force=--force ;;
        --timeout)
            [ $# -ge 2 ] || die "--timeout needs a number of seconds"
            timeout=$2
            shift
            ;;
        -*) die "unknown flag $1" ;;
        *=*) args="$args --app $1" ;;
        *)
            [ -z "$tag" ] || die "one orca tag, then app=tag arguments"
            tag=$1
            ;;
    esac
    shift
done
[ -n "$tag" ] || die "usage: deploy.sh [--dry-run] [--force] [--timeout SECONDS] <orca-tag> [<app>=<tag> ...]"

load_env
need_tool hcloud ssh
IP=$(server_ip)
log() { echo "deploy.sh $(date -u +%H:%M:%S): $*"; }

# The server's orca-stack from this checkout, so quiesce and resume match it.
put 755 /usr/local/sbin/orca-stack "$HERE/server/orca-stack"
if [ -n "$force" ]; then
    log "--force: not pausing; running runs are killed"
else
    log "pausing the pipelines and waiting for running runs (up to ${timeout}s)"
    remote orca-stack quiesce --timeout "$timeout" \
        || die "not deployed; the pipelines it paused stay paused (ssh root@$IP orca-stack resume)"
fi
log "deploying $tag"
# shellcheck disable=SC2086
remote orca-stack apply --tag "$tag" $args ${force:+"$force"} \
    || die "the deploy failed; the pipelines stay paused (fix it and rerun, or ssh root@$IP orca-stack resume)"
if [ -z "$force" ]; then
    log "unpausing the pipelines it paused"
    remote orca-stack resume
fi
remote orca-stack status
log "done: $tag"
