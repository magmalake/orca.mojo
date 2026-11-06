#!/bin/sh
# rescale.sh up|down [--dry-run] [--yes] [--timeout SECONDS]
#
# Moves the server between SERVER_TYPE_SMALL and SERVER_TYPE_BIG with
# `hcloud server change-type --keep-disk`, so the root disk stays the small
# type's size and the server can come back. The Volume and Postgres are not
# touched. In order:
#
#   1. pause every pipeline and wait for the running runs to end
#      (orca-stack quiesce; --timeout, default 3600 s; on a timeout the
#      pipelines stay paused and nothing else was done);
#   2. a last backup;
#   3. shut the server down, wait until it is off (power it off after 2 minutes);
#   4. change-type --keep-disk;
#   5. power on, wait for ssh and the containers;
#   6. unpause the pipelines that step 1 paused.
#
# The server is billed by the hour for each type, up to its monthly price.
# It is down for a few minutes, and so are the UI and the console.
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

timeout=3600
parse_common "$@"
# shellcheck disable=SC2086
set -- $REST
dir=${1:-}
[ $# -eq 0 ] || shift
while [ $# -gt 0 ]; do
    case "$1" in
        --timeout)
            [ $# -ge 2 ] || die "--timeout needs seconds"
            timeout=$2
            shift
            ;;
        *) die "unknown argument $1" ;;
    esac
    shift
done

load_env
case "$dir" in
    up) target=$SERVER_TYPE_BIG ;;
    down) target=$SERVER_TYPE_SMALL ;;
    *) die "usage: rescale.sh up|down [--dry-run] [--yes] [--timeout SECONDS]" ;;
esac
need_tool hcloud ssh jq
IP=$(server_ip)

if [ "$DRY_RUN" = 0 ]; then
    now=$(hcloud server describe "$SERVER" -o json | jq -r .server_type.name)
    if [ "$now" = "$target" ]; then
        echo "$SERVER is $target already"
        exit 0
    fi
    echo "$SERVER: $now -> $target"
fi
confirm "This pauses the pipelines, stops the server for a few minutes and changes its type to $target."

remote orca-stack quiesce --timeout "$timeout"
remote orca-stack backup
hc server shutdown "$SERVER"
if [ "$DRY_RUN" = 0 ]; then
    n=0
    while [ "$(hcloud server describe "$SERVER" -o json | jq -r .status)" != off ]; do
        n=$((n + 1))
        if [ "$n" -ge 24 ]; then
            echo "still running after 2 minutes; powering off"
            hcloud server poweroff "$SERVER"
            break
        fi
        sleep 5
    done
else
    echo "+ (wait until the server's status is off, else hcloud server poweroff $SERVER)"
fi
hc server change-type --keep-disk "$SERVER" "$target"
hc server poweron "$SERVER"
wait_for_ssh
if [ "$DRY_RUN" = 0 ]; then
    n=0
    until [ "$(remote docker inspect -f '{{.State.Running}}' scheduler 2> /dev/null)" = true ]; do
        n=$((n + 1))
        [ "$n" -lt 60 ] || die "the scheduler isn't running 5 minutes after boot; see docs/hetzner.md (pipelines are still paused)"
        sleep 5
    done
fi
remote orca-stack resume
remote orca-stack status
