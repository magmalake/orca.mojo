#!/bin/sh
# deploy.sh [--dry-run] [--force] [--yes] <orca-tag> [<app>=<tag> ...]
#
# Code changes: pulls the images at the tags and restarts the scheduler, ui,
# sampler, apps and tunnel from them (what `orca aws app` and `orca aws up`
# did on AWS). The orca tag also becomes the default for the apps; an
# <app>=<tag> argument gives one app another. Postgres is not restarted.
#
# It refuses while runs are running, because restarting the scheduler kills
# them; pause the pipelines and wait, or pass --force.
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

force=
parse_common "$@"
# shellcheck disable=SC2086
set -- $REST
args=
tag=
while [ $# -gt 0 ]; do
    case "$1" in
        --force) force=--force ;;
        -*) die "unknown flag $1" ;;
        *=*) args="$args --app $1" ;;
        *)
            [ -z "$tag" ] || die "one orca tag, then app=tag arguments"
            tag=$1
            ;;
    esac
    shift
done
[ -n "$tag" ] || die "usage: deploy.sh [--dry-run] [--force] <orca-tag> [<app>=<tag> ...]"

load_env
need_tool hcloud ssh
IP=$(server_ip)
# shellcheck disable=SC2086
remote orca-stack apply --tag "$tag" $args ${force:+"$force"}
remote orca-stack status
