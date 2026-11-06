#!/bin/sh
# down.sh [--dry-run] [--yes] [--all]
#
# Deletes the server (and with it the firewall's attachment). The Volume
# (Postgres's data) and the bucket stay, so `up.sh` brings it all back: on
# the same Volume it needs no restore. Billing for the server stops; the
# Volume is billed until deleted.
#
# --all also deletes the firewall, the ssh key and the Volume, and asks you
# to type the Volume's name. The Object Storage bucket is never touched:
# delete it in the Hetzner console if you want it gone.
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

all=0
parse_common "$@"
# shellcheck disable=SC2086
set -- $REST
while [ $# -gt 0 ]; do
    case "$1" in
        --all) all=1 ;;
        *) die "unknown argument $1" ;;
    esac
    shift
done

load_env
need_tool hcloud
if [ "$DRY_RUN" = 0 ]; then
    echo "This deletes the server $SERVER."
    [ "$all" = 0 ] || echo "--all: and the Volume $NAME-data (the database), the firewall and the ssh key."
fi
confirm "Take a backup first (backup.sh) if the database matters."
if [ "$all" = 1 ] && [ "$DRY_RUN" = 0 ] && [ "$YES" = 0 ]; then
    printf 'Type the Volume name (%s-data) to delete it: ' "$NAME" >&2
    read -r answer
    [ "$answer" = "$NAME-data" ] || die "not confirmed"
fi

if [ "$DRY_RUN" = 0 ]; then
    ip=$(hcloud server ip "$SERVER" 2> /dev/null || true)
    [ -z "$ip" ] || ssh-keygen -q -f "$KNOWN_HOSTS" -R "$ip" > /dev/null 2>&1 || true
fi
if exists server "$SERVER" || [ "$DRY_RUN" = 1 ]; then
    hc server delete "$SERVER"
fi
if [ "$all" = 1 ]; then
    if exists volume "$NAME-data" || [ "$DRY_RUN" = 1 ]; then
        hc volume disable-protection "$NAME-data" delete
        hc volume delete "$NAME-data"
    fi
    if exists firewall "$NAME" || [ "$DRY_RUN" = 1 ]; then
        hc firewall delete "$NAME"
    fi
    if exists ssh-key "$NAME" || [ "$DRY_RUN" = 1 ]; then
        hc ssh-key delete "$NAME"
    fi
fi
echo "down: done"
