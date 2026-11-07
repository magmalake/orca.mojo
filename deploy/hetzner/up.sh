#!/bin/sh
# up.sh [--dry-run] [--restore <backups/pg/<time>.dump|latest>] [--force] [--yes]
#
# Makes the orca stack on Hetzner Cloud, or brings what exists back to what
# hetzner.env says; running it again is how drift is fixed. Per step, what
# exists is left alone:
#
#   ssh key, firewall (ssh from ADMIN_IP only), Volume (delete-protected),
#   server (cloud-init: Docker and the Volume mount), then over ssh: the
#   settings and secrets, orca-stack, the backup timer, and `orca-stack
#   apply` (postgres, scheduler, ui, apps, sampler, cloudflared).
#
# --restore restores Postgres from Object Storage before the scheduler
# starts (a new server on an empty Volume). --force lets the final apply
# restart the scheduler although runs are running (it kills them).
# --dry-run prints every hcloud and ssh command and runs none.
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

restore=
force=
parse_common "$@"
# shellcheck disable=SC2086
set -- $REST
while [ $# -gt 0 ]; do
    case "$1" in
        --restore)
            [ $# -ge 2 ] || die "--restore needs a key or latest"
            restore=$2
            shift
            ;;
        --force) force=--force ;;
        *) die "unknown argument $1" ;;
    esac
    shift
done

load_env
need NAME LOCATION BUCKET S3_ENDPOINT S3_REGION ADMIN_IP
need_file SSH_PUBLIC_KEY_FILE S3_CREDENTIALS_FILE TUNNEL_TOKEN_FILE
[ -z "${GITHUB_TOKEN_FILE:-}" ] || need_file GITHUB_TOKEN_FILE
[ -z "${EXTRA_ENV_FILE:-}" ] || need_file EXTRA_ENV_FILE
[ -z "${GHCR_USER:-}" ] || need_file GHCR_TOKEN_FILE
need_tool hcloud ssh jq

tmp=$(mktemp -d "${TMPDIR:-/tmp}/orca-hetzner.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# --- ssh key ---------------------------------------------------------------
if exists ssh-key "$NAME"; then
    echo "ssh key $NAME exists"
else
    hc ssh-key create --name "$NAME" --public-key-from-file "$SSH_PUBLIC_KEY_FILE"
fi

# --- firewall: ssh from ADMIN_IP, nothing else -----------------------------
# (Outbound is open; the tunnel and Object Storage are outbound connections.)
cat > "$tmp/rules.json" << EOS
[{"direction": "in", "protocol": "tcp", "port": "22", "source_ips": ["$ADMIN_IP"], "description": "ssh from the admin"}]
EOS
if exists firewall "$NAME"; then
    echo "firewall $NAME exists; setting its rules"
    hc firewall replace-rules "$NAME" --rules-file "$tmp/rules.json"
else
    hc firewall create --name "$NAME" --rules-file "$tmp/rules.json"
fi

# --- Volume: Postgres's data, formatted once, protected from deletion ------
if exists volume "$NAME-data"; then
    echo "volume $NAME-data exists"
else
    hc volume create --name "$NAME-data" --size "$VOLUME_SIZE_GB" --location "$LOCATION" --format ext4
    hc volume enable-protection "$NAME-data" delete
fi
if [ "$DRY_RUN" = 1 ]; then
    volume_id="<volume-id>"
else
    volume_id=$(hcloud volume describe "$NAME-data" -o json | jq -r .id)
fi

# --- server ----------------------------------------------------------------
if exists server "$SERVER"; then
    echo "server $SERVER exists"
else
    sed -e "s|@@HOSTNAME@@|$NAME|" -e "s|@@VOLUME_ID@@|$volume_id|" "$HERE/cloud-init.yaml" > "$tmp/cloud-init.yaml"
    hc server create --name "$SERVER" --type "$SERVER_TYPE_SMALL" --image "$SERVER_IMAGE" \
        --location "$LOCATION" --ssh-key "$NAME" --firewall "$NAME" --volume "$NAME-data" \
        --user-data-from-file "$tmp/cloud-init.yaml" --label orca=true
fi
if [ "$DRY_RUN" = 0 ]; then
    applied=$(hcloud firewall describe "$NAME" -o json | jq -r --arg s "$SERVER" '[.applied_to[]? | select(.type == "server") | .server.id] | length')
    if [ "$applied" = 0 ]; then
        hc firewall apply-to-resource "$NAME" --type server --server "$SERVER"
    fi
else
    echo "+ (if the firewall isn't applied to the server:) hcloud firewall apply-to-resource $NAME --type server --server $SERVER"
fi

IP=$(server_ip)
echo "server $SERVER at $IP"
wait_for_ssh
remote cloud-init status --wait
remote mountpoint -q /mnt/orca-data

# --- settings and secrets (stdin only) --------------------------------------
{
    echo "AWS_REGION=$S3_REGION"
    echo "AWS_ENDPOINT_URL_S3=$S3_ENDPOINT"
    echo "ORCA_WAREHOUSE=s3://$BUCKET/warehouse"
    echo "ORCA_PUBLIC_WAREHOUSE=s3://$BUCKET/warehouse"
    echo "ORCA_LOGS=s3://$BUCKET/logs"
    echo "ORCA_RUN_ENV=ORCA_PUBLIC_WAREHOUSE"
    [ -z "${FIXED_COST:-}" ] || echo "ORCA_FIXED_COST=$FIXED_COST"
    if [ -n "${EXTRA_ENV_FILE:-}" ] && [ -r "$EXTRA_ENV_FILE" ]; then cat "$EXTRA_ENV_FILE"; fi
} > "$tmp/settings.env"
put 600 /etc/orca/settings.env "$tmp/settings.env"
put 600 /etc/orca/s3.env "$S3_CREDENTIALS_FILE"
put 600 /etc/orca/tunnel.env "$TUNNEL_TOKEN_FILE"
if [ -n "${GITHUB_TOKEN_FILE:-}" ]; then
    put 600 /etc/orca/secrets/orca/github-token "$GITHUB_TOKEN_FILE"
fi
if [ -n "${GHCR_USER:-}" ]; then
    put 600 /etc/orca/ghcr-token "$GHCR_TOKEN_FILE"
fi
put 755 /etc/orca/secret-fetch "$HERE/server/secret-fetch"
put 755 /usr/local/sbin/orca-stack "$HERE/server/orca-stack"
put 644 /etc/systemd/system/orca-backup.service "$HERE/server/orca-backup.service"
put 644 /etc/systemd/system/orca-backup.timer "$HERE/server/orca-backup.timer"

# --- stack settings; the tag is only set by the first up (deploy.sh moves it) --
remote orca-stack configure ORCA_IMAGE="$ORCA_IMAGE" CLOUDFLARED_IMAGE="$CLOUDFLARED_IMAGE" \
    BUCKET="$BUCKET" BACKUP_KEEP="$BACKUP_KEEP" ${GHCR_USER:+GHCR_USER="$GHCR_USER"}
remote orca-stack configure --keep-existing ORCA_TAG="${ORCA_TAG:-latest}"
for app in $APPS; do
    remote orca-stack app "${app%%:*}" "${app#*:}"
done
remote systemctl daemon-reload
remote systemctl enable --now orca-backup.timer

# --- restore, then start everything ------------------------------------------
if [ -n "$restore" ]; then
    remote orca-stack apply --db-only
    remote orca-stack restore "$restore"
fi
remote orca-stack apply ${force:+"$force"}

cat << EOS

up: done. Next (once per new database):
  - Cloudflare: the tunnel's public hostnames point at http://ui:8270 and the apps.
  - ssh root@$IP, then: docker exec -it scheduler orca user add <name>
  - register the pipelines: see docs/hetzner.md
EOS
