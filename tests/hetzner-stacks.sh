#!/bin/sh
# Two stacks from deploy/hetzner, one settings file each (docs/hetzner.md,
# A dev stack), against tests/hetzner's fakes:
#
# - up.sh and down.sh --all with NAME=orca-dev name only orca-dev's server,
#   firewall, Volume and ssh key, never the production stack's (orca), and
#   every script says which stack and settings file it acts on;
# - the settings file's HCLOUD_CONTEXT reaches hcloud;
# - same_ssh_key finds the other stack's key with the same public key
#   (Hetzner takes a public key once per project), and nothing for another;
# - up.sh writes the settings with lib.sh's sync_settings (deploy.sh's too),
#   then ORCA_TAG only if the server has none;
# - an ORCA_CONFIG in the settings (EXTRA_ENV_FILE's lines end up in
#   settings.env) is in the scheduler's env file, which `orca register`
#   runs with.
set -eu
cd "$(dirname "$0")/.."
FAKE=$(mktemp -d "${TMPDIR:-/tmp}/orca-hetzner.XXXXXX")
export FAKE
trap 'rm -rf "$FAKE"' EXIT INT TERM
export PATH="$PWD/tests/hetzner:$PATH"
XDG_STATE_HOME="$FAKE/xdg"
export XDG_STATE_HOME

fail() {
    echo "FAIL: $*"
    [ ! -f "$FAKE/out" ] || tail -20 "$FAKE/out"
    exit 1
}
has() { grep -q -- "$1" "$FAKE/out" || fail "no '$1' in the output"; }

cat > "$FAKE/dev.env" << EOS
NAME=orca-dev
HCLOUD_CONTEXT=orca-dev
BUCKET=orca-dev-bkt
S3_ENDPOINT=https://fsn1.example.invalid
S3_REGION=fsn1
ADMIN_IP=203.0.113.7/32
SSH_PUBLIC_KEY_FILE=$FAKE/k.pub
S3_CREDENTIALS_FILE=$FAKE/s3.env
TUNNEL_TOKEN_FILE=$FAKE/tunnel.env
EXTRA_ENV_FILE=$FAKE/extra.env
EOS
echo ORCA_CONFIG=/opt/orca/orca.dev.toml > "$FAKE/extra.env"

# up.sh: every Hetzner resource is orca-dev's.
HETZNER_ENV="$FAKE/dev.env" sh deploy/hetzner/up.sh --dry-run > "$FAKE/out" 2>&1 || fail "up.sh --dry-run"
has "up.sh: stack orca-dev (hcloud context orca-dev), settings $FAKE/dev.env"
has "+ hcloud ssh-key create --name orca-dev "
has "+ hcloud firewall create --name orca-dev "
has "+ hcloud volume create --name orca-dev-data "
has "+ hcloud server create --name orca-dev .* --ssh-key orca-dev --firewall orca-dev --volume orca-dev-data "
grep '^+ hcloud' "$FAKE/out" | grep -E '(^| )orca( |$)|[ ]orca-data( |$)' && fail "up.sh names the production stack's resources"
# Its settings, as deploy.sh writes them too (lib.sh's sync_settings), then
# the tag only if the server has none.
has "+ ssh root@<server-ip> orca-stack sync --keys settings.env < "
has "+ ssh root@<server-ip> orca-stack sync tunnel.env < $FAKE/tunnel.env"
has "+ ssh root@<server-ip> orca-stack configure ORCA_IMAGE=ghcr.io/morca-dev/census .*BUCKET=orca-dev-bkt BACKUP_KEEP=14 GHCR_USER=$"
has "+ ssh root@<server-ip> orca-stack configure --keep-existing ORCA_TAG=latest"
has "# settings.env from $FAKE/dev.env: AWS_REGION .* ORCA_CONFIG"
# down.sh --all: the same.
HETZNER_ENV="$FAKE/dev.env" sh deploy/hetzner/down.sh --dry-run --all > "$FAKE/out" 2>&1 || fail "down.sh --dry-run --all"
has "+ hcloud server delete orca-dev$"
has "+ hcloud volume delete orca-dev-data$"
has "+ hcloud firewall delete orca-dev$"
has "+ hcloud ssh-key delete orca-dev$"
grep '^+ hcloud' "$FAKE/out" | grep -E '(^| )orca( |$)|[ ]orca-data( |$)' && fail "down.sh names the production stack's resources"

# HCLOUD_CONTEXT reaches hcloud (deploy.sh's server ip, a real call).
printf 'NAME=orca\nHCLOUD_CONTEXT=prod-ctx\n' > "$FAKE/prod.env"
: > "$FAKE/hcloud.log"
HETZNER_ENV="$FAKE/prod.env" sh deploy/hetzner/backup.sh > "$FAKE/out" 2>&1 || true
grep -q '^prod-ctx: hcloud server ip orca$' "$FAKE/hcloud.log" || fail "hcloud didn't get HCLOUD_CONTEXT: $(cat "$FAKE/hcloud.log")"

# same_ssh_key: the key of another stack with the same public key.
ssh-keygen -q -t ed25519 -N '' -C test -f "$FAKE/k"
ssh-keygen -q -t ed25519 -N '' -C test -f "$FAKE/other"
fp=$(ssh-keygen -l -E md5 -f "$FAKE/k.pub" | awk '{print $2}')
fp=${fp#MD5:}
printf '[{"id": 1, "name": "laptop", "fingerprint": "00:11"}, {"id": 2, "name": "orca", "fingerprint": "%s"}]\n' "$fp" > "$FAKE/ssh-keys.json"
lib() { sh -c '. deploy/hetzner/lib.sh; same_ssh_key "$1"' sh "$1"; }
[ "$(lib "$FAKE/k.pub")" = orca ] || fail "same_ssh_key: '$(lib "$FAKE/k.pub")', not orca"
[ -z "$(lib "$FAKE/other.pub")" ] || fail "same_ssh_key found a key for another public key"
[ -z "$(lib "$FAKE/missing.pub")" ] || fail "same_ssh_key found a key for no file"

# ORCA_CONFIG from the settings is in the scheduler's env file.
export ORCA_STACK_TEST=1 ORCA_CONF="$FAKE/conf" ORCA_DATA="$FAKE/data" ORCA_STATE="$FAKE/state"
mkdir -p "$ORCA_CONF"
{
    printf 'AWS_REGION=fsn1\nAWS_ENDPOINT_URL_S3=https://example.invalid\n'
    cat "$FAKE/extra.env"
} > "$ORCA_CONF/settings.env"
printf 'AWS_ACCESS_KEY_ID=x\nAWS_SECRET_ACCESS_KEY=y\n' > "$ORCA_CONF/s3.env"
stack() { sh deploy/hetzner/server/orca-stack "$@"; }
stack configure ORCA_IMAGE=img ORCA_TAG=aaa1111 BUCKET=orca-dev-bkt > /dev/null
stack apply > "$FAKE/out" 2>&1 || fail "orca-stack apply"
grep -qx 'ORCA_CONFIG=/opt/orca/orca.dev.toml' "$ORCA_CONF/orca.env" || fail "no ORCA_CONFIG in orca.env"
grep -q "run -d --name scheduler .*--env-file $ORCA_CONF/orca.env" "$FAKE/docker.log" || fail "the scheduler doesn't get orca.env"

echo "ok   hetzner-stacks"
