# shellcheck shell=sh
# Shared by the scripts in this directory (sourced, not run). POSIX sh.
#
# - the settings: $HETZNER_ENV, else hetzner.env next to the scripts
#   (hetzner.env.example lists every one);
# - --dry-run: every hcloud and ssh command is printed, none is run, and
#   nothing is created, changed or read from the server;
# - nothing here prints a secret: files that hold one are only ever sent to
#   the server on ssh's standard input.

HERE=$(cd "$(dirname "$0")" && pwd)
DRY_RUN=0
YES=0

die() {
    echo "$(basename "$0"): $*" >&2
    exit 1
}

load_env() {
    HETZNER_ENV=${HETZNER_ENV:-$HERE/hetzner.env}
    [ -f "$HETZNER_ENV" ] || die "no $HETZNER_ENV (copy hetzner.env.example and fill it in, or set HETZNER_ENV)"
    # shellcheck disable=SC1090
    . "$HETZNER_ENV"
    NAME=${NAME:-orca}
    LOCATION=${LOCATION:-fsn1}
    SERVER_TYPE_SMALL=${SERVER_TYPE_SMALL:-cax21}
    SERVER_TYPE_BIG=${SERVER_TYPE_BIG:-cax41}
    SERVER_IMAGE=${SERVER_IMAGE:-ubuntu-24.04}
    VOLUME_SIZE_GB=${VOLUME_SIZE_GB:-50}
    ORCA_IMAGE=${ORCA_IMAGE:-ghcr.io/magmalake/orca}
    CLOUDFLARED_IMAGE=${CLOUDFLARED_IMAGE:-cloudflare/cloudflared:2026.10.7}
    APPS=${APPS:-console:8080}
    BACKUP_KEEP=${BACKUP_KEEP:-14}
    SERVER=$NAME
    # The settings file may name the hcloud context (the Hetzner project) the
    # stack is in; otherwise hcloud's active one.
    [ -z "${HCLOUD_CONTEXT:-}" ] || export HCLOUD_CONTEXT
    # Which stack, as there may be more than one (docs/hetzner.md, A dev stack).
    echo "$(basename "$0"): stack $NAME${HCLOUD_CONTEXT:+ (hcloud context $HCLOUD_CONTEXT)}, settings $HETZNER_ENV" >&2
}

# need VAR...: each is set in the settings file.
need() {
    for v in "$@"; do
        eval "val=\${$v:-}"
        [ -n "$val" ] || die "$v isn't set in $HETZNER_ENV"
    done
}

# need_file VAR...: each names a readable file (a warning in a dry run).
need_file() {
    for v in "$@"; do
        eval "val=\${$v:-}"
        [ -n "$val" ] || die "$v isn't set in $HETZNER_ENV"
        if [ ! -r "$val" ]; then
            if [ "$DRY_RUN" = 1 ]; then
                echo "# warning: $v ($val) isn't a readable file" >&2
            else
                die "$v ($val) isn't a readable file"
            fi
        fi
    done
}

need_tool() {
    for t in "$@"; do
        command -v "$t" > /dev/null 2>&1 || [ "$DRY_RUN" = 1 ] || die "$t isn't installed"
    done
}

# run CMD ARGS...: runs it, or prints it in a dry run.
run() {
    if [ "$DRY_RUN" = 1 ]; then
        printf '+'
        for a in "$@"; do printf ' %s' "$a"; done
        printf '\n'
    else
        "$@"
    fi
}

hc() {
    run hcloud "$@"
}

# exists KIND NAME: whether hcloud has it (never, in a dry run: so the
# creation commands are printed).
exists() {
    [ "$DRY_RUN" = 0 ] && hcloud "$1" describe "$2" > /dev/null 2>&1
}

confirm() { # QUESTION: yes with --yes or a typed "yes"
    [ "$YES" = 1 ] && return 0
    [ "$DRY_RUN" = 0 ] || return 0
    printf '%s Type yes to go on: ' "$1" >&2
    read -r answer
    [ "$answer" = yes ] || die "not confirmed"
}

# same_ssh_key FILE: the name of the project's ssh key whose public key is
# FILE's, if it has one. Hetzner refuses a second key with the same public
# key, so a second stack in the project (NAME=orca-dev) uses the first's.
same_ssh_key() {
    fp=$(ssh-keygen -l -E md5 -f "$1" 2> /dev/null | awk '{print $2}')
    fp=${fp#MD5:}
    [ -n "$fp" ] || return 0
    hcloud ssh-key list -o json | jq -r --arg fp "$fp" '[.[] | select(.fingerprint == $fp) | .name][0] // empty'
}

server_ip() {
    if [ "$DRY_RUN" = 1 ]; then
        echo "<server-ip>"
    else
        hcloud server ip "$SERVER"
    fi
}

KNOWN_HOSTS=${XDG_STATE_HOME:-$HOME/.local/state}/orca-hetzner/known_hosts

# remote ARGS...: ssh to the server as root; the arguments are the remote command.
remote() {
    if [ "$DRY_RUN" = 1 ]; then
        echo "+ ssh root@$IP $*"
        return 0
    fi
    mkdir -p "$(dirname "$KNOWN_HOSTS")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new \
        -o "UserKnownHostsFile=$KNOWN_HOSTS" ${SSH_PRIVATE_KEY_FILE:+-i "$SSH_PRIVATE_KEY_FILE"} \
        "root@$IP" "$@"
}

# put MODE DEST SRC: SRC's content to DEST on the server, over stdin.
put() {
    if [ "$DRY_RUN" = 1 ]; then
        echo "+ ssh root@$IP install -D -m $1 /dev/stdin $2 < $3"
    else
        remote "install -D -m $1 /dev/stdin $2" < "$3"
    fi
}

wait_for_ssh() {
    [ "$DRY_RUN" = 0 ] || return 0
    n=0
    until remote true > /dev/null 2>&1; do
        n=$((n + 1))
        [ "$n" -lt 60 ] || die "no ssh to $IP after 5 minutes"
        sleep 5
    done
}

parse_common() { # sets REST to the arguments not handled here
    REST=
    for a in "$@"; do
        case "$a" in
            --dry-run) DRY_RUN=1 ;;
            --yes | -y) YES=1 ;;
            *) REST="$REST $a" ;;
        esac
    done
}

# --- the server's settings, from hetzner.env (up.sh and deploy.sh) ---------

# need_settings: what settings.env, the secret files and the stack settings
# are made from is set.
need_settings() {
    need BUCKET S3_ENDPOINT S3_REGION
    need_file S3_CREDENTIALS_FILE TUNNEL_TOKEN_FILE
    [ -z "${GITHUB_TOKEN_FILE:-}" ] || need_file GITHUB_TOKEN_FILE
    [ -z "${EXTRA_ENV_FILE:-}" ] || need_file EXTRA_ENV_FILE
    [ -z "${NTFY_TOPIC_FILE:-}" ] || need_file NTFY_TOPIC_FILE
    [ -z "${NTFY_TOKEN_FILE:-}" ] || need_file NTFY_TOKEN_FILE
    [ -z "${GHCR_USER:-}" ] || need_file GHCR_TOKEN_FILE
}

# write_settings FILE: /etc/orca/settings.env as hetzner.env says. No
# secret is in it: those are files of their own, and only their ids are here.
# A setting hetzner.env leaves empty isn't written.
write_settings() {
    {
        echo "AWS_REGION=$S3_REGION"
        echo "AWS_ENDPOINT_URL_S3=$S3_ENDPOINT"
        echo "ORCA_WAREHOUSE=s3://$BUCKET/warehouse"
        echo "ORCA_PUBLIC_WAREHOUSE=s3://$BUCKET/warehouse"
        echo "ORCA_LOGS=s3://$BUCKET/logs"
        echo "ORCA_RUN_ENV=ORCA_PUBLIC_WAREHOUSE"
        # Alerts (ntfy): the topic and token are secret files, only their ids are here.
        if [ -n "${NTFY_TOPIC_FILE:-}" ]; then
            echo "ORCA_NOTIFY_SECRET=orca/ntfy-topic"
            [ -z "${NTFY_URL:-}" ] || echo "ORCA_NOTIFY_URL=$NTFY_URL"
            [ -z "${NTFY_TOKEN_FILE:-}" ] || echo "ORCA_NOTIFY_TOKEN_SECRET=orca/ntfy-token"
            [ -z "${UI_URL:-}" ] || echo "ORCA_UI_URL=$UI_URL"
        fi
        [ -z "${FIXED_COST:-}" ] || echo "ORCA_FIXED_COST=$FIXED_COST"
        [ -z "${POSTHOG_KEY:-}" ] || echo "ORCA_POSTHOG_KEY=$POSTHOG_KEY"
        if [ -n "${EXTRA_ENV_FILE:-}" ] && [ -r "$EXTRA_ENV_FILE" ]; then cat "$EXTRA_ENV_FILE"; fi
    } > "$1"
}

# sync_file [--keys] NAME SRC: SRC becomes /etc/orca/NAME (mode 600) if it
# differs, over ssh's standard input (orca-stack sync, which prints what
# changed: with --keys the keys, never a value).
sync_file() {
    k=
    if [ "$1" = --keys ]; then
        k=--keys
        shift
    fi
    if [ "$DRY_RUN" = 1 ]; then
        echo "+ ssh root@$IP orca-stack sync ${k:+$k }$1 < $2"
    else
        remote orca-stack sync ${k:+"$k"} "$1" < "$2"
    fi
}

# sync_settings DIR: brings the server's settings in line with hetzner.env,
# as up.sh made them: settings.env, the secret files, the stack settings
# (orca-stack configure; not ORCA_TAG, which deploy.sh moves) and the apps.
# Each prints what it changes, keys and file names only (a stack setting
# old -> new: none is a secret). DIR is a local temporary directory. The
# server's orca-stack must be this checkout's (it has sync). Returns
# non-zero at the first step that fails.
sync_settings() {
    write_settings "$1/settings.env" || return 1
    if [ "$DRY_RUN" = 1 ]; then
        echo "# settings.env from $HETZNER_ENV: $(sed -n 's/=.*//p' "$1/settings.env" | tr '\n' ' ')"
    fi
    sync_file --keys settings.env "$1/settings.env" || return 1
    sync_file s3.env "$S3_CREDENTIALS_FILE" || return 1
    sync_file tunnel.env "$TUNNEL_TOKEN_FILE" || return 1
    if [ -n "${GITHUB_TOKEN_FILE:-}" ]; then
        sync_file secrets/orca/github-token "$GITHUB_TOKEN_FILE" || return 1
    fi
    if [ -n "${NTFY_TOPIC_FILE:-}" ]; then
        sync_file secrets/orca/ntfy-topic "$NTFY_TOPIC_FILE" || return 1
    fi
    if [ -n "${NTFY_TOKEN_FILE:-}" ]; then
        sync_file secrets/orca/ntfy-token "$NTFY_TOKEN_FILE" || return 1
    fi
    if [ -n "${GHCR_USER:-}" ]; then
        sync_file ghcr-token "$GHCR_TOKEN_FILE" || return 1
    fi
    # GHCR_USER= (none in hetzner.env) removes it from stack.conf.
    remote orca-stack configure ORCA_IMAGE="$ORCA_IMAGE" CLOUDFLARED_IMAGE="$CLOUDFLARED_IMAGE" \
        BUCKET="$BUCKET" BACKUP_KEEP="$BACKUP_KEEP" GHCR_USER="${GHCR_USER:-}" || return 1
    for app in $APPS; do
        remote orca-stack app "${app%%:*}" "${app#*:}" || return 1
    done
}
