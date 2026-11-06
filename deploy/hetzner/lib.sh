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
    CLOUDFLARED_IMAGE=${CLOUDFLARED_IMAGE:-cloudflare/cloudflared:latest}
    APPS=${APPS:-console:8080}
    BACKUP_KEEP=${BACKUP_KEEP:-14}
    SERVER=$NAME
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
