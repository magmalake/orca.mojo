#!/bin/sh
# backup.sh [--dry-run] [list]
#
# A pg_dump of the database (the morca schema, custom format, as AWS's
# backups) to s3://BUCKET/backups/pg/<time>.dump, checked for readability,
# keeping the newest BACKUP_KEEP (14). The server does this every night at
# 02:30 UTC by itself (orca-backup.timer); this is for before a risky step.
# `backup.sh list` lists them.
set -eu
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

parse_common "$@"
load_env
need_tool hcloud ssh
IP=$(server_ip)
case "${REST# }" in
    "") remote morca-stack backup ;;
    list) remote morca-stack list-backups ;;
    *) die "usage: backup.sh [--dry-run] [list]" ;;
esac
