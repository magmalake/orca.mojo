#!/bin/sh
# Runs a command with a throwaway PostgreSQL server and ORCA_DATABASE_URL
# pointing at it, then stops the server and removes its files however the
# command exits. Unlike postgres.mojo's with-pg-server.sh, a server that
# won't start fails the run instead of skipping it.
#
# The server listens on a Unix socket only, in a short directory under
# ${TMPDIR:-/tmp} (macOS caps socket paths at 104 bytes), so no TCP port has
# to be free. initdb, pg_ctl and createdb come from conda-forge's postgresql.
#
# fsync is off (a crashed server process still leaves its writes in the OS
# cache), but synchronous_commit stays on: with it off, a server killed by
# tests/chaos.sh's restarts loses transactions it had already acknowledged,
# such as a run's creation. orca's database must never run with it off.
#
# Usage: with-postgres.sh <command...>
set -eu

if [ "$#" -eq 0 ]; then
    echo "usage: with-postgres.sh <command...>" >&2
    exit 2
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/orcapg.XXXXXX")
cleanup() {
    pg_ctl -D "$work/data" -m immediate stop > /dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT INT TERM

if ! initdb -D "$work/data" --auth=trust --username=postgres -E UTF8 \
        --locale=C > "$work/initdb.log" 2>&1; then
    tail -n 30 "$work/initdb.log" >&2
    exit 1
fi
if ! pg_ctl -D "$work/data" -w -l "$work/server.log" -o \
        "-k $work -c listen_addresses= -c fsync=off" \
        start > /dev/null; then
    tail -n 30 "$work/server.log" >&2
    exit 1
fi
createdb -h "$work" -U postgres orca

ORCA_DATABASE_URL="postgresql:///orca?host=$work&user=postgres"
# The data directory, for tests that restart the server (pg_ctl restart
# keeps the options it was started with).
ORCA_PGDATA="$work/data"
export ORCA_DATABASE_URL ORCA_PGDATA
set +e
"$@"
status=$?
set -e
exit "$status"
