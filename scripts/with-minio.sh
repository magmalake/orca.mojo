#!/bin/sh
# Runs a command with a throwaway MinIO server and the AWS_* environment
# pointing at it, with a bucket `orca` made, then stops the server and
# removes its files however the command exits. A server that won't start
# fails the run instead of skipping it.
#
# MinIO is conda-forge's minio-server (the last community release; dl.min.io
# serves no binaries any more). It listens on a free port on 127.0.0.1, and
# the bucket is made with a SigV4-signed PUT from curl. ORCA_S3_BUCKET is
# set to s3://orca.
#
# Usage: with-minio.sh <command...>
set -eu
. "$(dirname "$0")/../tests/port.sh"

if [ "$#" -eq 0 ]; then
    echo "usage: with-minio.sh <command...>" >&2
    exit 2
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/orcaminio.XXXXXX")
pid=
cleanup() {
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT INT TERM

free_port port
free_port console
export MINIO_ROOT_USER=orca-test MINIO_ROOT_PASSWORD=orca-test-secret
minio server "$work/data" --address "127.0.0.1:$port" --console-address "127.0.0.1:$console" \
    > "$work/server.log" 2>&1 &
pid=$!
i=0
until curl -fs "http://127.0.0.1:$port/minio/health/ready" > /dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 100 ] || ! kill -0 "$pid" 2> /dev/null; then
        tail -n 30 "$work/server.log" >&2
        exit 1
    fi
    sleep 0.1
done

export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD
export AWS_REGION=us-east-1 AWS_ENDPOINT_URL="http://127.0.0.1:$port" AWS_S3_FORCE_PATH_STYLE=true
# Ready can still answer 503 to the first PUT while MinIO settles: retry
# for a few seconds before giving up.
i=0
until curl -fsS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
    -X PUT "$AWS_ENDPOINT_URL/orca" > /dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 50 ]; then
        tail -n 30 "$work/server.log" >&2
        exit 1
    fi
    sleep 0.2
done
export ORCA_S3_BUCKET=s3://orca

"$@"
