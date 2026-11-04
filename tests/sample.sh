#!/bin/sh
# The sampler (orca.sample), on the machine the tests run on:
#
# - beside a process, it writes a line a second at first, then one per
#   interval, with sane values and that process's resident set, and ends
#   after a last line once the process has;
# - a process shorter than an interval still leaves a line;
# - a sampler that can't write carries on and exits 0;
# - told to stop with SIGTERM or --until, it writes a last line and exits 0;
# - `--out -` prints prefixed lines;
# - its own CPU, at the default 5 s, is under 0.5% of one core.
#
# With ORCA_TEST_S3 set (pixi run -e data test-sample-s3 starts a throwaway
# MinIO), its lines also go to the bucket, uploaded while it runs and in
# full when it ends.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca
dir=build/sample
rm -rf "$dir"
mkdir -p "$dir"
fail() {
    echo "FAIL: $*"
    exit 1
}
started=
cleanup() { [ -z "$started" ] || kill $started 2> /dev/null || true; }
trap cleanup EXIT

# Beside a 3-second process that holds 50 MB.
python3 -c "import time; b = bytearray(50_000_000); time.sleep(3)" &
p=$!
started=$p
t0=$(date +%s)
./build/orca sample --out "$dir/a.jsonl" --pid "$p" --seconds 0.5 || fail "the sampler exited $?"
t1=$(date +%s)
kill -0 "$p" 2> /dev/null && fail "the sampler ended before its process"
[ $((t1 - t0)) -le 6 ] || fail "the sampler took $((t1 - t0)) s to notice its process ended"
python3 tests/metrics_ok.py --min 4 "$dir/a.jsonl" > /dev/null || fail "a.jsonl"
rss=$(tail -2 "$dir/a.jsonl" | head -1 | python3 -c "import json, sys; print(json.load(sys.stdin)['mem']['rss'])")
[ "$rss" -gt 50000000 ] || fail "the process's resident set: $rss"
echo "ok   beside a process: $(wc -l < "$dir/a.jsonl" | tr -d ' ') lines, its 50 MB resident, ended with it"

# Shorter than an interval.
sleep 0.3 &
./build/orca sample --out "$dir/b.jsonl" --pid $! --seconds 5 || fail "the short sampler exited $?"
python3 tests/metrics_ok.py --min 1 "$dir/b.jsonl" > /dev/null || fail "b.jsonl"
echo "ok   a process shorter than an interval leaves a line"

# Somewhere it can't write: under a regular file.
: > "$dir/file"
sleep 1 &
out=$(./build/orca sample --out "$dir/file/c.jsonl" --pid $! --seconds 0.2 2>&1) || fail "a sampler that can't write exited $?"
[ "$(echo "$out" | grep -c "dropped a line")" -eq 1 ] || fail "the sampler said: $out"
echo "ok   a sampler that can't write says so once and exits 0"

# Killed before its first line was due: it writes one, then ends.
./build/orca sample --out "$dir/e.jsonl" --seconds 5 &
s=$!
started=$s
sleep 0.5
kill "$s"
wait "$s" || fail "a sampler told to stop exited $?"
started=
python3 tests/metrics_ok.py --min 1 "$dir/e.jsonl" > /dev/null || fail "e.jsonl"
echo "ok   SIGTERM: a last line, then exit 0"

# Told to stop with --until, even before it has started: a line, then exit.
touch "$dir/stop"
./build/orca sample --out "$dir/f.jsonl" --until "$dir/stop" --seconds 5 || fail "--until exited $?"
python3 tests/metrics_ok.py --min 1 "$dir/f.jsonl" > /dev/null || fail "f.jsonl"
echo "ok   --until: a last line, then exit 0"

line=$(./build/orca sample --out - --count 1 --seconds 0.2)
case "$line" in "orca-sample {"*"}") ;; *) fail "--out -: $line" ;; esac
echo "ok   --out - prints prefixed lines"

# Its overhead: CPU per line, from its own count, at the default interval.
sleep 4 &
./build/orca sample --out "$dir/d.jsonl" --pid $! --seconds 0.1
python3 - "$dir/d.jsonl" << 'PY' || fail "overhead"
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1])]
per_line = lines[-1]["self_cpu"] / len(lines)
# Lines at 5 s, plus 4 polls a second of the process it watches.
share = per_line / 5
print(f"ok   {per_line * 1000:.2f} ms of CPU a line: {share * 100:.3f}% of a core at 5 s")
assert share < 0.005, share
PY

if [ -n "${ORCA_TEST_S3:-}" ]; then
    # shellcheck disable=SC2086
    mojo build -I src $DEP_INC tests/s3cat.mojo -o build/s3cat
    loc="$ORCA_S3_BUCKET/sample-$$/metrics/local.jsonl"
    sleep 8 &
    p=$!
    ORCA_LOG_STAGE=$PWD/$dir/stage ORCA_SAMPLE_UPLOAD_SECONDS=1 \
        ./build/orca sample --out "$loc" --pid "$p" --seconds 0.5 &
    s=$!
    started="$p $s"
    n=0
    until [ "$n" -ge 2 ]; do
        kill -0 "$p" 2> /dev/null || fail "nothing uploaded while it ran"
        sleep 0.5
        n=$(./build/s3cat get "$loc" 2> /dev/null | wc -l | tr -d ' ')
    done
    wait "$s" || fail "the S3 sampler exited $?"
    started=
    ./build/s3cat get "$loc" > "$dir/s3.jsonl"
    cmp -s "$dir/s3.jsonl" "$dir/stage/orca/sample-$$/metrics/local.jsonl" || fail "the bucket's copy isn't all of it"
    python3 tests/metrics_ok.py --min 6 "$dir/s3.jsonl" > /dev/null || fail "s3.jsonl"
    echo "ok   in the bucket while it runs ($n lines), and all of it at the end"
fi
echo "ok   sample${ORCA_TEST_S3:+ on S3}"
