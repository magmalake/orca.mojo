#!/bin/sh
# Rust workers (docs/rust-workers.md, #392): the morca-worker crate's tests,
# then examples/rust-worker, a Rust worker and its Mojo proxy pipeline, over
# a local warehouse:
#
# - cargo test: the crate's ABI and schema rule, and the example's check of
#   its schema against schemas/numbers.json;
# - the library exports the four morca_worker_* functions and nothing else,
#   with Rust's standard library linked in;
# - a run writes the worker's rows in one commit, with its log lines;
# - a panic in the stream, and a schema that doesn't fit the table, fail
#   the attempt, naming the panic or the column, and commit nothing;
# - the stop flag ends the worker's stream early, and what came is
#   committed;
# - a run whose process dies after the commit is resumed: the retry finds
#   the commit, the worker doesn't run again, and the table has one
#   snapshot of the run.
#
# Run in the rust environment (pixi run -e rust test-rust-worker), which has
# cargo and the data environment's tins.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build/rust-worker
export CARGO_TARGET_DIR=$PWD/build/cargo
log=build/rust-worker/test.log
: > "$log"
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    exit 1
}

cargo test --quiet --manifest-path rust/morca-worker/Cargo.toml >> "$log" 2>&1 || fail "cargo test of morca-worker"
cargo test --quiet --manifest-path examples/rust-worker/Cargo.toml >> "$log" 2>&1 || fail "cargo test of the example"
cargo build --quiet --release --manifest-path examples/rust-worker/Cargo.toml >> "$log" 2>&1 || fail "cargo build"
echo "ok   cargo test, and the worker builds"

case $(uname) in
Darwin)
    lib=$CARGO_TARGET_DIR/release/libnumbers_worker.dylib
    syms=$(nm -gU "$lib" | awk '{print $3}' | sed 's/^_//')
    deps=$(otool -L "$lib")
    ;;
*)
    lib=$CARGO_TARGET_DIR/release/libnumbers_worker.so
    syms=$(nm -D --defined-only "$lib" | awk '{print $3}')
    deps=$(ldd "$lib")
    ;;
esac
want="morca_worker_abi_version
morca_worker_describe
morca_worker_free
morca_worker_run"
[ "$(echo "$syms" | sort)" = "$want" ] || fail "the library exports: $syms"
echo "$deps" | grep -q libstd && fail "the library links Rust's std dynamically: $deps"
echo "ok   the library exports only morca_worker_*, std linked in"

scripts/embed-json.sh examples/rust-worker/schemas/numbers.json NUMBERS_SCHEMA > build/rust-worker/numbers_schema.mojo
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src -I build/rust-worker $DEP_INC examples/rust-worker/numbers.mojo -o build/numbers
bin=./build/numbers

export MORCA_WAREHOUSE=$PWD/build/rust-worker/wh NUMBERS_LIB=$lib
unset MORCA_DATABASE_URL MORCA_LOGS
rm -rf "$MORCA_WAREHOUSE"
out=build/rust-worker/run.out
rows() { "$bin" check | sed -n 's/^rows \([0-9]*\) .*/\1/p'; }
run() {
    month=$1; shift
    env "$@" "$bin" run --partitions "$month" > "$out" 2>&1
}

# --- success ---
run 2024-01 NUMBERS_PARAMS='{"rows": 1000, "batch_rows": 100}' || { cat "$out" >> "$log"; fail "the run failed"; }
cat "$out" >> "$log"
grep -q "numbers worker 0.1.0: 1000 rows for 2024-01, attempt 1" "$out" || fail "no log line from the worker"
grep -q "progress 1000/1000" "$out" || fail "no progress line"
grep -q "numbers: 1000 rows, snapshot [0-9]*, stopped=False, resumed=False" "$out" || fail "the step's result"
[ "$(rows)" = 1000 ] || fail "rows: $(rows)"
"$bin" check | grep -qx "ids 499500" || fail "the ids: $("$bin" check)"
echo "ok   a run commits the worker's 1000 rows, with its logs and progress"

# --- a panic, and schemas that don't fit: nothing committed ---
run 2024-02 NUMBERS_PARAMS='{"panic": "batch"}' && fail "a panic didn't fail the run"
cat "$out" >> "$log"
grep -q "the Rust worker .* failed: .*the worker panicked: asked to panic in batch 1" "$out" || fail "the panic's message"
run 2024-02 NUMBERS_PARAMS='{"panic": "run"}' && fail "a panic in run didn't fail the run"
cat "$out" >> "$log"
grep -q "the Rust worker .* failed: the worker panicked: asked to panic in run" "$out" || fail "the panic in run's message"
[ "$(rows)" = 1000 ] || fail "a failed run committed: $(rows) rows"
echo "ok   a panic fails the attempt with its message, and commits nothing"

for m in type:"column square is string in the worker's schema, which doesn't fit the table's double" \
    extra:"column cube is in the worker's schema but not in the table" \
    missing:"column partition is in the table but not in the worker's schema"; do
    run 2024-03 NUMBERS_PARAMS="{\"mismatch\": \"${m%%:*}\"}" && fail "mismatch ${m%%:*} didn't fail"
    cat "$out" >> "$log"
    grep -qi "doesn't fit demo.numbers: ${m#*:}" "$out" || fail "mismatch ${m%%:*}: $(grep -i fit "$out" | head -3)"
done
[ "$(rows)" = 1000 ] || fail "a mismatch committed: $(rows) rows"
echo "ok   a schema that doesn't fit fails before writing, naming the column"

# --- a stop ---
stop=$PWD/build/rust-worker/stop
rm -f "$stop"
run 2024-04 NUMBERS_STOP_FILE="$stop" NUMBERS_PARAMS='{"rows": 100000, "batch_rows": 100, "sleep_ms": 100}' &
pid=$!
i=0
until grep -q "batch 3" "$out" 2> /dev/null; do
    i=$((i + 1)); [ "$i" -lt 300 ] || { kill "$pid"; fail "the slow worker didn't start"; }
    sleep 0.1
done
touch "$stop"
wait "$pid" || { cat "$out" >> "$log"; fail "the stopped run failed"; }
cat "$out" >> "$log"
grep -q "stopping after [0-9]* rows, as morca asks" "$out" || fail "the worker didn't stop"
got=$(sed -n 's/.*numbers: \([0-9]*\) rows, snapshot [0-9]*, stopped=True.*/\1/p' "$out")
[ -n "$got" ] && [ "$got" -gt 0 ] && [ "$got" -lt 100000 ] || fail "the stopped run's rows: [$got]"
[ "$(rows)" = $((1000 + got)) ] || fail "rows after the stop: $(rows), not $((1000 + got))"
echo "ok   the stop flag ends the stream early: $got rows committed"

# --- a crash after the commit, resumed ---
export MORCA_DATABASE_URL=sqlite:$PWD/build/rust-worker/runs.db MORCA_STALE_SECONDS=1
rm -f build/rust-worker/runs.db* build/rust-worker/crashed
run 2024-05 NUMBERS_CRASH_ONCE="$PWD/build/rust-worker/crashed" NUMBERS_PARAMS='{"rows": 500}' \
    && fail "the run didn't crash"
cat "$out" >> "$log"
[ -f build/rust-worker/crashed ] || fail "the step didn't get to its crash"
r=$(sed -n 's/^run \([0-9][0-9]*\)$/\1/p' "$out" | head -1)
[ -n "$r" ] || fail "no run number"
[ "$(rows)" = $((1500 + got)) ] || fail "rows after the crash: $(rows)"
sleep 1.5
"$bin" resume "$r" > "$out" 2>&1 || { cat "$out" >> "$log"; fail "the resume failed"; }
cat "$out" >> "$log"
grep -q "the worker's output is committed already; it doesn't run again" "$out" || fail "the retry didn't find its commit"
grep -q "numbers: 0 rows, snapshot [0-9]*, stopped=False, resumed=True" "$out" || fail "the resumed step's result"
grep -q "numbers worker 0.1.0" "$out" && fail "the worker ran again"
[ "$("$bin" check "$r" | head -1)" = "rows $((1500 + got)) snapshots 1" ] || fail "after the resume: $("$bin" check "$r")"
echo "ok   a crash after the commit is resumed: the commit is found, and the worker doesn't run again"
