#!/bin/sh
# Retries after `orca register` of a new build (examples/toy.mojo): a failed
# run with nothing done moves to the new code and is retried; one with steps
# done by the old code fails at once, saying how to resume it, instead of
# being started into a `resume` that refuses it until its retries are
# spent; and a process that exits before claiming its run fails the run
# with its exit and last line. Needs ORCA_DATABASE_URL (pixi run
# test-new-code starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/new-code.log
: > "$log"
export ORCA_LOGS=build/new-code-logs ORCA_STALE_SECONDS=2 ORCA_RETRY_SECONDS=60
rm -rf "$ORCA_LOGS"
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -qAtc "$1"; }
tick() { ORCA_NOW="$1" ./build/orca tick >> "$log" 2>&1 || fail "tick at $1 exited $?"; }
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")" -gt 0 ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "runs didn't finish"; sleep 0.1
    done
}
hash=$(./build/toy describe | jq -r .registry_hash)

ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
# January fails at measure, after days succeeded.
ORCA_TOY_FAIL=2024-01 ORCA_NOW=2024-02-02T03:00Z ./build/orca tick >> "$log" 2>&1
settle
old=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-01'")
[ "$(q "SELECT status FROM orca.runs WHERE id = $old")" = failed ] || fail "2024-01 didn't fail"
# Both ran an older build: January with a step done, March with none (its
# process died before its first step).
q "UPDATE orca.runs SET registry_hash = '0xold' WHERE id = $old" > /dev/null
empty=$(q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, starts, ended) VALUES ('toy', '0xold', '2024-03', 'backfill', 'failed', 1, now()) RETURNING id")

# The new build is registered while both wait for their retries.
ORCA_NOW=2024-02-02T03:00:10Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register again"
[ "$(q "SELECT registry_hash FROM orca.runs WHERE id = $empty")" = "$hash" ] \
    || fail "the failed run with nothing done kept the old code"
[ "$(q "SELECT registry_hash FROM orca.runs WHERE id = $old")" = 0xold ] \
    || fail "the failed run with a step done was moved to the new code"
mark=$(wc -l < "$log")

# Due: March is retried with the new code and succeeds; January isn't
# started, and fails saying why.
tick 2024-02-02T03:00:20Z
tick 2024-02-02T03:10Z
settle
tick 2024-02-02T03:20Z
settle
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $empty")" = "succeeded 2" ] \
    || fail "the moved run: $(q "SELECT status, starts FROM orca.runs WHERE id = $empty")"
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $old")" = "failed 1" ] \
    || fail "the old code's run was started: $(q "SELECT status, starts FROM orca.runs WHERE id = $old")"
! tail -n "+$((mark + 1))" "$log" | grep -q "started toy run $old " || fail "the old code's run was started"
q "SELECT error FROM orca.runs WHERE id = $old" | grep -q "its code is 0xold, and the pipeline's binary is now $hash.*resume $old --allow-new-code" \
    || fail "the old code's run's error: $(q "SELECT error FROM orca.runs WHERE id = $old")"
./build/toy status "$old" | grep -q "^error: not started: its code is 0xold" \
    || fail "status doesn't show the run's error"
# So do the run's page and its JSON.
free_port port
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pid=$!
i=0
until curl -fs "http://127.0.0.1:$port/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
curl -fsS -H 'Accept: application/json' "http://127.0.0.1:$port/runs/$old" | jq -e '.run.error | startswith("not started: its code is 0xold")' > /dev/null \
    || fail "the run's JSON has no error"
curl -fsS "http://127.0.0.1:$port/runs/$old" | grep -q "<dt>Error<span class=\"help\">.*</dt><dd>not started: its code is 0xold" \
    || fail "the run's page has no error"
kill "$pid"
wait "$pid" 2> /dev/null || true
pid=
# Resumed with the new code, it runs and its error is gone.
q "UPDATE orca.runs SET status = 'queued', allow_new_code = true, error = NULL WHERE id = $old" > /dev/null
tick 2024-02-02T03:30Z
settle
[ "$(q "SELECT status || ' ' || coalesce(error, '') FROM orca.runs WHERE id = $old")" = "succeeded " ] \
    || fail "the confirmed resume: $(q "SELECT status, error FROM orca.runs WHERE id = $old")"

# A binary that exits before claiming its run: the scheduler records why
# when it reaps the process, instead of waiting for the run to look dead.
cat > build/new-code-refuses <<'SH'
#!/bin/sh
echo "orca: this binary refuses run $2"
exit 3
SH
chmod +x build/new-code-refuses
q "UPDATE orca.pipelines SET binary_path = '$PWD/build/new-code-refuses' WHERE name = 'toy'" > /dev/null
out=$(ORCA_NOW=2024-02-02T03:40Z ./build/orca backfill toy 2024-05) || fail "backfill exited $?"
[ "$out" = "queued 1 runs" ] || fail "backfill: $out"
run=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-05'")
ORCA_NOW=2024-02-02T03:40Z ./build/orca scheduler >> "$log" 2>&1 &
pid=$!
i=0
until [ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = failed ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the refused run didn't fail"; sleep 0.1
done
kill "$pid"
wait "$pid" 2> /dev/null || true
pid=
q "SELECT error FROM orca.runs WHERE id = $run" | grep -q "^its process (pid [0-9]*) exited 3 before claiming it: orca: this binary refuses run $run$" \
    || fail "the refused run's error: $(q "SELECT error FROM orca.runs WHERE id = $run")"
[ "$(q "SELECT starts FROM orca.runs WHERE id = $run")" -eq 1 ] || fail "the refused run was restarted at once"
echo "ok   new code: retries move to it or fail saying why, and so does a process that exits early"
