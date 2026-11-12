#!/bin/sh
# Alerts to a person through ntfy (morca.notify, orca.alerts, #184), against a
# stand-in ntfy server (tests/fake_ntfy.py) that records every POST, over toy
# and daily on the local runner. The topic and the token are read as secrets
# (MORCA_SECRET_FETCH), so a leak would be in the log or the pages:
#
# - `morca notify test` is one POST with the title, a priority and tags, to the
#   secret topic, with the token;
# - a run that failed with no retry left is a POST linking to the run; one that
#   will be retried is not, and neither is a second tick;
# - a watchdog trip is one high-priority POST with Title, Priority, Tags and
#   Click headers; the same trip again inside the window is not sent again;
#   unpausing sends one low 'resolved';
# - a fill with no fill run finished in the stall limit, outside its window
#   nothing, inside it one high POST; a fill run finishing resolves it;
# - the control disk over 85% (seeded samples) and a worker quiet while it
#   held runs each send once, and the disk resolves;
# - `morca notify --kind` sends once per kind and subject, `--resolved` ends it;
# - ntfy down: the tick still succeeds at once, the alert waits in Postgres
#   and goes when the server is back;
# - Ops > Control and /api/system say alerts are configured and list them;
# - Ops > Alerts (/ops/alerts, where /ops goes) has a row per alert sent
#   (orca.alerts), the trip resolved by its 'resolved', the disk open; its
#   filters, pages, live fragment and JSON, and morca alerts (and --remote);
# - neither the topic nor the token is anywhere in the log or the pages.
#
# Needs MORCA_DATABASE_URL (pixi run test-notify starts a throwaway server),
# curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/daily
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/notify.log
: > "$log"
proj=$PWD/build/notify-project
posts=$PWD/build/notify-posts.jsonl
rm -rf "$proj" "$posts" "$posts.fail" build/notify-logs
mkdir -p "$proj"
free_port port
free_port uport
base="http://127.0.0.1:$uport"
fake=
ui=
stop() {
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    [ -z "$fake" ] || kill "$fake" 2> /dev/null || true
    ui=
    fake=
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    echo "posts:"
    cat "$posts" 2> /dev/null || true
    stop
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
start_fake() {
    python3 tests/fake_ntfy.py "$port" "$posts" >> "$log" 2>&1 &
    fake=$!
    i=0
    until curl -s -o /dev/null -X POST -d '' "http://127.0.0.1:$port/_up" 2> /dev/null || [ "$i" -ge 50 ]; do
        i=$((i + 1)); sleep 0.1
    done
}
count() { if [ -f "$posts" ]; then wc -l < "$posts" | tr -d ' '; else echo 0; fi; }
last() { tail -n 1 "$posts" | jq -r "$1"; }
expect_posts() {  # expect_posts N WHAT
    [ "$(count)" -eq "$1" ] || fail "$2: $(count) POSTs, not $1"
}
tick() { ./build/morca tick >> "$log" 2>&1 || fail "tick"; }

cat > build/notify-fetch.sh << 'EOS'
#!/bin/sh
case "$1" in
    orca/ntfy-topic) echo "t0pic-SECRETVALUE-9f3a" ;;
    orca/ntfy-token) echo "tk_SECRETTOKEN_77" ;;
    *) exit 1 ;;
esac
EOS
chmod +x build/notify-fetch.sh
export MORCA_SECRET_FETCH=$PWD/build/notify-fetch.sh
export MORCA_NOTIFY_URL="http://127.0.0.1:$port"
export MORCA_NOTIFY_SECRET=orca/ntfy-topic
export MORCA_NOTIFY_TOKEN_SECRET=orca/ntfy-token
export MORCA_UI_URL=https://morca.example.test
export MORCA_WATCHDOG_SECONDS=0 MORCA_SAMPLE_SECONDS=0 MORCA_LOGS=
export MORCA_NOTIFY_TIMEOUT_MS=2000
unset MORCA_NOTIFY_MIN_SECONDS

start_fake
: > "$posts"

# --- morca notify test -------------------------------------------------------
out=$(./build/morca notify test 2>&1) || fail "notify test: $out"
[ "$out" = sent ] || fail "notify test said: $out"
expect_posts 1 "notify test"
[ "$(last .path)" = "/t0pic-SECRETVALUE-9f3a" ] || fail "the topic path: $(last .path)"
[ "$(last '.headers.authorization')" = "Bearer tk_SECRETTOKEN_77" ] || fail "no token"
[ "$(last '.headers.title')" = "morca: test alert" ] || fail "the title: $(last '.headers.title')"
[ "$(last '.headers.priority')" = default ] || fail "the priority"
[ "$(last '.headers.tags')" = white_check_mark ] || fail "the tags"
echo "ok   morca notify test: one POST to the secret topic, with the token"

# --- an unconfigured morca sends nothing --------------------------------------
if env -u MORCA_NOTIFY_URL -u MORCA_NOTIFY_SECRET ./build/morca notify test > /dev/null 2>&1; then
    fail "notify test succeeded with no URL"
fi
expect_posts 1 "notify test with no URL"
echo "ok   with no MORCA_NOTIFY_URL there are no alerts"

# --- registered pipelines ----------------------------------------------------
cat > "$proj/morca.toml" << 'EOS'
[pipelines.daily]
fills = "all"
fill_window = "22..6"
fill_max_runs = 1
EOS
T0=2024-01-08T12:00Z
# toy on the real clock, so it isn't scheduled for the years it missed; daily
# on the clock of 2024, for its range to fill.
./build/morca register --no-config ./build/toy >> "$log" 2>&1 || fail "register toy"
MORCA_NOW="$T0" ./build/morca register --config "$proj/morca.toml" ./build/daily >> "$log" 2>&1 || fail "register daily"
q "UPDATE orca.pipelines SET paused = true WHERE name = 'daily'" > /dev/null

# --- a failed run -------------------------------------------------------------
MORCA_TOY_FAIL=2024-01 ./build/toy run --partitions 2024-01 >> "$log" 2>&1 && fail "the toy run didn't fail"
run=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'toy'")
# One the scheduler will retry (starts 1 of 3): no alert. One with no retry
# left (starts 99): an alert.
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, starts, error, ended)
   SELECT 'toy', registry_hash, '2024-02', 'schedule', 'failed', 1, 'will retry', now() FROM orca.pipelines WHERE name = 'toy'" > /dev/null
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, starts, error, ended)
   SELECT 'toy', registry_hash, '2024-03', 'schedule', 'failed', 99, E'out of retries\nsecond line', now() FROM orca.pipelines WHERE name = 'toy'" > /dev/null
spent=$(q "SELECT max(id) FROM orca.runs WHERE partitions = '2024-03'")
tick
expect_posts 3 "failed runs (the manual run and the one past its retries)"
tail -n 2 "$posts" | jq -e -s --arg manual "https://morca.example.test/runs/$run" --arg spent "https://morca.example.test/runs/$spent" \
    '(map(.headers.click) | sort) == ([$manual, $spent] | sort) and all(.[]; .headers.priority == "default")' > /dev/null \
    || fail "the failed runs' links: $(tail -n 2 "$posts")"
tail -n 2 "$posts" | jq -e -s 'any(.[]; .body | test("toy 2024-03\nout of retries$")) and all(.[]; .body | test("second line") | not)' > /dev/null \
    || fail "the first line of the error: $(tail -n 2 "$posts" | jq -r .body)"
tick
expect_posts 3 "a second tick over the same failures"
echo "ok   a run failed after its last retry is one POST linking to it; a retry waiting is none"

# --- the watchdog ------------------------------------------------------------
# 12 attempts of toy in the last hour, 11 failed: the failing rule.
toyrun=$run
hash=$(q "SELECT registry_hash FROM orca.pipelines WHERE name = 'toy'")
seed() {
    q "INSERT INTO orca.steps (run, step, partition, ord, status)
       SELECT $toyrun, 'seed' || n, '', 100 + n, 'failed' FROM generate_series(1, 12) n;
       INSERT INTO orca.attempts (step, attempt, status, host, registry_hash, started, ended, error)
       SELECT s.id, 1, CASE WHEN s.step = 'seed1' THEN 'succeeded' ELSE 'failed' END, 'seed', '$hash',
              now() - interval '5 minutes', now() - interval '4 minutes', 'boom'
       FROM orca.steps s WHERE s.step LIKE 'seed%'" > /dev/null
}
unseed() {
    q "DELETE FROM orca.attempts WHERE host = 'seed'; DELETE FROM orca.steps WHERE step LIKE 'seed%'" > /dev/null
}
seed
MORCA_WATCHDOG_SECONDS=1 ./build/morca tick >> "$log" 2>&1 || fail "tick"
expect_posts 4 "the trip"
[ "$(last '.headers.priority')" = high ] || fail "trip priority: $(last '.headers.priority')"
[ "$(last '.headers.title')" = "morca watchdog: failing tripped" ] || fail "trip title: $(last '.headers.title')"
[ "$(last '.headers.tags')" = "rotating_light,warning" ] || fail "trip tags"
[ "$(last '.headers.click')" = "https://morca.example.test/ops/control" ] || fail "trip click: $(last '.headers.click')"
last .body | grep -q "Paused: toy" || fail "trip body: $(last .body)"
last .body | grep -q "limit" || fail "trip body has no limit: $(last .body)"
echo "ok   a watchdog trip is one high-priority POST with Title, Priority, Tags and Click"

# Unpaused with nothing failing any more: one 'resolved'.
unseed
./build/morca unpause toy >> "$log" 2>&1 || fail "unpause"
MORCA_WATCHDOG_SECONDS=1 ./build/morca tick >> "$log" 2>&1 || fail "tick"
expect_posts 5 "the resolved"
[ "$(last '.headers.priority')" = low ] || fail "resolved priority: $(last '.headers.priority')"
last '.headers.title' | grep -q "failing resolved" || fail "resolved title: $(last '.headers.title')"
MORCA_WATCHDOG_SECONDS=1 ./build/morca tick >> "$log" 2>&1 || fail "tick"
expect_posts 5 "a second tick after the resolved"
echo "ok   unpausing sends one low 'resolved'"

# The same trip again inside the window: recorded and paused, not sent.
seed
MORCA_WATCHDOG_SECONDS=1 ./build/morca tick >> "$log" 2>&1 || fail "tick"
[ "$(q "SELECT paused FROM orca.pipelines WHERE name = 'toy'")" = t ] || fail "the second trip didn't pause toy: $(q "SELECT rule, value, detail, snoozed_until FROM orca.watchdog_rules")"
[ "$(q "SELECT count(*) FROM orca.watchdog_trips WHERE rule = 'failing'")" -eq 2 ] || fail "the second trip wasn't recorded"
expect_posts 5 "the same trip inside the window"
unseed
./build/morca unpause toy >> "$log" 2>&1 || fail "unpause"
echo "ok   the same alert inside MORCA_NOTIFY_MIN_SECONDS is not sent again"

# --- a stalled fill ------------------------------------------------------------
q "UPDATE orca.pipelines SET paused = false WHERE name = 'daily'" > /dev/null
sleep 2
export MORCA_NOTIFY_FILL_STALL_HOURS=0.0003
MORCA_NOW=$T0 ./build/morca tick >> "$log" 2>&1 || fail "tick"
expect_posts 5 "a fill outside its window"
# That tick started daily's first scheduled run. The next one has to find
# it ended, or the fill waits behind it ("start first") and no later tick
# here would start it.
i=0
while [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'daily' AND status = 'running'")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 600 ] || fail "daily's scheduled run didn't finish"; sleep 0.1
done
NOW=2024-01-08T23:00Z
MORCA_NOW=$NOW ./build/morca tick >> "$log" 2>&1 || fail "tick"
expect_posts 6 "the stalled fill"
[ "$(last '.headers.priority')" = high ] || fail "stall priority"
[ "$(last '.headers.title')" = "morca: fill of daily stalled" ] || fail "stall title: $(last '.headers.title')"
last .body | grep -q "0 of 7 partitions done" || fail "stall body: $(last .body)"
last .body | grep -q "no fill run has succeeded" || fail "stall body: $(last .body)"
[ "$(last '.headers.click')" = "https://morca.example.test/pipelines/daily" ] || fail "stall click"
echo "ok   a fill that has not finished a run is one high POST inside its window, none outside it"

# A fill run finishing resolves it.
i=0
while [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'fill' AND status = 'succeeded'")" -lt 1 ]; do
    i=$((i + 1)); [ "$i" -lt 300 ] || fail "no fill run finished"; sleep 0.1
done
MORCA_NOTIFY_FILL_STALL_HOURS=1 MORCA_NOW=2024-01-08T23:01Z ./build/morca tick >> "$log" 2>&1 || fail "tick"
expect_posts 7 "the fill moving again"
[ "$(last '.headers.priority')" = low ] || fail "fill resolved priority"
last '.headers.title' | grep -q "moving again" || fail "fill resolved title"
q "UPDATE orca.pipelines SET paused = true WHERE name = 'daily'" > /dev/null
i=0
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('running', 'queued')")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 600 ] || fail "runs didn't finish"; sleep 0.1
done
unset MORCA_NOTIFY_FILL_STALL_HOURS
echo "ok   a fill run finishing sends one low 'moving again'"

# --- the control host's disk -------------------------------------------------
sample() {  # sample DISK_PERCENT
    q "DELETE FROM orca.system_minutes;
       INSERT INTO orca.system_minutes (minute, n, last_t, cpu_sum, cpu_max, mem_sum, mem_max, mem_used, mem_total,
           disk_max, disk_used, disk_total, disk_path, net_in_sum, net_out_sum)
       VALUES (date_trunc('minute', now()), 1, now(), 10, 10, 10, 10, 400000000, 4000000000,
           $1, 18200000000, 20000000000, '/var/lib/orca/stage', 0, 0)" > /dev/null
}
sample 91
tick
expect_posts 8 "the disk"
[ "$(last '.headers.priority')" = high ] || fail "disk priority"
last .body | grep -q "/var/lib/orca/stage is 91% full" || fail "disk body: $(last .body)"
tick
expect_posts 8 "the disk again"
sample 40
tick
expect_posts 9 "the disk fine again"
[ "$(last '.headers.priority')" = low ] || fail "disk resolved priority"
echo "ok   the control disk over 85% is one high POST, and one low when it is fine"

# --- a lost worker -------------------------------------------------------------
q "INSERT INTO orca.workers (token_hash, pool, label, created_by, expires, last_seen, name, running)
   VALUES ('h1', 'edge', 'laptop', 'test', now() + interval '1 day', now() - interval '10 minutes', 'host1', '12,13'),
          ('h2', 'edge', 'idle', 'test', now() + interval '1 day', now() - interval '10 minutes', 'host2', NULL),
          ('h3', 'edge', 'fresh', 'test', now() + interval '1 day', now(), 'host3', '14')" > /dev/null
tick
expect_posts 10 "the lost worker"
[ "$(last '.headers.priority')" = default ] || fail "worker priority"
last .body | grep -q "worker laptop (host1) has been quiet for 10 min while holding 2 runs (12,13)" || fail "worker body: $(last .body)"
tick
expect_posts 10 "the lost worker again"
echo "ok   a worker quiet for 5 minutes while it held runs is one POST, once"

# --- morca notify (the backup's) ------------------------------------------------
./build/morca notify --kind backup-failed --subject nightly --priority high --tags rotating_light \
    --title "morca: nightly backup failed" "pg_dump: the dump doesn't read back" >> "$log" 2>&1 || fail "notify --kind"
expect_posts 11 "morca notify --kind"
[ "$(last '.headers.priority')" = high ] || fail "backup priority"
[ "$(last '.headers.title')" = "morca: nightly backup failed" ] || fail "backup title"
[ "$(last .body)" = "pg_dump: the dump doesn't read back" ] || fail "backup body: $(last .body)"
out=$(./build/morca notify --kind backup-failed --subject nightly "again" 2>&1) || fail "notify --kind again"
case "$out" in "held back"*) ;; *) fail "the second backup alert: $out" ;; esac
expect_posts 11 "morca notify --kind again"
./build/morca notify --kind backup-failed --subject nightly --resolved --title "morca: backup ok" "ok" >> "$log" 2>&1 || fail "notify --resolved"
expect_posts 12 "morca notify --resolved"
[ "$(last '.headers.priority')" = low ] || fail "backup resolved priority"
echo "ok   morca notify --kind sends once per kind and subject; --resolved ends it"

# --- ntfy down ------------------------------------------------------------------
kill "$fake"; wait "$fake" 2> /dev/null || true; fake=
if ./build/morca notify --kind probe --subject down "while ntfy is down" > build/notify-down.out 2>&1; then
    fail "notify succeeded with ntfy down"
fi
cat build/notify-down.out >> "$log"
grep -q "not sent yet" build/notify-down.out || fail "the down message: $(cat build/notify-down.out)"
[ "$(q "SELECT pending FROM orca.notifications WHERE kind = 'probe'")" = t ] || fail "the alert isn't waiting"
sample 92
q "DELETE FROM orca.notifications WHERE kind = 'disk'" > /dev/null
start=$(date +%s)
tick
[ $(($(date +%s) - start)) -lt 30 ] || fail "a tick with ntfy down took too long"
grep -q "morca: notify: .* alert not sent" "$log" || fail "no log line for the unsent alert"
[ "$(q "SELECT count(*) FROM orca.notifications WHERE pending")" -eq 2 ] || fail "pending: $(q "SELECT kind FROM orca.notifications WHERE pending")"
[ "$(q "SELECT attempts >= 1 FROM orca.notifications WHERE kind = 'disk'")" = t ] || fail "the disk alert's attempts"
before=$(count)
start_fake
q "UPDATE orca.notifications SET next_try = now()" > /dev/null
tick
[ "$(count)" -eq $((before + 2)) ] || fail "the waiting alerts weren't sent: $(count) POSTs after $before"
[ "$(q "SELECT count(*) FROM orca.notifications WHERE pending")" -eq 0 ] || fail "alerts still pending"
grep -q "while ntfy is down" "$posts" || fail "the probe wasn't sent"
echo "ok   ntfy down: ticks succeed, alerts wait in Postgres and go when it is back"

# --- the UI ----------------------------------------------------------------------
MORCA_UI_ADDR="127.0.0.1:$uport" ./build/morca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
done
page=$(curl -fsS "$base/ops/control")
echo "$page" | grep -q "<h2>Alerts</h2>" || fail "no Alerts on Ops > Control"
echo "$page" | grep -q "backup-failed" || fail "the alerts sent aren't listed"
curl -fsS "$base/api/system" | jq -e '.notify.configured == true and (.notify.recent | length) >= 5 and (.notify.recent[0] | has("kind"))' > /dev/null \
    || fail "/api/system's notify: $(curl -fsS "$base/api/system" | jq -c .notify)"
echo "$page" | grep -q "127.0.0.1:$port" && fail "the URL is on the page"
echo "ok   Ops > Control lists whether alerts are on and the last ones"

# --- Ops > Alerts ------------------------------------------------------------------
# Every alert sent has a row (orca.alerts): the test message is sent
# directly, so it has none.
[ "$(q "SELECT count(*) FROM orca.alerts WHERE delivery = 'sent'")" -eq $(($(count) - 1)) ] \
    || fail "rows sent: $(q "SELECT id, kind, event, delivery FROM orca.alerts ORDER BY id")"
[ "$(q "SELECT count(*) FROM orca.alerts WHERE delivery <> 'sent'")" -eq 0 ] || fail "rows not sent: $(q "SELECT id, kind, delivery, error FROM orca.alerts WHERE delivery <> 'sent'")"
[ "$(q "SELECT string_agg(run::text, ',' ORDER BY run) FROM orca.alerts WHERE kind = 'run-failed' AND pipelines = 'toy'")" = "$run,$spent" ] \
    || fail "the failed runs' rows: $(q "SELECT id, run, pipelines FROM orca.alerts WHERE kind = 'run-failed'")"
# The watchdog trip, resolved by its 'resolved'; the second trip was held back.
[ "$(q "SELECT count(*) FROM orca.alerts WHERE kind = 'watchdog' AND event = 'raised' AND pipelines = 'toy'
        AND resolved_by = (SELECT id FROM orca.alerts WHERE kind = 'watchdog' AND event = 'resolved')")" -eq 1 ] \
    || fail "the watchdog's rows: $(q "SELECT id, event, pipelines, resolved_at, resolved_by FROM orca.alerts WHERE kind = 'watchdog'")"
# The disk alert raised while ntfy was down failed first, then went.
[ "$(q "SELECT attempts >= 2 AND resolved_at IS NULL FROM orca.alerts WHERE kind = 'disk' ORDER BY id DESC LIMIT 1")" = t ] \
    || fail "the disk's last row: $(q "SELECT id, attempts, delivery, error, resolved_at FROM orca.alerts WHERE kind = 'disk'")"
location=$(curl -s -o /dev/null -w '%{redirect_url}' "$base/ops")
[ "$location" = "$base/ops/alerts" ] || fail "/ops goes to $location"
apage=$(curl -fsS "$base/ops/alerts")
echo "$apage" | grep -qF '<a href="/ops/alerts" aria-current="page">Alerts</a><a href="/ops/control">Control</a>' || fail "Alerts isn't first in the Ops menu, current"
echo "$apage" | grep -qF "morca: toy run $spent failed" || fail "the failed run isn't listed"
echo "$apage" | grep -qF "<a href=\"/runs/$spent\">run $spent</a>" || fail "no link to the failed run"
echo "$apage" | grep -qF '<a href="/pipelines/toy">toy</a>' || fail "no link to the pipeline"
echo "$apage" | grep -qF ">watchdog trip</span>" || fail "the trip isn't listed"
echo "$apage" | grep -qF '<a href="/watchdog">rule failing</a>' || fail "no link to the watchdog rule"
echo "$apage" | grep -qF 'class="alerts__open"' || fail "the open disk alert isn't marked"
echo "$apage" | grep -qF 'data-poll="/ops/alerts/live" data-poll-every="30"' || fail "the page doesn't refresh"
echo "$apage" | grep -qF 'class="help"' || fail "no help on the column headers"
echo "$apage" | grep -qE "SECRETVALUE|SECRETTOKEN|127.0.0.1:$port" && fail "a secret or the URL is on Alerts"
api() { curl -fsS "$base/api/alerts$1"; }
total=$(q "SELECT count(*) FROM orca.alerts")
newest=$(q "SELECT max(id) FROM orca.alerts")
api "" | jq -e --argjson total "$total" --argjson newest "$newest" \
    '.configured and .total == $total and .open == 1 and .alerts[0].id == $newest
     and ([.alerts[].id] == ([.alerts[].id] | sort | reverse))' > /dev/null \
    || fail "/api/alerts: $(api "" | jq -c '{total, open, ids: [.alerts[].id]}')"
api "?kind=run-failed" | jq -e --argjson spent "$spent" \
    '(.alerts | length) == 2 and all(.alerts[]; .kind == "run-failed" and .state == null and .delivery == "sent")
     and any(.alerts[]; .run == $spent and .pipelines == ["toy"] and (.links[0].href == "/runs/\($spent)"))' > /dev/null \
    || fail "?kind=run-failed: $(api "?kind=run-failed" | jq -c .alerts)"
api "?state=open" | jq -e '(.alerts | length) == 1 and .alerts[0].kind == "disk" and .alerts[0].state == "open"' > /dev/null \
    || fail "?state=open: $(api "?state=open" | jq -c .alerts)"
api "?state=resolved&kind=watchdog" | jq -e '(.alerts | length) == 2 and any(.alerts[]; .event == "raised" and .state == "resolved")
     and any(.alerts[]; .event == "resolved" and .priority == "low")' > /dev/null \
    || fail "?state=resolved&kind=watchdog: $(api "?state=resolved&kind=watchdog" | jq -c .alerts)"
curl -fsS "$base/ops/alerts?kind=run-failed" | grep -qF ">watchdog trip</span>" && fail "the kind filter shows a trip"
# A page at a time: the next page is older, its previous page the first.
first=$(api "?limit=3")
echo "$first" | jq -e '(.alerts | length) == 3 and .prev == null and .next != null' > /dev/null || fail "?limit=3: $(echo "$first" | jq -c '{next, prev}')"
second=$(curl -fsS -H 'Accept: application/json' "$base$(echo "$first" | jq -r .next_url)")
echo "$second" | jq -e --argjson last "$(echo "$first" | jq '.alerts[2].id')" \
    '(.alerts | length) == 3 and .alerts[0].id < $last and .prev != null' > /dev/null || fail "the second page: $(echo "$second" | jq -c '[.alerts[].id]')"
back=$(curl -fsS -H 'Accept: application/json' "$base$(echo "$second" | jq -r .prev_url)")
[ "$(echo "$back" | jq -c '[.alerts[].id]')" = "$(echo "$first" | jq -c '[.alerts[].id]')" ] || fail "back to the first page: $(echo "$back" | jq -c '[.alerts[].id]')"
# The page's JSON is the API's.
[ "$(curl -fsS -H 'Accept: application/json' "$base/ops/alerts?kind=disk" | jq -c .)" = "$(api "?kind=disk" | jq -c .)" ] || fail "the page's JSON isn't /api/alerts'"
# The live fragment: the newest page every 30 s, an older one done (286).
curl -fsS -D build/notify-live.h "$base/ops/alerts/live" | grep -qF '<table class="alerts">' || fail "the live fragment"
grep -qi '^x-poll-every: 30' build/notify-live.h || fail "the fragment's X-Poll-Every: $(cat build/notify-live.h)"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/ops/alerts/live?before=$newest")" = 286 ] || fail "an older page's fragment isn't done"
# morca alerts, over the database and --remote.
out=$(./build/morca alerts --kind run-failed 2>&1) || fail "morca alerts: $out"
[ "$(echo "$out" | grep -c "run-failed")" -eq 2 ] || fail "morca alerts --kind run-failed: $out"
echo "$out" | grep -qF "morca: toy run $spent failed" || fail "morca alerts' title: $out"
out=$(./build/morca --remote "http://127.0.0.1:$uport" alerts --open --json 2>&1) || fail "morca --remote alerts: $out"
echo "$out" | jq -e '.open == 1 and (.alerts | length) == 1 and .alerts[0].kind == "disk"' > /dev/null || fail "morca --remote alerts --open --json: $out"
echo "ok   Ops > Alerts lists each alert sent, with its links, delivery and state; filters, pages, JSON and morca alerts"

# --- no secret anywhere --------------------------------------------------------------
curl -fsS "$base/api/system" > build/notify-api.json
if grep -rE "SECRETVALUE|SECRETTOKEN|tk_SECRET" "$log" build/notify-api.json build/notify-down.out; then
    fail "a secret is in the log or a page"
fi
echo "$page" | grep -qE "SECRETVALUE|SECRETTOKEN" && fail "a secret is on the page"
stop
echo "ok   neither the topic nor the token is in a log or a page"
