"""The scheduler: fires schedules, senses datasets, queues backfills and
rebuilds, and starts runs.

```
orca register ./build/taxi             runs `taxi describe`, records the binary
orca backfill taxi 2011-01..2024-12    one queued run per partition
orca rebuild taxi.trips 2024-01        the pipelines downstream, in order
orca tick                              one pass, then exits
orca scheduler                         a pass every second
```

The scheduler links no pipeline. It knows one by the `describe` it printed,
kept in `orca.pipelines` with the binary's path, and reads the schedule,
partitions and limits from that JSON in SQL. Everything it decides is in
Postgres, so it can be stopped and restarted, or run twice, at any time:

- **A fire is a row.** `orca.fires` is keyed by (pipeline, fire time), and a
  fire's row and its `queued` run are inserted in one transaction, so a fire
  is never made twice. Fires are counted from the last one recorded (or from
  when the pipeline was registered), so the fires missed while the scheduler
  was down are made once each, or, with `catchup=False`, only the latest.
  A fire whose partition is before the pipeline's first is recorded without
  a run.
- **Starting a run is `resume`.** A queued run has nothing done, or what a
  resume or rerun from `orca ui` left it, which is what `<binary> resume
  <run>` takes (with `--allow-new-code` when the resume said so). The scheduler marks the run `running`,
  with no heartbeat and itself as owner, and execs the binary, which claims
  it. It doesn't wait: the run records its own progress.
- **`max_runs`.** Under a lock on the pipeline's row, the runs that are live
  (a fresh heartbeat, or started by the scheduler less than the stale limit
  ago) are counted, and only that many fewer than `max_runs` are started.
- **Sensors and rebuilds** (`orca.sensors`). A sensor pipeline's pass
  looks for snapshots of the datasets it reads, in `orca.commits` on every
  tick and from `<binary> sense` every `ORCA_SENSE_SECONDS`, and queues a
  run, trigger `sensor`, for those it hasn't seen. Each rebuild queues its
  next level once the one before has succeeded.
- **Retries.** A scheduled, backfilled or sensed run that failed is started again
  after `ORCA_RETRY_SECONDS` (default 60) doubling with each start, plus up
  to a tenth more as jitter, up to `run_retries` times. One whose process
  died (its heartbeat went stale) is started again at once, within the same
  budget. A manual run is never retried by the scheduler. A run asked to
  cancel whose process died ends `cancelled`.

The scheduler's clock is `orca.civil.now`, which `ORCA_NOW` fixes; fire
times and retry times are on it. Heartbeats are on the database's clock.
A run's output goes to `<ORCA_LOGS>/<pipeline>/<run>/driver.log` when
`ORCA_LOGS` is set.
"""

from std.os import getenv, makedirs
from std.time import perf_counter_ns

from postgres import Params

from orca.civil import Civil
from orca.cron import CronExpr
from orca.day import Day
from orca.key import parse_keys
from orca.month import Month
from orca.name import parse_names
from orca.proc import absolute, capture, reap, spawn
from orca.schedule import Schedule
from orca.sensors import (
    advance_rebuilds,
    join_keys,
    map_keys,
    parse_sense,
    split_keys,
)
from orca.state import State
from orca.tz import Zone

comptime _RETRIED = "trigger IN ('schedule', 'backfill', 'sensor')"
"""The runs the scheduler retries."""


@fieldwise_init
struct Registered(Copyable, Movable):
    """A registered pipeline, as the scheduler reads it from `describe`."""

    var name: String
    var binary: String
    var registry_hash: String
    var since: Int64
    var cron: String
    var tz: String
    var fills: String
    var catchup: Bool
    var kind: String
    var start: String
    var max_runs: Int
    var run_retries: Int
    var sensor: Bool


def fill_key(kind: String, fills: String, local: Civil) raises -> String:
    """The partition a fire fills, given its time on the schedule's wall
    clock: the key the time falls in, or the one before it."""
    if kind == "daily":
        var d = Day.containing(local.year, local.month, local.day)
        return String(d.prev() if fills == "previous" else d^)
    if kind == "monthly":
        var m = Month.containing(local.year, local.month, local.day)
        return String(m.prev() if fills == "previous" else m^)
    raise Error("orca: unknown partitions '", kind, "'")


def registered(mut db: State, only: String = "") raises -> List[Registered]:
    """Every pipeline registered with a binary, or the one named."""
    var out = List[Registered]()
    var rows = db.conn.query(
        (
            "SELECT name, binary_path, registry_hash, coalesce(since, 0) AS"
            " since, coalesce(describe->'schedule'->>'cron', '') AS cron,"
            " coalesce(describe->'schedule'->>'tz', 'UTC') AS tz,"
            " coalesce(describe->'schedule'->>'fills', 'previous') AS"
            " fills, coalesce((describe->'schedule'->>'catchup')::boolean,"
            " true) AS catchup, coalesce(describe->'partitions'->>'kind',"
            " 'monthly') AS kind, coalesce(describe->'partitions'->>"
            "'start', '') AS start, coalesce((describe->>'max_runs')"
            "::bigint, 1) AS max_runs, coalesce((describe->>"
            "'run_retries')::bigint, 3) AS run_retries,"
            " coalesce((describe->>'sensor')::boolean, false) AS sensor FROM"
            " orca.pipelines WHERE binary_path IS NOT NULL AND ($1 = '' OR"
            " name = $1) ORDER BY name"
        ),
        Params().text(only),
    )
    for r in rows:
        out.append(
            Registered(
                r.text("name"),
                r.text("binary_path"),
                r.text("registry_hash"),
                r.int64("since"),
                r.text("cron"),
                r.text("tz"),
                r.text("fills"),
                r.bool("catchup"),
                r.text("kind"),
                r.text("start"),
                Int(r.int64("max_runs")),
                Int(r.int64("run_retries")),
                r.bool("sensor"),
            )
        )
    return out^


def dataset_cycle(mut db: State, describe: String) raises -> String:
    """How the pipeline `describe` gives would close a cycle across
    pipelines, through the datasets they write and read, or empty if it
    wouldn't. A pipeline reading a dataset it writes itself isn't one."""
    var rows = db.conn.query(
        (
            "WITH d AS (SELECT name, describe FROM orca.pipelines WHERE name"
            " <> $1::jsonb->>'pipeline' UNION ALL SELECT $1::jsonb->>"
            "'pipeline', $1::jsonb), io AS (SELECT DISTINCT d.name, k.kind,"
            " x.ds FROM d, jsonb_array_elements(d.describe->'steps') AS s(st),"
            " (VALUES ('reads'), ('writes')) AS k(kind),"
            " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
            " x(ds)) SELECT w.name AS src, r.name AS dst, w.ds, $1::jsonb->>"
            "'pipeline' AS me FROM io w JOIN io r ON r.ds = w.ds AND r.kind ="
            " 'reads' WHERE w.kind = 'writes' AND w.name <> r.name ORDER BY"
            " 1, 2, 3"
        ),
        Params().text(describe),
    )
    if len(rows) == 0:
        return ""
    var me = rows.row(0).text("me")
    var src = List[String]()
    var dst = List[String]()
    var via = List[String]()
    for r in rows:
        src.append(r.text("src"))
        dst.append(r.text("dst"))
        via.append(r.text("ds"))
    # A path from `me` back to `me`, breadth first, by the edge taken.
    var came = List[Int](length=len(src), fill=-2)
    var frontier = List[Int]()
    for i in range(len(src)):
        if src[i] == me:
            came[i] = -1
            frontier.append(i)
    while len(frontier) > 0:
        var next = List[Int]()
        for e in frontier:
            if dst[e] == me:
                var path = List[Int]()
                var at = e
                while at >= 0:
                    path.append(at)
                    at = came[at]
                var out = String()
                for k in range(len(path) - 1, -1, -1):
                    var i = path[k]
                    out.write(
                        src[i],
                        " writes ",
                        via[i],
                        ", which ",
                        dst[i],
                        " reads",
                        "; " if k > 0 else "",
                    )
                return out^
            for i in range(len(src)):
                if src[i] == dst[e] and came[i] == -2:
                    came[i] = e
                    next.append(i)
        frontier = next^
    return ""


def partition_keys(p: Registered, keys: String) raises -> List[String]:
    """`keys` (`KEY[..KEY][,...]`) as the pipeline's partition keys, each
    checked and from its first partition on."""
    var texts = List[String]()
    if p.kind == "keyed":
        if keys.startswith("@"):
            raise Error(
                (
                    "orca: the scheduler takes a keyed pipeline's partitions as"
                    " a list of names; run '"
                ),
                keys,
                "' with the pipeline's binary",
            )
        for n in parse_names(keys):
            texts.append(n.text)
        return texts^
    if p.kind == "daily":
        for k in parse_keys[Day](keys, p.start):
            texts.append(String(k))
    else:
        for k in parse_keys[Month](keys, p.start):
            texts.append(String(k))
    return texts^


def backfill(mut db: State, p: Registered, keys: String) raises -> Int:
    """Queues a run for each partition in `keys` that has no run queued,
    running or succeeded; returns how many it queued."""
    var queued = 0
    for key in partition_keys(p, keys):
        var res = db.conn.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash, partitions,"
                " trigger, status) SELECT $1, $2, $3, 'backfill', 'queued'"
                " WHERE NOT EXISTS (SELECT 1 FROM orca.runs WHERE pipeline ="
                " $1 AND partitions = $3 AND status IN ('queued', 'running',"
                " 'succeeded')) RETURNING id"
            ),
            Params().text(p.name).text(p.registry_hash).text(key),
        )
        queued += len(res)
    return queued


struct Scheduler(Movable):
    var db: State
    var stale: Float64
    var retry_seconds: Int64
    var sense_seconds: Float64
    var logs: String
    var _polled: Dict[String, Int]
    """When each sensor's binary was last asked to `sense`, on the monotonic
    clock in nanoseconds."""

    def __init__(out self, url: String) raises:
        self.db = State(url)
        self.db.migrate()
        self.stale = Float64(getenv("ORCA_STALE_SECONDS", "30"))
        self.retry_seconds = Int64(Int(getenv("ORCA_RETRY_SECONDS", "60")))
        self.sense_seconds = Float64(getenv("ORCA_SENSE_SECONDS", "60"))
        self.logs = getenv("ORCA_LOGS")
        self._polled = Dict[String, Int]()

    def register(mut self, binary: String, now: Int64) raises -> String:
        """Records the pipeline `binary` describes, with its path; its
        schedule counts from `now` the first time, and a sensor from now the
        first time it's one, with the snapshots its datasets have now seen.
        Queued runs move to the new code. Returns the pipeline's name."""
        var path = absolute(binary)
        var got = capture([path, "describe"])
        if got[0] != 0 or not got[1].startswith('{"pipeline": '):
            raise Error(
                "orca: ", path, " describe exited ", got[0], ": ", got[1]
            )
        var cycle = dataset_cycle(self.db, got[1])
        if cycle:
            raise Error("orca: ", path, " can't be registered: ", cycle)
        var info = self.db.conn.query(
            (
                "SELECT coalesce(($1::jsonb->>'sensor')::boolean, false) AS"
                " sensor, NOT EXISTS (SELECT 1 FROM orca.pipelines WHERE name"
                " = $1::jsonb->>'pipeline' AND sensing_since IS NOT NULL) AS"
                " new_sensor"
            ),
            Params().text(got[1]),
        ).row(0)
        var sensor = info.bool("sensor")
        var seen = String()
        if sensor:
            var sensed = capture([path, "sense"])
            if sensed[0] != 0:
                raise Error(
                    "orca: ", path, " sense exited ", sensed[0], ": ", sensed[1]
                )
            if len(parse_sense(sensed[1])) == 0:
                raise Error(
                    "orca: ",
                    path,
                    " is a sensor, but reads no dataset it doesn't write",
                )
            if info.bool("new_sensor"):
                seen = sensed[1]
        var res = self.db.conn.query(
            (
                "INSERT INTO orca.pipelines (name, registry_hash, describe,"
                " binary_path, since, sensing_since) SELECT d->>'pipeline',"
                " d->>'registry_hash', d, $2, $3, CASE WHEN $4 THEN now() END"
                " FROM (SELECT $1::jsonb AS d) AS x ON CONFLICT (name) DO"
                " UPDATE SET registry_hash = EXCLUDED.registry_hash, describe ="
                " EXCLUDED.describe, binary_path = EXCLUDED.binary_path, since"
                " = coalesce(orca.pipelines.since, EXCLUDED.since),"
                " sensing_since = CASE WHEN $4 THEN"
                " coalesce(orca.pipelines.sensing_since, now()) END, last_seen"
                " = now() RETURNING name, registry_hash"
            ),
            Params().text(got[1]).text(path).int64(now).bool(sensor),
        )
        var name = res.row(0).text("name")
        # What its datasets hold now isn't new to it.
        for c in parse_sense(seen):
            if c.snapshot >= 0:
                _ = self.db.conn.execute(
                    (
                        "INSERT INTO orca.sensed (pipeline, dataset,"
                        " snapshot_id) VALUES ($1, $2, $3) ON CONFLICT DO"
                        " NOTHING"
                    ),
                    Params().text(name).text(c.dataset).int64(c.snapshot),
                )
        _ = self.db.conn.execute(
            (
                "UPDATE orca.runs SET registry_hash = $2 WHERE pipeline = $1"
                " AND status = 'queued'"
            ),
            Params().text(name).text(res.row(0).text("registry_hash")),
        )
        return name^

    def tick(mut self, now: Int64) raises:
        """One pass: reap ended runs, advance rebuilds, make due fires,
        sense new snapshots, schedule retries, and start what may start."""
        for done in reap():
            print("reaped pid", done[0], "exit", done[1])
        advance_rebuilds(self.db)
        for p in registered(self.db):
            if p.cron:
                try:
                    self._fire(p, now)
                except e:
                    print("orca: pipeline", p.name, "can't fire:", e)
            if p.sensor:
                try:
                    self._sense(p, now)
                except e:
                    print("orca: pipeline", p.name, "can't sense:", e)
            self._schedule_retries(p, now)
            self._start(p, now)

    def _sense(mut self, p: Registered, now: Int64) raises:
        """Queues a run for the snapshots of `p`'s datasets it hasn't seen:
        those other pipelines' succeeded runs committed since it became a
        sensor, and, when its binary is due to be asked, the current ones
        something other than orca committed. A snapshot with no partitions
        stands for the one `p` would fill now."""
        var datasets = List[String]()
        var snapshots = List[Int64]()
        var keys = List[String]()
        for r in self.db.conn.query(
            (
                "WITH io AS (SELECT k.kind, x.ds FROM orca.pipelines p,"
                " jsonb_array_elements(p.describe->'steps') AS s(st), (VALUES"
                " ('reads'), ('writes')) AS k(kind),"
                " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
                " x(ds) WHERE p.name = $1) SELECT DISTINCT ON (c.dataset,"
                " c.snapshot_id) c.dataset, c.snapshot_id,"
                " coalesce(nullif(s.partition, ''), r.partitions) AS keys FROM"
                " orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN"
                " orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id ="
                " s.run JOIN orca.pipelines me ON me.name = $1 WHERE"
                " r.pipeline <> $1 AND r.status = 'succeeded' AND a.status ="
                " 'succeeded' AND a.ended >= me.sensing_since AND c.dataset IN"
                " (SELECT ds FROM io WHERE kind = 'reads' EXCEPT SELECT ds FROM"
                " io WHERE kind = 'writes') AND NOT EXISTS (SELECT 1 FROM"
                " orca.sensed x WHERE x.pipeline = $1 AND x.dataset ="
                " c.dataset AND x.snapshot_id = c.snapshot_id) AND NOT EXISTS"
                " (SELECT 1 FROM orca.rebuild_pipelines rp WHERE rp.rebuild ="
                " r.rebuild AND rp.pipeline = $1) ORDER BY c.dataset,"
                " c.snapshot_id, c.attempt"
            ),
            Params().text(p.name),
        ):
            datasets.append(r.text("dataset"))
            snapshots.append(r.int64("snapshot_id"))
            keys.append(r.text("keys"))
        var t = Int(perf_counter_ns())
        var last = self._polled.get(p.name)
        if not last or Float64(t - last.value()) >= self.sense_seconds * 1e9:
            self._polled[p.name] = t
            var got = capture([p.binary, "sense"])
            if got[0] != 0:
                raise Error("sense exited ", got[0], ": ", got[1])
            for c in parse_sense(got[1]):
                if c.snapshot >= 0 and not c.by_orca:
                    datasets.append(c.dataset)
                    snapshots.append(c.snapshot)
                    keys.append(c.partitions)
        if len(datasets) == 0:
            return
        var fallback: String
        if p.cron:
            var z = Zone.load(p.tz)
            fallback = fill_key(p.kind, p.fills, Civil.of(z.to_local(now)))
        else:
            fallback = fill_key(p.kind, "current", Civil.of(now))
        var tx = self.db.conn.begin()
        var taken = List[Int]()
        var all = List[String]()
        for i in range(len(datasets)):
            # Seen by another tick already, or twice in this one.
            var new = tx.query(
                (
                    "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " snapshot_id"
                ),
                Params().text(p.name).text(datasets[i]).int64(snapshots[i]),
            )
            if len(new) == 0:
                continue
            taken.append(i)
            var ks = split_keys(keys[i])
            if len(ks) == 0:
                ks.append(fallback)
            all.extend(ks^)
        var run = Int64(-1)
        var mapped = map_keys(all, p.kind, p.start)
        if len(mapped) > 0:
            run = (
                tx.query(
                    (
                        "INSERT INTO orca.runs (pipeline, registry_hash,"
                        " partitions, trigger, status) VALUES ($1, $2, $3,"
                        " 'sensor', 'queued') RETURNING id"
                    ),
                    Params()
                    .text(p.name)
                    .text(p.registry_hash)
                    .text(join_keys(mapped)),
                )
                .row(0)
                .int64("id")
            )
            for i in taken:
                _ = tx.execute(
                    (
                        "UPDATE orca.sensed SET run = $4 WHERE pipeline = $1"
                        " AND dataset = $2 AND snapshot_id = $3"
                    ),
                    Params()
                    .text(p.name)
                    .text(datasets[i])
                    .int64(snapshots[i])
                    .int64(run),
                )
        tx.commit()
        for i in taken:
            print(
                "sensed",
                String(datasets[i], "@", snapshots[i]),
                "for",
                p.name,
                String("run ", run, " for ", join_keys(mapped)) if run
                >= 0 else "before its first partition",
            )

    def _fire(mut self, p: Registered, now: Int64) raises:
        var s = Schedule(CronExpr.parse(p.cron), Zone.load(p.tz))
        var last = (
            self.db.conn.query(
                (
                    "SELECT coalesce(max(fire_at), $2) AS last FROM orca.fires"
                    " WHERE pipeline = $1"
                ),
                Params().text(p.name).int64(p.since),
            )
            .row(0)
            .int64("last")
        )
        var times = List[Int64]()
        var t = last
        # Many missed fires are made a thousand at a time, one per tick.
        while len(times) < 1000:
            t = s.next_fire(t)
            if t > now:
                break
            times.append(t)
        if not p.catchup and len(times) > 1:
            times = [times[len(times) - 1]]
        for t in times:
            var key = fill_key(p.kind, p.fills, Civil.of(s.zone.to_local(t)))
            var tx = self.db.conn.begin()
            var made = tx.query(
                (
                    "INSERT INTO orca.fires (pipeline, fire_at, partition)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " fire_at"
                ),
                Params().text(p.name).int64(t).text(key),
            )
            var run = Int64(-1)
            # Keys of one kind order as text: `YYYY-MM`, `YYYY-MM-DD`.
            if len(made) == 1 and (not p.start or key >= p.start):
                run = (
                    tx.query(
                        (
                            "INSERT INTO orca.runs (pipeline, registry_hash,"
                            " partitions, trigger, status) VALUES ($1, $2, $3,"
                            " 'schedule', 'queued') RETURNING id"
                        ),
                        Params().text(p.name).text(p.registry_hash).text(key),
                    )
                    .row(0)
                    .int64("id")
                )
                _ = tx.execute(
                    (
                        "UPDATE orca.fires SET run = $3 WHERE pipeline = $1"
                        " AND fire_at = $2"
                    ),
                    Params().text(p.name).int64(t).int64(run),
                )
            tx.commit()
            if len(made) == 1:
                print(
                    "fired",
                    p.name,
                    "at",
                    String(Civil.of(t), "Z"),
                    "for",
                    key,
                    String("run ", run) if run >= 0 else "before the start",
                )

    def _schedule_retries(mut self, p: Registered, now: Int64) raises:
        """Gives each failed run that may be retried its time to start."""
        _ = self.db.conn.execute(
            String(
                (
                    "UPDATE orca.runs SET retry_at = $2 + ($3 * power(2, starts"
                    " - 1))::bigint + (id * 7919 % 1000) * $3 / 10000 WHERE"
                    " pipeline = $1 AND status = 'failed' AND "
                ),
                _RETRIED,
                " AND retry_at IS NULL AND starts BETWEEN 1 AND $4",
            ),
            Params()
            .text(p.name)
            .int64(now)
            .int64(self.retry_seconds)
            .int64(Int64(p.run_retries)),
        )

    def _start(mut self, p: Registered, now: Int64) raises:
        """Starts queued runs, retries that are due and runs whose process
        died, up to `max_runs` live at once."""
        var stale = "make_interval(secs => $2)"
        var dead = String(
            "((heartbeat IS NOT NULL AND heartbeat <= now() - ",
            stale,
            ") OR (heartbeat IS NULL AND (started IS NULL OR started <= now()",
            " - ",
            stale,
            ")))",
        )
        var tx = self.db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        # A run asked to cancel whose process died is cancelled.
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'cancelled', ended = now(),",
                " heartbeat = NULL WHERE pipeline = $1 AND status = 'running'",
                " AND cancel_requested AND ",
                dead,
            ),
            Params().text(p.name).float64(self.stale),
        )
        # A run whose process died and has no retries left has failed.
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'failed', ended = now() WHERE",
                " pipeline = $1 AND status = 'running' AND ",
                _RETRIED,
                " AND ",
                dead,
                " AND starts > $3",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries)),
        )
        var live = Int(
            tx.query(
                String(
                    "SELECT count(*) AS n FROM orca.runs WHERE pipeline = $1",
                    " AND status = 'running' AND NOT ",
                    dead,
                ),
                Params().text(p.name).float64(self.stale),
            )
            .row(0)
            .int64("n")
        )
        var free = p.max_runs - live
        var ids = List[Int64]()
        var new_code = List[Bool]()
        if free > 0:
            var rows = tx.query(
                String(
                    (
                        "SELECT id, allow_new_code FROM orca.runs WHERE"
                        " pipeline = $1 AND (status"
                    ),
                    " = 'queued' OR (status = 'failed' AND ",
                    _RETRIED,
                    " AND retry_at <= $4) OR (status = 'running' AND ",
                    _RETRIED,
                    " AND ",
                    dead,
                    " AND starts <= $3)) ORDER BY id LIMIT $5",
                ),
                Params()
                .text(p.name)
                .float64(self.stale)
                .int64(Int64(p.run_retries))
                .int64(now)
                .int64(Int64(free)),
            )
            for r in rows:
                ids.append(r.int64("id"))
                new_code.append(r.bool("allow_new_code"))
            for id in ids:
                _ = tx.execute(
                    (
                        "UPDATE orca.runs SET status = 'running', owner ="
                        " 'scheduler', heartbeat = NULL, started = now(),"
                        " ended = NULL, starts = starts + 1, retry_at = NULL"
                        " WHERE id = $1"
                    ),
                    Params().int64(id),
                )
        tx.commit()
        for i in range(len(ids)):
            var id = ids[i]
            var log = String()
            if self.logs:
                var dir = String(self.logs, "/", p.name, "/", id)
                makedirs(dir, exist_ok=True)
                log = String(dir, "/driver.log")
            var argv: List[String] = [p.binary, "resume", String(id)]
            if new_code[i]:
                argv.append("--allow-new-code")
            var pid = spawn(argv, log)
            print("started", p.name, "run", id, "pid", pid)

    def backfill(mut self, pipeline: String, keys: String) raises -> Int:
        """Queues a run for each partition in `keys` that has none; see
        `backfill`."""
        var ps = registered(self.db, pipeline)
        if len(ps) == 0:
            raise Error("orca: pipeline ", pipeline, " isn't registered")
        return backfill(self.db, ps[0], keys)
