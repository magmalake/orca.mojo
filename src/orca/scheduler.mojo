"""The scheduler: fires schedules, queues backfills, and starts runs.

```
orca register ./build/taxi             runs `taxi describe`, records the binary
orca backfill taxi 2011-01..2024-12    one queued run per partition
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
- **Starting a run is `resume`.** A queued run has nothing done, which is
  what `<binary> resume <run>` takes. The scheduler marks the run `running`,
  with no heartbeat and itself as owner, and execs the binary, which claims
  it. It doesn't wait: the run records its own progress.
- **`max_runs`.** Under a lock on the pipeline's row, the runs that are live
  (a fresh heartbeat, or started by the scheduler less than the stale limit
  ago) are counted, and only that many fewer than `max_runs` are started.
- **Retries.** A scheduled or backfilled run that failed is started again
  after `ORCA_RETRY_SECONDS` (default 60) doubling with each start, plus up
  to a tenth more as jitter, up to `run_retries` times. One whose process
  died (its heartbeat went stale) is started again at once, within the same
  budget. A manual run is never retried by the scheduler.

The scheduler's clock is `orca.civil.now`, which `ORCA_NOW` fixes; fire
times and retry times are on it. Heartbeats are on the database's clock.
A run's output goes to `<ORCA_LOGS>/<pipeline>/<run>/driver.log` when
`ORCA_LOGS` is set.
"""

from std.os import getenv, makedirs

from postgres import Params

from orca.civil import Civil
from orca.cron import CronExpr
from orca.day import Day
from orca.key import parse_keys
from orca.month import Month
from orca.proc import absolute, capture, reap, spawn
from orca.schedule import Schedule
from orca.state import State
from orca.tz import Zone

comptime _RETRIED = "trigger IN ('schedule', 'backfill')"
"""The runs the scheduler retries."""


@fieldwise_init
struct _Pipeline(Copyable, Movable):
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


struct Scheduler(Movable):
    var db: State
    var stale: Float64
    var retry_seconds: Int64
    var logs: String

    def __init__(out self, url: String) raises:
        self.db = State(url)
        self.db.migrate()
        self.stale = Float64(getenv("ORCA_STALE_SECONDS", "30"))
        self.retry_seconds = Int64(Int(getenv("ORCA_RETRY_SECONDS", "60")))
        self.logs = getenv("ORCA_LOGS")

    def register(mut self, binary: String, now: Int64) raises -> String:
        """Records the pipeline `binary` describes, with its path; its
        schedule counts from `now` the first time. Queued runs move to the
        new code. Returns the pipeline's name."""
        var path = absolute(binary)
        var got = capture([path, "describe"])
        if got[0] != 0 or not got[1].startswith('{"pipeline": '):
            raise Error(
                "orca: ", path, " describe exited ", got[0], ": ", got[1]
            )
        var res = self.db.conn.query(
            (
                "INSERT INTO orca.pipelines (name, registry_hash, describe,"
                " binary_path, since) SELECT d->>'pipeline',"
                " d->>'registry_hash', d, $2, $3 FROM (SELECT $1::jsonb AS d)"
                " AS x ON CONFLICT (name) DO UPDATE SET registry_hash ="
                " EXCLUDED.registry_hash, describe = EXCLUDED.describe,"
                " binary_path = EXCLUDED.binary_path, since ="
                " coalesce(orca.pipelines.since, EXCLUDED.since), last_seen ="
                " now() RETURNING name, registry_hash"
            ),
            Params().text(got[1]).text(path).int64(now),
        )
        var name = res.row(0).text("name")
        _ = self.db.conn.execute(
            (
                "UPDATE orca.runs SET registry_hash = $2 WHERE pipeline = $1"
                " AND status = 'queued'"
            ),
            Params().text(name).text(res.row(0).text("registry_hash")),
        )
        return name^

    def _pipelines(mut self, only: String = "") raises -> List[_Pipeline]:
        var out = List[_Pipeline]()
        var rows = self.db.conn.query(
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
                "'run_retries')::bigint, 3) AS run_retries FROM"
                " orca.pipelines WHERE binary_path IS NOT NULL AND ($1 = '' OR"
                " name = $1) ORDER BY name"
            ),
            Params().text(only),
        )
        for r in rows:
            out.append(
                _Pipeline(
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
                )
            )
        return out^

    def tick(mut self, now: Int64) raises:
        """One pass: reap ended runs, make due fires, schedule retries, and
        start what may start."""
        for done in reap():
            print("reaped pid", done[0], "exit", done[1])
        for p in self._pipelines():
            if p.cron:
                try:
                    self._fire(p, now)
                except e:
                    print("orca: pipeline", p.name, "can't fire:", e)
            self._schedule_retries(p, now)
            self._start(p, now)

    def _fire(mut self, p: _Pipeline, now: Int64) raises:
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

    def _schedule_retries(mut self, p: _Pipeline, now: Int64) raises:
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

    def _start(mut self, p: _Pipeline, now: Int64) raises:
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
        if free > 0:
            var rows = tx.query(
                String(
                    "SELECT id FROM orca.runs WHERE pipeline = $1 AND (status",
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
        for id in ids:
            var log = String()
            if self.logs:
                var dir = String(self.logs, "/", p.name, "/", id)
                makedirs(dir, exist_ok=True)
                log = String(dir, "/driver.log")
            var pid = spawn([p.binary, "resume", String(id)], log)
            print("started", p.name, "run", id, "pid", pid)

    def backfill(mut self, pipeline: String, keys: String) raises -> Int:
        """Queues a run for each partition in `keys` that has no run queued,
        running or succeeded; returns how many it queued."""
        var ps = self._pipelines(pipeline)
        if len(ps) == 0:
            raise Error("orca: pipeline ", pipeline, " isn't registered")
        ref p = ps[0]
        var texts = List[String]()
        if p.kind == "daily":
            for k in parse_keys[Day](keys, p.start):
                texts.append(String(k))
        else:
            for k in parse_keys[Month](keys, p.start):
                texts.append(String(k))
        var queued = 0
        for key in texts:
            var res = self.db.conn.query(
                (
                    "INSERT INTO orca.runs (pipeline, registry_hash,"
                    " partitions, trigger, status) SELECT $1, $2, $3,"
                    " 'backfill', 'queued' WHERE NOT EXISTS (SELECT 1 FROM"
                    " orca.runs WHERE pipeline = $1 AND partitions = $3 AND"
                    " status IN ('queued', 'running', 'succeeded')) RETURNING"
                    " id"
                ),
                Params().text(p.name).text(p.registry_hash).text(key),
            )
            queued += len(res)
        return queued
