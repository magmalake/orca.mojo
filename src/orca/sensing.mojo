"""Fires and sensors (`orca.scheduler`): a schedule's fires, and a sensor's
snapshots, keyed or by partition.
"""

from std.time import perf_counter_ns
from postgres import (
    Params,
    Transaction,
)
from orca.civil import Civil
from orca.cron import CronExpr
from orca.live import live_runs
from orca.schedule import Schedule
from orca.sensors import (
    join_keys,
    map_keys,
    parse_sense,
    split_keys,
)
from orca.tz import Zone
from orca.scheduler import Scheduler
from orca.registered import (
    Registered,
    fill_key,
)
from orca.queueing import (
    _queue,
    keep_keys,
    parse_keys_output,
)


def _sense(mut self: Scheduler, p: Registered, now: Int64) raises:
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
            " x(ds) WHERE p.name = $1 UNION SELECT 'reads',"
            " describe->'partitions'->>'dataset' FROM orca.pipelines WHERE"
            " name = $1 AND describe->'partitions'->>'dataset' IS NOT"
            " NULL) SELECT DISTINCT ON (c.dataset,"
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
    var polled = Optional[String](None)
    if p.name in self._unsensed:
        polled = self._unsensed.pop(p.name)
    else:
        var job = String("sense ", p.name)
        var t = Int(perf_counter_ns())
        var last = self._polled.get(p.name)
        if (
            job in self._asked
            or not last
            or Float64(t - last.value()) >= self.sense_seconds * 1e9
        ):
            if job not in self._asked:
                self._polled[p.name] = t
            polled = self._ask(job, "sense", [p.binary, "sense"])
    if polled:
        for c in parse_sense(polled.value()):
            if c.snapshot >= 0 and not c.by_orca:
                datasets.append(c.dataset)
                snapshots.append(c.snapshot)
                keys.append(c.partitions)
    if len(datasets) == 0:
        return
    if p.kind == "keyed":
        var current = _keys_for(self, p, datasets, snapshots)
        if not current:
            # Its keys are still being read: this pass waits for them,
            # with what `sense` said kept for it.
            if polled:
                self._unsensed[p.name] = polled.value()
            return
        _sense_keyed(
            self, p, current.value()[1].copy(), datasets, snapshots, keys
        )
        return
    var fallback: String
    if p.cron:
        var z = Zone.load(p.tz)
        fallback = fill_key(p.kind, p.fills, Civil.of(z.to_local(now)))
    else:
        fallback = fill_key(p.kind, "current", Civil.of(now))
    var tx = self.db.conn.begin()
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    var taken = List[Int]()
    var unusable = List[Tuple[Int, String]]()
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
        var ks = split_keys(keys[i])
        if len(ks) == 0:
            ks.append(fallback)
        # Keys that aren't partitions, such as a week another writer
        # put in `orca.partitions`, run nothing: the snapshot is seen,
        # with why, so it doesn't stop the others or the next tick.
        try:
            _ = map_keys(ks, p.kind, p.start)
        except e:
            _ = tx.execute(
                (
                    "UPDATE orca.sensed SET error = $4 WHERE pipeline = $1"
                    " AND dataset = $2 AND snapshot_id = $3"
                ),
                Params()
                .text(p.name)
                .text(datasets[i])
                .int64(snapshots[i])
                .text(String(e)),
            )
            unusable.append((i, String(e)))
            continue
        taken.append(i)
        all.extend(ks^)
    var run = Int64(-1)
    # Keys a live run has are left to it, which becomes the snapshot's
    # run if it left nothing else.
    var live = live_runs(tx, p.name)
    var mapped = List[String]()
    var held = Int64(-1)
    for k in map_keys(all, p.kind, p.start):
        var in_run = live.get(k)
        if not in_run:
            mapped.append(k)
        elif held < 0:
            held = Int64(in_run.value())
    if len(mapped) == 0:
        run = held
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
    if run >= 0:
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
            String("run ", run, " for ", join_keys(mapped)) if len(mapped)
            > 0 else (
                String("left to live run ", run) if run
                >= 0 else "before its first partition"
            ),
        )
    for u in unusable:
        print(
            "sensed",
            String(datasets[u[0]], "@", snapshots[u[0]]),
            "for",
            p.name,
            "and ran nothing:",
            u[1],
        )


def _sensed(mut self: Scheduler, p: Registered, error: String) raises:
    """Records how `p`'s pass went: the error of one that failed, which
    is printed when it's new rather than on every tick, or none. A
    pass still waiting for its `sense` or `keys` changes nothing."""
    if not error and (
        String("sense ", p.name) in self._asked
        or String("keys ", p.name) in self._asked
    ):
        return
    if not error:
        _ = self.db.conn.execute(
            (
                "UPDATE orca.pipelines SET sense_error = NULL,"
                " sense_error_at = NULL WHERE name = $1 AND sense_error IS"
                " NOT NULL"
            ),
            Params().text(p.name),
        )
        return
    var changed = self.db.conn.query(
        (
            "UPDATE orca.pipelines SET sense_error = $2, sense_error_at ="
            " now() WHERE name = $1 AND sense_error IS DISTINCT FROM $2"
            " RETURNING name"
        ),
        Params().text(p.name).text(error),
    )
    if len(changed) > 0:
        print("orca: pipeline", p.name, "can't sense:", error)


def _keys_for(
    mut self: Scheduler,
    p: Registered,
    datasets: List[String],
    snapshots: List[Int64],
) raises -> Optional[Tuple[Int64, List[String]]]:
    """A keyed sensor's current keys: those last read if no snapshot of
    its keys dataset among `snapshots` is newer than theirs, else from
    `<binary> keys`, which may still be going (None)."""
    var keys_snap = Int64(-1)
    for i in range(len(datasets)):
        if datasets[i] == p.keys_dataset:
            keys_snap = snapshots[i]
    var have = self._keys.get(p.name)
    if have and (keys_snap < 0 or have.value()[0] == keys_snap):
        return have.value().copy()
    var out = self._ask(
        String("keys ", p.name),
        String("orca: ", p.binary, " keys"),
        [p.binary, "keys"],
    )
    if not out:
        return None
    var got = parse_keys_output(p.binary, out.value())
    keep_keys(self.db, p.name, got)
    self._keys[p.name] = got.copy()
    return got^


def _sense_keyed(
    mut self: Scheduler,
    p: Registered,
    var all_keys: List[String],
    datasets: List[String],
    snapshots: List[Int64],
    keys: List[String],
) raises:
    """For a keyed sensor whose current keys are `all_keys`: a snapshot
    of its keys dataset offers every current key, and one of another
    dataset the names it holds that are current keys; the offered keys
    that aren't done or in a live run are queued, `keys_per_run` to a
    run."""
    var cur = Dict[String, Bool]()
    for k in all_keys:
        cur[k] = True
    var tx = self.db.conn.begin()
    var taken = List[Int]()
    var everything = False
    var offer = List[String]()
    for i in range(len(datasets)):
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
        if datasets[i] == p.keys_dataset:
            everything = True
        else:
            # Names a keyed pipeline upstream committed; others aren't
            # this pipeline's to run.
            for k in split_keys(keys[i]):
                if k in cur:
                    offer.append(k)
    var runs = _queue(
        tx, p, all_keys if everything else offer, "sensor"
    ).runs.copy()
    if len(runs) > 0:
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
                .int64(Int64(runs[0])),
            )
    tx.commit()
    for i in taken:
        print(
            "sensed",
            String(datasets[i], "@", snapshots[i]),
            "for",
            p.name,
            String(len(runs), " runs from run ", runs[0]) if len(runs)
            > 0 else "no new keys",
        )


def _fire(mut self: Scheduler, p: Registered, now: Int64) raises:
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
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        var made = tx.query(
            (
                "INSERT INTO orca.fires (pipeline, fire_at, partition)"
                " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                " fire_at"
            ),
            Params().text(p.name).int64(t).text(key),
        )
        var run = Int64(-1)
        var held = False
        # Keys of one kind order as text: `YYYY-MM`, `YYYY-MM-DD`.
        if len(made) == 1 and (not p.start or key >= p.start):
            # A live run of the key is the fire's run: a second one
            # would run beside it, and both would append.
            var live = live_runs(tx, p.name).get(key)
            held = Bool(live)
            run = Int64(live.value()) if live else _queue_fire(self, tx, p, key)
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
                String("left to live run ", run) if held else (
                    String("run ", run) if run >= 0 else "before the start"
                ),
            )


def _queue_fire(
    mut self: Scheduler, mut tx: Transaction, p: Registered, key: String
) raises -> Int64:
    """A queued run of `p` over `key`, trigger `schedule`."""
    return (
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
