"""The scheduler's part in a worker pool (`orca.workers`): due retries and
dead runs queued again for a worker, lost workers' runs, orphans of gone
instances, and fill runs queued for the pool.
"""

from postgres import (
    Params,
    Transaction,
)
from morca.batches import mark_batch
from morca.fill import fill_status
from morca.live import (
    dead_sql,
    hold_behind,
    stale_sql,
)
from morca.scheduler import (
    Scheduler,
    _RETRIED,
    _WORKER_OWNER,
)
from morca.registered import (
    Registered,
    fill_pipeline,
)


def _requeue_for_workers(
    mut self: Scheduler, p: Registered, now: Int64, pool: String
) raises:
    """What `_start` does for a pipeline placed `worker:<pool>`, whose
    runs a worker of the pool claims (`orca.workers`): queues again
    the runs of a worker that's gone (`_requeue_lost`), whatever their
    trigger; ends the runs that can't go on as for any pipeline; and
    queues again, for a worker to claim, the retries and deferred runs
    that are due and the runs whose process died with retries left.
    Its queued runs say they wait for a worker (its fill runs, while
    fills are off, that they wait for fills)."""
    var dead = self._dead("$2")
    var waiting = String("held: waiting for a worker of pool ", pool)
    var tx = self.db.conn.begin()
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    var lost = _requeue_lost(self, tx, p, dead, waiting)
    self._end_dead(tx, p, dead)
    # One behind an older live run of its keys says which; a worker
    # doesn't claim it until that run ends.
    hold_behind(tx, p.name, waiting)
    if not p.fill.all:
        _ = tx.execute(
            (
                "UPDATE orca.runs SET held = $2 WHERE pipeline = $1 AND"
                " status = 'queued' AND trigger = 'fill' AND held IS"
                " DISTINCT FROM $2"
            ),
            Params()
            .text(p.name)
            .text(
                'held: fills are off (fills isn\'t "all"), so no worker'
                " is given it"
            ),
        )
    var n = tx.execute(
        String(
            "UPDATE orca.runs SET status = 'queued', owner = NULL,",
            " heartbeat = NULL, retry_at = NULL, held = $5 WHERE pipeline",
            " = $1 AND trigger <> 'fill' AND ((status = 'deferred' AND",
            " retry_at <= $4) OR (status = 'failed' AND ",
            _RETRIED,
            " AND retry_at <= $4) OR (status = 'running' AND ",
            _RETRIED,
            " AND ",
            dead,
            " AND starts - deferrals <= $3))",
        ),
        Params()
        .text(p.name)
        .float64(self.stale)
        .int64(Int64(p.run_retries))
        .int64(now)
        .text(waiting),
    )
    _ = tx.execute(
        (
            "UPDATE orca.runs SET held = $2 WHERE pipeline = $1 AND status"
            " = 'queued' AND held IS NULL"
        ),
        Params().text(p.name).text(waiting),
    )
    tx.commit()
    _say_lost(p, pool, lost)
    if n > 0:
        print("requeued", n, "runs of", p.name, "for pool", pool)


def _say_lost(
    p: Registered, pool: String, lost: List[Tuple[Int, String]]
) raises:
    """Prints each run `_requeue_lost` queued again, with why."""
    for l in lost:
        print(
            String("requeued run ", l[0], " of ", p.name, " for pool ", pool),
            ": ",
            l[1],
            sep="",
        )


def _requeue_lost(
    mut self: Scheduler,
    mut tx: Transaction,
    p: Registered,
    dead: String,
    waiting: String,
) raises -> List[Tuple[Int, String]]:
    """Queues again, for the pool, `p`'s runs whose worker is gone,
    whatever their trigger (a manual, UI or rerun run too, which a dead
    process would fail): a running run whose worker hasn't been seen
    for half the stale limit (killed, or its machine asleep or off the
    network), or whose process died while the worker, seen since,
    doesn't say it has it (restarted). Nothing ran it past its last
    step, so its next start resumes it. A run the worker claimed but
    never started has its start given back; one that started counts
    it, and once `starts - deferrals` is past `run_retries` it is left
    to `_end_dead`, so a run that kills its workers can't loop. Each is
    a row of `orca.audit` (user `scheduler`, action
    `/api/worker/requeue`); returns each run with why."""
    var out = List[Tuple[Int, String]]()
    # In this query orca.runs is `r` (`dead` names it by the table).
    var gone = dead_sql("$2", self.start_grace, "r")
    var rows = tx.query(
        String(
            "WITH lost AS (SELECT r.id, r.owner = '",
            _WORKER_OWNER,
            "' AS unstarted, CASE WHEN w.last_seen IS NULL OR w.last_seen",
            " <= now() - make_interval(secs => $2 / 2) THEN 'worker ' ||",
            " w.label || ' (token ' || w.id || ') stopped: not seen for '",
            " || coalesce(extract(epoch FROM now() -",
            " w.last_seen)::bigint::text || ' s', 'ever') ELSE 'worker '",
            " || w.label || ' (token ' || w.id || ') no longer runs it'",
            " END AS why FROM orca.runs r JOIN orca.workers w ON w.id =",
            " r.worker WHERE r.pipeline = $1 AND r.status = 'running' AND",
            " r.placement LIKE 'worker:%' AND NOT r.cancel_requested AND",
            " (r.owner = '",
            _WORKER_OWNER,
            "' OR r.starts - r.deferrals <= $3) AND (((w.last_seen IS NULL",
            " OR w.last_seen <= now() - make_interval(secs => $2 / 2)) AND",
            " ((r.owner = '",
            _WORKER_OWNER,
            "' AND r.started <= now() - make_interval(secs => $2)) OR ",
            gone,
            ")) OR (",
            gone,
            " AND NOT (r.id::text = ANY (string_to_array(coalesce(",
            "w.running, ''), ','))))) FOR UPDATE OF r) UPDATE orca.runs u",
            " SET status = 'queued', owner = NULL, heartbeat = NULL,",
            " retry_at = NULL, held = $4, starts = CASE WHEN",
            " lost.unstarted THEN u.starts - 1 ELSE u.starts END FROM lost",
            " WHERE u.id = lost.id RETURNING u.id, lost.why,",
            " lost.unstarted",
        ),
        Params()
        .text(p.name)
        .float64(self.stale)
        .int64(Int64(p.run_retries))
        .text(waiting),
    )
    for r in rows:
        var why = r.text("why")
        if r.bool("unstarted"):
            why.write(", before starting it")
        out.append((Int(r.int64("id")), why))
        _ = tx.execute(
            (
                "INSERT INTO orca.audit (username, action, run, detail,"
                " source) VALUES ('scheduler', '/api/worker/requeue', $1,"
                " $2, 'scheduler')"
            ),
            Params().int64(r.int64("id")).text(String("queued again: ", why)),
        )
    return out^


def _requeue_orphans(
    mut self: Scheduler, mut tx: Transaction, p: Registered
) raises -> List[Tuple[Int, String]]:
    """Queues again `p`'s local and control-placed runs whose scheduler
    instance (`runs.instance`, docs/overlap.md) is gone (ended, or not
    seen for the stale limit) and whose heartbeat is stale,
    whatever their trigger: the deploy, or a crash, stopped them, not
    their own code. Nothing ran them past their last step, so the next
    start resumes them. The start counts, and once `starts - deferrals`
    is past `run_retries` a run is left to `_end_dead`, so a run that
    kills its scheduler can't loop. Each is a row of `orca.audit` (user
    `scheduler`, action `/api/scheduler/requeue`); returns each with
    why."""
    var out = List[Tuple[Int, String]]()
    var rows = tx.query(
        String(
            "WITH lost AS (SELECT r.id, 'scheduler instance ' || s.id ||",
            " ' on ' || s.host || CASE WHEN s.ended IS NOT NULL THEN",
            " ' ended' ELSE ' stopped: not seen for ' || extract(epoch",
            " FROM now() - s.last_seen)::bigint || ' s' END AS why FROM",
            " orca.runs r JOIN orca.schedulers s ON s.id = r.instance WHERE",
            " r.pipeline = $1 AND r.status = 'running' AND r.placement IN",
            " ('local', 'control') AND NOT r.cancel_requested AND",
            " r.starts - r.deferrals <= $3 AND (s.ended IS NOT NULL OR",
            " s.last_seen <= now() - make_interval(secs => $2)) AND ",
            stale_sql("$2", self.start_grace, "r"),
            " FOR UPDATE OF r) UPDATE orca.runs u SET status = 'queued',",
            " owner = NULL, heartbeat = NULL, retry_at = NULL, held = NULL,",
            " instance = NULL FROM lost WHERE u.id = lost.id RETURNING",
            " u.id, lost.why",
        ),
        Params().text(p.name).float64(self.stale).int64(Int64(p.run_retries)),
    )
    for r in rows:
        var why = r.text("why")
        out.append((Int(r.int64("id")), why))
        _ = tx.execute(
            (
                "INSERT INTO orca.audit (username, action, run, detail,"
                " source) VALUES ('scheduler', '/api/scheduler/requeue',"
                " $1, $2, 'scheduler')"
            ),
            Params().int64(r.int64("id")).text(String("queued again: ", why)),
        )
    return out^


def _fill_for_workers(
    mut self: Scheduler, p: Registered, now: Int64, pool: String
) raises:
    """`_start`'s fill pass for a pipeline placed `worker:<pool>`: as
    for a local one, but what may start is queued for a worker of the
    pool to claim rather than started. Within `morca.fill.fill_status`'s
    slots (`fill_max_runs` less its fill runs running or queued, so the
    cap holds across the whole pool), the due retries, deferred runs
    and fill runs whose process died with retries left are queued
    again first, then new fill runs over the partitions nothing has
    filled, in `fill_order`."""
    var dead = self._dead("$2")
    var waiting = String("held: waiting for a worker of pool ", pool)
    var tx = self.db.conn.begin()
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    # A gone worker's fill runs first, as the other pass does, so they
    # are queued again with why (and an audit row), not as merely dead:
    # the tick's passes run a moment apart, and either may see them first.
    var lost = _requeue_lost(self, tx, p, dead, waiting)
    var status = fill_status(tx, fill_pipeline(p), now, p.run_retries)
    var free = status.slots
    var again = 0
    if free > 0:
        again = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'queued', owner = NULL,",
                " heartbeat = NULL, retry_at = NULL, held = $5 WHERE id IN",
                " (SELECT id FROM orca.runs WHERE pipeline = $1 AND",
                " trigger = 'fill' AND ((status = 'deferred' AND retry_at",
                " <= $4) OR (status = 'failed' AND retry_at <= $4 AND",
                " starts - deferrals BETWEEN 1 AND $3) OR (status =",
                " 'running' AND ",
                dead,
                " AND starts - deferrals <= $3)) ORDER BY id LIMIT $6)",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries))
            .int64(now)
            .text(waiting)
            .int64(Int64(free)),
        )
    var ids = List[Int64]()
    ref next = status.next_keys
    for i in range(min(free - again, len(next))):
        ids.append(
            tx.query(
                (
                    "INSERT INTO orca.runs (pipeline, registry_hash,"
                    " partitions, trigger, status, held) VALUES ($1, $2,"
                    " $3, 'fill', 'queued', $4) RETURNING id"
                ),
                Params()
                .text(p.name)
                .text(p.registry_hash)
                .text(next[i])
                .text(waiting),
            )
            .row(0)
            .int64("id")
        )
    mark_batch(tx, ids)
    tx.commit()
    _say_lost(p, pool, lost)
    if again > 0:
        print("requeued", again, "fill runs of", p.name, "for pool", pool)
    for i in range(len(ids)):
        print("fill", p.name, next[i], "run", ids[i], "for pool", pool)
