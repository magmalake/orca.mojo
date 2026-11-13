"""The EC2 runner's queries (`morca._internal.ec2`): a run's start as it was given
(`orca.runs.spec`), its instances in `orca.instances`, and the warm pool's
hand-overs, sweeps and rescues."""

from postgres import Connection, Params, Row, Transaction


comptime _SPEC_COLUMNS = (
    "coalesce(r.spec->>'image', p.image, '') AS image,"
    " coalesce(r.spec->>'binary', p.binary_path) AS binary_path,"
    " coalesce((r.spec->>'tasks')::boolean,"
    " jsonb_array_length(coalesce(p.describe->'tasks', '[]')) > 0) AS tasks,"
    " coalesce(r.spec->>'head_type', p.head_type, '') AS head_type,"
    " coalesce(r.spec->>'worker_type', p.worker_type, '') AS worker_type,"
    " coalesce((r.spec->>'workers')::bigint, p.ec2_workers,"
    " (p.describe->>'workers')::bigint, 2) AS hi,"
    " coalesce((r.spec->>'spot')::boolean, p.spot, false) AS spot,"
    " coalesce((r.spec->>'min_workers')::bigint, p.ec2_min_workers, -1) AS lo,"
    " coalesce((r.spec->'secrets')::text, p.secrets::text, '') AS secrets,"
    " coalesce((r.spec->>'worker_procs')::bigint, p.worker_procs, 1) AS procs,"
    " coalesce(r.spec->>'registry_hash', p.registry_hash) AS registry_hash,"
    " coalesce((r.spec->'env')::text, p.env::text, '') AS env"
)
"""A run's spec as SQL over `orca.runs r` and `orca.pipelines p`: the
registration its start was given (`orca.runs.spec`, kept by `save_spec`),
else, for a run from before that was kept, the pipeline's now."""


@fieldwise_init
struct SpecRow(Copyable, Movable):
    """A run's start as `_SPEC_COLUMNS` reads it."""

    var run: Int
    var start: Int
    var pipeline: String
    var allow_new_code: Bool
    var image: String
    var binary_path: String
    var tasks: Bool
    var head_type: String
    var worker_type: String
    var hi: Int
    """Workers at most."""
    var spot: Bool
    var lo: Int
    """Workers it starts with; -1 when unset."""
    var secrets: String
    """JSON, or empty."""
    var procs: Int
    var registry_hash: String
    var env: String
    """JSON, or empty."""


def _spec_row(r: Row, run: Int, start: Int) raises -> SpecRow:
    return SpecRow(
        run,
        start,
        r.text("pipeline"),
        r.bool("allow_new_code"),
        r.text("image"),
        r.text("binary_path"),
        r.bool("tasks"),
        r.text("head_type"),
        r.text("worker_type"),
        Int(r.int64("hi")),
        r.bool("spot"),
        Int(r.int64("lo")),
        r.text("secrets"),
        Int(r.int64("procs")),
        r.text("registry_hash"),
        r.text("env"),
    )


@fieldwise_init
struct ScalingRun(Copyable, Movable):
    """A running run with a live head that has an address."""

    var spec: SpecRow
    var waited: Float64
    """Seconds it has waited for workers; -1 when it isn't."""
    var head_address: String


@fieldwise_init
struct PoolCandidate(Copyable, Movable):
    """An idle instance of the pool, locked for a hand-over."""

    var id: String
    var run: Int
    var start: Int
    var instance_type: String
    var market: String
    var address: String
    var procs: Int
    var cpu_millis: Int


@fieldwise_init
struct SweepRow(Copyable, Movable):
    """A live instance, as the sweep sees it."""

    var id: String
    var serves: String
    """`<run>-<start>` of its live row."""
    var pooling: Bool
    """In the pool, or on its way: keyed, not drained, its run ended."""
    var idle: Bool
    var idle_for: Float64
    var since_poll: Float64
    var since_end: Float64


@fieldwise_init
struct Unjoined(Copyable, Movable):
    """An instance the pool handed a running run that hasn't joined it."""

    var id: String
    var run: Int
    var start: Int
    var role: String


def save_spec(mut conn: Connection, run: Int, start: Int, spec: String) raises:
    """Keeps `spec` (JSON) with `run`, if `start` is its current start."""
    _ = conn.execute(
        (
            "UPDATE orca.runs SET spec = $3::text::jsonb WHERE id = $1 AND"
            " starts = $2"
        ),
        Params().int64(Int64(run)).int64(Int64(start)).text(spec),
    )


def pool_candidates(
    mut tx: Transaction, key: String, role: String, live: Float64, n: Int
) raises -> List[PoolCandidate]:
    """Up to `n` idle instances of `key` in `role` that asked for a run in
    the last `live` seconds, whose run has ended and that weren't drained,
    longest idle first; their rows locked (`FOR UPDATE SKIP LOCKED`), so
    two starts can't take the same one."""
    var out = List[PoolCandidate]()
    for row in tx.query(
        (
            "SELECT i.id, i.run, i.start, i.instance_type, i.market,"
            " coalesce(i.address, '') AS address, coalesce(i.procs, 1) AS"
            " procs, coalesce(i.cpu_millis, 0) AS cpu FROM orca.instances i"
            " JOIN orca.runs r ON r.id = i.run WHERE i.ended IS NULL AND"
            " i.pool_key = $1 AND i.role = $2 AND i.idle IS NOT NULL AND"
            " i.drained IS NULL AND i.polled > now() - make_interval(secs"
            " => $3) AND r.status <> 'running' ORDER BY i.idle DESC, i.id"
            " LIMIT $4 FOR UPDATE OF i SKIP LOCKED"
        ),
        Params().text(key).text(role).float64(live).int64(Int64(n)),
    ):
        out.append(
            PoolCandidate(
                row.text("id"),
                Int(row.int64("run")),
                Int(row.int64("start")),
                row.text("instance_type"),
                row.text("market"),
                row.text("address"),
                Int(row.int64("procs")),
                Int(row.int64("cpu")),
            )
        )
    return out^


def hand_on(
    mut tx: Transaction,
    c: PoolCandidate,
    run: Int,
    start: Int,
    role: String,
    key: String,
) raises:
    """Ends pooled instance `c`'s row for its old run and makes its row for
    `run`'s `start`."""
    _ = tx.execute(
        (
            "UPDATE orca.instances SET ended = now(), why = 'pooled: on"
            " to run ' || $4 WHERE id = $1 AND run = $2 AND start = $3"
        ),
        Params()
        .text(c.id)
        .int64(Int64(c.run))
        .int64(Int64(c.start))
        .text(String(run)),
    )
    _ = tx.execute(
        (
            "INSERT INTO orca.instances (id, run, start, role,"
            " instance_type, market, address, procs, pool_key,"
            " cpu_millis, pooled) VALUES ($1, $2, $3, $4, $5, $6,"
            " nullif($7, ''), $8, $9, $10, true)"
        ),
        Params()
        .text(c.id)
        .int64(Int64(run))
        .int64(Int64(start))
        .text(role)
        .text(c.instance_type)
        .text(c.market)
        .text(c.address)
        .int64(Int64(c.procs))
        .text(key)
        .int64(Int64(c.cpu_millis)),
    )


def settling(
    mut conn: Connection, a: String, b: String, grace: Float64
) raises -> Bool:
    """Whether an instance of key `a` or `b`, not idle nor drained, belongs
    to a run that ended within `grace` seconds."""
    return (
        conn.query(
            (
                "SELECT count(*) > 0 AS any FROM orca.instances i JOIN"
                " orca.runs r ON r.id = i.run WHERE i.ended IS NULL AND"
                " i.pool_key IN ($1, $2) AND i.idle IS NULL AND i.drained"
                " IS NULL AND r.status <> 'running' AND r.ended > now() -"
                " make_interval(secs => $3)"
            ),
            Params().text(a).text(b).float64(grace),
        )
        .row(0)
        .bool("any")
    )


def scaling_runs(mut conn: Connection) raises -> List[ScalingRun]:
    """The running runs whose current start has a live head with an
    address, with their specs."""
    var out = List[ScalingRun]()
    for row in conn.query(
        String(
            "SELECT r.id, r.starts, r.pipeline, r.allow_new_code,",
            (
                " coalesce(extract(epoch FROM now() -"
                " r.waiting_since)::float8, -1)"
            ),
            " AS waited, h.address, ",
            _SPEC_COLUMNS,
            " FROM orca.runs r JOIN orca.pipelines p ON p.name =",
            " r.pipeline JOIN orca.instances h ON h.run = r.id AND h.start",
            " = r.starts AND h.role = 'head' AND h.ended IS NULL AND",
            " h.address IS NOT NULL WHERE r.status = 'running'",
        ),
        Params(),
    ):
        out.append(
            ScalingRun(
                _spec_row(row, Int(row.int64("id")), Int(row.int64("starts"))),
                row.float64("waited"),
                row.text("address"),
            )
        )
    return out^


def start_waiting(mut conn: Connection, run: Int, start: Int) raises:
    """Notes that `run`'s `start` has begun waiting for workers, unless it
    is already."""
    _ = conn.execute(
        (
            "UPDATE orca.runs SET waiting_since = now() WHERE"
            " id = $1 AND starts = $2 AND waiting_since IS"
            " NULL"
        ),
        Params().int64(Int64(run)).int64(Int64(start)),
    )


def stop_waiting(mut conn: Connection, run: Int, start: Int) raises:
    _ = conn.execute(
        (
            "UPDATE orca.runs SET waiting_since = NULL WHERE"
            " id = $1 AND starts = $2"
        ),
        Params().int64(Int64(run)).int64(Int64(start)),
    )


def worker_procs_up(mut conn: Connection, run: Int, start: Int) raises -> Int:
    """The worker processes `run`'s `start` has on its live, undrained
    worker instances."""
    return Int(
        conn.query(
            (
                "SELECT coalesce(sum(coalesce(procs, 1) -"
                " procs_drained), 0) AS n FROM orca.instances WHERE"
                " run = $1 AND start = $2 AND role = 'worker' AND"
                " ended IS NULL AND drained IS NULL"
            ),
            Params().int64(Int64(run)).int64(Int64(start)),
        )
        .row(0)
        .int64("n")
    )


def drained_one(
    mut conn: Connection, run: Int, start: Int, address: String
) raises:
    """Counts a process drained on the oldest live worker of `run`'s
    `start` at `address`, and marks it drained once all its are."""
    _ = conn.execute(
        (
            "UPDATE orca.instances SET procs_drained ="
            " procs_drained + 1, drained = CASE WHEN procs_drained"
            " + 1 >= coalesce(procs, 1) THEN now() END WHERE id ="
            " (SELECT id FROM orca.instances WHERE run = $1 AND"
            " start = $2 AND role = 'worker' AND ended IS NULL AND"
            " drained IS NULL AND address = $3 ORDER BY launched"
            " LIMIT 1)"
        ),
        Params().int64(Int64(run)).int64(Int64(start)).text(address),
    )


def end_start(
    mut conn: Connection, run: Int, start: Int, why: String
) raises -> List[String]:
    """Ends the live instances of `run`'s `start`, saying `why`; returns
    their ids."""
    var ids = List[String]()
    for o in conn.query(
        (
            "UPDATE orca.instances SET ended = now(), why = $3 WHERE run ="
            " $1 AND start = $2 AND ended IS NULL RETURNING id"
        ),
        Params().int64(Int64(run)).int64(Int64(start)).text(why),
    ):
        ids.append(o.text("id"))
    return ids^


def running_starts(mut conn: Connection) raises -> List[String]:
    """`<run>-<start>` of each running run."""
    var out = List[String]()
    for row in conn.query(
        (
            "SELECT id || '-' || starts AS k FROM orca.runs WHERE status ="
            " 'running'"
        ),
        Params(),
    ):
        out.append(row.text("k"))
    return out^


def ended_starts(mut conn: Connection, within: Float64) raises -> List[String]:
    """`<run>-<start>` of each run not running that ended within `within`
    seconds."""
    var out = List[String]()
    for row in conn.query(
        (
            "SELECT id || '-' || starts AS k FROM orca.runs WHERE status"
            " <> 'running' AND ended > now() - make_interval(secs => $1)"
        ),
        Params().float64(within),
    ):
        out.append(row.text("k"))
    return out^


def sweep_rows(mut conn: Connection) raises -> List[SweepRow]:
    """Every live instance, with what the sweep decides on."""
    var out = List[SweepRow]()
    for row in conn.query(
        (
            "SELECT i.id, i.run || '-' || i.start AS k, i.pool_key IS NOT"
            " NULL AND i.drained IS NULL AND r.status <> 'running' AS"
            " pooling, i.idle IS NOT NULL AS idle, coalesce(extract(epoch"
            " FROM now() - i.idle), 0)::float8 AS idle_for,"
            " coalesce(extract(epoch FROM now() - i.polled),"
            " 1e9)::float8 AS since_poll, coalesce(extract(epoch FROM"
            " now() - r.ended), 1e9)::float8 AS since_end FROM"
            " orca.instances i JOIN orca.runs r ON r.id = i.run WHERE"
            " i.ended IS NULL"
        ),
        Params(),
    ):
        out.append(
            SweepRow(
                row.text("id"),
                row.text("k"),
                row.bool("pooling"),
                row.bool("idle"),
                row.float64("idle_for"),
                row.float64("since_poll"),
                row.float64("since_end"),
            )
        )
    return out^


def utc_ago(mut conn: Connection, seconds: Float64) raises -> String:
    """The database's time `seconds` ago, as `YYYY-MM-DDTHH:MM:SS` in UTC
    (how EC2 gives launch times)."""
    return (
        conn.query(
            (
                "SELECT to_char((now() - make_interval(secs => $1)) AT TIME"
                " ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') AS t"
            ),
            Params().float64(seconds),
        )
        .row(0)
        .text("t")
    )


def end_row(
    mut conn: Connection, id: String, run: Int, start: Int, why: String
) raises -> Int:
    """Ends instance `id`'s live row for `run`'s `start`, saying `why`;
    returns 1 if it was live, else 0."""
    return conn.execute(
        (
            "UPDATE orca.instances SET ended = now(), why = $4 WHERE id"
            " = $1 AND run = $2 AND start = $3 AND ended IS NULL"
        ),
        Params().text(id).int64(Int64(run)).int64(Int64(start)).text(why),
    )


def unjoined(mut conn: Connection, join: Float64) raises -> List[Unjoined]:
    """The instances the pool handed a running run's current start over
    `join` seconds ago that haven't joined it, by role."""
    var out = List[Unjoined]()
    for row in conn.query(
        (
            "SELECT i.id, i.run, i.start, i.role FROM orca.instances i JOIN"
            " orca.runs r ON r.id = i.run AND r.starts = i.start AND"
            " r.status = 'running' WHERE i.ended IS NULL AND i.pooled AND"
            " i.joined IS NULL AND i.launched < now() - make_interval(secs"
            " => $1) ORDER BY i.role, i.id"
        ),
        Params().float64(join),
    ):
        out.append(
            Unjoined(
                row.text("id"),
                Int(row.int64("run")),
                Int(row.int64("start")),
                row.text("role"),
            )
        )
    return out^


def end_never_joined(
    mut conn: Connection, id: String, run: Int, start: Int
) raises -> Int:
    """Ends pooled instance `id`'s live row for `run`'s `start`: it never
    took the run up. Returns 1 if it was live, else 0."""
    return conn.execute(
        (
            "UPDATE orca.instances SET ended = now(), why = 'pooled:"
            " never took the run up' WHERE id = $1 AND run = $2 AND"
            " start = $3 AND ended IS NULL"
        ),
        Params().text(id).int64(Int64(run)).int64(Int64(start)),
    )


def head_address(
    mut conn: Connection, run: Int, start: Int
) raises -> Optional[String]:
    """The address of `run`'s `start`'s live head (empty if it has none
    yet), or None without a live head."""
    var head = conn.query(
        (
            "SELECT coalesce(address, '') AS a FROM orca.instances"
            " WHERE run = $1 AND start = $2 AND role = 'head' AND"
            " ended IS NULL"
        ),
        Params().int64(Int64(run)).int64(Int64(start)),
    )
    if len(head) == 0:
        return None
    return head.row(0).text("a")


def end_lost_head(
    mut conn: Connection, run: Int, start: Int
) raises -> List[String]:
    """Ends the live instances of `run`'s `start`, whose pooled head never
    took the run up; returns their ids."""
    var others = List[String]()
    for o in conn.query(
        (
            "UPDATE orca.instances SET ended = now(), why = 'its pooled"
            " head never took the run up' WHERE run = $1 AND start = $2"
            " AND ended IS NULL RETURNING id"
        ),
        Params().int64(Int64(run)).int64(Int64(start)),
    ):
        others.append(o.text("id"))
    return others^


def spec_of(
    mut conn: Connection, run: Int, start: Int
) raises -> Optional[SpecRow]:
    """`run`'s `start` as it was given, if `start` is its current one."""
    var rows = conn.query(
        String(
            "SELECT r.pipeline, r.allow_new_code, ",
            _SPEC_COLUMNS,
            (
                " FROM orca.runs r JOIN orca.pipelines p ON p.name ="
                " r.pipeline WHERE r.id = $1 AND r.starts = $2"
            ),
        ),
        Params().int64(Int64(run)).int64(Int64(start)),
    )
    if len(rows) == 0:
        return None
    return _spec_row(rows.row(0), run, start)


def live_ids(mut conn: Connection) raises -> List[String]:
    """Every instance `orca.instances` has as live."""
    var out = List[String]()
    for row in conn.query(
        "SELECT id FROM orca.instances WHERE ended IS NULL", Params()
    ):
        out.append(row.text("id"))
    return out^


def add_instance(
    mut conn: Connection,
    id: String,
    run: Int,
    start: Int,
    role: String,
    instance_type: String,
    market: String,
    address: String,
    procs: Int,
    key: String,
    cpu_millis: Int,
) raises:
    """Records a launched instance, unless it's recorded already."""
    _ = conn.execute(
        (
            "INSERT INTO orca.instances (id, run, start, role,"
            " instance_type, market, address, procs, pool_key, cpu_millis)"
            " VALUES ($1, $2, $3, $4, $5, $6, nullif($7, ''), $8,"
            " nullif($9, ''), $10) ON CONFLICT DO NOTHING"
        ),
        Params()
        .text(id)
        .int64(Int64(run))
        .int64(Int64(start))
        .text(role)
        .text(instance_type)
        .text(market)
        .text(address)
        .int64(Int64(procs))
        .text(key)
        .int64(Int64(cpu_millis)),
    )


def ended(mut conn: Connection, id: String, why: String) raises:
    """Records that instance `id` ended, unless it's recorded already; a
    drained one that shut itself down was idle."""
    _ = conn.execute(
        (
            "UPDATE orca.instances SET ended = now(), why = CASE WHEN drained"
            " IS NOT NULL AND $2 = 'shut itself down' THEN 'drained: idle'"
            " ELSE $2 END WHERE id = $1 AND ended IS NULL"
        ),
        Params().text(id).text(why),
    )


# ---------------------------------------------------------------------------
# The instance's side of the warm pool (`morca._internal.pool`)
# ---------------------------------------------------------------------------


def run_over(
    mut conn: Connection, id: String, run: Int, start: Int
) raises -> Bool:
    """Whether `run` isn't running at `start` any more, or instance `id`'s
    row for it has ended (or there's no such run)."""
    var rows = conn.query(
        (
            "SELECT r.status <> 'running' OR r.starts <> $3 OR i.id IS NULL OR"
            " i.ended IS NOT NULL AS over FROM orca.runs r LEFT JOIN"
            " orca.instances i ON i.id = $1 AND i.run = r.id AND i.start = $3"
            " WHERE r.id = $2"
        ),
        Params().text(id).int64(Int64(run)).int64(Int64(start)),
    )
    return len(rows) == 0 or rows.row(0).bool("over")


def beat_idle(
    mut conn: Connection, id: String, run: Int, start: Int
) raises -> Int:
    """Marks instance `id`'s live row for `run` at `start` idle (from the
    first time) and polled now; returns how many rows (0 once it's ended)."""
    return conn.execute(
        (
            "UPDATE orca.instances SET idle = coalesce(idle, now()),"
            " polled = now() WHERE id = $1 AND run = $2 AND start = $3"
            " AND ended IS NULL"
        ),
        Params().text(id).int64(Int64(run)).int64(Int64(start)),
    )


@fieldwise_init
struct Handed(Copyable, Movable):
    """The run the pool handed an instance, as it joins it."""

    var run: Int
    var start: Int
    var allow_new_code: Bool
    var head: String
    """The run's live head's address, or empty."""


def join_handed(mut conn: Connection, id: String) raises -> Optional[Handed]:
    """Records that instance `id` joined the run its live pooled row is
    for, and returns that run; None when it has no such row."""
    var got = conn.query(
        (
            "UPDATE orca.instances i SET joined = now() FROM"
            " orca.runs r WHERE r.id = i.run AND i.id = $1 AND"
            " i.ended IS NULL AND i.pooled RETURNING i.run,"
            " i.start, r.allow_new_code, coalesce((SELECT"
            " h.address FROM orca.instances h WHERE h.run = i.run"
            " AND h.start = i.start AND h.role = 'head' AND"
            " h.ended IS NULL), '') AS head"
        ),
        Params().text(id),
    )
    if len(got) == 0:
        return None
    var r = got.row(0)
    return Handed(
        Int(r.int64("run")),
        Int(r.int64("start")),
        r.bool("allow_new_code"),
        r.text("head"),
    )
