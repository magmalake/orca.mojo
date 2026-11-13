"""Fires and sensors' queries (`morca.sensing`): `orca.sensed`, the
snapshots a sensor has seen, `orca.fires`, a schedule's, the runs they
queue, and a pipeline's `sense_error`."""

from postgres import Connection, Params, Transaction

from morca._internal.db.live import is_live_sql


@fieldwise_init
struct Commit(Copyable, Movable):
    """A snapshot of a dataset a sensor reads, and the keys it holds."""

    var dataset: String
    var snapshot: Int64
    var keys: String
    """Comma-separated: the step's partition, else its run's."""


def unsensed(mut conn: Connection, pipeline: String) raises -> List[Commit]:
    """The snapshots of `pipeline`'s datasets (those it reads and doesn't
    write) that other pipelines' succeeded runs committed since it became
    a sensor, and that it hasn't sensed or rebuilt."""
    var out = List[Commit]()
    for r in conn.query(
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
        Params().text(pipeline),
    ):
        out.append(
            Commit(r.text("dataset"), r.int64("snapshot_id"), r.text("keys"))
        )
    return out^


def see(
    mut tx: Transaction, pipeline: String, dataset: String, snapshot: Int64
) raises -> Bool:
    """Records that `pipeline` sensed `dataset` at `snapshot`; False if it
    had already."""
    var new = tx.query(
        (
            "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id)"
            " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
            " snapshot_id"
        ),
        Params().text(pipeline).text(dataset).int64(snapshot),
    )
    return len(new) > 0


def sensed_error(
    mut tx: Transaction,
    pipeline: String,
    dataset: String,
    snapshot: Int64,
    error: String,
) raises:
    """Records why the snapshot `pipeline` sensed ran nothing."""
    _ = tx.execute(
        (
            "UPDATE orca.sensed SET error = $4 WHERE pipeline = $1"
            " AND dataset = $2 AND snapshot_id = $3"
        ),
        Params().text(pipeline).text(dataset).int64(snapshot).text(error),
    )


def sensed_run(
    mut tx: Transaction,
    pipeline: String,
    dataset: String,
    snapshot: Int64,
    run: Int64,
) raises:
    """Records `run` as the run of the snapshot `pipeline` sensed."""
    _ = tx.execute(
        (
            "UPDATE orca.sensed SET run = $4 WHERE pipeline = $1"
            " AND dataset = $2 AND snapshot_id = $3"
        ),
        Params().text(pipeline).text(dataset).int64(snapshot).int64(run),
    )


@fieldwise_init
struct LiveKey(Copyable, Movable):
    """A key a live run has."""

    var key: String
    var run: Int
    var behind: Bool
    """The run read an older snapshot of a dataset offering the key."""


def live_keys(
    mut tx: Transaction,
    pipeline: String,
    keys: String,
    datasets: String,
    snapshots: String,
) raises -> List[LiveKey]:
    """Of the keys offered (`keys`, each beside its dataset in `datasets`
    and snapshot in `snapshots`, all comma-separated), those a live run of
    `pipeline` has, by key and run, each saying whether the run read an
    older snapshot: a step over the key (or over all the run's keys) read
    the dataset at another one, unless morca committed both and that one
    later."""
    var out = List[LiveKey]()
    for r in tx.query(
        String(
            (
                "WITH x AS (SELECT x.key, x.ds, x.snap FROM"
                " unnest(string_to_array($2, ','),"
            ),
            " string_to_array($3, ','), string_to_array($4,",
            " ',')::bigint[]) AS x(key, ds, snap)), l AS (SELECT DISTINCT",
            " k.key, r.id FROM orca.runs r, unnest(string_to_array(",
            "r.partitions, ',')) AS k(key) WHERE r.pipeline = $1 AND ",
            is_live_sql("r"),
            " AND k.key IN (SELECT key FROM x)) SELECT l.key, l.id, EXISTS",
            " (SELECT 1 FROM x JOIN orca.steps s ON s.run = l.id AND",
            " s.partition IN (l.key, '') JOIN orca.attempts a ON a.step =",
            " coalesce(s.copied_from, s.id) JOIN orca.reads rd ON",
            " rd.attempt = a.id AND rd.dataset = x.ds WHERE x.key = l.key",
            " AND rd.snapshot_id <> x.snap AND NOT EXISTS (SELECT 1 FROM",
            " orca.commits cr, orca.commits cx WHERE cr.dataset = x.ds AND",
            " cr.snapshot_id = rd.snapshot_id AND cx.dataset = x.ds AND",
            " cx.snapshot_id = x.snap AND cr.attempt > cx.attempt)) AS",
            " behind FROM l ORDER BY l.key, l.id",
        ),
        Params().text(pipeline).text(keys).text(datasets).text(snapshots),
    ):
        out.append(LiveKey(r.text("key"), Int(r.int64("id")), r.bool("behind")))
    return out^


def add_sensor_run(
    mut tx: Transaction, pipeline: String, registry_hash: String, keys: String
) raises -> Int64:
    """A new queued run of `pipeline` over `keys`, trigger `sensor`."""
    return (
        tx.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash,"
                " partitions, trigger, status) VALUES ($1, $2, $3,"
                " 'sensor', 'queued') RETURNING id"
            ),
            Params().text(pipeline).text(registry_hash).text(keys),
        )
        .row(0)
        .int64("id")
    )


@fieldwise_init
struct Unstarted(Copyable, Movable):
    """A queued sensor run that never started."""

    var id: Int64
    var partitions: String


def unstarted_sensor_runs(
    mut tx: Transaction, pipeline: String, registry_hash: String
) raises -> List[Unstarted]:
    """`pipeline`'s sensor runs of `registry_hash` queued and never started,
    oldest first."""
    var out = List[Unstarted]()
    for r in tx.query(
        (
            "SELECT id, partitions FROM orca.runs WHERE pipeline = $1 AND"
            " trigger = 'sensor' AND status = 'queued' AND starts = 0 AND"
            " registry_hash = $2 ORDER BY id"
        ),
        Params().text(pipeline).text(registry_hash),
    ):
        out.append(Unstarted(r.int64("id"), r.text("partitions")))
    return out^


def set_partitions(mut tx: Transaction, run: Int64, keys: String) raises:
    """Has `run` run over `keys` (comma-separated)."""
    _ = tx.execute(
        "UPDATE orca.runs SET partitions = $2 WHERE id = $1",
        Params().int64(run).text(keys),
    )


def clear_sense_error(mut conn: Connection, pipeline: String) raises:
    """`pipeline`'s last pass sensed without an error."""
    _ = conn.execute(
        (
            "UPDATE orca.pipelines SET sense_error = NULL,"
            " sense_error_at = NULL WHERE name = $1 AND sense_error IS"
            " NOT NULL"
        ),
        Params().text(pipeline),
    )


def set_sense_error(
    mut conn: Connection, pipeline: String, error: String
) raises -> Bool:
    """`pipeline`'s last pass failed with `error`; True if that's new."""
    var changed = conn.query(
        (
            "UPDATE orca.pipelines SET sense_error = $2, sense_error_at ="
            " now() WHERE name = $1 AND sense_error IS DISTINCT FROM $2"
            " RETURNING name"
        ),
        Params().text(pipeline).text(error),
    )
    return len(changed) > 0


def last_fire(
    mut conn: Connection, pipeline: String, since: Int64
) raises -> Int64:
    """When `pipeline` last fired, Unix time, else `since`."""
    return (
        conn.query(
            (
                "SELECT coalesce(max(fire_at), $2) AS last FROM orca.fires"
                " WHERE pipeline = $1"
            ),
            Params().text(pipeline).int64(since),
        )
        .row(0)
        .int64("last")
    )


def add_fire(
    mut tx: Transaction, pipeline: String, at: Int64, key: String
) raises -> Bool:
    """Records `pipeline`'s fire at `at`, for `key`; False if it was."""
    var made = tx.query(
        (
            "INSERT INTO orca.fires (pipeline, fire_at, partition)"
            " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
            " fire_at"
        ),
        Params().text(pipeline).int64(at).text(key),
    )
    return len(made) == 1


def fire_run(
    mut tx: Transaction, pipeline: String, at: Int64, run: Int64
) raises:
    """Records `run` as the run of `pipeline`'s fire at `at`."""
    _ = tx.execute(
        "UPDATE orca.fires SET run = $3 WHERE pipeline = $1 AND fire_at = $2",
        Params().text(pipeline).int64(at).int64(run),
    )


def add_schedule_run(
    mut tx: Transaction, pipeline: String, registry_hash: String, key: String
) raises -> Int64:
    """A new queued run of `pipeline` over `key`, trigger `schedule`."""
    return (
        tx.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash,"
                " partitions, trigger, status) VALUES ($1, $2, $3,"
                " 'schedule', 'queued') RETURNING id"
            ),
            Params().text(pipeline).text(registry_hash).text(key),
        )
        .row(0)
        .int64("id")
    )


comptime _UPSTREAM_SQL = (
    "WITH me AS (SELECT describe FROM orca.pipelines WHERE name = $1 AND"
    " coalesce((describe->>'sensor')::boolean, false)), io AS (SELECT k.kind,"
    " x.ds FROM me, jsonb_array_elements(me.describe->'steps') AS s(st),"
    " (VALUES ('reads'), ('writes')) AS k(kind),"
    " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS x(ds) UNION"
    " SELECT 'reads', me.describe->'partitions'->>'dataset' FROM me WHERE"
    " me.describe->'partitions'->>'dataset' IS NOT NULL), ds AS (SELECT ds"
    " FROM io WHERE kind = 'reads' EXCEPT SELECT ds FROM io WHERE kind ="
    " 'writes') SELECT ds AS dataset, '' AS pipeline, '' AS keys FROM ds UNION"
    " SELECT c.dataset, r.pipeline, coalesce(nullif(s.partition, ''),"
    " r.partitions) FROM ds JOIN orca.commits c ON c.dataset = ds.ds JOIN"
    " orca.attempts a ON a.id = c.attempt JOIN orca.steps s ON s.id = a.step"
    " JOIN orca.runs r ON r.id = s.run WHERE r.pipeline <> $1 AND r.status ="
    " 'succeeded' AND a.status = 'succeeded' UNION SELECT x.dataset, '',"
    " r.partitions FROM orca.sensed x JOIN orca.runs r ON r.id = x.run WHERE"
    " x.pipeline = $1 AND x.dataset IN (SELECT ds FROM ds)"
)


@fieldwise_init
struct Upstream(Copyable, Movable):
    """A dataset a sensor follows (`keys` empty), or the keys of one of its
    snapshots."""

    var dataset: String
    var pipeline: String
    """The pipeline that committed it; empty if not morca's."""
    var keys: String


def upstream(mut conn: Connection, pipeline: String) raises -> List[Upstream]:
    """For `pipeline` if it's a sensor: a row per dataset it follows, then
    the keys of each snapshot of them (`morca.follow`); none if it isn't."""
    var out = List[Upstream]()
    for r in conn.query(_UPSTREAM_SQL, Params().text(pipeline)):
        out.append(
            Upstream(r.text("dataset"), r.text("pipeline"), r.text("keys"))
        )
    return out^
