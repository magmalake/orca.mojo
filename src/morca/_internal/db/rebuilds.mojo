"""Rebuilds' queries (`morca.sensors`): `orca.rebuilds`, their plans in
`orca.rebuild_pipelines`, and the runs they queue."""

from postgres import Connection, Params, Transaction


@fieldwise_init
struct Planned(Copyable, Movable):
    """A pipeline a rebuild runs: its level, partitions and run."""

    var pipeline: String
    var level: Int
    var partitions: String
    var run: Int
    """0 until queued."""
    var status: String
    """The run's status, or empty."""


@fieldwise_init
struct Rebuild(Copyable, Movable):
    var id: Int
    var dataset: String
    var partitions: String
    var status: String
    var created: String
    var ended: String
    var plan: List[Planned]


@fieldwise_init
struct PipelineKeys(Copyable, Movable):
    """A pipeline's partitions, as a rebuild maps keys to them."""

    var name: String
    var has_binary: Bool
    var kind: String
    var start: String


def pipeline_keys(mut conn: Connection) raises -> List[PipelineKeys]:
    """Every pipeline's partition kind and first partition, by name."""
    var out = List[PipelineKeys]()
    for r in conn.query(
        (
            "SELECT name, binary_path IS NOT NULL AS has_binary,"
            " coalesce(describe->'partitions'->>'kind', 'monthly') AS kind,"
            " coalesce(describe->'partitions'->>'start', '') AS start FROM"
            " orca.pipelines ORDER BY name"
        ),
        Params(),
    ):
        out.append(
            PipelineKeys(
                r.text("name"),
                r.bool("has_binary"),
                r.text("kind"),
                r.text("start"),
            )
        )
    return out^


def add_rebuild(
    mut tx: Transaction, dataset: String, partitions: String
) raises -> Int64:
    """A new running rebuild of `dataset` over `partitions`; its id."""
    return (
        tx.query(
            (
                "INSERT INTO orca.rebuilds (dataset, partitions, status) VALUES"
                " ($1, $2, 'running') RETURNING id"
            ),
            Params().text(dataset).text(partitions),
        )
        .row(0)
        .int64("id")
    )


def add_planned(
    mut tx: Transaction,
    rebuild: Int64,
    pipeline: String,
    level: Int,
    partitions: String,
) raises:
    """Plans `pipeline`'s run of `rebuild` at `level`, over `partitions`."""
    _ = tx.execute(
        (
            "INSERT INTO orca.rebuild_pipelines (rebuild, pipeline, level,"
            " partitions) VALUES ($1, $2, $3, $4)"
        ),
        Params()
        .int64(rebuild)
        .text(pipeline)
        .int64(Int64(level))
        .text(partitions),
    )


def rebuilds(
    mut conn: Connection, id: Int, dataset: String, limit: Int
) raises -> List[Rebuild]:
    """Rebuilds, newest first: the one `id` names (0 for any), of `dataset`
    (empty for any), at most `limit`; each with its plan in run order."""
    var out = List[Rebuild]()
    var rows = conn.query(
        (
            "SELECT id, dataset, partitions, status, to_char(created AT TIME"
            " ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created,"
            " coalesce(to_char(ended AT TIME ZONE 'UTC',"
            " 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '') AS ended FROM"
            " orca.rebuilds WHERE ($1 = 0 OR id = $1) AND ($2 = '' OR dataset"
            " = $2) ORDER BY id DESC LIMIT $3"
        ),
        Params().int64(Int64(id)).text(dataset).int64(Int64(limit)),
    )
    for r in rows:
        var rid = r.int64("id")
        var plan = List[Planned]()
        for p in conn.query(
            (
                "SELECT rp.pipeline, rp.level, rp.partitions, coalesce(rp.run,"
                " 0) AS run, coalesce(x.status, '') AS status FROM"
                " orca.rebuild_pipelines rp LEFT JOIN orca.runs x ON x.id ="
                " rp.run WHERE rp.rebuild = $1 ORDER BY rp.level, rp.pipeline"
            ),
            Params().int64(rid),
        ):
            plan.append(
                Planned(
                    p.text("pipeline"),
                    Int(p.int64("level")),
                    p.text("partitions"),
                    Int(p.int64("run")),
                    p.text("status"),
                )
            )
        out.append(
            Rebuild(
                Int(rid),
                r.text("dataset"),
                r.text("partitions"),
                r.text("status"),
                r.text("created"),
                r.text("ended"),
                plan^,
            )
        )
    return out^


def unfinished(mut conn: Connection) raises -> List[Int64]:
    """The rebuilds running or failed, oldest first."""
    var ids = List[Int64]()
    for r in conn.query(
        (
            "SELECT id FROM orca.rebuilds WHERE status IN ('running',"
            " 'failed') ORDER BY id"
        ),
        Params(),
    ):
        ids.append(r.int64("id"))
    return ids^


def lock_unfinished(mut tx: Transaction, id: Int64) raises -> Bool:
    """Locks rebuild `id` (`FOR UPDATE`) if it's running or failed; False
    if it's neither."""
    var still = tx.query(
        (
            "SELECT id FROM orca.rebuilds WHERE id = $1 AND status IN"
            " ('running', 'failed') FOR UPDATE"
        ),
        Params().int64(id),
    )
    return len(still) > 0


@fieldwise_init
struct Step(Copyable, Movable):
    """A pipeline's place in a rebuild's plan, as advancing it sees it."""

    var pipeline: String
    var level: Int
    var partitions: String
    var unqueued: Bool
    var status: String
    """Its run's status, or empty."""


def plan(mut tx: Transaction, id: Int64) raises -> List[Step]:
    """Rebuild `id`'s plan, in run order."""
    var out = List[Step]()
    for r in tx.query(
        (
            "SELECT rp.pipeline, rp.level, rp.partitions, rp.run IS NULL AS"
            " unqueued, coalesce(x.status, '') AS status FROM"
            " orca.rebuild_pipelines rp LEFT JOIN orca.runs x ON x.id ="
            " rp.run WHERE rp.rebuild = $1 ORDER BY rp.level, rp.pipeline"
        ),
        Params().int64(id),
    ):
        out.append(
            Step(
                r.text("pipeline"),
                Int(r.int64("level")),
                r.text("partitions"),
                r.bool("unqueued"),
                r.text("status"),
            )
        )
    return out^


def queue_run(
    mut tx: Transaction, pipeline: String, keys: String, rebuild: Int64
) raises -> Int64:
    """A new queued run of `pipeline` over `keys` for `rebuild`, at its
    registration now; its id."""
    return (
        tx.query(
            (
                "INSERT INTO orca.runs (pipeline, registry_hash,"
                " partitions, trigger, status, rebuild) SELECT"
                " name, registry_hash, $2, 'rebuild', 'queued', $3"
                " FROM orca.pipelines WHERE name = $1 RETURNING id"
            ),
            Params().text(pipeline).text(keys).int64(rebuild),
        )
        .row(0)
        .int64("id")
    )


def set_run(
    mut tx: Transaction, rebuild: Int64, pipeline: String, run: Int64
) raises:
    """Records `run` as `pipeline`'s run of `rebuild`."""
    _ = tx.execute(
        (
            "UPDATE orca.rebuild_pipelines SET run = $3 WHERE"
            " rebuild = $1 AND pipeline = $2"
        ),
        Params().int64(rebuild).text(pipeline).int64(run),
    )


def set_status(mut tx: Transaction, id: Int64, status: String) raises:
    """Sets rebuild `id`'s status, ended now once it's `succeeded`."""
    _ = tx.execute(
        (
            "UPDATE orca.rebuilds SET status = $2, ended = CASE WHEN $2 ="
            " 'succeeded' THEN now() END WHERE id = $1"
        ),
        Params().int64(id).text(status),
    )
