"""The home page's recent runs (#354): one row per batch (`morca.batches`),
so one backfill doesn't fill the list, and a run with none as its own row;
what needs attention first.
"""

from postgres import Params

from morca.json import quote
from morca.state import State
from morca.views.runs import RunRow, runs_one, utc_sql


comptime FAILED_LATELY = "1 day"
"""How recently a failure ended for its row to come first."""


comptime _CANDIDATES = 1000
"""The newest runs whose batches the home page looks at, beside every live
run's and every recent failure's."""


@fieldwise_init
struct RecentRow(Copyable, Movable):
    """A row of the home page's recent runs: a batch, or one run (`run`)."""

    var batch: Int
    """The batch's id (its first run's), or 0 for a run of its own."""
    var first: Int
    var last: Int
    var runs: Int
    var pipeline: String
    var trigger: String
    var running: Int
    var queued: Int
    """Queued or deferred: waiting to start."""
    var succeeded: Int
    var failed: Int
    var cancelled: Int
    var created: String
    """When its first run was made, UTC."""
    var seconds: Int
    """From its first start to its last end (or now), -1 before any
    start."""
    var first_partitions: String
    var last_partitions: String
    var run: Optional[RunRow]
    """The run, for a row of one run."""

    def live(self) -> Bool:
        """Whether a run in it is queued or running."""
        return self.running > 0 or self.queued > 0

    def done(self) -> Int:
        """Its runs that have ended."""
        return self.succeeded + self.failed + self.cancelled


def recent_rows(mut db: State, limit: Int) raises -> List[RecentRow]:
    """The `limit` rows the home page's Recent runs shows: batches with a
    run running or failed in the last `FAILED_LATELY` first, then those
    with runs waiting, newest first; then the rest, the latest to end or
    start first. A batch counts all its runs; a run with no batch is a row
    of its own."""
    var out = List[RecentRow]()
    for r in db.conn.query(
        String(
            "WITH c AS (SELECT coalesce(batch, -id) AS g FROM orca.runs WHERE",
            " status IN ('queued', 'deferred', 'running') UNION SELECT",
            " coalesce(batch, -id) FROM orca.runs WHERE status = 'failed'",
            " AND ended > now() - interval '",
            FAILED_LATELY,
            "' UNION SELECT g FROM (SELECT coalesce(batch, -id) AS g FROM",
            " orca.runs ORDER BY id DESC LIMIT $2) n), g AS (SELECT",
            " coalesce(batch, -id) AS g, coalesce(max(batch), 0) AS batch,",
            " min(id) AS first, max(id) AS last, count(*) AS n, min(pipeline)",
            " AS pipeline, min(trigger) AS trigger, count(*) FILTER (WHERE",
            " status = 'running') AS running, count(*) FILTER (WHERE status",
            " IN ('queued', 'deferred')) AS queued, count(*) FILTER (WHERE",
            " status = 'succeeded') AS succeeded, count(*) FILTER (WHERE",
            " status = 'failed') AS failed, count(*) FILTER (WHERE status =",
            " 'cancelled') AS cancelled, coalesce(bool_or(status = 'failed'",
            " AND ended > now() - interval '",
            FAILED_LATELY,
            "'), false) AS failed_lately, ",
            utc_sql("min(created)"),
            " AS created, coalesce(extract(epoch FROM max(coalesce(ended,",
            " now())) - min(started))::bigint, -1) AS seconds,",
            " max(coalesce(ended, started, created)) AS latest,",
            " (array_agg(partitions ORDER BY id))[1] AS first_parts,",
            " (array_agg(partitions ORDER BY id DESC))[1] AS last_parts FROM",
            " orca.runs WHERE batch IN (SELECT g FROM c WHERE g > 0) OR (batch",
            " IS NULL AND id IN (SELECT -g FROM c WHERE g < 0)) GROUP BY 1),",
            " t AS (SELECT g.*, CASE WHEN running > 0 OR failed_lately THEN 0",
            " WHEN queued > 0 THEN 1 ELSE 2 END AS tier FROM g) SELECT batch,",
            " first, last, n, pipeline, trigger, running, queued, succeeded,",
            " failed, cancelled, created, seconds, first_parts, last_parts",
            " FROM t ORDER BY tier, CASE WHEN tier = 2 THEN latest END DESC,",
            " last DESC LIMIT $1",
        ),
        Params().int64(Int64(limit)).int64(Int64(_CANDIDATES)),
    ):
        out.append(
            RecentRow(
                Int(r.int64("batch")),
                Int(r.int64("first")),
                Int(r.int64("last")),
                Int(r.int64("n")),
                r.text("pipeline"),
                r.text("trigger"),
                Int(r.int64("running")),
                Int(r.int64("queued")),
                Int(r.int64("succeeded")),
                Int(r.int64("failed")),
                Int(r.int64("cancelled")),
                r.text("created"),
                Int(r.int64("seconds")),
                r.text("first_parts"),
                r.text("last_parts"),
                None,
            )
        )
    # A row of one run shows the run, as the runs lists do.
    for ref row in out:
        if row.runs == 1:
            row.run = runs_one(db, row.first)
    return out^


def json_recent(mut j: String, rows: List[RecentRow]):
    """The rows as a JSON list: each batch's id (null for a run of its
    own), its first and last runs, its runs and each status's count."""
    j.write("[")
    for i in range(len(rows)):
        ref r = rows[i]
        j.write(
            ", " if i else "",
            '{"batch": ',
            String(r.batch) if r.batch else String("null"),
            ', "first": ',
            r.first,
            ', "last": ',
            r.last,
            ', "runs": ',
            r.runs,
            ', "pipeline": ',
            quote(r.pipeline),
            ', "trigger": ',
            quote(r.trigger),
            ', "created": ',
            quote(r.created),
            ', "counts": {"running": ',
            r.running,
            ', "queued": ',
            r.queued,
            ', "succeeded": ',
            r.succeeded,
            ', "failed": ',
            r.failed,
            ', "cancelled": ',
            r.cancelled,
            "}}",
        )
    j.write("]")
