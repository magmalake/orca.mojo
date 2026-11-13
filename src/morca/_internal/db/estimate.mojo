"""Estimates' queries (`morca._internal.estimate`): how long a pipeline's recent runs
took."""

from postgres import Connection, Params

comptime _PACE_SQL = (
    "SELECT count(*)::bigint AS n, coalesce(sum(extract(epoch FROM ended -"
    " started)), 0)::float8 AS secs, coalesce(sum(cardinality("
    "string_to_array(partitions, ','))), 0)::bigint AS keys FROM (SELECT"
    " started, ended, partitions FROM orca.runs WHERE pipeline = $1 AND"
    " status = 'succeeded' AND started IS NOT NULL AND ended IS NOT NULL AND"
    " ended >= started ORDER BY ended DESC LIMIT 20) r"
)
"""Over the last `SAMPLE` (20) of them."""


@fieldwise_init
struct Recent(Copyable, Movable):
    """A pipeline's last succeeded runs, added up."""

    var runs: Int
    var seconds: Float64
    var keys: Int


def recent(mut conn: Connection, pipeline: String) raises -> Recent:
    """`pipeline`'s last 20 succeeded runs: how many, their seconds and
    their partitions."""
    var r = conn.query(_PACE_SQL, Params().text(pipeline)).row(0)
    return Recent(Int(r.int64("n")), r.float64("secs"), Int(r.int64("keys")))
