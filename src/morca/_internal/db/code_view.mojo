"""What the code links read (`morca.views.code`): which version of a
pipeline's code a run ran or the pipeline is registered with, its
`describe`, and a runs table's runs' versions and failed steps.
"""

from postgres import Connection, Params


def describe(mut conn: Connection, pipeline: String) raises -> String:
    """`pipeline`'s registration's `describe`, as JSON text."""
    return (
        conn.query(
            "SELECT describe::text AS d FROM orca.pipelines WHERE name = $1",
            Params().text(pipeline),
        )
        .row(0)
        .text("d")
    )


def run_code_version(
    mut conn: Connection, run: Int
) raises -> Optional[Tuple[String, Int]]:
    """Run `run`'s pipeline and the version it ran, else the one its
    pipeline is registered with (0 for none); None for no such run."""
    var rows = conn.query(
        (
            "SELECT r.pipeline, coalesce(r.version, p.version, 0) AS v"
            " FROM orca.runs r LEFT JOIN orca.pipelines p ON p.name ="
            " r.pipeline WHERE r.id = $1"
        ),
        Params().int64(Int64(run)),
    )
    if len(rows) == 0:
        return None
    return (rows.row(0).text("pipeline"), Int(rows.row(0).int64("v")))


def pipeline_code_version(
    mut conn: Connection, pipeline: String
) raises -> Optional[Tuple[String, Int]]:
    """`pipeline` and the version it's registered with (0 for none); None
    if it isn't registered."""
    var rows = conn.query(
        (
            "SELECT name AS pipeline, coalesce(version, 0) AS v FROM"
            " orca.pipelines WHERE name = $1"
        ),
        Params().text(pipeline),
    )
    if len(rows) == 0:
        return None
    return (rows.row(0).text("pipeline"), Int(rows.row(0).int64("v")))


@fieldwise_init
struct RunVersion(Copyable, Movable):
    """A run's version (else its pipeline's; 0 for none) and its first
    failed step (empty for none)."""

    var id: Int
    var pipeline: String
    var version: Int
    var failed: String


def run_versions(mut conn: Connection, ids: String) raises -> List[RunVersion]:
    """The `RunVersion` of each run in `ids` (comma-separated)."""
    var out = List[RunVersion]()
    for row in conn.query(
        (
            "SELECT r.id, r.pipeline, coalesce(r.version, p.version, 0) AS"
            " v, coalesce((SELECT s.step FROM orca.steps s WHERE s.run ="
            " r.id AND s.status = 'failed' ORDER BY s.id LIMIT 1), '') AS"
            " failed FROM orca.runs r LEFT JOIN orca.pipelines p ON p.name"
            " = r.pipeline WHERE r.id = ANY(string_to_array($1,"
            " ',')::bigint[])"
        ),
        Params().text(ids),
    ):
        out.append(
            RunVersion(
                Int(row.int64("id")),
                row.text("pipeline"),
                Int(row.int64("v")),
                row.text("failed"),
            )
        )
    return out^
