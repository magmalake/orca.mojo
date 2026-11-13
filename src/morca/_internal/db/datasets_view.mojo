"""The datasets' pages' queries (`morca._internal.views.datasets`,
`morca._internal.ui.datasets`): the steps that write and read a dataset, and whether
morca knows a snapshot of it.
"""

from postgres import Connection, Params


@fieldwise_init
struct StepUse(Copyable, Movable):
    """A step that writes (or reads) a dataset: declared in its `writes=`
    (`reads=`), or seen committing to (reading) it; and the newest run that
    did, or 0."""

    var pipeline: String
    var step: String
    var run: Int


def dataset_steps(
    mut conn: Connection, dataset: String, writes: Bool
) raises -> List[StepUse]:
    """The steps that write `dataset` (or, not `writes`, read it), by
    pipeline and step."""
    var kind = "writes" if writes else "reads"
    var table = "orca.commits" if writes else "orca.reads"
    var rows = conn.query(
        String(
            "WITH d AS (SELECT p.name AS pipeline, st->>'name' AS step FROM",
            " orca.pipelines p, jsonb_array_elements(p.describe->'steps') AS",
            " x(st) WHERE EXISTS (SELECT 1 FROM",
            " jsonb_array_elements_text(coalesce(st->'",
            kind,
            "', '[]')) AS v WHERE v = $1)), seen AS (SELECT DISTINCT ON",
            " (r.pipeline, s.step) r.pipeline, s.step, r.id AS run FROM ",
            table,
            " c JOIN orca.attempts a ON a.id = c.attempt JOIN orca.steps s ON",
            " s.id = a.step JOIN orca.runs r ON r.id = s.run WHERE c.dataset",
            " = $1 ORDER BY r.pipeline, s.step, c.attempt DESC) SELECT",
            " coalesce(d.pipeline, seen.pipeline) AS pipeline,",
            " coalesce(d.step, seen.step) AS step, coalesce(seen.run, 0) AS",
            " run FROM d FULL JOIN seen ON seen.pipeline = d.pipeline AND",
            " seen.step = d.step ORDER BY 1, 2",
        ),
        Params().text(dataset),
    )
    var out = List[StepUse]()
    for r in rows:
        out.append(
            StepUse(r.text("pipeline"), r.text("step"), Int(r.int64("run")))
        )
    return out^


def snapshot_known(
    mut conn: Connection, dataset: String, snapshot: Int64
) raises -> Bool:
    """Whether a step committed or read snapshot `snapshot` of
    `dataset`."""
    var known = conn.query(
        (
            "SELECT 1 FROM orca.commits WHERE dataset = $1 AND snapshot_id = $2"
            " UNION ALL SELECT 1 FROM orca.reads WHERE dataset = $1 AND"
            " snapshot_id = $2 LIMIT 1"
        ),
        Params().text(dataset).int64(snapshot),
    )
    return len(known) > 0
