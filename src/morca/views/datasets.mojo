"""Datasets as their pages read them: the steps that write and read one,
and what a rebuild of it would queue (`RebuildPreview`).
"""

from postgres import Params

from morca.html import Html, url_encode
from morca.sensors import plan_rebuild
from morca.state import State
from morca.ui.parts import code_link
from morca.views.code import CodeLinks, code_of


@fieldwise_init
struct StepUse(Copyable, Movable):
    """A step that writes (or reads) a dataset: declared in its `writes=`
    (`reads=`), or seen committing to (reading) it; and the newest run that
    did, or 0."""

    var pipeline: String
    var step: String
    var run: Int


def dataset_steps(
    mut db: State, dataset: String, writes: Bool
) raises -> List[StepUse]:
    var kind = "writes" if writes else "reads"
    var table = "orca.commits" if writes else "orca.reads"
    var rows = db.conn.query(
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


def steps_using(
    mut h: Html,
    mut db: State,
    mut cache: Dict[String, CodeLinks],
    uses: List[StepUse],
):
    """Each pipeline, then its steps, each linked to its line at the commit
    of the newest run that used the dataset."""
    if len(uses) == 0:
        h.raw('<span class="muted">none</span>')
        return
    for i in range(len(uses)):
        ref u = uses[i]
        if i == 0 or uses[i - 1].pipeline != u.pipeline:
            if i:
                h.raw("; ")
            h.link(String("/pipelines/", url_encode(u.pipeline)), u.pipeline)
            h.raw(' <span class="steps-of">step ')
        else:
            h.raw(", ")
        h.text(u.step)
        var at = code_of(db, cache, u.pipeline, u.run).step(u.step)
        if at:
            h.raw(" ")
            code_link(h, at)
        if i + 1 == len(uses) or uses[i + 1].pipeline != u.pipeline:
            h.raw("</span>")


@fieldwise_init
struct RebuildPreview(Copyable, Movable):
    """What a rebuild over a range would queue, level by level, or why it's
    refused: `plan_rebuild`, which writes nothing."""

    var refused: String
    var partitions: List[String]
    var levels: List[List[Tuple[String, String]]]
    """Each level's runs: the pipeline and its partitions."""


def rebuild_preview(
    mut db: State, dataset: String, keys: String
) raises -> RebuildPreview:
    var levels = List[List[Tuple[String, String]]]()
    try:
        var got = plan_rebuild(db, dataset, keys)
        for p in got[1]:
            while len(levels) <= p.level:
                levels.append(List[Tuple[String, String]]())
            levels[p.level].append((p.pipeline, p.partitions))
        return RebuildPreview("", got[0].copy(), levels^)
    except e:
        var why = String(e)
        if why.startswith("morca: "):
            var rest = String(why[byte=7:])
            why = rest^
        return RebuildPreview(why^, List[String](), levels^)


comptime SNAPSHOTS_PAGE = 50
"""Snapshots to a page of a dataset's list."""
