"""Pausing a pipeline: what `morca pause`, the UI's Pause button and the
watchdog (`morca.watchdog`) all do."""

from postgres import Params

from morca.state import State


def set_paused(mut db: State, pipeline: String, paused: Bool) raises -> String:
    """Sets `pipelines.paused`: while it's true the scheduler starts none of
    the pipeline's runs. Returns why it wasn't changed, or empty."""
    var rows = db.conn.query(
        (
            "UPDATE orca.pipelines SET paused = $2 WHERE name = $1 AND paused"
            " <> $2 RETURNING name"
        ),
        Params().text(pipeline).bool(paused),
    )
    if len(rows) == 1:
        return String()
    var have = db.conn.query(
        "SELECT 1 FROM orca.pipelines WHERE name = $1", Params().text(pipeline)
    )
    if len(have) == 0:
        return String("no pipeline ", pipeline)
    return String(
        "pipeline ",
        pipeline,
        " is ",
        "paused" if paused else "not paused",
        " already",
    )
