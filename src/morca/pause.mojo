"""Pausing a pipeline: what `morca pause`, the UI's Pause button and the
watchdog (`morca.watchdog`) all do."""

import morca.db.pipelines as pipelines_db

from morca.state import State


def set_paused(mut db: State, pipeline: String, paused: Bool) raises -> String:
    """Sets `pipelines.paused`: while it's true the scheduler starts none of
    the pipeline's runs. Returns why it wasn't changed, or empty."""
    if pipelines_db.change_paused(db.conn, pipeline, paused):
        return String()
    if not pipelines_db.exists(db.conn, pipeline):
        return String("no pipeline ", pipeline)
    return String(
        "pipeline ",
        pipeline,
        " is ",
        "paused" if paused else "not paused",
        " already",
    )
