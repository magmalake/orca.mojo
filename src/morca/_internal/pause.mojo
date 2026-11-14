"""Pausing a pipeline: what `morca pause`, the UI's Pause button and the
watchdog (`morca._internal.watchdog`) all do; and retiring one (`morca
retire`)."""

import morca._internal.db.pipelines as pipelines_db

from morca._internal.state import State


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


def retire(mut db: State, pipeline: String) raises -> String:
    """`morca retire` (#427): the pipeline stays in the database, its runs,
    history and tables kept, but it's no longer registered: the scheduler
    starts none of its runs, and `morca status`, the home page and
    `morca-stack check` leave it out. `morca register` of a binary of it
    brings it back. Returns why it wasn't retired, or empty."""
    if pipelines_db.change_retired(db.conn, pipeline, True):
        return String()
    if not pipelines_db.exists(db.conn, pipeline):
        return String("no pipeline ", pipeline)
    return String("pipeline ", pipeline, " is retired already")
