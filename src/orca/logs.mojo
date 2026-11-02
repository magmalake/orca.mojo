"""Where attempts' logs and runs' timelines go.

With `ORCA_LOGS` set to a directory, each attempt logs to
`<ORCA_LOGS>/<pipeline>/<run>/<step>/<partition>/<attempt>.log` (the
partition is `_run` for a gather step), and a run of a pipeline with dista
tasks leaves its timeline at `<ORCA_LOGS>/<pipeline>/<run>/timeline.json`.
Without it, logs go to stdout only. Logs in object storage come with phase 5.
"""

from std.ffi import external_call
from std.os import getenv, makedirs

from orca.civil import Civil


def logs_dir() -> String:
    return getenv("ORCA_LOGS")


def run_dir(pipeline: String, run: Int) -> String:
    """The run's directory under `ORCA_LOGS`, or "" without it."""
    var root = logs_dir()
    if not root:
        return ""
    return String(root, "/", pipeline, "/", run)


def attempt_log(
    pipeline: String, run: Int, step: String, partition: String, attempt: Int
) raises -> String:
    """The attempt's log file, its directory created; "" without
    `ORCA_LOGS`."""
    var dir = run_dir(pipeline, run)
    if not dir:
        return ""
    dir.write("/", step, "/", partition if partition else "_run")
    makedirs(dir, exist_ok=True)
    return String(dir, "/", attempt, ".log")


def utc_now() -> String:
    """The time now, to the second, as `YYYY-MM-DDTHH:MM:SSZ`."""
    return String(Civil.of(external_call["time", Int64](Int(0))), "Z")
