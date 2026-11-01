"""Where attempts' logs and runs' timelines go.

With `ORCA_LOGS` set to a directory, each attempt logs to
`<ORCA_LOGS>/<pipeline>/<run>/<step>/<partition>/<attempt>.log` (the
partition is `_run` for a gather step), and a run of a pipeline with dista
tasks leaves its timeline at `<ORCA_LOGS>/<pipeline>/<run>/timeline.json`.
Without it, logs go to stdout only. Logs in object storage come with phase 5.
"""

from std.ffi import external_call
from std.os import getenv, makedirs


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
    var t = Int(external_call["time", Int64](Int(0)))
    var days = t // 86400
    var secs = t % 86400
    # Howard Hinnant's days-to-civil.
    var z = days + 719468
    var era = z // 146097
    var doe = z - era * 146097
    var yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    var doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    var mp = (5 * doy + 2) // 153
    var d = doy - (153 * mp + 2) // 5 + 1
    var m = mp + 3 if mp < 10 else mp - 9
    var y = yoe + era * 400 + (1 if m <= 2 else 0)
    var out = String(y)
    var parts = [m, d, secs // 3600, secs % 3600 // 60, secs % 60]
    var seps = ["-", "-", "T", ":", ":"]
    for i in range(len(parts)):
        out.write(seps[i], "0" if parts[i] < 10 else "", parts[i])
    out.write("Z")
    return out^
