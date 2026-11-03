"""Where attempts' logs, drivers' logs and runs' timelines go.

With `ORCA_LOGS` set, each attempt logs to
`<ORCA_LOGS>/<pipeline>/<run>/<step>/<partition>/<attempt>.log` (the
partition is `_run` for a gather step), a run of a pipeline with dista
tasks leaves its timeline at `<ORCA_LOGS>/<pipeline>/<run>/timeline.json`,
and the scheduler keeps a run's output at `.../<run>/driver.log`. Without
it, logs go to stdout only.

`ORCA_LOGS` is a directory, or a location in object storage such as
`s3://bucket/orca/logs` (objectstore's `FileIOResolver`, configured from the
`AWS_*` environment). An object can't be appended to, so for a remote root
a log is written to a staging file under `ORCA_LOG_STAGE` (default
`$TMPDIR/orca-logs`) and `publish`ed when it's complete: an attempt's when
the attempt ends, a timeline when the run does. `attempts.log` records
where the log ends up, which `read_location` reads either way.
"""

from std.ffi import external_call
from std.os import getenv, makedirs
from std.os.path import exists

from objectstore.fileio import FileIOResolver

from orca.civil import Civil


def logs_dir() -> String:
    return getenv("ORCA_LOGS")


def is_remote(location: String) -> Bool:
    """Whether `location` is in object storage, not on this machine."""
    return "://" in location and not location.startswith("file://")


def run_dir(pipeline: String, run: Int) -> String:
    """The run's directory under `ORCA_LOGS`, or "" without it."""
    var root = logs_dir()
    if not root:
        return ""
    return String(root.rstrip("/"), "/", pipeline, "/", run)


def stage_of(location: String) -> String:
    """Where a log bound for `location` is written first: `location` itself
    on this machine, else its path under `ORCA_LOG_STAGE`."""
    if not is_remote(location):
        return location
    var root = getenv("ORCA_LOG_STAGE")
    if not root:
        root = String(getenv("TMPDIR", "/tmp").rstrip("/"), "/orca-logs")
    var rel = location[byte = location.find("://") + 3 :]
    return String(root, "/", rel)


def _parent(path: String) -> String:
    var at = path.rfind("/")
    return String(path[byte=:at]) if at > 0 else String(".")


def attempt_log(
    pipeline: String, run: Int, step: String, partition: String, attempt: Int
) raises -> String:
    """Where the attempt's log ends up, `stage_of` it ready to be written;
    "" without `ORCA_LOGS`."""
    var dir = run_dir(pipeline, run)
    if not dir:
        return ""
    dir.write("/", step, "/", partition if partition else "_run")
    var log = String(dir, "/", attempt, ".log")
    makedirs(_parent(stage_of(log)), exist_ok=True)
    return log^


def publish(location: String) raises:
    """Copies a remote `location`'s staging file to it; nothing for a local
    one, which was written in place."""
    if not is_remote(location):
        return
    var data: List[UInt8]
    with open(stage_of(location), "r") as f:
        data = f.read_bytes()
    FileIOResolver().write(location, Span(data))


def write_location(location: String, data: Span[UInt8, _]) raises:
    """Writes `data` to `location`, local or remote, replacing it."""
    if is_remote(location):
        FileIOResolver().write(location, data)
        return
    makedirs(_parent(location), exist_ok=True)
    with open(location, "w") as f:
        f.write_bytes(data)


def read_location(location: String) raises -> List[UInt8]:
    """What's at `location`, local or remote; raises if there's nothing."""
    if is_remote(location):
        return FileIOResolver().read_all(location)
    with open(location, "r") as f:
        return f.read_bytes()


def location_exists(location: String) -> Bool:
    if not is_remote(location):
        return exists(location)
    try:
        _ = FileIOResolver().read_all(location)
        return True
    except:
        return False


def utc_now() -> String:
    """The time now, to the second, as `YYYY-MM-DDTHH:MM:SSZ`."""
    return String(Civil.of(external_call["time", Int64](Int(0))), "Z")
