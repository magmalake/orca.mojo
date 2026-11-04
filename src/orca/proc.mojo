"""Starting processes: what the scheduler needs to run pipeline binaries.

`spawn` forks and executes a binary without waiting for it, its output
appended to a log file if one is given; `reap` collects the ones that ended.
`capture` runs a command to the end, killing it past a timeout, and returns
what it printed; `Capture` is the same started without waiting, for the
scheduler to check on from tick to tick. Everything a child needs is
prepared before `fork`, so the child calls only `setpgid`, `fopen`, `dup2`,
`execv` (or `execvp`) and `_exit`.
"""

from std.ffi import c_int, external_call
from std.memory import stack_allocation
from std.os import getenv, remove
from std.time import perf_counter_ns, sleep


def _cstr(s: String) -> List[UInt8]:
    var b = List[UInt8](capacity=s.byte_length() + 1)
    for c in s.as_bytes():
        b.append(c)
    b.append(0)
    return b^


def spawn(argv: List[String], log: String = "") raises -> Int:
    """Starts `argv[0]` (a path) with arguments `argv`, and returns its pid.
    Its stdout and stderr are appended to `log`, if given, else shared with
    this process."""
    if len(argv) == 0:
        raise Error("spawn: no program")
    var args = List[List[UInt8]]()
    for a in argv:
        args.append(_cstr(a))
    var ptrs = List[Int]()
    for i in range(len(args)):
        ptrs.append(Int(args[i].unsafe_ptr()))
    ptrs.append(0)
    var log_c = _cstr(log)
    var mode = _cstr("a")
    var pid = external_call["fork", c_int]()
    if pid < 0:
        raise Error("spawn: fork failed")
    if pid == 0:
        if log:
            var f = external_call["fopen", Int](
                log_c.unsafe_ptr(), mode.unsafe_ptr()
            )
            if f != 0:
                var fd = external_call["fileno", c_int](f)
                _ = external_call["dup2", c_int](fd, c_int(1))
                _ = external_call["dup2", c_int](fd, c_int(2))
        _ = external_call["execv", c_int](
            args[0].unsafe_ptr(), ptrs.unsafe_ptr()
        )
        external_call["_exit", NoneType](c_int(127))
    _ = args^
    _ = ptrs^
    return Int(pid)


def reap() -> List[Tuple[Int, Int]]:
    """The children that ended, as (pid, exit code), without waiting; a
    child killed by a signal has code 128 + the signal."""
    var out = List[Tuple[Int, Int]]()
    var status = stack_allocation[1, c_int]()
    while True:
        status[] = 0
        # WNOHANG is 1 on macOS and Linux.
        var pid = external_call["waitpid", c_int](c_int(-1), status, c_int(1))
        if pid <= 0:
            break
        out.append((Int(pid), _code(Int(status[]))))
    return out^


def binary_seconds() -> Float64:
    """`ORCA_BINARY_SECONDS` (default 300): how long a pipeline binary's
    `describe`, `sense` or `keys` may take before it's killed."""
    try:
        return Float64(getenv("ORCA_BINARY_SECONDS", "300"))
    except:
        return 300.0


def _code(s: Int) -> Int:
    """An exit code from a `waitpid` status: 128 + the signal if killed."""
    return (s >> 8) & 0xFF if s & 0x7F == 0 else 128 + (s & 0x7F)


@fieldwise_init
struct Capture(Copyable, Movable):
    """A command started with its standard output going to a file of its
    own, to be read when it ends (`capture` waits; the scheduler checks on
    it from tick to tick). It leads a process group of its own, so killing
    it kills what it started too."""

    var pid: Int
    var path: String
    var what: String
    """The command, for messages."""
    var started: Int
    """When it started, on the monotonic clock in nanoseconds."""
    var code: Int
    """Its exit code once it has ended, else -1."""

    @staticmethod
    def start(argv: List[String]) raises -> Capture:
        """Starts `argv` (`argv[0]` looked up on `PATH`), its stdout to a
        new file under `TMPDIR`, its stderr shared with this process."""
        if len(argv) == 0:
            raise Error("capture: no program")
        var what = String()
        for i in range(len(argv)):
            what.write(" " if i > 0 else "", argv[i])
        var t = Int(perf_counter_ns())
        var path = String(
            getenv("TMPDIR", "/tmp").rstrip("/"),
            "/orca-out-",
            external_call["getpid", c_int](),
            "-",
            t,
        )
        var args = List[List[UInt8]]()
        for a in argv:
            args.append(_cstr(a))
        var ptrs = List[Int]()
        for i in range(len(args)):
            ptrs.append(Int(args[i].unsafe_ptr()))
        ptrs.append(0)
        var path_c = _cstr(path)
        var mode = _cstr("w")
        var pid = external_call["fork", c_int]()
        if pid < 0:
            raise Error("capture: fork failed for ", what)
        if pid == 0:
            _ = external_call["setpgid", c_int](c_int(0), c_int(0))
            var f = external_call["fopen", Int](
                path_c.unsafe_ptr(), mode.unsafe_ptr()
            )
            if f == 0:
                external_call["_exit", NoneType](c_int(126))
            var fd = external_call["fileno", c_int](f)
            _ = external_call["dup2", c_int](fd, c_int(1))
            _ = external_call["execvp", c_int](
                args[0].unsafe_ptr(), ptrs.unsafe_ptr()
            )
            external_call["_exit", NoneType](c_int(127))
        _ = args^
        _ = ptrs^
        return Capture(Int(pid), path^, what^, t, -1)

    def seconds(self) -> Float64:
        """How long it has been going."""
        return Float64(Int(perf_counter_ns()) - self.started) / 1e9

    def ended(mut self) -> Bool:
        """Whether it has ended, waiting for it if it has (not if the
        scheduler's `reap` got to it first, which sets `code`)."""
        if self.code >= 0:
            return True
        var status = stack_allocation[1, c_int]()
        status[] = 0
        var got = external_call["waitpid", c_int](
            c_int(self.pid), status, c_int(1)
        )
        if got == c_int(self.pid):
            self.code = _code(Int(status[]))
        elif got < 0:
            self.code = 255  # not our child any more: someone reaped it
        return self.code >= 0

    def kill(mut self):
        """Kills it and what it started, and forgets its output."""
        _ = external_call["kill", c_int](c_int(-self.pid), c_int(9))
        _ = external_call["kill", c_int](c_int(self.pid), c_int(9))
        self._remove()

    def output(mut self) raises -> String:
        """What it printed, once it has ended; the file is removed."""
        var text: String
        with open(self.path, "r") as f:
            text = f.read()
        self._remove()
        return text^

    def _remove(self):
        try:
            remove(self.path)
        except:
            pass

    def timed_out(self, limit: Float64) -> Error:
        return Error(
            "`",
            self.what,
            "` took longer than ",
            Int(limit),
            " s, and was killed",
        )


def capture(
    argv: List[String], timeout: Float64 = 0
) raises -> Tuple[Int, String]:
    """Runs `argv` to the end and returns its exit code and standard output.
    With a `timeout` (seconds), a command still going then is killed, with
    what it started, and this raises saying so."""
    var c = Capture.start(argv)
    while not c.ended():
        if timeout > 0 and c.seconds() > timeout:
            c.kill()
            var status = stack_allocation[1, c_int]()
            _ = external_call["waitpid", c_int](c_int(c.pid), status, c_int(0))
            raise c.timed_out(timeout)
        sleep(0.005)
    return (c.code, c.output())


def absolute(path: String) raises -> String:
    """`path`, made absolute against the working directory."""
    if path.startswith("/"):
        return path
    var buf = List[UInt8](length=4096, fill=0)
    var r = external_call["getcwd", Int](buf.unsafe_ptr(), Int(4096))
    if r == 0:
        raise Error("absolute: getcwd failed")
    var n = 0
    while buf[n] != 0:
        n += 1
    var cwd = String(from_utf8_lossy=Span(buf)[:n])
    var rel = path.removeprefix("./")
    return String(cwd, "/", rel)
