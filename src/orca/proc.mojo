"""Starting processes: what the scheduler needs to run pipeline binaries.

`spawn` forks and executes a binary without waiting for it, its output
appended to a log file if one is given; `reap` collects the ones that ended.
`capture` runs a command to the end and returns what it printed. Everything a
child needs is prepared before `fork`, so the child calls only `fopen`,
`dup2`, `execv` and `_exit`.
"""

from std.ffi import c_int, external_call
from std.memory import stack_allocation


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
        var s = Int(status[])
        var code = (s >> 8) & 0xFF if s & 0x7F == 0 else 128 + (s & 0x7F)
        out.append((Int(pid), code))
    return out^


def _quote(s: String) -> String:
    """`s` as one shell word."""
    return String("'", s.replace("'", "'\\''"), "'")


def capture(argv: List[String]) raises -> Tuple[Int, String]:
    """Runs `argv` to the end through the shell and returns its exit code and
    standard output."""
    var cmd = String()
    for i in range(len(argv)):
        cmd.write(" " if i > 0 else "", _quote(argv[i]))
    var cmd_c = _cstr(cmd)
    var mode = _cstr("r")
    var f = external_call["popen", Int](cmd_c.unsafe_ptr(), mode.unsafe_ptr())
    if f == 0:
        raise Error("capture: popen failed for ", cmd)
    var out = List[UInt8]()
    var buf = List[UInt8](length=4096, fill=0)
    while True:
        var n = external_call["fread", Int](
            buf.unsafe_ptr(), Int(1), Int(4096), f
        )
        if n <= 0:
            break
        for k in range(n):
            out.append(buf[k])
    var s = Int(external_call["pclose", c_int](f))
    var code = (s >> 8) & 0xFF if s & 0x7F == 0 else 128 + (s & 0x7F)
    return (code, String(from_utf8_lossy=Span(out)))


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
