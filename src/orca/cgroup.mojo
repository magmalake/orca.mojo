"""Containment (#201): a cgroup v2 memory limit per run, and retrying bigger
after an out-of-memory kill.

A local or control-placed run (and a remote worker's, `orca.worker`) gets a
cgroup of its own, `orca-run-<run>-<start>-<owner pid>`, beside the leaf
`orca` the scheduler moved itself into. Its process is placed in it between
`fork` and `exec` (`orca.proc.spawn`: the parent opens the cgroup's
`cgroup.procs` before forking, and the child only `write`s `0` to it, which
is async-signal-safe; the descriptor closes at `exec`), so everything the
run starts is in it from its first instruction.

- `memory.high` is the run's **limit**: its grant (`orca.resources`), else
  its request's maximum, else `ORCA_RUN_MEMORY_MB` (default: what this host
  can grant, its capacity less the reserve); on the control placement at
  most `ORCA_CONTROL_MEMORY_MB`. Past it the kernel throttles the run and
  reclaims its memory before anything dies.
- `memory.max` is the limit plus `ORCA_RUN_MEMORY_MARGIN` percent (25):
  past it the kernel kills the run's processes, all of them
  (`memory.oom.group`), and none of anything else's. `memory.swap.max` is 0,
  so the limit is one of memory, not of memory and swap.
- **Held at the limit.** A run past `memory.high` whose memory the kernel
  can't reclaim (anonymous, and no swap) is throttled, not killed: each
  allocation waits, so it may creep towards `memory.max` for hours.
  The scheduler checks its runs each tick: one at its limit (`at_limit`)
  with every task stalled on memory most of the time (the cgroup's
  `memory.pressure` `full avg10` at least `HELD_PRESSURE`) for
  `ORCA_RUN_MEMORY_HELD_SECONDS` (120; 0: never) in a row is killed, and
  counts as out of memory as below.
- At the reap, `memory.events`' `oom_kill` says whether the kernel killed
  them for it: the attempts fail with `out of memory at <limit>` rather
  than signal 9.
- **Retrying bigger.** Such a kill raises the pipeline's memory
  (`orca.pipelines.memory_raised_mb`) to the limit times
  `ORCA_RUN_MEMORY_GROWTH` (1.5), at most its request's maximum (with no
  request, what the host can grant; on the control placement, the control
  cap). The retry, and every later run of the pipeline, asks for at least
  that: it's its request's minimum, which admission waits for, or with no
  request its limit. Registering a different `memory_gb` forgets it.
- The cgroup is removed (`rmdir`) once the tree has left it, then or on a
  later tick; a scheduler that starts removes the empty ones a previous
  scheduler left (`stale`).

Where orca can't make cgroups (macOS, cgroup v1, a container whose
`/sys/fs/cgroup` is read-only or not delegated, `ORCA_CGROUPS=0`), it says
why once and runs as before: `oom_score_adj` 1000 and, for control-placed
runs, the sampled `ORCA_CONTROL_MEMORY_MB` cap. docs/hetzner.md says what
the Hetzner deploy needs.
"""

from std.ffi import c_int, external_call
from std.os import getenv, listdir, mkdir, rmdir
from std.os.path import exists, isdir
from std.sys.info import CompilationTarget
from std.time import perf_counter_ns

from orca.resources import gb_text
from orca.sample import _read

comptime ROOT = "/sys/fs/cgroup"
comptime LEAF = "orca"
"""The cgroup the scheduler moves itself (and what shared its cgroup) into,
so the one above can hold the runs' cgroups (cgroup v2 lets a cgroup with
controllers for its children hold no processes)."""
comptime RUN_PREFIX = "orca-run-"


# ── arithmetic ───────────────────────────────────────────────────────────────


def max_mb(limit_mb: Int, margin_pct: Int) -> Int:
    """`memory.max` for a limit of `limit_mb`: the margin above it, rounded
    up to a whole MB."""
    return limit_mb + (limit_mb * margin_pct + 99) // 100


def raised_mb(limit_mb: Int, growth_pct: Int, ceiling_mb: Int) -> Int:
    """What a run killed at `limit_mb` asks for next: `growth_pct` percent of
    it, rounded up, at most `ceiling_mb` (when over 0), and never less than
    `limit_mb`."""
    var up = (limit_mb * growth_pct + 99) // 100
    if ceiling_mb > 0:
        up = min(up, ceiling_mb)
    return max(up, limit_mb)


def start_limit_mb(
    grant_mb: Int,
    request_max_mb: Int,
    default_mb: Int,
    raised: Int,
    cap_mb: Int,
) -> Int:
    """A start's limit: its grant, else its request's maximum, else the
    larger of `default_mb` and the pipeline's `raised` memory; at most
    `cap_mb` when that's over 0 (the control placement's cap)."""
    var mb = grant_mb
    if mb <= 0:
        mb = request_max_mb
    if mb <= 0:
        mb = max(default_mb, raised)
    if cap_mb > 0:
        mb = min(mb, cap_mb)
    return mb


def ceiling_mb(request_max_mb: Int, room_mb: Int, cap_mb: Int) -> Int:
    """The most a retry may ask for: the request's maximum, else what the
    host can grant; at most `cap_mb` when that's over 0."""
    var mb = request_max_mb if request_max_mb > 0 else room_mb
    if cap_mb > 0:
        mb = min(mb, cap_mb) if mb > 0 else cap_mb
    return mb


def raised_request(min_mb: Int, max_mb: Int, raised: Int) -> Int:
    """A request's minimum, `min_mb`, raised to the pipeline's `raised`
    memory, up to its maximum."""
    if raised <= min_mb:
        return min_mb
    return min(raised, max_mb)


def _mb_text(mb: Int) -> String:
    return String(gb_text(mb), " GB") if mb >= 1024 else String(mb, " MB")


def oom_text(
    limit_mb: Int, kill_mb: Int, next_mb: Int, stalled: Bool = False
) -> String:
    """An attempt's error after the kernel killed its run for its memory,
    or, `stalled`, after the scheduler killed it while it was held at its
    limit (`held`, or a heartbeat that went stale there)."""
    var t = String("out of memory at ", _mb_text(limit_mb), ": ")
    if stalled:
        t.write(
            "the run's processes were held at their cgroup's memory.high,",
            " throttled, until orca killed them",
        )
    else:
        t.write(
            "the run's processes passed its cgroup's memory.max (",
            _mb_text(kill_mb),
            ") and the kernel killed them",
        )
    if next_mb > limit_mb:
        t.write("; its retry asks for ", _mb_text(next_mb))
    else:
        t.write("; that's the most it may ask for (memory_gb's maximum)")
    return t^


# ── the files ────────────────────────────────────────────────────────────────


def own_path(proc_self_cgroup: String) -> String:
    """The cgroup v2 path in `/proc/self/cgroup`'s text (its `0::` line), or
    empty with no unified hierarchy (cgroup v1)."""
    for line in proc_self_cgroup.splitlines():
        if line.startswith("0::"):
            var p = String(String(line)[byte=3:].strip())
            return p if p else String("/")
    return ""


def event(text: String, name: String) -> Int:
    """The count `name` in a `memory.events` file's `key value` lines; 0
    without it."""
    for line in text.splitlines():
        var words = line.split()
        if len(words) == 2 and String(words[0]) == name:
            try:
                return Int(String(words[1]))
            except:
                return 0
    return 0


def has_word(text: String, word: String) -> Bool:
    """Whether `cgroup.controllers` or `cgroup.subtree_control` names
    `word`."""
    for w in text.split():
        if String(w) == word:
            return True
    return False


def at_limit(current_bytes: Int, limit_mb: Int) -> Bool:
    """Whether a cgroup's memory, `current_bytes`, is at its limit (within
    5%): one throttled there."""
    return current_bytes >= 0 and current_bytes * 100 >= (
        limit_mb * 1024 * 1024 * 95
    )


comptime HELD_PRESSURE: Float64 = 50.0
"""The `full avg10` (percent of the last 10 s all the run's tasks were
stalled on memory) at or above which a run at its limit is held there."""


def full_avg10(text: String) -> Float64:
    """`full avg10` from a `memory.pressure` file's `some ...` and
    `full ...` lines; -1 without it."""
    for line in text.splitlines():
        var words = line.split()
        if len(words) < 2 or String(words[0]) != "full":
            continue
        for w in words:
            var kv = String(w)
            if kv.startswith("avg10="):
                try:
                    return Float64(String(kv[byte=6:]))
                except:
                    return -1.0
    return -1.0


def held(current_bytes: Int, limit_mb: Int, full_pct: Float64) -> Bool:
    """Whether a run's cgroup is held at its limit now: at it (`at_limit`)
    and its tasks all stalled on memory at least `HELD_PRESSURE` percent
    of the time."""
    return at_limit(current_bytes, limit_mb) and full_pct >= HELD_PRESSURE


def run_name(run: Int, start: Int, owner: Int) -> String:
    """A run's cgroup's name: its run, its start and the pid of the scheduler
    (or worker) that made it."""
    return String(RUN_PREFIX, run, "-", start, "-", owner)


def owner_of(name: String) -> Int:
    """The pid in a run cgroup's name, or -1 for another name."""
    if not name.startswith(RUN_PREFIX):
        return -1
    var dash = name.rfind("-")
    try:
        return Int(String(name[byte = dash + 1 :]))
    except:
        return -1


def _cstr(s: String) -> List[UInt8]:
    var b = List[UInt8](capacity=s.byte_length() + 1)
    for c in s.as_bytes():
        b.append(c)
    b.append(0)
    return b^


def put(path: String, text: String) -> Bool:
    """Writes `text` to a cgroup file at once; whether the kernel took it
    (stdio, as `orca.proc`: `open` is declared elsewhere with another
    signature)."""
    var p = _cstr(path)
    var mode = _cstr("w")
    var f = external_call["fopen", Int](p.unsafe_ptr(), mode.unsafe_ptr())
    if f == 0:
        return False
    var b = _cstr(text)
    var n = external_call["fwrite", Int](
        b.unsafe_ptr(), Int(1), text.byte_length(), f
    )
    var flushed = external_call["fflush", c_int](f)
    var closed = external_call["fclose", c_int](f)
    _ = p^
    _ = mode^
    _ = b^
    return n == text.byte_length() and flushed == 0 and closed == 0


def _alive(pid: Int) -> Bool:
    return external_call["kill", c_int](c_int(pid), c_int(0)) == 0


# ── the knobs ────────────────────────────────────────────────────────────────


def _env_int(name: StaticString, default: Int, least: Int) raises -> Int:
    var s = getenv(name)
    if not s:
        return default
    try:
        var n = Int(s)
        if n >= least:
            return n
    except:
        pass
    raise Error("orca: ", name, " must be a number from ", least, ", not ", s)


def growth_pct_of(s: String) raises -> Int:
    """`ORCA_RUN_MEMORY_GROWTH`'s factor, from 1 (`1.5`), as a percentage."""
    try:
        var f = Float64(s)
        if f >= 1.0 and f <= 100.0:
            return Int(f * 100.0 + 0.5)
    except:
        pass
    raise Error(
        "orca: ORCA_RUN_MEMORY_GROWTH must be a factor from 1 to 100, not ", s
    )


@fieldwise_init
struct MemoryKnobs(ImplicitlyCopyable, Movable):
    """The scheduler's (or worker's) settings for its runs' limits."""

    var default_mb: Int
    """`ORCA_RUN_MEMORY_MB`: the limit of a run without a request; 0 for
    what the host can grant."""
    var margin_pct: Int
    """`ORCA_RUN_MEMORY_MARGIN` (25): `memory.max` this many percent over
    the limit."""
    var growth_pct: Int
    """`ORCA_RUN_MEMORY_GROWTH` (1.5) as a percentage: a retry's memory
    after an out-of-memory kill."""
    var held_s: Int
    """`ORCA_RUN_MEMORY_HELD_SECONDS` (120): how long a run may be held at
    its limit (`held`) before it's killed as out of memory; 0: never."""

    @staticmethod
    def from_env() raises -> MemoryKnobs:
        var g = getenv("ORCA_RUN_MEMORY_GROWTH")
        return MemoryKnobs(
            _env_int("ORCA_RUN_MEMORY_MB", 0, 0),
            _env_int("ORCA_RUN_MEMORY_MARGIN", 25, 0),
            growth_pct_of(g) if g else 150,
            _env_int("ORCA_RUN_MEMORY_HELD_SECONDS", 120, 0),
        )


struct HeldWatch(Copyable, Movable):
    """Since when each contained run has been held at its memory limit
    (`held`), to say which have been held `ORCA_RUN_MEMORY_HELD_SECONDS`
    in a row: the scheduler's and a worker's runs alike."""

    var since: Dict[Int, Int]
    """A run's process to the first check (monotonic ns) of the unbroken
    stretch it has been held; -1 once `due` has named it."""

    def __init__(out self):
        self.since = Dict[Int, Int]()

    def due(
        mut self, contained: Dict[Int, Contained], held_s: Int
    ) -> List[Int]:
        """The processes of `contained` held for `held_s` seconds in a row,
        each named once; none when `held_s` is 0."""
        var out = List[Int]()
        if held_s <= 0:
            return out^
        var t = Int(perf_counter_ns())
        var gone = List[Int]()
        for e in self.since.items():
            if e.key not in contained:
                gone.append(e.key)
        for pid in gone:
            _ = self.since.pop(pid, 0)
        for e in contained.items():
            ref c = e.value
            var pressure = Cgroups.full_pressure(c.path)
            if not held(Cgroups.current(c.path), c.limit_mb, pressure):
                if self.since.get(e.key, 0) >= 0:
                    _ = self.since.pop(e.key, 0)
                continue
            var since = self.since.get(e.key, t)
            if since < 0:
                continue
            if t - since >= held_s * 1_000_000_000:
                out.append(e.key)
                self.since[e.key] = -1
            else:
                self.since[e.key] = since
        return out^


def held_text(c: Contained, held_s: Int) -> String:
    """Why a held run's process was killed, for the log."""
    return String(
        "held at its memory limit (",
        _mb_text(c.limit_mb),
        ") for ",
        held_s,
        " s",
    )


# ── the hierarchy ────────────────────────────────────────────────────────────


@fieldwise_init
struct Contained(Copyable, Movable):
    """A run's cgroup, as its starter keeps it until the process is reaped."""

    var run: Int
    var start: Int
    var pipeline: String
    var path: String
    var limit_mb: Int
    var kill_mb: Int
    """Its `memory.max` in MB."""
    var ceiling_mb: Int
    """The most a retry may ask for (`ceiling_mb`)."""
    var grant_mb: Int


struct Cgroups(Copyable, Movable):
    """Where this process makes its runs' cgroups, or why it can't."""

    var base: String
    """The cgroup the runs' cgroups go in; empty when off."""
    var why: String
    """Why it's off, or how it's on, for the log."""
    var knobs: MemoryKnobs

    def __init__(out self, base: String, why: String, knobs: MemoryKnobs):
        self.base = base
        self.why = why
        self.knobs = knobs

    @staticmethod
    def off(why: String) -> Cgroups:
        return Cgroups(String(), why, MemoryKnobs(0, 25, 150, 120))

    def on(self) -> Bool:
        return self.base != ""

    @staticmethod
    def setup(knobs: MemoryKnobs) -> Cgroups:
        """Finds or makes the cgroup runs' cgroups go in: this process's
        own, after moving what's in it into a leaf `orca` (or, if it's in
        such a leaf already, the one above), with the memory controller on
        for its children. Never raises: off says why."""
        var c = Cgroups._setup()
        c.knobs = knobs
        return c^

    @staticmethod
    def _setup() -> Cgroups:
        if getenv("ORCA_CGROUPS") == "0":
            return Cgroups.off("off (ORCA_CGROUPS=0)")
        comptime if not CompilationTarget.is_linux():
            return Cgroups.off("off: no cgroups on this system")
        var own = own_path(_read("/proc/self/cgroup"))
        if not own:
            return Cgroups.off("off: no cgroup v2 hierarchy (cgroup v1?)")
        if not exists(String(ROOT, "/cgroup.controllers")):
            return Cgroups.off(String("off: no cgroup v2 mounted at ", ROOT))
        var dir = String(ROOT, own) if own != "/" else String(ROOT)
        var base = dir
        var leaf_name = String(own[byte = own.rfind("/") + 1 :])
        if leaf_name == LEAF and own != "/":
            # Moved here by an earlier scheduler, or another scheduler on
            # this host: the runs go beside it.
            var up = String(dir[byte = : dir.rfind("/")])
            if has_word(_read(String(up, "/cgroup.subtree_control")), "memory"):
                base = up
        if not has_word(_read(String(base, "/cgroup.controllers")), "memory"):
            return Cgroups.off(
                String(
                    "off: ",
                    base,
                    " has no memory controller (cgroup.controllers): its",
                    " parent doesn't delegate it",
                )
            )
        if not has_word(
            _read(String(base, "/cgroup.subtree_control")), "memory"
        ):
            # A cgroup that gives its children controllers holds no
            # processes: what's in it goes to a leaf of its own first.
            var leaf = String(base, "/", LEAF)
            try:
                if not isdir(leaf):
                    mkdir(leaf)
            except e:
                return Cgroups.off(
                    String(
                        "off: can't make a cgroup in ",
                        base,
                        " (",
                        e,
                        "): /sys/fs/cgroup is read-only or not delegated",
                        " to orca",
                    )
                )
            var me = Int(external_call["getpid", c_int]())
            for p in _read(String(base, "/cgroup.procs")).split():
                var pid = String(p)
                # One that has exited since is no matter.
                if not put(String(leaf, "/cgroup.procs"), pid):
                    if pid == String(me):
                        return Cgroups.off(
                            String("off: can't move orca into ", leaf)
                        )
            if not put(String(base, "/cgroup.subtree_control"), "+memory"):
                return Cgroups.off(
                    String(
                        "off: can't turn the memory controller on in ",
                        base,
                        "/cgroup.subtree_control (processes left in it,",
                        " or not delegated)",
                    )
                )
        # A run's cgroup, made and removed, to be sure.
        var probe = String(
            base,
            "/",
            run_name(0, 0, Int(external_call["getpid", c_int]())),
        )
        try:
            if not isdir(probe):
                mkdir(probe)
        except e:
            return Cgroups.off(
                String("off: can't make a cgroup in ", base, " (", e, ")")
            )
        var writable = put(String(probe, "/memory.high"), "max")
        try:
            rmdir(probe)
        except:
            pass
        if not writable:
            return Cgroups.off(
                String("off: can't set memory.high in a cgroup in ", base)
            )
        return Cgroups(
            base,
            String("on: runs' cgroups go in ", base),
            MemoryKnobs(0, 25, 150, 120),
        )

    def make(
        self, run: Int, start: Int, limit_mb: Int
    ) raises -> Tuple[String, Int]:
        """Makes a start's cgroup with `memory.high` at `limit_mb` and
        `memory.max` the margin above, and returns its path and
        `memory.max` in MB."""
        var path = String(
            self.base,
            "/",
            run_name(run, start, Int(external_call["getpid", c_int]())),
        )
        if not isdir(path):
            mkdir(path)
        var kill = max_mb(limit_mb, self.knobs.margin_pct)
        var mb = 1024 * 1024
        # max first: memory.high above memory.max means nothing.
        if not put(String(path, "/memory.max"), String(kill * mb)):
            raise Error("can't set memory.max in ", path)
        if not put(String(path, "/memory.high"), String(limit_mb * mb)):
            raise Error("can't set memory.high in ", path)
        _ = put(String(path, "/memory.oom.group"), "1")
        if exists(String(path, "/memory.swap.max")):
            _ = put(String(path, "/memory.swap.max"), "0")
        return (path^, kill)

    @staticmethod
    def oom_kills(path: String) -> Int:
        """The processes the kernel killed in the cgroup for its memory."""
        return event(_read(String(path, "/memory.events")), "oom_kill")

    @staticmethod
    def full_pressure(path: String) -> Float64:
        """The cgroup's `full avg10` (`memory.pressure`); -1 if it can't be
        read (a kernel without PSI)."""
        return full_avg10(_read(String(path, "/memory.pressure")))

    @staticmethod
    def current(path: String) -> Int:
        """The cgroup's memory now, in bytes (`memory.current`); -1 if it
        can't be read."""
        var t = String(_read(String(path, "/memory.current")).strip())
        try:
            return Int(t)
        except:
            return -1

    @staticmethod
    def remove(path: String) -> Bool:
        """Removes the cgroup once nothing is in it; whether it's gone."""
        if not isdir(path):
            return True
        try:
            rmdir(path)
            return True
        except:
            return False

    def stale(self) -> List[String]:
        """The runs' cgroups no live process of the same pid made, or this
        one before it started again (a restart of the container keeps the
        pid), to be removed once empty."""
        var out = List[String]()
        if not self.on():
            return out^
        var me = Int(external_call["getpid", c_int]())
        try:
            for name in listdir(self.base):
                var owner = owner_of(name)
                if owner < 0:
                    continue
                if owner == me or not _alive(owner):
                    out.append(String(self.base, "/", name))
        except:
            pass
        return out^
