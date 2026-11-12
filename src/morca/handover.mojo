"""Whether a run's code is outdated, a newer version leading (#397): how a
long run hands over to the next version on its own terms.

A run may last months. A deploy doesn't wait for it and morca never stops
it; instead the run can see that it's outdated, and its code decides what
to do. By default nothing happens.

    from morca import Ctx, outdated

    def follow(mut ctx: Ctx, key: Name) raises -> Int:
        var polls = 0
        while not outdated():
            poll_once()          # one unit of work
            polls += 1
        ctx.log("a newer version leads: handing over")
        return polls

**Where it comes from.** The run's heartbeat thread (`morca.heartbeat`)
asks on each beat, over the connection it beats with (`outdated_sql`), or
in the reply to its beat for a worker's run (`morca.http_store`), and keeps
the answer here, in the process: `outdated()` reads an atomic, so a loop
can call it as often as it likes. It's false until the first beat, and
always for a run with no heartbeat (in memory, or over SQLite).

A run is outdated, while it runs, when either:

- its pipeline is registered with other code than the run's: the version
  `morca register` recorded last (`orca.pipelines.version`) and the one the
  run recorded (`orca.runs.version`), rows of `orca.pipeline_versions`
  (`morca.source`), differ in the source commit (`-D MORCA_COMMIT`), its
  tree's dirtiness or the registry hash. The registry hash alone isn't
  enough: it covers the DAG, not what a step's body does, so a fix inside
  a step keeps it. The image isn't compared: a registration's is where the
  pipeline's remote runs go (`--image`), a run's the one it ran in;
- or the scheduler instance holding the run drains (docs/overlap.md): a
  newer one leads, or it was asked to (`morca-stack drain-old`). This
  covers a deploy of the same code (new settings, say), and builds that
  record no commit.

`code_version()` is the run's version and `desired_code_version()` the
registered one, each as `version_label` writes it: the commit's first 12
characters (`+dirty` after a dirty tree's), else `code <registry hash>`.
"""

from std.ffi import _get_global
from std.memory.alloc import unsafe_alloc

from postgres import Connection, Params
from threads import AtomicCounter, Mutex

from morca.source import source_commit, source_dirty


def version_label(commit: String, dirty: Bool, registry_hash: String) -> String:
    """A version as it's shown: the commit's first 12 characters, with
    `+dirty` for a dirty tree's, else `code <registry hash>`; empty for
    none. `label_sql` is the same over a row of `orca.pipeline_versions`."""
    if commit:
        var c = String(commit[byte = : min(12, commit.byte_length())])
        if dirty:
            c.write("+dirty")
        return c^
    if registry_hash:
        return String("code ", registry_hash)
    return String()


def label_sql(v: String) -> String:
    """`version_label` in SQL, over the `orca.pipeline_versions` row `v`
    (null for none: empty)."""
    return String(
        "CASE WHEN ",
        v,
        ".id IS NULL THEN '' WHEN ",
        v,
        ".commit_sha <> '' THEN left(",
        v,
        ".commit_sha, 12) || CASE WHEN ",
        v,
        ".dirty THEN '+dirty' ELSE '' END ELSE 'code ' || ",
        v,
        ".registry_hash END",
    )


def differs_sql(a: String, b: String) -> String:
    """SQL true when the versions `a` and `b` (rows of
    `orca.pipeline_versions`) are other code: the commit, its dirtiness or
    the registry hash differ; false when either is null."""
    return String(
        "coalesce(",
        a,
        ".registry_hash <> ",
        b,
        ".registry_hash OR ",
        a,
        ".commit_sha <> ",
        b,
        ".commit_sha OR ",
        a,
        ".dirty <> ",
        b,
        ".dirty, false)",
    )


def outdated_sql() -> String:
    """One run's outdatedness, for the run `$1` (module doc): `outdated`,
    `mine` and `desired` (labels), `differs` (other code is registered),
    `newer` (registered after the run's), `draining` (its instance drains)
    and `leader` (the newest instance that doesn't drain, which leads or
    is about to, or 0)."""
    var draining = String(
        "coalesce((s.draining_since IS NOT NULL OR s.drain_asked IS NOT NULL)",
        " AND s.ended IS NULL AND r.id::text = ANY(string_to_array(s.running,",
        " ',')), false)",
    )
    return String(
        "SELECT r.status = 'running' AND (",
        differs_sql("rv", "pv"),
        " OR ",
        draining,
        ") AS outdated, ",
        label_sql("rv"),
        " AS mine, ",
        label_sql("pv"),
        " AS desired, ",
        differs_sql("rv", "pv"),
        " AS differs, coalesce(pv.id > rv.id, false) AS newer, ",
        draining,
        " AS draining, coalesce((SELECT max(l.id) FROM orca.schedulers l",
        " WHERE l.ended IS NULL AND l.draining_since IS NULL AND",
        " l.drain_asked IS NULL), 0) AS leader",
        " FROM orca.runs r JOIN orca.pipelines p ON p.name = r.pipeline",
        " LEFT JOIN orca.pipeline_versions rv ON rv.id = r.version",
        " LEFT JOIN orca.pipeline_versions pv ON pv.id = p.version",
        " LEFT JOIN orca.schedulers s ON s.id = r.instance WHERE r.id = $1",
    )


def outdated_text(
    differs: Bool, desired: String, newer: Bool, leader: Int
) -> String:
    """How the UI and `morca status` say why a run is outdated: "newer
    version X is leading" for other code, else that its scheduler drains."""
    if differs:
        return String(
            "newer version " if newer else "version ", desired, " is leading"
        )
    if leader > 0:
        return String(
            "its scheduler drains: scheduler instance ", leader, " takes over"
        )
    return "its scheduler drains"


@fieldwise_init
struct RunOutdated(Copyable, Movable):
    """One run's outdatedness, as `run_outdated` reads it."""

    var outdated: Bool
    var mine: String
    """The run's version (`version_label`), or empty if it recorded none."""
    var desired: String
    """Its pipeline's registered version, or empty."""
    var why: String
    """`outdated_text`, when outdated; else empty."""


def run_outdated(mut conn: Connection, run: Int) raises -> RunOutdated:
    """Whether `run` is outdated (`outdated_sql`); not for a run that
    isn't running."""
    var rows = conn.query(outdated_sql(), Params().int64(Int64(run)))
    if len(rows) == 0:
        return RunOutdated(False, String(), String(), String())
    var r = rows.row(0)
    var o = r.bool("outdated")
    return RunOutdated(
        o,
        r.text("mine"),
        r.text("desired"),
        outdated_text(
            r.bool("differs"),
            r.text("desired"),
            r.bool("newer"),
            Int(r.int64("leader")),
        ) if o else String(),
    )


# --- the process's state, which the heartbeat thread sets ---


struct _State(Movable):
    var flag: AtomicCounter
    """1 while the run is outdated."""
    var known: AtomicCounter
    """1 once a beat has asked."""
    var lock: Mutex
    var mine: String
    var desired: String
    var why: String

    def __init__(out self) raises:
        self.flag = AtomicCounter.alloc(0)
        self.known = AtomicCounter.alloc(0)
        self.lock = Mutex()
        self.mine = String()
        self.desired = String()
        self.why = String()


comptime _Ptr = Pointer[NoneType, MutUntrackedOrigin]


def _init() -> Optional[_Ptr]:
    try:
        var p = unsafe_alloc[_State](1)
        p.unsafe_write(_State())
        return _Ptr(unsafe_from_address=Int(p))
    except:
        return None


def _keep(p: Optional[_Ptr]):
    """Kept for the process's life: a beat may still be setting it."""
    pass


def _state() -> Optional[Pointer[_State, MutUntrackedOrigin]]:
    var p = _get_global["MORCA_OUTDATED_STATE", _init, _keep]()
    if not p:
        return None
    return Pointer[_State, MutUntrackedOrigin](
        unsafe_from_address=Int(p.value())
    )


def set_outdated(outdated: Bool, mine: String, desired: String, why: String):
    """What the heartbeat found on its beat."""
    var s = _state()
    if not s:
        return
    ref st = s.value()[]
    st.lock.lock()
    st.mine = mine
    st.desired = desired
    st.why = why
    st.lock.unlock()
    st.flag.store(Int64(1) if outdated else Int64(0))
    st.known.store(Int64(1))


def outdated() -> Bool:
    """Whether a newer version than this run's leads (module doc): its
    pipeline is registered with other code, or its scheduler drains. Read
    from the heartbeat's last beat, so cheap enough to call between units of
    work; false until the first beat, and for a run without a heartbeat.
    Morca does nothing about it: the run decides whether to end."""
    var s = _state()
    return Bool(s) and s.value()[].flag.load() == 1


def code_version() -> String:
    """This run's version, as the heartbeat found it recorded; before the
    first beat, this binary's commit, if it was built with one."""
    var s = _state()
    if s:
        ref st = s.value()[]
        if st.known.load() == 1:
            st.lock.lock()
            var m = st.mine
            st.lock.unlock()
            if m:
                return m^
    return version_label(source_commit(), source_dirty(), "")


def desired_code_version() -> String:
    """The version its pipeline is registered with now, what a new run
    would start with, as of the heartbeat's last beat; empty before it."""
    var s = _state()
    if not s:
        return String()
    ref st = s.value()[]
    st.lock.lock()
    var d = st.desired
    st.lock.unlock()
    return d^


def outdated_why() -> String:
    """Why the run is outdated (`outdated_text`), or empty."""
    var s = _state()
    if not s:
        return String()
    ref st = s.value()[]
    if st.flag.load() != 1:
        return String()
    st.lock.lock()
    var w = st.why
    st.lock.unlock()
    return w^
