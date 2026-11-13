"""Admission by memory pressure, where it shows (#202): each host's newest
reading in `orca.pressure`, for Ops › Control, `/api/workers`, `/metrics`,
`morca status` and the home and runs pages, and the alert when a hold has
nothing of morca's to wait for.

The scheduler records its own host (`scheduler`) every few seconds and on
each change of hold; the UI records a worker's (`worker:<token id>`) from
each claim, where the worker sends its `Report`. The decision itself is
`orca.pressure.Admission`'s, made where the runs start: the scheduler for
the local runner and the control host, each worker for the runs it claims.

A row older than `FRESH_SECONDS` is shown as no reading: a scheduler or
worker that stopped isn't holding anything.
"""

from json import Value
import morca._internal.db.pressure as pressure_db
from morca._internal.db.pressure import Reading

from morca._internal.html import Html
from morca._internal.http_store import Body
from morca._internal.json import quote
from morca._internal.notify import (
    alert,
    configured,
    is_firing,
    resolve,
    ui_link,
)
from morca._internal.pressure import Admission, Source, pct_text
from morca._internal.state import State

comptime SCHEDULER_HOST = "scheduler"
"""`orca.pressure.host` of the scheduler's own host."""
comptime FRESH_SECONDS = 60
comptime ALERT_KIND = "memory-pressure"


def worker_host(token_id: Int) -> String:
    return String("worker:", token_id)


@fieldwise_init
struct Report(Copyable, Movable):
    """A host's reading and whether it holds starts, as recorded and sent."""

    var kind: String
    """`orca.pressure.Source.kind`; empty for no PSI here."""
    var path: String
    var has: Bool
    """Whether there is a reading."""
    var some10: Float64
    var some60: Float64
    var some300: Float64
    var full10: Float64
    var full60: Float64
    var full300: Float64
    var limit: Float64
    """`MORCA_ADMIT_MEMORY_PRESSURE`; 0 for off."""
    var held: String
    """The reason runs wait (`waiting: memory pressure ...`), or empty."""
    var held_seconds: Float64
    var stuck: Bool
    """Held past its grace with nothing of morca's going: one at a time
    starts (`Admission.escape`)."""

    @staticmethod
    def of(s: Source, a: Admission, now: Float64) -> Report:
        """What `a`, reading `s`, says at `now` (its clock)."""
        var r = Report(
            s.kind,
            s.path,
            False,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            a.limit,
            a.hold,
            now - a.held_since if a.holding else 0.0,
            a.escape,
        )
        if a.reading:
            ref p = a.reading.value()
            r.has = True
            r.some10 = p.some.avg10
            r.some60 = p.some.avg60
            r.some300 = p.some.avg300
            if p.has_full:
                r.full10 = p.full.avg10
                r.full60 = p.full.avg60
                r.full300 = p.full.avg300
        return r^

    def json(self) -> String:
        var b = Body().text("kind", self.kind).text("path", self.path)
        if self.has:
            b = b^.raw(
                "some",
                String(
                    Body()
                    .float("avg10", self.some10)
                    .float("avg60", self.some60)
                    .float("avg300", self.some300)
                    .finish()
                ),
            )
            b = b^.raw(
                "full",
                String(
                    Body()
                    .float("avg10", self.full10)
                    .float("avg60", self.full60)
                    .float("avg300", self.full300)
                    .finish()
                ),
            )
        else:
            b = b^.raw("some", "null").raw("full", "null")
        b = b^.float("limit", self.limit)
        if self.held:
            b = b^.text("held", self.held)
        else:
            b = b^.raw("held", "null")
        return (
            b
            ^.float("held_seconds", self.held_seconds)
            .bool("stuck", self.stuck)
            .finish()
        )

    @staticmethod
    def from_json(v: Value) raises -> Report:
        var r = Report(
            _str(v, "kind"),
            _str(v, "path"),
            False,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            _float(v, "limit"),
            _str(v, "held"),
            _float(v, "held_seconds"),
            String("stuck") in v and v["stuck"].bool_or(False),
        )
        if String("some") in v and v["some"].is_object():
            r.has = True
            r.some10 = _float(v["some"], "avg10")
            r.some60 = _float(v["some"], "avg60")
            r.some300 = _float(v["some"], "avg300")
        if String("full") in v and v["full"].is_object():
            r.full10 = _float(v["full"], "avg10")
            r.full60 = _float(v["full"], "avg60")
            r.full300 = _float(v["full"], "avg300")
        return r^


def _str(v: Value, key: String) raises -> String:
    if key not in v or not v[key].is_string():
        return ""
    return v[key].string_value()


def _float(v: Value, key: String) raises -> Float64:
    if key not in v:
        return 0.0
    return v[key].float_or(0.0)


def record(mut db: State, host: String, r: Report) raises:
    """Keeps `host`'s newest reading."""
    pressure_db.record(
        db.conn,
        host,
        Reading(
            r.kind,
            r.path,
            r.has,
            r.some10,
            r.some60,
            r.some300,
            r.full10,
            r.full60,
            r.full300,
            r.limit,
            r.held,
            r.held_seconds,
        ),
    )


def alert_stuck(mut db: State, subject: String, r: Report) raises:
    """'memory-pressure': `subject` has held starts past its grace with
    nothing of morca's running, so its pressure is someone else's; runs now
    start there one at a time."""
    print(
        String(
            "morca: ",
            subject,
            " has held runs for ",
            Int(r.held_seconds),
            " s with none of morca's going (",
            r.held,
            "): starting them one at a time",
        )
    )
    if not configured():
        return
    _ = alert(
        db,
        ALERT_KIND,
        subject,
        "high",
        "warning,hourglass",
        String("morca: memory pressure on ", subject),
        String(
            "Memory pressure (PSI some avg10) on ",
            subject,
            " is ",
            pct_text(r.some10),
            "%, over ",
            pct_text(r.limit),
            "%, for ",
            Int(r.held_seconds) // 60,
            " min with none of morca's runs going there: something else is",
            " using the memory. Runs now start one at a time.",
        ),
        ui_link("/ops/control"),
    )


def resolve_stuck(mut db: State, subject: String, r: Report) raises:
    """Ends `alert_stuck`'s alert once `subject` no longer holds."""
    if not configured() or not is_firing(db, ALERT_KIND, subject):
        return
    resolve(
        db,
        ALERT_KIND,
        subject,
        String("morca: memory pressure on ", subject, " has eased"),
        String(
            "Memory pressure on ", subject, " is ", pct_text(r.some10), "%."
        ),
        ui_link("/ops/control"),
    )


# ---------------------------------------------------------------------------
# Reading it back
# ---------------------------------------------------------------------------


@fieldwise_init
struct PressureRow(Copyable, Movable):
    var host: String
    var report: Report
    var age: Int
    """Seconds since it was recorded."""

    def fresh(self) -> Bool:
        return self.age <= FRESH_SECONDS


def pressure_rows(mut db: State, host: String = "") raises -> List[PressureRow]:
    """Every host's newest reading, or `host`'s alone, the scheduler's
    first."""
    var out = List[PressureRow]()
    for h in pressure_db.readings(db.conn, host):
        ref r = h.reading
        out.append(
            PressureRow(
                h.host,
                Report(
                    r.kind,
                    r.path,
                    r.has,
                    r.some10,
                    r.some60,
                    r.some300,
                    r.full10,
                    r.full60,
                    r.full300,
                    r.limit,
                    r.held,
                    r.held_seconds,
                    False,
                ),
                h.age,
            )
        )
    return out^


def scheduler_pressure(mut db: State) raises -> Optional[PressureRow]:
    """The scheduler's host's reading, if one is fresh."""
    var rows = pressure_rows(db, SCHEDULER_HOST)
    if len(rows) == 0 or not rows[0].fresh():
        return None
    return rows[0].copy()


def row_json(row: Optional[PressureRow]) -> String:
    """A reading as JSON (`Report.json`, with `host` and `age_seconds`), or
    null with none fresh."""
    if not row or not row.value().fresh():
        return "null"
    ref r = row.value()
    var j = r.report.json()
    return String(
        '{"host": ',
        quote(r.host),
        ', "age_seconds": ',
        r.age,
        ", ",
        j[byte=1:],
    )


def state_text(r: Report) -> String:
    """What the host does about its pressure, in words."""
    if not r.has:
        return "no reading: nothing is held"
    if r.held:
        if r.stuck:
            return String(r.held, ", one at a time with nothing else going")
        return r.held
    if r.limit <= 0.0:
        return "admission by pressure is off (MORCA_ADMIT_MEMORY_PRESSURE=0)"
    return String("starting runs (limit ", pct_text(r.limit), "%)")


def held_warning(row: Optional[PressureRow]) -> String:
    """The home and runs pages' line while the scheduler's host holds
    starts, or empty."""
    if not row or not row.value().report.held:
        return ""
    return String(
        "No new runs start on the scheduler's host: ",
        row.value().report.held.removeprefix("waiting: "),
        ". Runs already going go on.",
    )


def pressure_html(mut h: Html, row: Optional[PressureRow]):
    """Ops › Control's Memory pressure card."""
    h.raw('<section class="card"><h2>Memory pressure</h2>')
    if not row:
        h.raw(
            '<p class="muted">No reading from the scheduler in the last',
            " minute. It reads Linux's pressure stall information",
            " (<code>/proc/pressure/memory</code>, or its cgroup's",
            " <code>memory.pressure</code>) each tick.</p></section>",
        )
        return
    ref r = row.value().report
    if not r.has:
        h.raw('<p class="muted">')
        if r.path:
            h.text(
                "The scheduler can't read ",
                r.path,
                " now, so nothing is held for memory pressure.",
            )
        else:
            h.text(
                "The scheduler's host has no memory pressure to read (no",
                " PSI: macOS, or a kernel without it), so runs start on",
                " counts and grants alone.",
            )
        h.raw("</p></section>")
        return
    h.raw('<div class="stats">')
    _stat(
        h,
        "Stalled (some)",
        String(pct_text(r.some10), "%"),
        String(
            "avg10; ",
            pct_text(r.some60),
            "% avg60, ",
            pct_text(r.some300),
            "% avg300",
        ),
        r.held.byte_length() > 0,
        (
            "The share of the last 10 seconds in which at least one task on"
            " this host waited for memory: reclaiming, or reading back what was"
            ' swapped or dropped (Linux\'s pressure stall information, "some").'
            " Near 0% is healthy. This is the number admission compares with"
            " its limit; avg60 and avg300 are the same over 1 and 5 minutes."
        ),
    )
    _stat(
        h,
        "Stalled (full)",
        String(pct_text(r.full10), "%"),
        String(
            "avg10; ",
            pct_text(r.full60),
            "% avg60, ",
            pct_text(r.full300),
            "% avg300",
        ),
        False,
        (
            "The share of time in which every task on this host was waiting for"
            ' memory at once, so no work got done at all (PSI "full"). Any'
            " sustained value means the host is thrashing."
        ),
    )
    _stat(
        h,
        "Admission",
        "holding" if r.held else (
            "off" if r.limit
            <= 0.0 else String("under ", pct_text(r.limit), "%")
        ),
        state_text(r),
        r.held.byte_length() > 0,
        (
            "Whether the scheduler starts new runs on this host. While Stalled"
            " (some) avg10 is over the limit (MORCA_ADMIT_MEMORY_PRESSURE, 10%"
            " by default) it holds them, and runs already going go on; it"
            " starts them again under half the limit, or after 30 s at or"
            " under it."
        ),
    )
    h.raw('</div><p class="muted">The share of time tasks waited for memory,')
    h.raw(" from <code>")
    h.text(r.path)
    h.raw("</code> (")
    h.text(_kind_text(r.kind))
    h.raw(
        "). While avg10 is over the limit no new run starts on this host; it",
        " starts them again under half the limit, or after 30 s at most",
        " it.</p></section>",
    )


def _kind_text(kind: String) -> String:
    if kind == "cgroup":
        return "the scheduler's cgroup, which has a memory limit"
    if kind == "host":
        return "the host's"
    if kind == "file":
        return "MORCA_MEMORY_PRESSURE_FILE"
    if kind == "cgroup-unlimited":
        return "the scheduler's cgroup, with no limit"
    return kind


def _stat(
    mut h: Html,
    key: String,
    value: String,
    sub: String,
    bad: Bool = False,
    help: StaticString = "",
):
    h.raw('<div class="stat', " stat--errors" if bad else "", '">')
    h.raw('<span class="stat__k">')
    h.text(key)
    if help:
        h.help(
            String(
                "pressure-",
                key.lower().replace(" ", "-").replace("(", "").replace(")", ""),
            ),
            key,
            help,
        )
    h.raw('</span><span class="stat__v">')
    h.text(value)
    h.raw('</span><span class="stat__s">')
    h.text(sub)
    h.raw("</span></div>")


def _label(s: String) -> String:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def pressure_prometheus(rows: List[PressureRow]) -> String:
    """`/metrics`' gauges, a series per host with a fresh reading."""
    var out = String()
    var fresh = List[Int]()
    for i in range(len(rows)):
        if rows[i].fresh() and rows[i].report.has:
            fresh.append(i)
    if len(fresh) == 0:
        return out^
    comptime names: List[String] = [
        "morca_memory_pressure_some_avg10",
        "morca_memory_pressure_some_avg60",
        "morca_memory_pressure_some_avg300",
        "morca_memory_pressure_full_avg10",
        "morca_memory_pressure_full_avg60",
        "morca_memory_pressure_full_avg300",
        "morca_admission_memory_pressure_held",
    ]
    comptime helps: List[String] = [
        "Percent of the last 10 s some task stalled on memory (PSI).",
        "Percent of the last 60 s some task stalled on memory (PSI).",
        "Percent of the last 300 s some task stalled on memory (PSI).",
        "Percent of the last 10 s all tasks stalled on memory (PSI).",
        "Percent of the last 60 s all tasks stalled on memory (PSI).",
        "Percent of the last 300 s all tasks stalled on memory (PSI).",
        "1 while no new run starts on the host for its memory pressure.",
    ]
    var ns = materialize[names]()
    var hs = materialize[helps]()
    for k in range(len(ns)):
        out.write("# HELP ", ns[k], " ", hs[k], "\n# TYPE ", ns[k], " gauge\n")
        for i in fresh:
            ref r = rows[i].report
            var v: Float64
            if k == 0:
                v = r.some10
            elif k == 1:
                v = r.some60
            elif k == 2:
                v = r.some300
            elif k == 3:
                v = r.full10
            elif k == 4:
                v = r.full60
            elif k == 5:
                v = r.full300
            else:
                v = 1.0 if r.held else 0.0
            out.write(ns[k], '{host="', _label(rows[i].host), '"} ', v, "\n")
    return out^
