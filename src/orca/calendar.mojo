"""A dated pipeline's partitions on a calendar: what state each day (or, for
a monthly pipeline, month) is in, read from the database alone.

`orca ui` draws it twice, as the picker on the Start runs form and as the
read-only coverage on a pipeline's page, and serves it as JSON at
`/pipelines/<name>/calendar?month=2023-01`. Daily pipelines have a month of
days at a time; monthly ones a year of months. The states, in the order
they win:

- `outside`: before the pipeline's first partition, or after today.
- `live`: in a queued, deferred or running run (`live_sql`).
- `done`: what a backfill counts as done (`DONE_SQL`).
- `failed`: not done, and the last run over it failed.
- `open`: none of the above, so a backfill would queue it.

Daily and monthly keys are zero-padded, so they compare as text.
"""

from std.collections import Dict

from postgres import Params

from orca.civil import Civil, now
from orca.day import Day
from orca.json import quote
from orca.live import live_sql
from orca.month import Month, days_in
from orca.scheduler import done_keys
from orca.state import State


comptime _LAST_SQL = (
    "SELECT DISTINCT ON (k.key) k.key, r.status FROM orca.runs r,"
    " unnest(string_to_array(r.partitions, ',')) AS k(key) WHERE r.pipeline ="
    " $1 AND k.key = ANY (string_to_array($2, ',')) ORDER BY k.key, r.id DESC"
)
"""Of the comma-separated keys `$2`, the status of the last run of pipeline
`$1` that had each (`key`, `status`)."""


@fieldwise_init
struct Cell(Copyable, Movable):
    var key: String
    var state: String


struct Calendar(Movable):
    var pipeline: String
    var kind: String
    """`daily` (cells are days of one month) or `monthly` (months of one
    year)."""
    var period: String
    """`YYYY-MM` for days, `YYYY` for months."""
    var prev: String
    """The period before, empty if it ends before the pipeline starts."""
    var next: String
    """The period after, empty if it starts after today."""
    var start: String
    var today: String
    """Today's key: `YYYY-MM-DD` or `YYYY-MM`."""
    var weekday: Int
    """Of a month's first day, with Monday as 0."""
    var cells: List[Cell]

    def __init__(out self, pipeline: String, kind: String):
        self.pipeline = pipeline
        self.kind = kind
        self.period = String()
        self.prev = String()
        self.next = String()
        self.start = String()
        self.today = String()
        self.weekday = 0
        self.cells = List[Cell]()

    def count(self, state: String) -> Int:
        var n = 0
        for c in self.cells:
            if c.state == state:
                n += 1
        return n


def _pad(n: Int, width: Int) -> String:
    var s = String(n)
    while s.byte_length() < width:
        s = String("0", s)
    return s


def _period_of(
    kind: String, text: String, today: Civil
) raises -> Tuple[Int, Int]:
    """The year and month `text` names: `YYYY-MM`, or for a monthly
    pipeline `YYYY` too; today's if it's empty."""
    if not text:
        return (today.year, today.month)
    if kind == "monthly" and text.byte_length() == 4:
        var y = Int(text)
        if y < 0:
            raise Error("not a year (YYYY): '", text, "'")
        return (y, 1)
    var m = Month.parse(text)
    return (m.year, m.month)


def calendar(
    mut db: State, pipeline: String, kind: String, start: String, period: String
) raises -> Calendar:
    """`pipeline`'s calendar of `period` (see `Calendar.period`; today's if
    empty), with `start` its first partition (or empty)."""
    if kind != "daily" and kind != "monthly":
        raise Error(pipeline, " has ", kind, " partitions, not dated ones")
    var t = Civil.of(now())
    var ym = _period_of(kind, period, t)
    var cal = Calendar(pipeline, kind)
    cal.start = start
    var daily = kind == "daily"
    cal.today = String(Day(t.year, t.month, t.day)) if daily else String(
        Month(t.year, t.month)
    )
    var keys = List[String]()
    if daily:
        cal.period = String(Month(ym[0], ym[1]))
        var before = Month(ym[0], ym[1]).prev()
        var after = Month(ym[0], ym[1]).next()
        var n = days_in(ym[0], ym[1])
        for d in range(1, n + 1):
            keys.append(String(Day(ym[0], ym[1], d)))
        var before_last = String(
            Day(before.year, before.month, days_in(before.year, before.month))
        )
        cal.prev = String(before) if (
            not start or before_last >= start
        ) else String()
        cal.next = (
            String(after) if String(Day(after.year, after.month, 1))
            <= cal.today else String()
        )
        cal.weekday = (Civil(ym[0], ym[1], 1, 0, 0, 0).weekday() + 6) % 7
    else:
        cal.period = _pad(ym[0], 4)
        for m in range(1, 13):
            keys.append(String(Month(ym[0], m)))
        cal.prev = _pad(ym[0] - 1, 4) if (
            ym[0] > 0 and (not start or String(Month(ym[0] - 1, 12)) >= start)
        ) else String()
        cal.next = (
            _pad(ym[0] + 1, 4) if String(Month(ym[0] + 1, 1))
            <= cal.today else String()
        )
    var joined = String()
    for i in range(len(keys)):
        joined.write("," if i else "", keys[i])
    var done = done_keys(db, pipeline, keys)
    var live = Dict[String, Bool]()
    for r in db.conn.query(live_sql(), Params().text(pipeline)):
        live[r.text("key")] = True
    var failed = Dict[String, Bool]()
    for r in db.conn.query(_LAST_SQL, Params().text(pipeline).text(joined)):
        if r.text("status") == "failed":
            failed[r.text("key")] = True
    for k in keys:
        var state: String
        if (start and k < start) or k > cal.today:
            state = "outside"
        elif k in live:
            state = "live"
        elif k in done:
            state = "done"
        elif k in failed:
            state = "failed"
        else:
            state = "open"
        cal.cells.append(Cell(k, state))
    return cal^


def calendar_json(c: Calendar) -> String:
    """`c` as JSON, with each state's count."""
    var j = String('{"pipeline": ', quote(c.pipeline))
    j.write(
        ', "kind": ',
        quote(c.kind),
        ', "unit": ',
        quote("day" if c.kind == "daily" else "month"),
        ', "period": ',
        quote(c.period),
        ', "prev": ',
        quote(c.prev) if c.prev else String("null"),
        ', "next": ',
        quote(c.next) if c.next else String("null"),
        ', "start": ',
        quote(c.start) if c.start else String("null"),
        ', "today": ',
        quote(c.today),
        ', "weekday": ',
        c.weekday,
        ', "cells": [',
    )
    for i in range(len(c.cells)):
        j.write(
            ", " if i else "",
            '{"key": ',
            quote(c.cells[i].key),
            ', "state": ',
            quote(c.cells[i].state),
            "}",
        )
    j.write('], "counts": {')
    var states = ["done", "failed", "live", "open", "outside"]
    for i in range(len(states)):
        j.write(
            ", " if i else "",
            quote(states[i]),
            ": ",
            c.count(String(states[i])),
        )
    j.write("}}")
    return j^
