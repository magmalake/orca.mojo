"""Ops › Alerts' queries (`morca._internal.ui.alerts`): a page of `orca.alerts`,
newest first by id, filtered by kind and state, and its counts.
"""

from postgres import Connection, Params


comptime _RESOLVABLE = (
    "(a.event = 'raised' AND (a.kind IN ('watchdog', 'fill-stalled', 'disk',"
    " 'memory-pressure') OR a.resolved_at IS NOT NULL OR EXISTS (SELECT 1"
    " FROM orca.alerts r WHERE r.kind = a.kind AND r.event = 'resolved')))"
)
"""Whether an alert resolves: those of the kinds morca resolves (a watchdog
trip, a stalled fill, the disk, memory pressure), and of a kind that
`morca notify --resolved` has ended once."""

comptime _OPEN = (
    "(a.event = 'raised' AND (a.kind IN ('watchdog', 'fill-stalled', 'disk',"
    " 'memory-pressure') OR EXISTS (SELECT 1 FROM orca.alerts r WHERE r.kind"
    " = a.kind AND r.event = 'resolved')) AND a.resolved_at IS NULL)"
)


@fieldwise_init
struct AlertRow(Copyable, Movable):
    var id: Int
    var kind: String
    var subject: String
    var event: String
    var title: String
    var body: String
    var priority: String
    var url: String
    var pipelines: String
    var run: Int
    var created: String
    var delivery: String
    var attempts: Int
    var sent: String
    var error: String
    var resolvable: Bool
    var resolved: String
    var resolved_by: Int
    var resolved_by_event: String

    def state(self) -> String:
        """open, resolved, repeated (raised again before it resolved), or
        empty for an alert that doesn't resolve and for a 'resolved'."""
        if not self.resolvable:
            return String()
        if not self.resolved:
            return "open"
        if self.resolved_by_event == "raised":
            return "repeated"
        return "resolved"


def _state_sql(state: String) -> String:
    if state == "open":
        return String(" AND ", _OPEN)
    if state == "resolved":
        return " AND (a.resolved_at IS NOT NULL OR a.event = 'resolved')"
    return String()


def _cond(kind: String, state: String) -> String:
    """The page's filters: `kind` (all if empty) as `$1`, and `state`
    (`open`, `resolved`, or empty for any)."""
    var cond = String("WHERE true", _state_sql(state))
    if kind:
        cond.write(" AND a.kind = $1")
    else:
        cond.write(" AND $1 = ''")
    return cond^


def alert_rows(
    mut conn: Connection,
    kind: String,
    state: String,
    before: Int,
    after: Int,
    limit: Int,
) raises -> List[AlertRow]:
    """At most `limit` alerts of `kind` in `state`: with `after` over 0 the
    oldest above it, oldest first; else the newest (under `before`, if it's
    over 0), newest first."""
    comptime ts = '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\''
    var cond = _cond(kind, state)
    var cursor: String
    var order = "DESC"
    if after > 0:
        cursor = " AND a.id > $2"
        order = "ASC"
    elif before > 0:
        cursor = " AND a.id < $2"
    else:
        cursor = " AND $2 = 0"
    var sql = String(
        "SELECT a.id, a.kind, a.subject, a.event, a.title, a.body,",
        " a.priority, a.url, coalesce(a.pipelines, '') AS pipelines,",
        " coalesce(a.run, 0) AS run, to_char(a.created AT TIME ZONE 'UTC', ",
        ts,
        ") AS created, a.delivery, a.attempts, coalesce(to_char(a.sent_at AT",
        " TIME ZONE 'UTC', ",
        ts,
        "), '') AS sent, coalesce(a.error, '') AS error, ",
        _RESOLVABLE,
        " AS resolvable, coalesce(to_char(a.resolved_at AT TIME ZONE 'UTC', ",
        ts,
        "), '') AS resolved, coalesce(a.resolved_by, 0) AS resolved_by,",
        " coalesce(b.event, '') AS resolved_by_event FROM orca.alerts a LEFT",
        " JOIN orca.alerts b ON b.id = a.resolved_by ",
        cond,
        cursor,
        " ORDER BY a.id ",
        order,
        " LIMIT $3",
    )
    var cur = after if after > 0 else before
    var got = List[AlertRow]()
    for r in conn.query(
        sql,
        Params().text(kind).int64(Int64(cur)).int64(Int64(limit)),
    ):
        got.append(
            AlertRow(
                Int(r.int64("id")),
                r.text("kind"),
                r.text("subject"),
                r.text("event"),
                r.text("title"),
                r.text("body"),
                r.text("priority"),
                r.text("url"),
                r.text("pipelines"),
                Int(r.int64("run")),
                r.text("created"),
                r.text("delivery"),
                Int(r.int64("attempts")),
                r.text("sent"),
                r.text("error"),
                r.bool("resolvable"),
                r.text("resolved"),
                Int(r.int64("resolved_by")),
                r.text("resolved_by_event"),
            )
        )
    return got^


def alerts_counted(
    mut conn: Connection, kind: String, state: String, above: Int
) raises -> Tuple[Int, Int]:
    """How many alerts of `kind` in `state` there are, and how many of them
    have an id over `above`."""
    var counts = conn.query(
        String(
            "SELECT count(*) AS total, count(*) FILTER (WHERE a.id > $2) AS",
            " newer FROM orca.alerts a ",
            _cond(kind, state),
        ),
        Params().text(kind).int64(Int64(above)),
    )
    return (
        Int(counts.row(0).int64("total")),
        Int(counts.row(0).int64("newer")),
    )


def open_alerts(mut conn: Connection) raises -> Int:
    """How many alerts are open, whatever their kind."""
    return Int(
        conn.query(
            String("SELECT count(*) AS n FROM orca.alerts a WHERE ", _OPEN),
            Params(),
        )
        .row(0)
        .int64("n")
    )


def alert_kinds(
    mut conn: Connection, state: String
) raises -> List[Tuple[String, Int]]:
    """Each kind's alerts in `state`, by kind."""
    var out = List[Tuple[String, Int]]()
    for r in conn.query(
        String(
            "SELECT a.kind, count(*) AS n FROM orca.alerts a WHERE true",
            _state_sql(state),
            " GROUP BY a.kind ORDER BY a.kind",
        ),
        Params(),
    ):
        out.append((r.text("kind"), Int(r.int64("n"))))
    return out^
