"""Alerts' queries (`morca._internal.notify`): `orca.notifications`, an alert's
state per kind and subject and what is still to send, and `orca.alerts`,
a row per alert raised or resolved, as Ops › Alerts lists them."""

from postgres import Connection, Params


@fieldwise_init
struct Message(Copyable, Movable):
    """What an alert says."""

    var title: String
    var body: String
    var priority: String
    var tags: String
    var click: String
    """The link it opens, or empty."""


@fieldwise_init
struct NotificationState(Copyable, Movable):
    var state: String
    """`firing` or `resolved`."""
    var pending: Bool
    var recent: Bool
    """It was last sent within the window asked for."""


@fieldwise_init
struct Due(Copyable, Movable):
    """A pending alert taken to send."""

    var kind: String
    var subject: String
    var message: Message
    var attempts: Int


@fieldwise_init
struct SentRow(Copyable, Movable):
    var kind: String
    var subject: String
    var state: String
    var title: String
    var priority: String
    var sent: String
    """When it was sent (UTC), or empty."""
    var ago: Int
    """Seconds since, or -1."""
    var pending: Bool
    var error: String


def _msg(p: Params, m: Message) -> Params:
    """`p` with the message's title, body, priority, tags and click."""
    return (
        p.copy()
        .text(m.title)
        .text(m.body)
        .text(m.priority)
        .text(m.tags)
        .text(m.click)
    )


def state_of(
    mut conn: Connection, kind: String, subject: String, window: Float64
) raises -> Optional[NotificationState]:
    """The alert `kind` about `subject`'s state, `recent` if it was sent in
    the last `window` seconds; None if it never fired."""
    var rows = conn.query(
        (
            "SELECT state, pending, (last_sent IS NOT NULL AND last_sent >"
            " now() - make_interval(secs => $3)) AS recent FROM"
            " orca.notifications WHERE kind = $1 AND subject = $2"
        ),
        Params().text(kind).text(subject).float64(window),
    )
    if len(rows) != 1:
        return None
    var r = rows.row(0)
    return NotificationState(
        r.text("state"), r.bool("pending"), r.bool("recent")
    )


def reword_pending(
    mut conn: Connection, kind: String, subject: String, m: Message
) raises:
    """Gives a firing alert not delivered yet, and its pending raise, the
    newest words, on the same schedule."""
    _ = conn.execute(
        (
            "UPDATE orca.notifications SET title = $3, body = $4,"
            " priority = $5, tags = $6, click = $7 WHERE kind = $1"
            " AND subject = $2"
        ),
        _msg(Params().text(kind).text(subject), m),
    )
    _ = conn.execute(
        (
            "UPDATE orca.alerts SET title = $3, body = $4, priority ="
            " $5, tags = $6, url = $7 WHERE id = (SELECT max(id) FROM"
            " orca.alerts WHERE kind = $1 AND subject = $2 AND event ="
            " 'raised' AND delivery = 'pending')"
        ),
        _msg(Params().text(kind).text(subject), m),
    )


def fire(
    mut conn: Connection, kind: String, subject: String, m: Message
) raises:
    """Marks the alert firing and pending, to be sent now."""
    _ = conn.execute(
        (
            "INSERT INTO orca.notifications (kind, subject, state, pending,"
            " title, body, priority, tags, click, attempts, next_try) VALUES"
            " ($1, $2, 'firing', true, $3, $4, $5, $6, $7, 0, now()) ON"
            " CONFLICT (kind, subject) DO UPDATE SET state = 'firing',"
            " pending = true, title = EXCLUDED.title, body = EXCLUDED.body,"
            " priority = EXCLUDED.priority, tags = EXCLUDED.tags, click ="
            " EXCLUDED.click, attempts = 0, next_try = now()"
        ),
        _msg(Params().text(kind).text(subject), m),
    )


def resolve_older_raises(
    mut conn: Connection, kind: String, subject: String, by: Int64
) raises:
    """Has the open raises of the alert before `by` resolved by it."""
    _ = conn.execute(
        (
            "UPDATE orca.alerts SET resolved_at = now(), resolved_by = $3"
            " WHERE kind = $1 AND subject = $2 AND event = 'raised' AND"
            " resolved_at IS NULL AND id < $3"
        ),
        Params().text(kind).text(subject).int64(by),
    )


def drop_pending(
    mut conn: Connection, kind: String, subject: String, why: String
) raises:
    """Drops the alert's rows of `orca.alerts` not sent yet, saying `why`."""
    _ = conn.execute(
        (
            "UPDATE orca.alerts SET delivery = 'dropped', error = $3"
            " WHERE kind = $1 AND subject = $2 AND delivery = 'pending'"
        ),
        Params().text(kind).text(subject).text(why),
    )


def add_alert(
    mut conn: Connection,
    kind: String,
    subject: String,
    event: String,
    m: Message,
    pipelines: String,
    run: Int,
) raises -> Int64:
    """A row of `orca.alerts` for the alert raised or resolved (`event`),
    about `pipelines` (comma-separated, or empty) and `run` (0: none);
    returns its id."""
    var p = _msg(Params().text(kind).text(subject).text(event), m)
    var rows = conn.query(
        (
            "INSERT INTO orca.alerts (kind, subject, event, title, body,"
            " priority, tags, url, pipelines, run) VALUES ($1, $2, $3, $4, $5,"
            " $6, $7, $8, nullif($9, ''), nullif($10, 0)) RETURNING id"
        ),
        p^.text(pipelines).int64(Int64(run)),
    )
    return rows.row(0).int64("id")


def is_firing(
    mut conn: Connection, kind: String, subject: String
) raises -> Bool:
    return (
        len(
            conn.query(
                (
                    "SELECT 1 FROM orca.notifications WHERE kind = $1 AND"
                    " subject = $2 AND state = 'firing'"
                ),
                Params().text(kind).text(subject),
            )
        )
        > 0
    )


def firing_subjects(mut conn: Connection, kind: String) raises -> List[String]:
    var out = List[String]()
    for r in conn.query(
        (
            "SELECT subject FROM orca.notifications WHERE kind = $1 AND state"
            " = 'firing' ORDER BY subject"
        ),
        Params().text(kind),
    ):
        out.append(r.text("subject"))
    return out^


def mark_resolved(
    mut conn: Connection,
    kind: String,
    subject: String,
    title: String,
    body: String,
    click: String,
) raises -> Optional[Bool]:
    """Resolves the firing alert, to be sent if it ever was; returns
    whether it is pending, or None if it wasn't firing."""
    var rows = conn.query(
        (
            "UPDATE orca.notifications SET state = 'resolved', pending ="
            " (last_sent IS NOT NULL), title = $3, body = $4, priority ="
            " 'low', tags = 'white_check_mark', click = $5, attempts = 0,"
            " next_try = now() WHERE kind = $1 AND subject = $2 AND state ="
            " 'firing' RETURNING pending"
        ),
        Params().text(kind).text(subject).text(title).text(body).text(click),
    )
    if len(rows) != 1:
        return None
    return rows.row(0).bool("pending")


def resolve_raises(
    mut conn: Connection, kind: String, subject: String, by: Int64
) raises:
    """Has the alert's open raises resolved, by row `by` (0: none)."""
    _ = conn.execute(
        (
            "UPDATE orca.alerts SET resolved_at = now(), resolved_by ="
            " nullif($3, 0) WHERE kind = $1 AND subject = $2 AND event ="
            " 'raised' AND resolved_at IS NULL"
        ),
        Params().text(kind).text(subject).int64(by),
    )


def take_due(mut conn: Connection) raises -> List[Due]:
    """Up to 5 pending alerts that are due, put off a minute while they're
    sent (`FOR UPDATE SKIP LOCKED`, so two flushes don't send one)."""
    var out = List[Due]()
    for r in conn.query(
        (
            "UPDATE orca.notifications n SET next_try = now() + interval '60"
            " seconds' WHERE (kind, subject) IN (SELECT kind, subject FROM"
            " orca.notifications WHERE pending AND next_try <= now() ORDER BY"
            " next_try LIMIT 5 FOR UPDATE SKIP LOCKED) RETURNING kind, subject,"
            " title, body, priority, tags, click, attempts"
        ),
        Params(),
    ):
        out.append(
            Due(
                r.text("kind"),
                r.text("subject"),
                Message(
                    r.text("title"),
                    r.text("body"),
                    r.text("priority"),
                    r.text("tags"),
                    r.text("click"),
                ),
                Int(r.int64("attempts")),
            )
        )
    return out^


def sent(mut conn: Connection, kind: String, subject: String) raises:
    """Records that the alert was delivered."""
    _ = conn.execute(
        (
            "UPDATE orca.notifications SET pending = false, attempts ="
            " 0, last_error = NULL, sent_at = now(), last_sent = CASE"
            " WHEN state = 'firing' THEN now() ELSE last_sent END"
            " WHERE kind = $1 AND subject = $2"
        ),
        Params().text(kind).text(subject),
    )
    _ = conn.execute(
        (
            "UPDATE orca.alerts SET delivery = 'sent', sent_at ="
            " now(), attempts = attempts + 1, error = NULL WHERE id ="
            " (SELECT max(id) FROM orca.alerts WHERE kind = $1 AND"
            " subject = $2 AND delivery = 'pending')"
        ),
        Params().text(kind).text(subject),
    )


def not_sent(
    mut conn: Connection,
    kind: String,
    subject: String,
    attempts: Int,
    failure: String,
    wait: Int,
) raises:
    """Records a failed delivery, the alert's `attempts`th, to be tried
    again in `wait` seconds."""
    _ = conn.execute(
        (
            "UPDATE orca.notifications SET attempts = $3, last_error = $4,"
            " next_try = now() + make_interval(secs => $5) WHERE kind = $1"
            " AND subject = $2"
        ),
        Params()
        .text(kind)
        .text(subject)
        .int64(Int64(attempts))
        .text(failure)
        .float64(Float64(wait)),
    )
    _ = conn.execute(
        (
            "UPDATE orca.alerts SET attempts = attempts + 1, error = $3"
            " WHERE id = (SELECT max(id) FROM orca.alerts WHERE kind = $1"
            " AND subject = $2 AND delivery = 'pending')"
        ),
        Params().text(kind).text(subject).text(failure),
    )


def prune(mut conn: Connection) raises:
    """Forgets delivered alerts after 30 days, and rows of `orca.alerts`
    after 90."""
    _ = conn.execute(
        (
            "DELETE FROM orca.notifications WHERE NOT pending AND coalesce("
            "sent_at, last_sent) < now() - interval '30 days'"
        ),
        Params(),
    )
    _ = conn.execute(
        "DELETE FROM orca.alerts WHERE created < now() - interval '90 days'",
        Params(),
    )


def recent(mut conn: Connection, limit: Int) raises -> List[SentRow]:
    """The alerts last sent or waiting to be, newest first."""
    comptime ts = '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\''
    var out = List[SentRow]()
    for r in conn.query(
        String(
            "SELECT kind, subject, state, title, priority, pending,",
            " coalesce(last_error, '') AS error, coalesce(to_char(sent_at AT",
            " TIME ZONE 'UTC', ",
            ts,
            "), '') AS sent, coalesce(extract(epoch FROM now() -",
            " sent_at)::bigint, -1) AS ago FROM orca.notifications WHERE",
            " sent_at IS NOT NULL OR pending ORDER BY coalesce(sent_at,",
            " next_try) DESC LIMIT $1",
        ),
        Params().int64(Int64(limit)),
    ):
        out.append(
            SentRow(
                r.text("kind"),
                r.text("subject"),
                r.text("state"),
                r.text("title"),
                r.text("priority"),
                r.text("sent"),
                Int(r.int64("ago")),
                r.bool("pending"),
                r.text("error"),
            )
        )
    return out^


def is_known(
    mut conn: Connection, kind: String, subject: String
) raises -> Bool:
    """Whether the alert `kind` about `subject` ever fired."""
    return (
        len(
            conn.query(
                (
                    "SELECT 1 FROM orca.notifications WHERE kind = $1 AND"
                    " subject = $2"
                ),
                Params().text(kind).text(subject),
            )
        )
        > 0
    )


@fieldwise_init
struct FailedRun(Copyable, Movable):
    var id: Int64
    var starts: Int64
    var partitions: String
    var error: String
    """The run's error, else its last attempt's, or empty."""


def failed_unretried(
    mut conn: Connection, pipeline: String, retries: Int, retried_sql: String
) raises -> List[FailedRun]:
    """Up to 20 of `pipeline`'s runs that failed in the last hour with no
    retry left: past `retries` starts, of a trigger the scheduler doesn't
    retry (not `retried_sql`), or not started for their code."""
    var out = List[FailedRun]()
    for r in conn.query(
        String(
            "SELECT r.id, r.starts, r.partitions, coalesce(r.error, (SELECT",
            " a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step",
            " WHERE s.run = r.id AND a.error IS NOT NULL ORDER BY a.id DESC",
            " LIMIT 1), '') AS error FROM orca.runs r WHERE r.pipeline = $1",
            " AND r.status = 'failed' AND r.retry_at IS NULL AND r.ended >",
            " now() - interval '1 hour' AND (NOT (",
            retried_sql,
            ") OR r.starts - r.deferrals > $2 OR r.error LIKE 'not started:",
            " its code%') ORDER BY r.id LIMIT 20",
        ),
        Params().text(pipeline).int64(Int64(retries)),
    ):
        out.append(
            FailedRun(
                r.int64("id"),
                r.int64("starts"),
                r.text("partitions"),
                r.text("error"),
            )
        )
    return out^


def fill_success_age(
    mut conn: Connection, pipeline: String
) raises -> Optional[Tuple[Int, Bool]]:
    """Seconds since `pipeline`'s last fill run succeeded (else since its
    first fill run, else since it was first registered), and whether one
    ever did."""
    var rows = conn.query(
        (
            "SELECT extract(epoch FROM now() - coalesce((SELECT max(ended)"
            " FROM orca.runs WHERE pipeline = $1 AND trigger = 'fill' AND"
            " status = 'succeeded'), (SELECT min(created) FROM orca.runs"
            " WHERE pipeline = $1 AND trigger = 'fill'), (SELECT first_seen"
            " FROM orca.pipelines WHERE name = $1)))::bigint AS ago, EXISTS"
            " (SELECT 1 FROM orca.runs WHERE pipeline = $1 AND trigger ="
            " 'fill' AND status = 'succeeded') AS ever"
        ),
        Params().text(pipeline),
    )
    if len(rows) != 1:
        return None
    return (Int(rows.row(0).int64("ago")), rows.row(0).bool("ever"))


@fieldwise_init
struct QuietWorker(Copyable, Movable):
    var id: Int64
    var pool: String
    var label: String
    var name: String
    var running: String
    var seen: Int64
    """When it was last seen (Unix time)."""
    var ago: Int


def quiet_workers(
    mut conn: Connection, quiet: Float64
) raises -> List[QuietWorker]:
    """The live workers that last said they ran runs, quiet for over
    `quiet` seconds but less than a day."""
    var out = List[QuietWorker]()
    for r in conn.query(
        (
            "SELECT id, pool, label, coalesce(name, '') AS name, running,"
            " extract(epoch FROM last_seen)::bigint AS seen,"
            " extract(epoch FROM now() - last_seen)::bigint AS ago FROM"
            " orca.workers WHERE revoked IS NULL AND expires > now() AND"
            " last_seen IS NOT NULL AND last_seen < now() -"
            " make_interval(secs => $1) AND last_seen > now() - interval '1"
            " day' AND running IS NOT NULL ORDER BY id"
        ),
        Params().float64(quiet),
    ):
        out.append(
            QuietWorker(
                r.int64("id"),
                r.text("pool"),
                r.text("label"),
                r.text("name"),
                r.text("running"),
                r.int64("seen"),
                Int(r.int64("ago")),
            )
        )
    return out^


def is_pending(
    mut conn: Connection, kind: String, subject: String
) raises -> Bool:
    """Whether the alert is still waiting to be delivered."""
    return (
        len(
            conn.query(
                (
                    "SELECT 1 FROM orca.notifications WHERE kind = $1 AND"
                    " subject = $2 AND pending"
                ),
                Params().text(kind).text(subject),
            )
        )
        > 0
    )
