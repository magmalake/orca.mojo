"""Who may use morca ui and its API (`morca.auth`, `morca.tokens`):
`orca.users`, `orca.sessions`, `orca.api_tokens`, and the failed logins
`orca.audit` keeps."""

from postgres import Connection, Params


def any_user(mut conn: Connection) raises -> Bool:
    """Whether there is a user."""
    return (
        conn.query("SELECT EXISTS (SELECT 1 FROM orca.users) AS any", Params())
        .row(0)
        .bool("any")
    )


def set_user(
    mut conn: Connection, name: String, password: String
) raises -> Bool:
    """Adds the user, or sets an existing one's password, ending her
    sessions and revoking her API tokens, in one transaction; True if the
    user is new."""
    var tx = conn.begin()
    var new = (
        tx.query(
            (
                "INSERT INTO orca.users (name, password_hash) VALUES ($1,"
                " crypt($2, gen_salt('bf', 12))) ON CONFLICT (name) DO UPDATE"
                " SET password_hash = EXCLUDED.password_hash RETURNING (xmax ="
                " 0) AS new"
            ),
            Params().text(name).text(password),
        )
        .row(0)
        .bool("new")
    )
    if not new:
        _ = tx.execute(
            "DELETE FROM orca.sessions WHERE username = $1", Params().text(name)
        )
        _ = tx.execute(
            "DELETE FROM orca.api_tokens WHERE username = $1",
            Params().text(name),
        )
    tx.commit()
    return new


def remove_user(mut conn: Connection, name: String) raises -> Bool:
    """Removes the user (and so their sessions and tokens); False if there
    was none."""
    return (
        conn.execute(
            "DELETE FROM orca.users WHERE name = $1", Params().text(name)
        )
        == 1
    )


def users(mut conn: Connection) raises -> List[String]:
    var out = List[String]()
    for r in conn.query("SELECT name FROM orca.users ORDER BY name", Params()):
        out.append(r.text("name"))
    return out^


def login_failures(
    mut conn: Connection, name: String, address: String, minutes: Int
) raises -> Tuple[Int, Int]:
    """The logins that failed in the last `minutes`: for `name`, and from
    `address` (`login-failed` rows of `orca.audit`, the address their
    detail)."""
    var n = conn.query(
        (
            "SELECT count(*) FILTER (WHERE username = $1) AS by_name,"
            " count(*) FILTER (WHERE detail = $2) AS by_address FROM"
            " orca.audit WHERE action = 'login-failed' AND at > now() -"
            " make_interval(mins => $3)"
        ),
        Params().text(name).text(address).int32(Int32(minutes)),
    ).row(0)
    return (Int(n.int64("by_name")), Int(n.int64("by_address")))


def password_ok(
    mut conn: Connection, name: String, password: String
) raises -> Bool:
    """Whether `password` is the user `name`'s."""
    var ok = conn.query(
        (
            "SELECT password_hash = crypt($2, password_hash) AS ok FROM"
            " orca.users WHERE name = $1"
        ),
        Params().text(name).text(password),
    )
    return len(ok) == 1 and ok.row(0).bool("ok")


def add_session(
    mut conn: Connection, token: String, name: String, seconds: Float64
) raises:
    """Records session `token` of `name` for `seconds`, and removes the
    expired ones."""
    _ = conn.execute(
        (
            "INSERT INTO orca.sessions (token_hash, username, expires) VALUES"
            " (digest($1, 'sha256'), $2, now() + make_interval(secs => $3))"
        ),
        Params().text(token).text(name).float64(seconds),
    )
    # Expired sessions go as new ones come.
    _ = conn.execute(
        "DELETE FROM orca.sessions WHERE expires < now()", Params()
    )


def session_user(mut conn: Connection, token: String) raises -> String:
    """The user whose live session `token` is, or empty."""
    var rows = conn.query(
        (
            "SELECT username FROM orca.sessions WHERE token_hash = digest($1,"
            " 'sha256') AND expires > now()"
        ),
        Params().text(token),
    )
    return rows.row(0).text("username") if len(rows) == 1 else String()


def close_session(mut conn: Connection, token: String) raises:
    _ = conn.execute(
        "DELETE FROM orca.sessions WHERE token_hash = digest($1, 'sha256')",
        Params().text(token),
    )


comptime _TS = '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\''
"""`to_char`'s format for a UTC time, `YYYY-MM-DDTHH:MM:SSZ`."""


def add_token(
    mut conn: Connection, hash: String, user: String, label: String, days: Int
) raises -> String:
    """Records the API token whose hash is `hash`, of `user`, for `days`,
    and removes the expired ones; returns when it expires (UTC)."""
    var expires = (
        conn.query(
            (
                "INSERT INTO orca.api_tokens (token_hash, username, label,"
                " expires) VALUES ($1, $2, $3, now() + make_interval(days =>"
                " $4)) RETURNING to_char(expires AT TIME ZONE 'UTC',"
                ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS expires'
            ),
            Params().text(hash).text(user).text(label).int32(Int32(days)),
        )
        .row(0)
        .text("expires")
    )
    # Expired tokens go as new ones come, as sessions do.
    _ = conn.execute(
        "DELETE FROM orca.api_tokens WHERE expires < now()", Params()
    )
    return expires^


def use_token(mut conn: Connection, hash: String) raises -> String:
    """The user whose live token has hash `hash`, or empty; marks it
    used."""
    var rows = conn.query(
        (
            "UPDATE orca.api_tokens SET last_used = now() WHERE token_hash = $1"
            " AND expires > now() RETURNING username"
        ),
        Params().text(hash),
    )
    return rows.row(0).text("username") if len(rows) == 1 else String()


def remove_token(mut conn: Connection, hash: String) raises -> Bool:
    """Removes the token whose hash is `hash`; False if there was none."""
    return (
        conn.execute(
            "DELETE FROM orca.api_tokens WHERE token_hash = $1",
            Params().text(hash),
        )
        == 1
    )


@fieldwise_init
struct TokenRow(Copyable, Movable):
    var id: Int
    var label: String
    var created: String
    var last_used: String
    """Empty if it hasn't been."""
    var expires: String
    var expired: Bool


def user_tokens(mut conn: Connection, user: String) raises -> List[TokenRow]:
    """`user`'s tokens, newest first, expired ones too until they're
    removed."""
    var out = List[TokenRow]()
    for r in conn.query(
        String(
            "SELECT id, label, to_char(created AT TIME ZONE 'UTC', ",
            _TS,
            ") AS created, coalesce(to_char(last_used AT TIME ZONE 'UTC', ",
            _TS,
            "), '') AS last_used, to_char(expires AT TIME ZONE 'UTC', ",
            _TS,
            ") AS expires, expires < now() AS expired FROM orca.api_tokens",
            " WHERE username = $1 ORDER BY id DESC",
        ),
        Params().text(user),
    ):
        out.append(
            TokenRow(
                Int(r.int64("id")),
                r.text("label"),
                r.text("created"),
                r.text("last_used"),
                r.text("expires"),
                r.bool("expired"),
            )
        )
    return out^


def remove_user_tokens(
    mut conn: Connection, user: String, id: Int
) raises -> Int:
    """Removes `user`'s token `id`, or all of them for 0; how many."""
    return conn.execute(
        (
            "DELETE FROM orca.api_tokens WHERE username = $1 AND ($2 = 0 OR"
            " id = $2)"
        ),
        Params().text(user).int64(Int64(id)),
    )
