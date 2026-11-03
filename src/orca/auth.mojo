"""Who may act in `orca ui`: users with passwords, their sessions, and a
record of what each did.

```
orca user add alice       reads the password from standard input
orca user remove alice    and ends their sessions
orca user list
```

Anyone who can reach the UI can read it. Its actions (start, backfill,
resume, rerun, cancel, rebuild) need a logged-in user once login is on:
when `ORCA_UI_LOGIN=required`, or as soon as any user exists, so a
deployment can't be left open by forgetting the variable once it has users.

Passwords are kept as bcrypt hashes made by Postgres's pgcrypto
(`crypt(password, gen_salt('bf', 12))`), so neither orca nor its logs see
one after it's checked. Logging in makes a session: 32 random bytes from
the system's generator, given to the browser as the cookie `orca_session`
and kept only as their SHA-256 digest, for `ORCA_UI_SESSION_HOURS` (default
12). Every action a user takes is a row of `orca.audit`, and so is every failed
login, which `too_many_failures` counts: after 5 for one name, or 20 from
one address, in 15 minutes, `/login` answers 429 without checking a
password, so a public UI can't be guessed at.
"""

from std.os import getenv

from flare.crypto.random import random_bytes
from postgres import Params

from orca.state import State

comptime SESSION_COOKIE = "orca_session"


def _name_ok(name: String) -> Bool:
    var b = name.as_bytes()
    if len(b) == 0 or len(b) > 64:
        return False
    for c in b:
        if not (
            (c >= UInt8(ord("a")) and c <= UInt8(ord("z")))
            or (c >= UInt8(ord("A")) and c <= UInt8(ord("Z")))
            or (c >= UInt8(ord("0")) and c <= UInt8(ord("9")))
            or c == UInt8(ord("."))
            or c == UInt8(ord("_"))
            or c == UInt8(ord("-"))
            or c == UInt8(ord("@"))
        ):
            return False
    return True


def login_required(mut db: State) raises -> Bool:
    if getenv("ORCA_UI_LOGIN") == "required":
        return True
    return (
        db.conn.query(
            "SELECT EXISTS (SELECT 1 FROM orca.users) AS any", Params()
        )
        .row(0)
        .bool("any")
    )


def add_user(mut db: State, name: String, password: String) raises -> Bool:
    """Adds the user, or sets an existing one's password (ending her
    sessions); True if the user is new."""
    if not _name_ok(name):
        raise Error(
            "orca: a user's name is 1 to 64 of A-Z a-z 0-9 . _ - @, not '",
            name,
            "'",
        )
    var n = password.byte_length()
    # bcrypt reads at most 72 bytes; a longer password would be cut.
    if n < 8 or n > 72:
        raise Error("orca: a password is 8 to 72 bytes")
    var tx = db.conn.begin()
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
    tx.commit()
    return new


def remove_user(mut db: State, name: String) raises -> Bool:
    """Removes the user and their sessions; False if there was none."""
    return (
        db.conn.execute(
            "DELETE FROM orca.users WHERE name = $1", Params().text(name)
        )
        == 1
    )


def users(mut db: State) raises -> List[String]:
    var out = List[String]()
    for r in db.conn.query(
        "SELECT name FROM orca.users ORDER BY name", Params()
    ):
        out.append(r.text("name"))
    return out^


def _hex(b: List[UInt8]) -> String:
    comptime digits = "0123456789abcdef"
    var out = String()
    for c in b:
        out.write(digits[byte=Int(c >> 4)], digits[byte=Int(c & 15)])
    return out^


comptime FAILURES_PER_NAME = 5
comptime FAILURES_PER_ADDRESS = 20
comptime FAILURE_WINDOW_MINUTES = 15


def too_many_failures(
    mut db: State, name: String, address: String
) raises -> Bool:
    """Whether logins for `name`, or from `address`, have failed too often
    of late to check another: `FAILURES_PER_NAME` for a name or
    `FAILURES_PER_ADDRESS` from an address in `FAILURE_WINDOW_MINUTES`.
    Failures are `login-failed` rows of `orca.audit`, with the address as
    their detail."""
    var n = db.conn.query(
        (
            "SELECT count(*) FILTER (WHERE username = $1) AS by_name,"
            " count(*) FILTER (WHERE detail = $2) AS by_address FROM"
            " orca.audit WHERE action = 'login-failed' AND at > now() -"
            " make_interval(mins => $3)"
        ),
        Params().text(name).text(address).int32(Int32(FAILURE_WINDOW_MINUTES)),
    ).row(0)
    return (
        n.int64("by_name") >= FAILURES_PER_NAME
        or n.int64("by_address") >= FAILURES_PER_ADDRESS
    )


def login_failed(mut db: State, name: String, address: String) raises:
    audit(db, name, "login-failed", 0, address)


def open_session(
    mut db: State, name: String, password: String
) raises -> String:
    """A new session's token if the password is the user's, else empty."""
    var ok = db.conn.query(
        (
            "SELECT password_hash = crypt($2, password_hash) AS ok FROM"
            " orca.users WHERE name = $1"
        ),
        Params().text(name).text(password),
    )
    if len(ok) == 0 or not ok.row(0).bool("ok"):
        return ""
    var token = _hex(random_bytes(32))
    var hours = Float64(getenv("ORCA_UI_SESSION_HOURS", "12"))
    _ = db.conn.execute(
        (
            "INSERT INTO orca.sessions (token_hash, username, expires) VALUES"
            " (digest($1, 'sha256'), $2, now() + make_interval(secs => $3))"
        ),
        Params().text(token).text(name).float64(hours * 3600.0),
    )
    # Expired sessions go as new ones come.
    _ = db.conn.execute(
        "DELETE FROM orca.sessions WHERE expires < now()", Params()
    )
    return token^


def session_user(mut db: State, token: String) raises -> String:
    """The user whose session `token` is, or empty."""
    if not token:
        return ""
    var rows = db.conn.query(
        (
            "SELECT username FROM orca.sessions WHERE token_hash = digest($1,"
            " 'sha256') AND expires > now()"
        ),
        Params().text(token),
    )
    return rows.row(0).text("username") if len(rows) == 1 else String()


def close_session(mut db: State, token: String) raises:
    _ = db.conn.execute(
        "DELETE FROM orca.sessions WHERE token_hash = digest($1, 'sha256')",
        Params().text(token),
    )


def audit(
    mut db: State, user: String, action: String, run: Int, detail: String
) raises:
    """Records that `user` did `action` (the action's path), to `run` (0 for
    none), with `detail` (what the form gave)."""
    var p = Params().text(user).text(action)
    p = p^.null() if run == 0 else p^.int64(Int64(run))
    _ = db.conn.execute(
        (
            "INSERT INTO orca.audit (username, action, run, detail) VALUES ($1,"
            " $2, $3, $4)"
        ),
        p^.text(detail),
    )
