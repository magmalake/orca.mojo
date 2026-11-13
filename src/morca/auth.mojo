"""Who may act in `morca ui`: users with passwords, their sessions, and a
record of what each did.

```
morca user add alice       reads the password from standard input
morca user remove alice    and ends their sessions
morca user list
```

Anyone who can reach the UI can read it. Its actions (start, backfill,
resume, rerun, cancel, rebuild) need a logged-in user once login is on:
when `MORCA_UI_LOGIN=required`, or as soon as any user exists, so a
deployment can't be left open by forgetting the variable once it has users.

Passwords are kept as bcrypt hashes made by Postgres's pgcrypto
(`crypt(password, gen_salt('bf', 12))`), so neither morca nor its logs see
one after it's checked. Logging in makes a session: 32 random bytes from
the system's generator, given to the browser as the cookie `morca_session`
and kept only as their SHA-256 digest, for `MORCA_UI_SESSION_HOURS` (default
12). Every action a user takes is a row of `orca.audit`, and so is every failed
login, which `too_many_failures` counts: after 5 for one name, or 20 from
one address, in 15 minutes, `/login` (and `/api/login`) answers 429
without checking a password, so a public UI can't be guessed at. API
tokens, for the `morca` command on another machine, are `morca.tokens`.
"""

from std.os import getenv

from flare.crypto.random import random_bytes
import morca.db.audit as audit_db
import morca.db.auth as auth_db

from morca.state import State

comptime SESSION_COOKIE = "morca_session"


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
    if getenv("MORCA_UI_LOGIN") == "required":
        return True
    return auth_db.any_user(db.conn)


def add_user(mut db: State, name: String, password: String) raises -> Bool:
    """Adds the user, or sets an existing one's password (ending her
    sessions and revoking her API tokens); True if the user is new."""
    if not _name_ok(name):
        raise Error(
            "morca: a user's name is 1 to 64 of A-Z a-z 0-9 . _ - @, not '",
            name,
            "'",
        )
    var n = password.byte_length()
    # bcrypt reads at most 72 bytes; a longer password would be cut.
    if n < 8 or n > 72:
        raise Error("morca: a password is 8 to 72 bytes")
    return auth_db.set_user(db.conn, name, password)


def remove_user(mut db: State, name: String) raises -> Bool:
    """Removes the user, their sessions and their API tokens; False if
    there was none."""
    return auth_db.remove_user(db.conn, name)


def users(mut db: State) raises -> List[String]:
    return auth_db.users(db.conn)


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
    var n = auth_db.login_failures(
        db.conn, name, address, FAILURE_WINDOW_MINUTES
    )
    return n[0] >= FAILURES_PER_NAME or n[1] >= FAILURES_PER_ADDRESS


def login_failed(
    mut db: State, name: String, address: String, source: String = "ui"
) raises:
    audit(db, name, "login-failed", 0, address, source)


def password_ok(mut db: State, name: String, password: String) raises -> Bool:
    """Whether `password` is the user `name`'s."""
    return auth_db.password_ok(db.conn, name, password)


def open_session(
    mut db: State, name: String, password: String
) raises -> String:
    """A new session's token if the password is the user's, else empty."""
    if not password_ok(db, name, password):
        return ""
    var token = _hex(random_bytes(32))
    var hours = Float64(getenv("MORCA_UI_SESSION_HOURS", "12"))
    auth_db.add_session(db.conn, token, name, hours * 3600.0)
    return token^


def session_user(mut db: State, token: String) raises -> String:
    """The user whose session `token` is, or empty."""
    if not token:
        return ""
    return auth_db.session_user(db.conn, token)


def close_session(mut db: State, token: String) raises:
    auth_db.close_session(db.conn, token)


def audit(
    mut db: State,
    user: String,
    action: String,
    run: Int,
    detail: String,
    source: String = "ui",
) raises:
    """Records that `user` did `action` (the action's path), to `run` (0 for
    none), with `detail` (what the form gave), from `source`: `ui` for a
    browser's session, `cli` for an API token (`morca.tokens`)."""
    audit_db.add(db.conn, user, action, run, detail, source)
