"""API tokens: what `morca login` gets from `morca ui`, so the `morca` command
on another machine can act as a user of it (`morca.remote`).

```
morca user tokens alice          her tokens: label, created, last used, expires
morca user revoke alice 3        or @all
```

A token is `orca_` and 64 hex digits, 32 random bytes from the system's
generator; the prefix lets a secret scanner recognise one that leaked. The
UI gives it out once, to `POST /api/login` with the right password, and
keeps only its SHA-256 digest in `orca.api_tokens`, with its user, a label
(the machine that logged in), when it was made and last used, and when it
expires: `MORCA_API_TOKEN_DAYS` (default 30) days on, or fewer if the login
asked for fewer. A request sends it as `Authorization: Bearer <token>`;
one that's unknown, expired or revoked is answered 401. `morca logout`
revokes its own (`POST /api/logout`), `morca user revoke` any of a user's,
and changing a user's password or removing the user revokes all of hers,
as it ends her sessions (`morca.auth`).
"""

from std.os import getenv

from flare.crypto.random import random_bytes
from objectstore.crypto import sha256_hex
import morca.db.auth as auth_db
from morca.db.auth import TokenRow

from morca.state import State

comptime TOKEN_PREFIX = "orca_"  # tokens issued before the rename stay valid
comptime TOKEN_BYTES = 32
"""256 bits: as many as a session's."""
comptime DEFAULT_TOKEN_DAYS = 30
comptime MAX_TOKEN_DAYS = 3650
comptime MAX_LABEL = 64


def _hex(b: List[UInt8]) -> String:
    comptime digits = "0123456789abcdef"
    var out = String()
    for c in b:
        out.write(digits[byte=Int(c >> 4)], digits[byte=Int(c & 15)])
    return out^


def new_token() raises -> String:
    """A new token: `orca_` and `TOKEN_BYTES` random bytes in hex."""
    return String(TOKEN_PREFIX, _hex(random_bytes(TOKEN_BYTES)))


def is_token(s: String) -> Bool:
    """Whether `s` has a token's form, which anything else sent as one is
    refused for before the database is asked."""
    var b = s.as_bytes()
    if len(b) != TOKEN_PREFIX.byte_length() + 2 * TOKEN_BYTES:
        return False
    if not s.startswith(TOKEN_PREFIX):
        return False
    for i in range(TOKEN_PREFIX.byte_length(), len(b)):
        var c = b[i]
        if not (
            (c >= UInt8(ord("0")) and c <= UInt8(ord("9")))
            or (c >= UInt8(ord("a")) and c <= UInt8(ord("f")))
        ):
            return False
    return True


def token_hash(token: String) -> String:
    """What `orca.api_tokens` keeps of a token: its SHA-256, in hex. A token
    is 256 random bits, so a fast digest is enough: there's nothing to
    guess that a slow one would protect."""
    return sha256_hex(token)


def token_days(asked: Int = 0) raises -> Int:
    """How many days a new token lasts: `asked` if it's from 1 to the most
    allowed, `MORCA_API_TOKEN_DAYS` (default 30, at most 3650), else that
    most. Raises for a setting that isn't a count of days."""
    var most: Int
    try:
        most = Int(getenv("MORCA_API_TOKEN_DAYS", String(DEFAULT_TOKEN_DAYS)))
    except:
        raise Error("morca: MORCA_API_TOKEN_DAYS isn't a count of days")
    if most < 1 or most > MAX_TOKEN_DAYS:
        raise Error(
            "morca: MORCA_API_TOKEN_DAYS is 1 to ",
            MAX_TOKEN_DAYS,
            ", not ",
            most,
        )
    if asked >= 1 and asked < most:
        return asked
    return most


def clean_label(label: String) -> String:
    """A token's label as it's kept: printable ASCII, at most `MAX_LABEL`
    bytes, `-` for anything else, and `cli` for none."""
    var out = String()
    for c in label.as_bytes():
        if out.byte_length() >= MAX_LABEL:
            break
        if c >= 0x20 and c < 0x7F:
            out.write(chr(Int(c)))
        else:
            out.write("-")
    var s = String(out.strip())
    return s^ if s else String("cli")


@fieldwise_init
struct Issued(Movable):
    var token: String
    var expires: String
    """When it expires, in UTC (`YYYY-MM-DDTHH:MM:SSZ`)."""


def issue_token(
    mut db: State, user: String, label: String, days: Int
) raises -> Issued:
    """A new token for `user`, which only this answer ever holds."""
    var token = new_token()
    var expires = auth_db.add_token(
        db.conn, token_hash(token), user, clean_label(label), days
    )
    return Issued(token^, expires^)


def token_user(mut db: State, token: String) raises -> String:
    """The user whose live token `token` is, or empty; marks it used."""
    if not is_token(token):
        return ""
    return auth_db.use_token(db.conn, token_hash(token))


def revoke_token(mut db: State, token: String) raises -> Bool:
    """Revokes `token`; False if it wasn't a live one."""
    if not is_token(token):
        return False
    return auth_db.remove_token(db.conn, token_hash(token))


def user_tokens(mut db: State, user: String) raises -> List[TokenRow]:
    """`user`'s tokens, newest first, expired ones too until they're
    removed."""
    return auth_db.user_tokens(db.conn, user)


def revoke_user_tokens(mut db: State, user: String, id: Int) raises -> Int:
    """Revokes `user`'s token `id`, or all of them for 0; how many."""
    return auth_db.remove_user_tokens(db.conn, user, id)
