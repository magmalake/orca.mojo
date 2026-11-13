"""`orca.audit`: a row per action a user, the scheduler or the watchdog
took."""

from postgres import Connection, Params, Transaction


comptime _INSERT = (
    "INSERT INTO orca.audit (username, action, run, detail, source)"
    " VALUES ($1, $2, $3, $4, $5)"
)


def _params(
    user: String, action: String, run: Int, detail: String, source: String
) -> Params:
    var p = Params().text(user).text(action)
    p = p^.null() if run == 0 else p^.int64(Int64(run))
    return p^.text(detail).text(source)


def add(
    mut conn: Connection,
    user: String,
    action: String,
    run: Int,
    detail: String,
    source: String,
) raises:
    """Records that `user` did `action` to `run` (0 for none), with
    `detail`, from `source`."""
    _ = conn.execute(_INSERT, _params(user, action, run, detail, source))


def add(
    mut tx: Transaction,
    user: String,
    action: String,
    run: Int,
    detail: String,
    source: String,
) raises:
    """`add`, in `tx`."""
    _ = tx.execute(_INSERT, _params(user, action, run, detail, source))
