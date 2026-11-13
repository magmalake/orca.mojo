"""`State.migrate`'s bookkeeping on Postgres: the advisory lock it takes,
`orca.schema_version` and the floor each version leaves. The migrations'
statements themselves are `morca._internal.migrations`'; the SQLite file's are
`morca._internal.db.sqlite_runs`'."""

from postgres import Params, Transaction


def quiet(mut tx: Transaction) raises:
    """Keeps `tx`'s notices from libpq's output: IF NOT EXISTS on an
    existing object is a NOTICE, which libpq prints."""
    _ = tx.execute("SET LOCAL client_min_messages = warning")


def lock(mut tx: Transaction, key: Int64) raises:
    """Takes the transaction-scoped advisory lock `key`, so two processes
    starting at once don't both apply a migration."""
    _ = tx.execute("SELECT pg_advisory_xact_lock($1)", Params().int64(key))


def ensure(mut tx: Transaction) raises:
    """Creates the `orca` schema and `orca.schema_version`, a row per
    migration applied, with the fewest migrations a binary must know to
    run on the schema it left (`min_binary`, null on the rows of binaries
    before it), if there are none yet."""
    _ = tx.execute("CREATE SCHEMA IF NOT EXISTS orca")
    _ = tx.execute(
        "CREATE TABLE IF NOT EXISTS orca.schema_version (version int NOT NULL)"
    )
    _ = tx.execute(
        "ALTER TABLE orca.schema_version ADD COLUMN IF NOT EXISTS"
        " min_binary int"
    )


def unfloored(mut tx: Transaction, known: Int) raises -> List[Int]:
    """The versions up to `known` applied with no floor recorded."""
    var out = List[Int]()
    for r in tx.query(
        (
            "SELECT version FROM orca.schema_version WHERE min_binary IS"
            " NULL AND version BETWEEN 1 AND $1"
        ),
        Params().int64(Int64(known)),
    ):
        out.append(Int(r.int64("version")))
    return out^


def set_floor(mut tx: Transaction, version: Int, floor: Int) raises:
    """Records `floor` as the floor version `version` leaves."""
    _ = tx.execute(
        "UPDATE orca.schema_version SET min_binary = $2 WHERE version = $1",
        Params().int64(Int64(version)).int64(Int64(floor)),
    )


def current(mut tx: Transaction) raises -> Tuple[Int, Int]:
    """The schema's version (0 for none) and its floor: the most
    migrations a binary must know to run on it, -1 while a version has
    none recorded."""
    var row = tx.query(
        "SELECT coalesce(max(version), 0) AS v, CASE WHEN bool_or("
        "min_binary IS NULL) THEN -1 ELSE coalesce(max(min_binary), 0)"
        " END AS floor FROM orca.schema_version"
    ).row(0)
    return (Int(row.int64("v")), Int(row.int64("floor")))


def applied(mut tx: Transaction, version: Int, floor: Int) raises:
    """Records that `version` was applied, leaving `floor`."""
    _ = tx.execute(
        "INSERT INTO orca.schema_version (version, min_binary) VALUES ($1, $2)",
        Params().int64(Int64(version)).int64(Int64(floor)),
    )
