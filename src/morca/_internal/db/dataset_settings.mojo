"""`orca.dataset_settings`, what morca.toml's `[datasets.<name>]` tables
say (`morca._internal.dataset_config`)."""

from postgres import Connection, Params, Transaction


def upsert_dataset(mut tx: Transaction, name: String, public: Bool) raises:
    """Records dataset `name`'s settings as morca.toml has them."""
    _ = tx.execute(
        (
            "INSERT INTO orca.dataset_settings (name, public) VALUES ($1, $2)"
            " ON CONFLICT (name) DO UPDATE SET public = EXCLUDED.public,"
            " updated = now()"
        ),
        Params().text(name).bool(public),
    )


def delete_datasets_but(
    mut tx: Transaction, keep: String
) raises -> List[String]:
    """Deletes every dataset's settings but those named in `keep`, a JSON
    array of names; returns the names deleted."""
    var gone = List[String]()
    for r in tx.query(
        (
            "DELETE FROM orca.dataset_settings WHERE NOT name = ANY (SELECT"
            " jsonb_array_elements_text($1::jsonb)) RETURNING name"
        ),
        Params().text(keep),
    ):
        gone.append(r.text("name"))
    return gone^


def is_public(mut conn: Connection, name: String) raises -> Bool:
    """Whether dataset `name`'s data is public: true unless morca.toml says
    `public = false`."""
    var rows = conn.query(
        (
            "SELECT public FROM orca.dataset_settings WHERE name = $1 AND NOT"
            " public"
        ),
        Params().text(name),
    )
    return len(rows) == 0
