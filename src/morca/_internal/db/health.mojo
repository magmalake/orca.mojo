"""Whether the database answers (`morca ui`'s `/healthz`)."""

from postgres import Connection, Params


def ping(mut conn: Connection) raises:
    """Raises unless the database answers a query."""
    _ = conn.query("SELECT 1 AS ok", Params())
