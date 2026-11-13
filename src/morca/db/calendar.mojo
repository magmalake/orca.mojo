"""The calendar's queries (`morca.calendar`): which keys of a pipeline a
live run has, and how each key's last run ended."""

from postgres import Connection, Params

from morca.db.live import live_sql

comptime _LAST_SQL = (
    "SELECT DISTINCT ON (k.key) k.key, r.status FROM orca.runs r,"
    " unnest(string_to_array(r.partitions, ',')) AS k(key) WHERE r.pipeline ="
    " $1 AND k.key = ANY (string_to_array($2, ',')) ORDER BY k.key, r.id DESC"
)
"""Of the comma-separated keys `$2`, the status of the last run of pipeline
`$1` that had each (`key`, `status`)."""


def live_keys(mut conn: Connection, pipeline: String) raises -> List[String]:
    """The keys of `pipeline`'s live runs (`live_sql`)."""
    var out = List[String]()
    for r in conn.query(live_sql(), Params().text(pipeline)):
        out.append(r.text("key"))
    return out^


def failed_keys(
    mut conn: Connection, pipeline: String, keys: String
) raises -> List[String]:
    """Of the comma-separated `keys`, those whose last run of `pipeline`
    failed."""
    var out = List[String]()
    for r in conn.query(_LAST_SQL, Params().text(pipeline).text(keys)):
        if r.text("status") == "failed":
            out.append(r.text("key"))
    return out^
