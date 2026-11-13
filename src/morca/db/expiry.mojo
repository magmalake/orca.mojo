"""Snapshot expiry's queries (`morca.expiry`): which datasets morca
committed to, and which of their snapshots it still needs."""

from postgres import Connection, Params


def retained(mut conn: Connection, dataset: String) raises -> List[Int64]:
    """The snapshots of `dataset` a live run read or committed, and those
    the last run that committed to it did, oldest first."""
    var out = List[Int64]()
    for r in conn.query(
        (
            "WITH live AS (SELECT a.id AS attempt FROM orca.attempts a JOIN"
            " orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id = s.run"
            " WHERE r.status NOT IN ('succeeded', 'cancelled')), last_run AS"
            " (SELECT s.run FROM orca.commits c JOIN orca.attempts a ON a.id ="
            " c.attempt JOIN orca.steps s ON s.id = a.step WHERE c.dataset ="
            " $1 ORDER BY c.attempt DESC LIMIT 1) SELECT DISTINCT snapshot_id"
            " FROM (SELECT rd.snapshot_id FROM orca.reads rd JOIN live ON"
            " live.attempt = rd.attempt WHERE rd.dataset = $1 UNION SELECT"
            " c.snapshot_id FROM orca.commits c JOIN live ON live.attempt ="
            " c.attempt WHERE c.dataset = $1 UNION SELECT c.snapshot_id FROM"
            " orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN"
            " orca.steps s ON s.id = a.step WHERE c.dataset = $1 AND s.run IN"
            " (SELECT run FROM last_run)) x WHERE snapshot_id >= 0 ORDER BY"
            " snapshot_id"
        ),
        Params().text(dataset),
    ):
        out.append(r.int64("snapshot_id"))
    return out^


def committed(mut conn: Connection) raises -> List[String]:
    """Every dataset morca has committed to, by name."""
    var out = List[String]()
    for r in conn.query(
        "SELECT DISTINCT dataset FROM orca.commits WHERE dataset IS NOT"
        " NULL AND dataset <> '' ORDER BY dataset"
    ):
        out.append(r.text("dataset"))
    return out^
