"""Queries on `orca.pipelines`: a pipeline's row, flags and names."""

from postgres import Connection, Params


def exists(mut conn: Connection, pipeline: String) raises -> Bool:
    """Whether `pipeline` has a row."""
    return (
        len(
            conn.query(
                "SELECT 1 FROM orca.pipelines WHERE name = $1",
                Params().text(pipeline),
            )
        )
        > 0
    )


def change_paused(
    mut conn: Connection, pipeline: String, paused: Bool
) raises -> Bool:
    """Sets `paused` if it isn't so already; whether it changed."""
    return (
        len(
            conn.query(
                (
                    "UPDATE orca.pipelines SET paused = $2 WHERE name = $1 AND"
                    " paused <> $2 RETURNING name"
                ),
                Params().text(pipeline).bool(paused),
            )
        )
        == 1
    )


def names(mut conn: Connection) raises -> List[String]:
    """Every pipeline's name, in order."""
    var out = List[String]()
    for r in conn.query(
        "SELECT name FROM orca.pipelines ORDER BY name", Params()
    ):
        out.append(r.text("name"))
    return out^


def unpaused(mut conn: Connection) raises -> List[String]:
    """The names of the pipelines not paused."""
    var out = List[String]()
    for row in conn.query(
        "SELECT name FROM orca.pipelines WHERE NOT paused", Params()
    ):
        out.append(row.text("name"))
    return out^
