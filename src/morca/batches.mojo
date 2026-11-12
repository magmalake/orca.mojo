"""Batches of runs (`orca.runs.batch`, #354): the runs one command or pass
queued together, a backfill, a fill pass or a sensor pass, share one id,
their first run's, so the home page shows them as one row and `/runs?batch=`
lists them. A run queued on its own has none.
"""

from postgres import Params, Transaction


def mark_batch(mut tx: Transaction, ids: List[Int64]) raises:
    """Puts the runs `ids`, just queued together in `tx`, in one batch,
    numbered by the first of them; nothing for fewer than two."""
    if len(ids) < 2:
        return
    var first = ids[0]
    var joined = String()
    for i in range(len(ids)):
        if ids[i] < first:
            first = ids[i]
        joined.write("," if i else "", ids[i])
    _ = tx.execute(
        (
            "UPDATE orca.runs SET batch = $1 WHERE id ="
            " ANY(string_to_array($2, ',')::bigint[])"
        ),
        Params().int64(first).text(joined),
    )
