"""A run's dista cluster over several workers of its pool (#268,
`morca._internal.run_cluster`), as its page and JSON show it: the worker that started
it, holding its head, then each worker that joined it, with their grants
and when and why each ended (`morca._internal.db.run_view.cluster_rows`).
"""

import morca._internal.db.run_view as run_db
from morca._internal.db.run_view import ClusterRow

from morca._internal.state import State


def cluster_rows(mut db: State, run: Int) raises -> List[ClusterRow]:
    """Run `run`'s cluster: its head at its current start, if it spread,
    then every member it had, each start's, in the order they joined. Empty
    for a run on one machine."""
    return run_db.cluster_rows(db.conn, run)
