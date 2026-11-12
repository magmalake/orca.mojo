"""A run's dista cluster over several workers of its pool (#268,
`morca.run_cluster`), as its page and JSON show it: the worker that started
it, holding its head, then each worker that joined it, with their grants
and when and why each ended.
"""

from postgres import Params

from morca.state import State

comptime _TS = '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\''


@fieldwise_init
struct ClusterRow(Copyable, Movable):
    var role: String
    """`head` (the worker that started the run) or `member`."""
    var start: Int
    var worker: String
    """The worker's name, else its token's label."""
    var address: String
    """The head's `host:port`; empty for a member."""
    var grant_cpus: Int
    var grant_memory_mb: Int
    var joined: String
    var ended: String
    var why: String


def cluster_rows(mut db: State, run: Int) raises -> List[ClusterRow]:
    """Run `run`'s cluster: its head at its current start, if it spread,
    then every member it had, each start's, in the order they joined. Empty
    for a run on one machine."""
    var out = List[ClusterRow]()
    var head = db.conn.query(
        String(
            "SELECT r.starts, coalesce(w.name, w.label, '') AS worker,",
            " r.cluster_head, coalesce(r.grant_cpus, 0) AS cpus,",
            " coalesce(r.grant_memory_mb, 0) AS mb, coalesce(to_char(r.started",
            " AT TIME ZONE 'UTC', ",
            _TS,
            "), '') AS joined, coalesce(to_char(r.ended AT TIME ZONE 'UTC', ",
            _TS,
            "), '') AS ended FROM orca.runs r LEFT JOIN orca.workers w ON w.id",
            " = r.worker WHERE r.id = $1 AND r.cluster_head IS NOT NULL",
        ),
        Params().int64(Int64(run)),
    )
    for r in head:
        out.append(
            ClusterRow(
                "head",
                Int(r.int64("starts")),
                r.text("worker"),
                r.text("cluster_head"),
                Int(r.int64("cpus")),
                Int(r.int64("mb")),
                r.text("joined"),
                r.text("ended"),
                "",
            )
        )
    for r in db.conn.query(
        String(
            "SELECT m.start, coalesce(w.name, w.label, '') AS worker,",
            " coalesce(m.grant_cpus, 0) AS cpus, coalesce(m.grant_memory_mb,",
            " 0) AS mb, to_char(m.joined AT TIME ZONE 'UTC', ",
            _TS,
            ") AS joined, coalesce(to_char(m.ended AT TIME ZONE 'UTC', ",
            _TS,
            "), '') AS ended, coalesce(m.why, '') AS why FROM",
            " orca.run_members m LEFT JOIN orca.workers w ON w.id = m.worker",
            " WHERE m.run = $1 ORDER BY m.id",
        ),
        Params().int64(Int64(run)),
    ):
        out.append(
            ClusterRow(
                "member",
                Int(r.int64("start")),
                r.text("worker"),
                "",
                Int(r.int64("cpus")),
                Int(r.int64("mb")),
                r.text("joined"),
                r.text("ended"),
                r.text("why"),
            )
        )
    return out^
