"""One run's dista cluster over several workers of a pool (#268), the UI's
side: the run's head, the members that join it, and when they stop.

- **The head.** A pipeline placed `worker:<pool>` with `workers = N` set
  (its run spreads over up to N more machines) is started by the worker
  that claims it as a cluster of its own, when that worker says the
  address the pool's machines reach it at (`morca worker --advertise`): a
  dista head on a free port, a dista worker, and the run's driver. It
  reports the head's address and the run's cluster token
  (`/api/worker/cluster`, `report_head`), kept on the run
  (`orca.runs.cluster_head`, `cluster_token`) for this start only.
- **Members.** A worker of the pool with room, and an address, that has no
  run to start is given one to join (`join_claim`): a running run of the
  pool whose head is known, with fewer live members than its `workers`,
  that isn't already this worker's. It's sized as a run of the pipeline
  would be on that machine, counted against the machine's room with its
  runs, and kept in `orca.run_members`. The worker starts a dista worker
  against the head, with the token (dista's `DISTA_TOKEN`: nothing without
  it joins), and says when it ends (`/api/worker/left`, `member_left`).
- **Stopping.** Each claim's answer lists this worker's members whose run
  isn't running at that start any more (`members_to_stop`): the worker
  kills them, and they're ended "its run ended".

The token reaches a member only in a claim's answer, over the worker API's
TLS, and is never in a log or a page.
"""

from json import Value
from postgres import Params

from morca.http_store import Body
from morca.registered import registered, run_request
from morca.cgroup import raised_request
from morca.resources import Grant, Room, admit
from morca.state import State

comptime MEMBER_GRANTS_SQL = (
    " + coalesce((SELECT sum(m.grant_cpus) FROM orca.run_members m WHERE"
    " m.worker = $1 AND m.ended IS NULL), 0)"
)
"""Added to a worker's runs' granted CPUs: its live members'."""
comptime MEMBER_MB_SQL = (
    " + coalesce((SELECT sum(m.grant_memory_mb) FROM orca.run_members m"
    " WHERE m.worker = $1 AND m.ended IS NULL), 0)"
)


def members_of(mut db: State, pipeline: String) raises -> Int:
    """How many more machines `pipeline`'s runs may spread over: its
    `workers`, when set for it; 0 (one machine) when not."""
    var rows = db.conn.query(
        (
            "SELECT coalesce(ec2_workers, 0) AS n FROM orca.pipelines WHERE"
            " name = $1"
        ),
        Params().text(pipeline),
    )
    if len(rows) == 0:
        return 0
    return Int(rows.row(0).int64("n"))


def report_head(mut db: State, worker: Int, v: Value) raises -> String:
    """`/api/worker/cluster`: the run's head is at `head`, with `token`; kept
    if the run is still this worker's at that start."""
    var n = db.conn.execute(
        (
            "UPDATE orca.runs SET cluster_head = $4, cluster_token = $5 WHERE"
            " id = $1 AND starts = $2 AND worker = $3 AND status = 'running'"
        ),
        Params()
        .int64(v["run"].int_value())
        .int64(v["start"].int_value())
        .int64(Int64(worker))
        .text(v["head"].string_value())
        .text(v["token"].string_value()),
    )
    return String('{"ok": ', "true" if n == 1 else "false", "}")


def member_left(mut db: State, worker: Int, v: Value) raises -> String:
    """`/api/worker/left`: member `member`'s process ended, `code`."""
    var code = v["code"].int_value() if String("code") in v else Int64(0)
    _ = db.conn.execute(
        (
            "UPDATE orca.run_members SET ended = now(), why = coalesce(why,"
            " $3) WHERE id = $1 AND worker = $2 AND ended IS NULL"
        ),
        Params()
        .int64(v["member"].int_value())
        .int64(Int64(worker))
        .text(String("its process exited ", code)),
    )
    return String('{"ok": true}')


def members_to_stop(mut db: State, worker: Int) raises -> List[Int]:
    """This worker's live members whose run isn't running at their start
    any more, ended "its run ended": the worker kills them."""
    var out = List[Int]()
    for r in db.conn.query(
        (
            "UPDATE orca.run_members m SET ended = now(), why = 'its run ended'"
            " FROM orca.runs r WHERE m.run = r.id AND m.worker = $1 AND"
            " m.ended IS NULL AND (r.status <> 'running' OR r.starts <>"
            " m.start) RETURNING m.id"
        ),
        Params().int64(Int64(worker)),
    ):
        out.append(Int(r.int64("id")))
    return out^


@fieldwise_init
struct MemberSpec(Copyable, Movable):
    """What a worker is given to join: a dista worker for a run's cluster."""

    var member: Int
    var run: Int
    var start: Int
    var pipeline: String
    var binary: String
    var head: String
    """The run's head, `host:port`."""
    var token: String
    """The run's cluster token: dista's `DISTA_TOKEN`."""
    var env: List[Tuple[String, String]]
    var secrets: List[Tuple[String, String]]
    var grant: Grant


def _pairs_json(
    pairs: List[Tuple[String, String]], a: String, b: String
) -> String:
    var out = String("[")
    for i in range(len(pairs)):
        out.write(", " if i else "")
        out.write(Body().text(a, pairs[i][0]).text(b, pairs[i][1]).finish())
    out.write("]")
    return out^


def _pairs_of(
    v: Value, a: String, b: String
) raises -> List[Tuple[String, String]]:
    var out = List[Tuple[String, String]]()
    for p in v.array_items():
        out.append((p[a].string_value(), p[b].string_value()))
    return out^


def member_spec_json(m: MemberSpec) -> String:
    return (
        Body()
        .int("member", Int64(m.member))
        .int("run", Int64(m.run))
        .int("start", Int64(m.start))
        .text("pipeline", m.pipeline)
        .text("binary", m.binary)
        .text("head", m.head)
        .text("token", m.token)
        .raw("env", _pairs_json(m.env, "name", "value"))
        .raw("secrets", _pairs_json(m.secrets, "name", "id"))
        .int("grant_cpus", Int64(m.grant.cpus))
        .int("grant_memory_mb", Int64(m.grant.memory_mb))
        .finish()
    )


def member_spec_of(v: Value) raises -> MemberSpec:
    return MemberSpec(
        Int(v["member"].int_value()),
        Int(v["run"].int_value()),
        Int(v["start"].int_value()),
        v["pipeline"].string_value(),
        v["binary"].string_value(),
        v["head"].string_value(),
        v["token"].string_value(),
        _pairs_of(v["env"], "name", "value"),
        _pairs_of(v["secrets"], "name", "id"),
        Grant(
            Int(v["grant_cpus"].int_value()),
            Int(v["grant_memory_mb"].int_value()),
        ),
    )


def join_claim(
    mut db: State,
    worker: Int,
    placement: String,
    room_cpus: Int,
    room_mb: Int,
    used_cpus: Int,
    used_mb: Int,
    label: String,
) raises -> Optional[MemberSpec]:
    """A run for this worker to join, recorded as its member, or None: the
    oldest running run of the pool with its head known, fewer live members
    than its `workers`, none of them or its head this worker, whose request
    fits what this machine hasn't granted (`used_*`)."""
    var rows = db.conn.query(
        (
            "SELECT r.id, r.starts, r.pipeline, r.cluster_head,"
            " r.cluster_token FROM orca.runs r JOIN orca.pipelines p ON p.name"
            " = r.pipeline WHERE r.status = 'running' AND p.placement = $1 AND"
            " r.cluster_head IS NOT NULL AND r.worker <> $2 AND"
            " coalesce(p.ec2_workers, 0) > (SELECT count(*) FROM"
            " orca.run_members m WHERE m.run = r.id AND m.start = r.starts AND"
            " m.ended IS NULL) AND NOT EXISTS (SELECT 1 FROM orca.run_members m"
            " WHERE m.run = r.id AND m.start = r.starts AND m.worker = $2 AND"
            " m.ended IS NULL) ORDER BY r.id"
        ),
        Params().text(placement).int64(Int64(worker)),
    )
    for c in rows:
        var ps = registered(db, c.text("pipeline"))
        if len(ps) == 0:
            continue
        ref p = ps[0]
        var grant = Grant.none()
        var sized = run_request(db, p, room_cpus)
        var request = sized.request
        if request.memory_min_mb > 0:
            request.memory_min_mb = raised_request(
                request.memory_min_mb, request.memory_max_mb, p.memory_raised_mb
            )
        if request.any() and room_cpus >= 0:
            var fit = admit(
                request,
                Room(room_cpus, room_mb, room_cpus, room_mb),
                used_cpus,
                used_mb,
                1,
                label,
            )
            if len(fit[0]) == 0:
                continue
            grant = fit[0][0]
        var got = db.conn.query(
            (
                "INSERT INTO orca.run_members (run, start, worker, grant_cpus,"
                " grant_memory_mb) SELECT $1, $2, $3, nullif($4, 0), nullif($5,"
                " 0) WHERE NOT EXISTS (SELECT 1 FROM orca.run_members WHERE run"
                " = $1 AND start = $2 AND worker = $3 AND ended IS NULL)"
                " RETURNING id"
            ),
            Params()
            .int64(c.int64("id"))
            .int64(c.int64("starts"))
            .int64(Int64(worker))
            .int64(Int64(grant.cpus))
            .int64(Int64(grant.memory_mb)),
        )
        if len(got) != 1:
            continue
        return MemberSpec(
            Int(got.row(0).int64("id")),
            Int(c.int64("id")),
            Int(c.int64("starts")),
            p.name,
            p.binary,
            c.text("cluster_head"),
            c.text("cluster_token"),
            p.env.copy(),
            p.secrets.copy(),
            grant,
        )
    return None


def stop_json(ids: List[Int]) -> String:
    var out = String("[")
    for i in range(len(ids)):
        out.write(", " if i else "", ids[i])
    out.write("]")
    return out^
