"""Ops' Pools (`/ops/pools`): every machine runs are placed on, one row
each, by the pool a placement names.

| Pool | Kind | A machine is |
|---|---|---|
| `ec2` | EC2 | an instance orca launched for a run (`orca.instances`), live or ended in the last hour |
| `kubernetes` | k8s | a live run's pods (`orca-<run>-<start>`), on the Kubernetes runner: the runner records no pods or nodes, so the run is what there is |
| `worker:<pool>` | self-hosted | the machine a worker token's `orca worker` runs on (`orca.workers`), with the worker as columns of its row |

The scheduler's own machine (the local runner, and control-placed runs)
isn't a pool: it is Control (`/ops/control`).

A machine's state is `live`, `draining` (an EC2 worker the scheduler
drained) or `gone` (an instance that ended; a worker whose token was
revoked or expired, or that hasn't been seen for `ORCA_STALE_SECONDS`),
with what it's doing in `detail`. Like Fleet, a page view reads
`orca.instances`, `orca.fleet_samples`, `orca.workers` and `orca.runs`,
never a sampler's file.
"""

from std.os import getenv

from postgres import Params

from orca.live import dead_sql, stale_limit, start_grace
from orca.state import State


comptime EC2 = "ec2"
"""The kind (and the pool) of the EC2 instances orca launches."""
comptime K8S = "k8s"
"""The kind of a Kubernetes run's pods; their pool is `kubernetes`."""
comptime SELF_HOSTED = "self-hosted"
"""The kind of a machine running `orca worker`; its pool `worker:<pool>`."""
comptime KUBERNETES = "kubernetes"

comptime GONE_MINUTES = 60
"""How long an ended instance stays listed, as gone."""
comptime _SPARK_SECONDS = 600


@fieldwise_init
struct Machine(Copyable, Movable):
    """A machine of a pool, and what's running on it."""

    var pool: String
    """`ec2`, `kubernetes` or `worker:<pool>`."""
    var kind: String
    """`ec2`, `k8s` or `self-hosted`."""
    var id: String
    """What its page is named by: the instance id, the pod name, the
    worker token's id."""
    var name: String
    """The instance id, the pod name, or the worker's host name (its
    token's label until it has claimed)."""
    var state: String
    """`live`, `draining` or `gone`."""
    var detail: String
    """What it's doing: `starting`, `running`, `idle (pooled)`, `ended`,
    `idle`, `revoked`, `expired`, `not seen`."""
    var image: String
    var cpus: Int
    """Its vCPUs; 0 if unknown."""
    var memory_mb: Int
    """Its memory; 0 if unknown."""
    var runs: List[Int]
    var seen_ago: Int
    """Seconds since its last heartbeat (a sample, a worker's claim, a
    run's heartbeat); -1 if never."""
    var process: String
    """What orca runs there: `head` or `worker` of a run, `orca worker`,
    or `pods`."""
    var instance_type: String
    var market: String
    var uptime: Int
    """Seconds since it launched; -1 if unknown."""
    var cpu: Float64
    """Its CPU % at the last sample; -1 without one."""
    var mem_pct: Float64
    var spark_x: List[Float64]
    """Its CPU over the last 10 minutes, as in Fleet."""
    var spark_y: List[Float64]
    var token: Int
    """Its worker token's id; 0 if it isn't a worker."""
    var label: String
    """The worker token's label."""
    var expires: String
    var ended: String
    """When an instance ended, and why, in UTC; empty while it's up."""
    var run_start: Int
    """The run's start an instance or a pod belongs to; 0 for a worker."""

    def live(self) -> Bool:
        return self.state != "gone"


def type_size(instance_type: String) -> Tuple[Int, Int]:
    """The vCPUs and memory (MiB) of the instance types orca runs on, as
    `orca.fleet.hourly_price` prices them; (0, 0) for any other."""
    if instance_type == "c7g.large":
        return (2, 4096)
    if instance_type == "c7g.xlarge":
        return (4, 8192)
    if instance_type == "c7g.2xlarge":
        return (8, 16384)
    if instance_type == "c7g.4xlarge":
        return (16, 32768)
    if instance_type == "t4g.medium":
        return (2, 4096)
    if instance_type == "t4g.large":
        return (2, 8192)
    return (0, 0)


comptime _TS = '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\''


def _sparks(
    mut db: State,
) raises -> Tuple[Dict[String, List[Float64]], Dict[String, List[Float64]]]:
    var xs = Dict[String, List[Float64]]()
    var ys = Dict[String, List[Float64]]()
    for r in db.conn.query(
        (
            "SELECT host, extract(epoch FROM now() - t)::float8 AS ago, cpu"
            " FROM orca.fleet_samples WHERE t > now() - make_interval(secs =>"
            " $1::int) ORDER BY host, t"
        ),
        Params().int32(Int32(_SPARK_SECONDS)),
    ):
        var h = r.text("host")
        if h not in xs:
            xs[h] = List[Float64]()
            ys[h] = List[Float64]()
        xs[h].append(1.0 - r.float64("ago") / Float64(_SPARK_SECONDS))
        ys[h].append(r.float64("cpu"))
    return (xs^, ys^)


def _ec2(mut db: State, mut out: List[Machine], one: String) raises:
    """The instances: live, then those that ended in the last hour."""
    var sparks = _sparks(db)
    var sql = String(
        "SELECT i.id, i.role, i.instance_type, i.market, i.run, i.start,",
        " extract(epoch FROM now() - i.launched)::bigint AS up, i.ended IS",
        " NOT NULL AS gone, i.drained IS NOT NULL AS draining, i.idle IS NOT",
        " NULL AS idle, coalesce(to_char(i.ended AT TIME ZONE 'UTC', ",
        _TS,
        "), '') AS ended, coalesce(i.why, '') AS why, coalesce(p.image, '')",
        " AS image, s.cores, s.cpu, s.mem, s.ago FROM orca.instances i JOIN",
        " orca.runs r ON r.id = i.run LEFT JOIN orca.pipelines p ON p.name =",
        " r.pipeline LEFT JOIN LATERAL (SELECT cores, cpu, mem,",
        " extract(epoch FROM now() - t)::bigint AS ago FROM",
        " orca.fleet_samples WHERE host = i.id ORDER BY t DESC LIMIT 1) s ON",
        " true WHERE (i.ended IS NULL OR i.ended > now() - make_interval(mins",
        " => $1::int))",
        " AND i.id = $2" if one else " AND $2 = ''",
        " ORDER BY i.ended IS NOT NULL, i.launched, i.id",
    )
    for r in db.conn.query(sql, Params().int32(Int32(GONE_MINUTES)).text(one)):
        var id = r.text("id")
        var sampled = not r.is_null("cpu")
        var size = type_size(r.text("instance_type"))
        var cpus = size[0]
        if cpus == 0 and sampled:
            cpus = Int(r.int64("cores"))
        var gone = r.bool("gone")
        var state = String("gone") if gone else (
            String("draining") if r.bool("draining") else String("live")
        )
        var detail = String("ended") if gone else (
            String("idle (pooled)") if r.bool("idle") else (
                String("draining") if r.bool("draining") else (
                    String("running") if sampled else String("starting")
                )
            )
        )
        var ended = String()
        if gone:
            ended = String(r.text("ended"))
            if r.text("why"):
                ended.write(": ", r.text("why"))
        out.append(
            Machine(
                pool=EC2,
                kind=EC2,
                id=id,
                name=id,
                state=state^,
                detail=detail^,
                image=r.text("image"),
                cpus=cpus,
                memory_mb=size[1],
                runs=[Int(r.int64("run"))],
                seen_ago=Int(r.int64("ago")) if sampled else -1,
                process=r.text("role"),
                instance_type=r.text("instance_type"),
                market=r.text("market"),
                uptime=Int(r.int64("up")),
                cpu=r.float64("cpu") if sampled else -1.0,
                mem_pct=r.float64("mem") if sampled else -1.0,
                spark_x=sparks[0].pop(id, List[Float64]()),
                spark_y=sparks[1].pop(id, List[Float64]()),
                token=0,
                label=String(),
                expires=String(),
                ended=ended^,
                run_start=Int(r.int64("start")),
            )
        )


def _kubernetes(mut db: State, mut out: List[Machine], one: String) raises:
    """On the Kubernetes runner, a row per live run of the default
    placement: its pods, which the runner names `orca-<run>-<start>`."""
    if getenv("ORCA_RUNNER") != KUBERNETES:
        return
    var stale = stale_limit()
    var sql = String(
        "SELECT r.id, r.starts, coalesce(r.cpu_millis, 0) AS cpu_millis,",
        " coalesce(p.image, '') AS image, coalesce(extract(epoch FROM now()",
        " - r.heartbeat)::bigint, -1) AS ago, coalesce(extract(epoch FROM",
        " now() - r.started)::bigint, -1) AS up FROM orca.runs r LEFT JOIN",
        " orca.pipelines p ON p.name = r.pipeline WHERE r.status =",
        " 'running' AND coalesce(r.placement, 'default') IN ('default', '')",
        " AND NOT ",
        dead_sql(String(stale), start_grace(stale)),
        " AND ('orca-' || r.id || '-' || r.starts) = $1" if one else (
            " AND $1 = ''"
        ),
        " ORDER BY r.id",
    )
    for r in db.conn.query(sql, Params().text(one)):
        var run = Int(r.int64("id"))
        var start = Int(r.int64("starts"))
        var pod = String("orca-", run, "-", start)
        var millis = Int(r.int64("cpu_millis"))
        out.append(
            Machine(
                pool=KUBERNETES,
                kind=K8S,
                id=pod.copy(),
                name=pod^,
                state="live",
                detail="running" if r.int64("ago") >= 0 else "starting",
                image=r.text("image"),
                cpus=(millis + 999) // 1000,
                memory_mb=0,
                runs=[run],
                seen_ago=Int(r.int64("ago")),
                process="pods",
                instance_type=String(),
                market=String(),
                uptime=Int(r.int64("up")),
                cpu=-1.0,
                mem_pct=-1.0,
                spark_x=List[Float64](),
                spark_y=List[Float64](),
                token=0,
                label=String(),
                expires=String(),
                ended=String(),
                run_start=start,
            )
        )


def _workers(mut db: State, mut out: List[Machine], one: Int) raises:
    """A row per worker token: the machine its worker runs on."""
    var stale = Int(stale_limit())
    var sql = String(
        "SELECT id, pool, label, CASE WHEN revoked IS NOT NULL THEN",
        " 'revoked' WHEN expires <= now() THEN 'expired' WHEN last_seen IS",
        " NULL OR last_seen <= now() - make_interval(secs => $1::int) THEN",
        " 'not seen' ELSE 'live' END AS state, coalesce(name, '') AS name,",
        " coalesce(image, '') AS image, coalesce(extract(epoch FROM now() -",
        " last_seen)::bigint, -1) AS ago, coalesce(cpus, 0) AS cpus,",
        " coalesce(memory_mb, 0) AS mem, coalesce(running, '') AS running,",
        " to_char(expires AT TIME ZONE 'UTC', ",
        _TS,
        ") AS expires FROM orca.workers",
        " WHERE id = $2" if one else " WHERE $2 = 0",
        " ORDER BY pool, id DESC",
    )
    for r in db.conn.query(sql, Params().int32(Int32(stale)).int64(Int64(one))):
        var s = r.text("state")
        var runs = List[Int]()
        if r.text("running"):
            for x in r.text("running").split(","):
                try:
                    runs.append(Int(String(x).strip()))
                except:
                    pass
        var name = r.text("name")
        out.append(
            Machine(
                pool=String("worker:", r.text("pool")),
                kind=SELF_HOSTED,
                id=String(r.int64("id")),
                name=name if name else r.text("label"),
                state="live" if s == "live" else "gone",
                detail=(
                    String("running") if len(runs) > 0 else String("idle")
                ) if s
                == "live" else s,
                image=r.text("image"),
                cpus=Int(r.int64("cpus")),
                memory_mb=Int(r.int64("mem")),
                runs=runs^,
                seen_ago=Int(r.int64("ago")),
                process="orca worker",
                instance_type=String(),
                market=String(),
                uptime=-1,
                cpu=-1.0,
                mem_pct=-1.0,
                spark_x=List[Float64](),
                spark_y=List[Float64](),
                token=Int(r.int64("id")),
                label=r.text("label"),
                expires=r.text("expires"),
                ended=String(),
                run_start=0,
            )
        )


def kind_of(v: String) -> String:
    """`?kind=`: `ec2`, `k8s` or `self-hosted`, else empty (all)."""
    if v == EC2 or v == K8S or v == SELF_HOSTED:
        return v
    return String()


def machines(mut db: State) raises -> List[Machine]:
    """Every machine of every pool: EC2, Kubernetes, then the workers by
    pool."""
    var out = List[Machine]()
    _ec2(db, out, "")
    _kubernetes(db, out, "")
    _workers(db, out, 0)
    return out^


def machine(
    mut db: State, kind: String, id: String
) raises -> Optional[Machine]:
    """One machine, by its kind and the id its page is named by."""
    var out = List[Machine]()
    if kind == EC2:
        _ec2(db, out, id)
    elif kind == K8S:
        _kubernetes(db, out, id)
    elif kind == SELF_HOSTED:
        var n: Int
        try:
            n = Int(id)
        except:
            return None
        if n <= 0:
            return None
        _workers(db, out, n)
    if len(out) == 0:
        return None
    return out[0].copy()


@fieldwise_init
struct PoolCount(Copyable, Movable):
    """A pool, its kind, and how many of its machines are live."""

    var pool: String
    var kind: String
    var machines: Int
    var live: Int


def pool_counts(ms: List[Machine]) -> List[PoolCount]:
    """The pools the machines are in, in their order."""
    var out = List[PoolCount]()
    for m in ms:
        var found = False
        for ref p in out:
            if p.pool == m.pool:
                p.machines += 1
                p.live += 1 if m.live() else 0
                found = True
                break
        if not found:
            out.append(PoolCount(m.pool, m.kind, 1, 1 if m.live() else 0))
    return out^


def filtered(ms: List[Machine], pool: String, kind: String) -> List[Machine]:
    """The machines of `pool` (any if empty) and `kind` (any if empty)."""
    var out = List[Machine]()
    for m in ms:
        if (not pool or m.pool == pool) and (not kind or m.kind == kind):
            out.append(m.copy())
    return out^


@fieldwise_init
struct WorkerRun(Copyable, Movable):
    """A run a worker claimed: for its machine's page."""

    var id: Int
    var pipeline: String
    var status: String
    var partitions: String
    var started: String
    var ended: String


def worker_runs(
    mut db: State, token: Int, n: Int = 20
) raises -> List[WorkerRun]:
    """The runs a worker token's latest starts were claimed by, newest
    first."""
    var out = List[WorkerRun]()
    for r in db.conn.query(
        String(
            "SELECT id, pipeline, status, partitions, coalesce(to_char(started",
            " AT TIME ZONE 'UTC', ",
            _TS,
            "), '') AS started, coalesce(to_char(ended AT TIME ZONE 'UTC', ",
            _TS,
            "), '') AS ended FROM orca.runs WHERE worker = $1 ORDER BY id DESC",
            " LIMIT $2",
        ),
        Params().int64(Int64(token)).int32(Int32(n)),
    ):
        out.append(
            WorkerRun(
                Int(r.int64("id")),
                r.text("pipeline"),
                r.text("status"),
                r.text("partitions"),
                r.text("started"),
                r.text("ended"),
            )
        )
    return out^


def run_pipeline(mut db: State, run: Int) raises -> String:
    """The pipeline of a run, or empty."""
    var rows = db.conn.query(
        "SELECT pipeline FROM orca.runs WHERE id = $1",
        Params().int64(Int64(run)),
    )
    if len(rows) == 0:
        return ""
    return rows.row(0).text("pipeline")
