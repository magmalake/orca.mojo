"""The scheduler: fires schedules, senses datasets, queues backfills and
rebuilds, and starts runs.

```
orca register ./build/taxi             runs `taxi describe`, records the binary
orca backfill taxi 2011-01..2024-12    a run per partition that isn't done
orca backfill labels @all              keyed: runs of keys_per_run undone keys
orca rebuild taxi.trips 2024-01        the pipelines downstream, in order
orca tick                              one pass, then exits
orca scheduler                         a pass every second
```

The scheduler links no pipeline. It knows one by the `describe` it printed,
kept in `orca.pipelines` with the binary's path, and reads the schedule,
partitions and limits from that JSON in SQL. Everything it decides is in
Postgres, so it can be stopped and restarted, or run twice, at any time:

- **A fire is a row.** `orca.fires` is keyed by (pipeline, fire time), and a
  fire's row and its `queued` run are inserted in one transaction, so a fire
  is never made twice. Fires are counted from the last one recorded (or from
  when the pipeline was registered), so the fires missed while the scheduler
  was down are made once each, or, with `catchup=False`, only the latest.
  A fire whose partition is before the pipeline's first is recorded without
  a run.
- **Starting a run is `resume`.** A queued run has nothing done, or what a
  resume or rerun from `orca ui` left it, which is what `<binary> resume
  <run>` takes (with `--allow-new-code` when the resume said so). The scheduler marks the run `running`,
  with no heartbeat and itself as owner, and execs the binary, which claims
  it. It doesn't wait: the run records its own progress.
- **`max_runs`.** Under a lock on the pipeline's row, the runs that are live
  (a fresh heartbeat, or started by the scheduler less than the stale limit
  ago) are counted, and only that many fewer than `max_runs` are started.
- **Backfills queue what isn't done.** A key is done when every mapped
  step has succeeded for it in a run of the pipeline's current code
  (`DONE_SQL`). A backfill drops the keys that are done or in a queued or
  running run, sorts the rest and queues a run per `keys_per_run` of them.
- **Where runs run.** `ORCA_RUNNER` is `local` (a process here, the
  default), `kubernetes` (a dista cluster of pods per run, `orca.kube`) or
  `ec2` (a head instance and worker instances per run, `orca.ec2`).
- **Sensors and rebuilds** (`orca.sensors`). A sensor pipeline's pass
  looks for snapshots of the datasets it reads, in `orca.commits` on every
  tick and from `<binary> sense` every `ORCA_SENSE_SECONDS`, and queues a
  run, trigger `sensor`, for those it hasn't seen. A keyed sensor's keys
  dataset is sensed too: a new snapshot of it queues the keys that aren't
  done, in runs of `keys_per_run`. Each rebuild queues its
  next level once the one before has succeeded.
- **Retries.** A scheduled, backfilled or sensed run that failed is started again
  after `ORCA_RETRY_SECONDS` (default 60) doubling with each start, plus up
  to a tenth more as jitter, up to `run_retries` times. One whose process
  died (its heartbeat went stale) is started again at once, within the same
  budget. A manual run is never retried by the scheduler. A run asked to
  cancel whose process died ends `cancelled`.

The scheduler's clock is `orca.civil.now`, which `ORCA_NOW` fixes; fire
times and retry times are on it. Heartbeats are on the database's clock.
A run's output goes to `<ORCA_LOGS>/<pipeline>/<run>/driver.log` when
`ORCA_LOGS` is set: written there by a local run, published there when the
process is reaped if `ORCA_LOGS` is in object storage, or kept from the
driver pod's log on Kubernetes (`orca.kube`).
"""

from std.os import getenv, makedirs
from std.os.path import exists
from std.time import perf_counter_ns

from postgres import Params, Transaction

from orca.civil import Civil
from orca.cron import CronExpr
from orca.day import Day
from orca.key import parse_keys
from orca.ec2 import Ec2Runner, Ec2Spec
from orca.kube import KubeRunner, RunSpec, cpu_millis
from orca.logs import is_remote, publish, run_dir, stage_of
from orca.month import Month
from orca.name import parse_names, sort_texts
from orca.proc import absolute, capture, reap, spawn
from orca.schedule import Schedule
from orca.sensors import (
    advance_rebuilds,
    join_keys,
    map_keys,
    parse_sense,
    split_keys,
)
from orca.state import State
from orca.tz import Zone

comptime _BUDGET_LOCK = 0x6F726362  # "orcb"
"""Held while a start is weighed against `ORCA_VCPU_BUDGET`."""

comptime _RETRIED = "trigger IN ('schedule', 'backfill', 'sensor')"
"""The runs the scheduler retries."""


@fieldwise_init
struct Registered(Copyable, Movable):
    """A registered pipeline, as the scheduler reads it from `describe`."""

    var name: String
    var binary: String
    var registry_hash: String
    var since: Int64
    var cron: String
    var tz: String
    var fills: String
    var catchup: Bool
    var kind: String
    var start: String
    var max_runs: Int
    var run_retries: Int
    var sensor: Bool
    var keys_per_run: Int
    var keys_dataset: String
    """A keyed pipeline's keys dataset; empty for time partitions."""
    var image: String
    """The image its runs' pods run (`orca register --image`), or empty."""
    var workers: Int
    """Its cluster's dista workers; 0 if it has no tasks."""
    var head_type: String
    """On EC2, its runs' head instance type; empty for the default."""
    var worker_type: String
    var spot: Bool
    """On EC2, whether its workers are Spot instances."""
    var min_workers: Int
    """On EC2, its runs' fewest workers, scaling up to `workers`; equal to
    `workers` for a fixed count."""


def fill_key(kind: String, fills: String, local: Civil) raises -> String:
    """The partition a fire fills, given its time on the schedule's wall
    clock: the key the time falls in, or the one before it."""
    if kind == "daily":
        var d = Day.containing(local.year, local.month, local.day)
        return String(d.prev() if fills == "previous" else d^)
    if kind == "monthly":
        var m = Month.containing(local.year, local.month, local.day)
        return String(m.prev() if fills == "previous" else m^)
    raise Error("orca: unknown partitions '", kind, "'")


def registered(mut db: State, only: String = "") raises -> List[Registered]:
    """Every pipeline registered with a binary, or the one named."""
    var out = List[Registered]()
    var rows = db.conn.query(
        (
            "SELECT name, binary_path, registry_hash, coalesce(since, 0) AS"
            " since, coalesce(describe->'schedule'->>'cron', '') AS cron,"
            " coalesce(describe->'schedule'->>'tz', 'UTC') AS tz,"
            " coalesce(describe->'schedule'->>'fills', 'previous') AS"
            " fills, coalesce((describe->'schedule'->>'catchup')::boolean,"
            " true) AS catchup, coalesce(describe->'partitions'->>'kind',"
            " 'monthly') AS kind, coalesce(describe->'partitions'->>"
            "'start', '') AS start, coalesce((describe->>'max_runs')"
            "::bigint, 1) AS max_runs, coalesce((describe->>"
            "'run_retries')::bigint, 3) AS run_retries,"
            " coalesce((describe->>'sensor')::boolean, false) AS sensor,"
            " coalesce((describe->>'keys_per_run')::bigint, 1) AS"
            " keys_per_run, coalesce(describe->'partitions'->>'dataset', '')"
            " AS keys_dataset, coalesce(image, '') AS image, CASE WHEN"
            " jsonb_array_length(coalesce(describe->'tasks', '[]')) > 0 THEN"
            " coalesce(ec2_workers, (describe->>'workers')::bigint, 2) ELSE 0"
            " END AS workers, coalesce(head_type, '') AS head_type,"
            " coalesce(worker_type, '') AS worker_type, coalesce(spot, false)"
            " AS spot, coalesce(ec2_min_workers, -1) AS min_workers FROM"
            " orca.pipelines WHERE binary_path IS NOT NULL AND ($1 = '' OR"
            " name = $1) ORDER BY name"
        ),
        Params().text(only),
    )
    for r in rows:
        out.append(
            Registered(
                r.text("name"),
                r.text("binary_path"),
                r.text("registry_hash"),
                r.int64("since"),
                r.text("cron"),
                r.text("tz"),
                r.text("fills"),
                r.bool("catchup"),
                r.text("kind"),
                r.text("start"),
                Int(r.int64("max_runs")),
                Int(r.int64("run_retries")),
                r.bool("sensor"),
                Int(r.int64("keys_per_run")),
                r.text("keys_dataset"),
                r.text("image"),
                Int(r.int64("workers")),
                r.text("head_type"),
                r.text("worker_type"),
                r.bool("spot"),
                Int(r.int64("min_workers")),
            )
        )
    # A fixed count, or no tasks: no scaling.
    for ref p in out:
        if p.min_workers < 0 or p.workers == 0:
            p.min_workers = p.workers
        p.min_workers = min(p.min_workers, p.workers)
    return out^


def dataset_cycle(mut db: State, describe: String) raises -> String:
    """How the pipeline `describe` gives would close a cycle across
    pipelines, through the datasets they write and read, or empty if it
    wouldn't. A pipeline reading a dataset it writes itself isn't one."""
    var rows = db.conn.query(
        (
            "WITH d AS (SELECT name, describe FROM orca.pipelines WHERE name"
            " <> $1::jsonb->>'pipeline' UNION ALL SELECT $1::jsonb->>"
            "'pipeline', $1::jsonb), io AS (SELECT DISTINCT d.name, k.kind,"
            " x.ds FROM d, jsonb_array_elements(d.describe->'steps') AS s(st),"
            " (VALUES ('reads'), ('writes')) AS k(kind),"
            " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
            " x(ds)) SELECT w.name AS src, r.name AS dst, w.ds, $1::jsonb->>"
            "'pipeline' AS me FROM io w JOIN io r ON r.ds = w.ds AND r.kind ="
            " 'reads' WHERE w.kind = 'writes' AND w.name <> r.name ORDER BY"
            " 1, 2, 3"
        ),
        Params().text(describe),
    )
    if len(rows) == 0:
        return ""
    var me = rows.row(0).text("me")
    var src = List[String]()
    var dst = List[String]()
    var via = List[String]()
    for r in rows:
        src.append(r.text("src"))
        dst.append(r.text("dst"))
        via.append(r.text("ds"))
    # A path from `me` back to `me`, breadth first, by the edge taken.
    var came = List[Int](length=len(src), fill=-2)
    var frontier = List[Int]()
    for i in range(len(src)):
        if src[i] == me:
            came[i] = -1
            frontier.append(i)
    while len(frontier) > 0:
        var next = List[Int]()
        for e in frontier:
            if dst[e] == me:
                var path = List[Int]()
                var at = e
                while at >= 0:
                    path.append(at)
                    at = came[at]
                var out = String()
                for k in range(len(path) - 1, -1, -1):
                    var i = path[k]
                    out.write(
                        src[i],
                        " writes ",
                        via[i],
                        ", which ",
                        dst[i],
                        " reads",
                        "; " if k > 0 else "",
                    )
                return out^
            for i in range(len(src)):
                if src[i] == dst[e] and came[i] == -2:
                    came[i] = e
                    next.append(i)
        frontier = next^
    return ""


def current_keys(p: Registered) raises -> Tuple[Int64, List[String]]:
    """A keyed pipeline's keys snapshot and keys, as `<binary> keys`
    prints them."""
    var got = capture([p.binary, "keys"])
    if got[0] != 0:
        raise Error("orca: ", p.binary, " keys exited ", got[0], ": ", got[1])
    var lines = got[1].splitlines()
    if len(lines) == 0:
        raise Error("orca: ", p.binary, " keys printed nothing")
    var keys = List[String]()
    for i in range(1, len(lines)):
        if lines[i]:
            keys.append(String(lines[i]))
    return (Int64(Int(lines[0])), keys^)


def _file_names(path: String) raises -> String:
    var text: String
    with open(path, "r") as f:
        text = f.read()
    var joined = String()
    for line in text.splitlines():
        var n = line.strip()
        if n:
            joined.write("," if joined else "", n)
    if not joined:
        raise Error("orca: no names in ", path)
    return joined^


def kind_mismatch(mut db: State, describe: String) raises -> String:
    """How the pipeline `describe` gives would make a sensor run keys of the
    other kind, a keyed sensor reading a dataset a time-partitioned
    pipeline writes or the other way round, or empty if it wouldn't."""
    var rows = db.conn.query(
        (
            "WITH d AS (SELECT name, describe FROM orca.pipelines WHERE name"
            " <> $1::jsonb->>'pipeline' UNION ALL SELECT $1::jsonb->>"
            "'pipeline', $1::jsonb), io AS (SELECT DISTINCT d.name,"
            " coalesce(d.describe->'partitions'->>'kind', 'monthly') AS"
            " pkind, coalesce((d.describe->>'sensor')::boolean, false) AS"
            " sensor, k.kind, x.ds FROM d, jsonb_array_elements(d.describe->"
            "'steps') AS s(st), (VALUES ('reads'), ('writes')) AS k(kind),"
            " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
            " x(ds) UNION SELECT d.name, 'keyed', coalesce((d.describe->>"
            "'sensor')::boolean, false), 'reads', d.describe->'partitions'->>"
            "'dataset' FROM d WHERE d.describe->'partitions'->>'dataset' IS"
            " NOT NULL) SELECT w.name AS writer, w.pkind AS wkind, r.name AS"
            " reader, r.pkind AS rkind, w.ds FROM io w JOIN io r ON r.ds ="
            " w.ds AND r.kind = 'reads' WHERE w.kind = 'writes' AND w.name <>"
            " r.name AND r.sensor AND (w.pkind = 'keyed') <> (r.pkind ="
            " 'keyed') AND $1::jsonb->>'pipeline' IN (w.name, r.name) ORDER BY"
            " 1, 3, 5 LIMIT 1"
        ),
        Params().text(describe),
    )
    if len(rows) == 0:
        return ""
    var r = rows.row(0)
    return String(
        "the sensor ",
        r.text("reader"),
        " (",
        r.text("rkind"),
        ") reads ",
        r.text("ds"),
        ", which ",
        r.text("writer"),
        " (",
        r.text("wkind"),
        ") writes, and no rule turns a name into a date or a date into a",
        " name",
    )


def partition_keys(p: Registered, keys: String) raises -> List[String]:
    """`keys` as the pipeline's partition keys, each checked: for time keys
    `KEY[..KEY][,...]` from its first partition on; for keyed partitions a
    list of names, `@all` or `@file:<path>`, each a current key."""
    var texts = List[String]()
    if p.kind == "keyed":
        var current = current_keys(p)[1].copy()
        if keys == "@all":
            return current^
        var have = Dict[String, Bool]()
        for k in current:
            have[k] = True
        var list = _file_names(String(keys[byte=6:])) if keys.startswith(
            "@file:"
        ) else keys
        for n in parse_names(list):
            if n.text not in have:
                raise Error("orca: ", n, " isn't a key of ", p.keys_dataset)
            texts.append(n.text)
        return texts^
    if p.kind == "daily":
        for k in parse_keys[Day](keys, p.start):
            texts.append(String(k))
    else:
        for k in parse_keys[Month](keys, p.start):
            texts.append(String(k))
    return texts^


comptime DONE_SQL = (
    "WITH k AS (SELECT DISTINCT unnest(string_to_array($2, ',')) AS key), p"
    " AS (SELECT registry_hash, describe FROM orca.pipelines WHERE name ="
    " $1), m AS (SELECT x->>'name' AS step FROM p,"
    " jsonb_array_elements(p.describe->'steps') AS x WHERE x->>'kind' ="
    " 'mapped') SELECT k.key FROM k WHERE CASE WHEN EXISTS (SELECT 1 FROM m)"
    " THEN NOT EXISTS (SELECT 1 FROM m WHERE NOT EXISTS (SELECT 1 FROM"
    " orca.steps st JOIN orca.runs r ON r.id = st.run JOIN p ON"
    " r.registry_hash = p.registry_hash WHERE r.pipeline = $1 AND st.step ="
    " m.step AND st.partition = k.key AND st.status = 'succeeded')) ELSE"
    " EXISTS (SELECT 1 FROM orca.runs r JOIN p ON r.registry_hash ="
    " p.registry_hash WHERE r.pipeline = $1 AND r.status = 'succeeded' AND"
    " k.key = ANY (string_to_array(r.partitions, ','))) END"
)
"""Of the comma-separated keys `$2`, those done for pipeline `$1`: every
mapped step has succeeded for the key in a run of the pipeline's current
code. A pipeline with no mapped step has a key done when a run of its
current code over it succeeded."""

comptime LIVE_SQL = (
    "SELECT DISTINCT unnest(string_to_array(partitions, ',')) AS key FROM"
    " orca.runs WHERE pipeline = $1 AND status IN ('queued', 'running')"
)
"""The keys of pipeline `$1` in a queued or running run."""


def done_keys(
    mut db: State, pipeline: String, keys: List[String]
) raises -> Dict[String, Bool]:
    """Which of `keys` are done for `pipeline` (`DONE_SQL`)."""
    var joined = String()
    for i in range(len(keys)):
        joined.write("," if i else "", keys[i])
    var out = Dict[String, Bool]()
    for r in db.conn.query(DONE_SQL, Params().text(pipeline).text(joined)):
        out[r.text("key")] = True
    return out^


def queue_keys(
    mut db: State, p: Registered, keys: List[String], trigger: String
) raises -> List[Int]:
    """Queues runs of `p` over those of `keys` that aren't done and aren't
    in a live run, sorted, `keys_per_run` to a run; returns the runs. Under
    a lock on the pipeline's row, so two backfills don't both queue a key."""
    var tx = db.conn.begin()
    var runs = _queue(tx, p, keys, trigger)
    tx.commit()
    return runs^


def _queue(
    mut tx: Transaction, p: Registered, keys: List[String], trigger: String
) raises -> List[Int]:
    """`queue_keys` in `tx`."""
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    var skip = Dict[String, Bool]()
    for r in tx.query(LIVE_SQL, Params().text(p.name)):
        skip[r.text("key")] = True
    var joined = String()
    for i in range(len(keys)):
        joined.write("," if i else "", keys[i])
    for r in tx.query(DONE_SQL, Params().text(p.name).text(joined)):
        skip[r.text("key")] = True
    var todo = List[String]()
    for k in keys:
        if k not in skip:
            skip[k] = True  # each once
            todo.append(k)
    sort_texts(todo)
    var runs = List[Int]()
    var i = 0
    while i < len(todo):
        var batch = String()
        var j = i
        while j < len(todo) and j < i + p.keys_per_run:
            batch.write("," if j > i else "", todo[j])
            j += 1
        runs.append(
            Int(
                tx.query(
                    (
                        "INSERT INTO orca.runs (pipeline, registry_hash,"
                        " partitions, trigger, status) VALUES ($1, $2, $3, $4,"
                        " 'queued') RETURNING id"
                    ),
                    Params()
                    .text(p.name)
                    .text(p.registry_hash)
                    .text(batch)
                    .text(trigger),
                )
                .row(0)
                .int64("id")
            )
        )
        i = j
    return runs^


def backfill(mut db: State, p: Registered, keys: String) raises -> Int:
    """Queues runs for the keys in `keys` that aren't done and aren't in a
    queued or running run, `keys_per_run` to a run; returns how many runs
    it queued."""
    return len(queue_keys(db, p, partition_keys(p, keys), "backfill"))


struct Scheduler(Movable):
    var db: State
    var stale: Float64
    var retry_seconds: Int64
    var sense_seconds: Float64
    var start_grace: Float64
    """How long a run the scheduler started may go without a heartbeat."""
    var budget: Int
    """`ORCA_VCPU_BUDGET` in thousandths of a vCPU; 0 for none."""
    var runner: Optional[KubeRunner]
    """Set with `ORCA_RUNNER=kubernetes`."""
    var ec2: Optional[Ec2Runner]
    """Set with `ORCA_RUNNER=ec2`; with neither, runs are local processes."""
    var logs: String
    var _polled: Dict[String, Int]
    """When each sensor's binary was last asked to `sense`, on the monotonic
    clock in nanoseconds."""
    var _driver_logs: Dict[Int, String]
    """A local run's process to where its driver log goes, when that's in
    object storage: it's published once the process is reaped."""

    def __init__(out self, url: String) raises:
        self.db = State(url)
        self.db.migrate()
        self.stale = Float64(getenv("ORCA_STALE_SECONDS", "30"))
        self.retry_seconds = Int64(Int(getenv("ORCA_RETRY_SECONDS", "60")))
        self.sense_seconds = Float64(getenv("ORCA_SENSE_SECONDS", "60"))
        var runner = getenv("ORCA_RUNNER", "local")
        self.runner = None
        self.ec2 = None
        if runner == "kubernetes":
            self.runner = KubeRunner()
        elif runner == "ec2":
            self.ec2 = Ec2Runner()
        elif runner != "local":
            raise Error(
                "orca: ORCA_RUNNER is local, kubernetes or ec2, not ", runner
            )
        # A pod can take minutes to be scheduled and pull its image.
        self.start_grace = Float64(
            getenv(
                "ORCA_START_SECONDS",
                "300" if self.runner or self.ec2 else String(self.stale),
            )
        )
        var budget = getenv("ORCA_VCPU_BUDGET")
        self.budget = cpu_millis(budget) if budget else 0
        self.logs = getenv("ORCA_LOGS")
        self._polled = Dict[String, Int]()
        self._driver_logs = Dict[Int, String]()

    def register(
        mut self,
        binary: String,
        now: Int64,
        image: String = "",
        head_type: String = "",
        worker_type: String = "",
        workers: Int = 0,
        spot: Bool = False,
        min_workers: Int = -1,
    ) raises -> String:
        """Records the pipeline `binary` describes, with its path; its
        schedule counts from `now` the first time, and a sensor from now the
        first time it's one, with the snapshots its datasets have now seen.
        Queued runs move to the new code. `min_workers`, with `workers` as the
        most, makes an EC2 run's workers scale between them; -1 keeps a
        fixed count. Returns the pipeline's name."""
        var path = absolute(binary)
        var got = capture([path, "describe"])
        if got[0] != 0 or not got[1].startswith('{"pipeline": '):
            raise Error(
                "orca: ", path, " describe exited ", got[0], ": ", got[1]
            )
        var cycle = dataset_cycle(self.db, got[1])
        if cycle:
            raise Error("orca: ", path, " can't be registered: ", cycle)
        var mismatch = kind_mismatch(self.db, got[1])
        if mismatch:
            raise Error("orca: ", path, " can't be registered: ", mismatch)
        var info = self.db.conn.query(
            (
                "SELECT coalesce(($1::jsonb->>'sensor')::boolean, false) AS"
                " sensor, NOT EXISTS (SELECT 1 FROM orca.pipelines WHERE name"
                " = $1::jsonb->>'pipeline' AND sensing_since IS NOT NULL) AS"
                " new_sensor"
            ),
            Params().text(got[1]),
        ).row(0)
        var sensor = info.bool("sensor")
        var seen = String()
        if sensor:
            var sensed = capture([path, "sense"])
            if sensed[0] != 0:
                raise Error(
                    "orca: ", path, " sense exited ", sensed[0], ": ", sensed[1]
                )
            if len(parse_sense(sensed[1])) == 0:
                raise Error(
                    "orca: ",
                    path,
                    " is a sensor, but reads no dataset it doesn't write",
                )
            if info.bool("new_sensor"):
                seen = sensed[1]
        var res = self.db.conn.query(
            (
                "INSERT INTO orca.pipelines (name, registry_hash, describe,"
                " binary_path, since, sensing_since, image, head_type,"
                " worker_type, ec2_workers, spot, ec2_min_workers) SELECT"
                " d->>'pipeline',"
                " d->>'registry_hash', d, $2, $3, CASE WHEN $4 THEN now() END,"
                " nullif($5, ''), nullif($6, ''), nullif($7, ''), nullif($8,"
                " 0), $9, nullif($10, -1)"
                " FROM (SELECT $1::jsonb AS d) AS x ON CONFLICT (name) DO"
                " UPDATE SET registry_hash = EXCLUDED.registry_hash, describe ="
                " EXCLUDED.describe, binary_path = EXCLUDED.binary_path, since"
                " = coalesce(orca.pipelines.since, EXCLUDED.since),"
                " sensing_since = CASE WHEN $4 THEN"
                " coalesce(orca.pipelines.sensing_since, now()) END, image ="
                " coalesce(EXCLUDED.image, orca.pipelines.image), head_type ="
                " coalesce(EXCLUDED.head_type, orca.pipelines.head_type),"
                " worker_type = coalesce(EXCLUDED.worker_type,"
                " orca.pipelines.worker_type), ec2_workers ="
                " coalesce(EXCLUDED.ec2_workers, orca.pipelines.ec2_workers),"
                " ec2_min_workers = CASE WHEN $8 > 0 THEN"
                " EXCLUDED.ec2_min_workers ELSE orca.pipelines.ec2_min_workers"
                " END,"
                " spot = CASE WHEN $6 <> '' OR $7 <> '' OR $8 > 0 OR $9 THEN $9"
                " ELSE orca.pipelines.spot END, last_seen"
                " = now() RETURNING name, registry_hash, coalesce(image, '')"
                " AS image"
            ),
            Params()
            .text(got[1])
            .text(path)
            .int64(now)
            .bool(sensor)
            .text(image)
            .text(head_type)
            .text(worker_type)
            .int64(Int64(workers))
            .bool(spot)
            .int64(Int64(min_workers)),
        )
        var name = res.row(0).text("name")
        var version = self.db.version(
            name,
            res.row(0).text("registry_hash"),
            got[1],
            res.row(0).text("image"),
        )
        _ = self.db.conn.execute(
            "UPDATE orca.pipelines SET version = $2 WHERE name = $1",
            Params().text(name).int64(Int64(version)),
        )
        # Where its functions are, if `orca locate` left a map beside it.
        var map = String(path, ".source.json")
        if exists(map):
            var text: String
            with open(map, "r") as f:
                text = f.read()
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.pipeline_versions SET locations = $2::jsonb"
                    " WHERE id = $1"
                ),
                Params().int64(Int64(version)).text(text),
            )
        # What its datasets hold now isn't new to it.
        for c in parse_sense(seen):
            if c.snapshot >= 0:
                _ = self.db.conn.execute(
                    (
                        "INSERT INTO orca.sensed (pipeline, dataset,"
                        " snapshot_id) VALUES ($1, $2, $3) ON CONFLICT DO"
                        " NOTHING"
                    ),
                    Params().text(name).text(c.dataset).int64(c.snapshot),
                )
        _ = self.db.conn.execute(
            (
                "UPDATE orca.runs SET registry_hash = $2 WHERE pipeline = $1"
                " AND status = 'queued'"
            ),
            Params().text(name).text(res.row(0).text("registry_hash")),
        )
        return name^

    def tick(mut self, now: Int64) raises:
        """One pass: reap ended runs, advance rebuilds, make due fires,
        sense new snapshots, schedule retries, and start what may start."""
        for done in reap():
            print("reaped pid", done[0], "exit", done[1])
            var log = self._driver_logs.pop(done[0], "")
            if log:
                try:
                    publish(log)
                except e:
                    print("orca: can't keep the driver log at", log, ":", e)
        if self.runner:
            try:
                var n = self.runner.value().sweep(self.db)
                if n:
                    print("deleted", n, "objects of runs that ended")
            except e:
                print("orca: can't sweep run clusters:", e)
        if self.ec2:
            try:
                var n = self.ec2.value().sweep(self.db)
                if n:
                    print("terminated", n, "instances of runs that ended")
            except e:
                print("orca: can't sweep run instances:", e)
            try:
                var moved = self.ec2.value().scale(self.db)
                if moved[0] or moved[1]:
                    print(
                        "scaled run workers:",
                        moved[0],
                        "started,",
                        moved[1],
                        "drained",
                    )
            except e:
                print("orca: can't scale run workers:", e)
        advance_rebuilds(self.db)
        for p in registered(self.db):
            if p.cron:
                try:
                    self._fire(p, now)
                except e:
                    print("orca: pipeline", p.name, "can't fire:", e)
            if p.sensor:
                try:
                    self._sense(p, now)
                except e:
                    print("orca: pipeline", p.name, "can't sense:", e)
            self._schedule_retries(p, now)
            self._start(p, now)

    def _sense(mut self, p: Registered, now: Int64) raises:
        """Queues a run for the snapshots of `p`'s datasets it hasn't seen:
        those other pipelines' succeeded runs committed since it became a
        sensor, and, when its binary is due to be asked, the current ones
        something other than orca committed. A snapshot with no partitions
        stands for the one `p` would fill now."""
        var datasets = List[String]()
        var snapshots = List[Int64]()
        var keys = List[String]()
        for r in self.db.conn.query(
            (
                "WITH io AS (SELECT k.kind, x.ds FROM orca.pipelines p,"
                " jsonb_array_elements(p.describe->'steps') AS s(st), (VALUES"
                " ('reads'), ('writes')) AS k(kind),"
                " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
                " x(ds) WHERE p.name = $1 UNION SELECT 'reads',"
                " describe->'partitions'->>'dataset' FROM orca.pipelines WHERE"
                " name = $1 AND describe->'partitions'->>'dataset' IS NOT"
                " NULL) SELECT DISTINCT ON (c.dataset,"
                " c.snapshot_id) c.dataset, c.snapshot_id,"
                " coalesce(nullif(s.partition, ''), r.partitions) AS keys FROM"
                " orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN"
                " orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id ="
                " s.run JOIN orca.pipelines me ON me.name = $1 WHERE"
                " r.pipeline <> $1 AND r.status = 'succeeded' AND a.status ="
                " 'succeeded' AND a.ended >= me.sensing_since AND c.dataset IN"
                " (SELECT ds FROM io WHERE kind = 'reads' EXCEPT SELECT ds FROM"
                " io WHERE kind = 'writes') AND NOT EXISTS (SELECT 1 FROM"
                " orca.sensed x WHERE x.pipeline = $1 AND x.dataset ="
                " c.dataset AND x.snapshot_id = c.snapshot_id) AND NOT EXISTS"
                " (SELECT 1 FROM orca.rebuild_pipelines rp WHERE rp.rebuild ="
                " r.rebuild AND rp.pipeline = $1) ORDER BY c.dataset,"
                " c.snapshot_id, c.attempt"
            ),
            Params().text(p.name),
        ):
            datasets.append(r.text("dataset"))
            snapshots.append(r.int64("snapshot_id"))
            keys.append(r.text("keys"))
        var t = Int(perf_counter_ns())
        var last = self._polled.get(p.name)
        if not last or Float64(t - last.value()) >= self.sense_seconds * 1e9:
            self._polled[p.name] = t
            var got = capture([p.binary, "sense"])
            if got[0] != 0:
                raise Error("sense exited ", got[0], ": ", got[1])
            for c in parse_sense(got[1]):
                if c.snapshot >= 0 and not c.by_orca:
                    datasets.append(c.dataset)
                    snapshots.append(c.snapshot)
                    keys.append(c.partitions)
        if len(datasets) == 0:
            return
        if p.kind == "keyed":
            self._sense_keyed(p, datasets, snapshots, keys)
            return
        var fallback: String
        if p.cron:
            var z = Zone.load(p.tz)
            fallback = fill_key(p.kind, p.fills, Civil.of(z.to_local(now)))
        else:
            fallback = fill_key(p.kind, "current", Civil.of(now))
        var tx = self.db.conn.begin()
        var taken = List[Int]()
        var all = List[String]()
        for i in range(len(datasets)):
            # Seen by another tick already, or twice in this one.
            var new = tx.query(
                (
                    "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " snapshot_id"
                ),
                Params().text(p.name).text(datasets[i]).int64(snapshots[i]),
            )
            if len(new) == 0:
                continue
            taken.append(i)
            var ks = split_keys(keys[i])
            if len(ks) == 0:
                ks.append(fallback)
            all.extend(ks^)
        var run = Int64(-1)
        var mapped = map_keys(all, p.kind, p.start)
        if len(mapped) > 0:
            run = (
                tx.query(
                    (
                        "INSERT INTO orca.runs (pipeline, registry_hash,"
                        " partitions, trigger, status) VALUES ($1, $2, $3,"
                        " 'sensor', 'queued') RETURNING id"
                    ),
                    Params()
                    .text(p.name)
                    .text(p.registry_hash)
                    .text(join_keys(mapped)),
                )
                .row(0)
                .int64("id")
            )
            for i in taken:
                _ = tx.execute(
                    (
                        "UPDATE orca.sensed SET run = $4 WHERE pipeline = $1"
                        " AND dataset = $2 AND snapshot_id = $3"
                    ),
                    Params()
                    .text(p.name)
                    .text(datasets[i])
                    .int64(snapshots[i])
                    .int64(run),
                )
        tx.commit()
        for i in taken:
            print(
                "sensed",
                String(datasets[i], "@", snapshots[i]),
                "for",
                p.name,
                String("run ", run, " for ", join_keys(mapped)) if run
                >= 0 else "before its first partition",
            )

    def _fire(mut self, p: Registered, now: Int64) raises:
        var s = Schedule(CronExpr.parse(p.cron), Zone.load(p.tz))
        var last = (
            self.db.conn.query(
                (
                    "SELECT coalesce(max(fire_at), $2) AS last FROM orca.fires"
                    " WHERE pipeline = $1"
                ),
                Params().text(p.name).int64(p.since),
            )
            .row(0)
            .int64("last")
        )
        var times = List[Int64]()
        var t = last
        # Many missed fires are made a thousand at a time, one per tick.
        while len(times) < 1000:
            t = s.next_fire(t)
            if t > now:
                break
            times.append(t)
        if not p.catchup and len(times) > 1:
            times = [times[len(times) - 1]]
        for t in times:
            var key = fill_key(p.kind, p.fills, Civil.of(s.zone.to_local(t)))
            var tx = self.db.conn.begin()
            var made = tx.query(
                (
                    "INSERT INTO orca.fires (pipeline, fire_at, partition)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " fire_at"
                ),
                Params().text(p.name).int64(t).text(key),
            )
            var run = Int64(-1)
            # Keys of one kind order as text: `YYYY-MM`, `YYYY-MM-DD`.
            if len(made) == 1 and (not p.start or key >= p.start):
                run = (
                    tx.query(
                        (
                            "INSERT INTO orca.runs (pipeline, registry_hash,"
                            " partitions, trigger, status) VALUES ($1, $2, $3,"
                            " 'schedule', 'queued') RETURNING id"
                        ),
                        Params().text(p.name).text(p.registry_hash).text(key),
                    )
                    .row(0)
                    .int64("id")
                )
                _ = tx.execute(
                    (
                        "UPDATE orca.fires SET run = $3 WHERE pipeline = $1"
                        " AND fire_at = $2"
                    ),
                    Params().text(p.name).int64(t).int64(run),
                )
            tx.commit()
            if len(made) == 1:
                print(
                    "fired",
                    p.name,
                    "at",
                    String(Civil.of(t), "Z"),
                    "for",
                    key,
                    String("run ", run) if run >= 0 else "before the start",
                )

    def _sense_keyed(
        mut self,
        p: Registered,
        datasets: List[String],
        snapshots: List[Int64],
        keys: List[String],
    ) raises:
        """For a keyed sensor: a snapshot of its keys dataset offers every
        current key, and one of another dataset the names it holds that are
        current keys; the offered keys that aren't done or in a live run are
        queued, `keys_per_run` to a run."""
        var cur = Dict[String, Bool]()
        var all_keys = current_keys(p)[1].copy()
        for k in all_keys:
            cur[k] = True
        var tx = self.db.conn.begin()
        var taken = List[Int]()
        var everything = False
        var offer = List[String]()
        for i in range(len(datasets)):
            var new = tx.query(
                (
                    "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id)"
                    " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING"
                    " snapshot_id"
                ),
                Params().text(p.name).text(datasets[i]).int64(snapshots[i]),
            )
            if len(new) == 0:
                continue
            taken.append(i)
            if datasets[i] == p.keys_dataset:
                everything = True
            else:
                # Names a keyed pipeline upstream committed; others aren't
                # this pipeline's to run.
                for k in split_keys(keys[i]):
                    if k in cur:
                        offer.append(k)
        var runs = _queue(tx, p, all_keys if everything else offer, "sensor")
        if len(runs) > 0:
            for i in taken:
                _ = tx.execute(
                    (
                        "UPDATE orca.sensed SET run = $4 WHERE pipeline = $1"
                        " AND dataset = $2 AND snapshot_id = $3"
                    ),
                    Params()
                    .text(p.name)
                    .text(datasets[i])
                    .int64(snapshots[i])
                    .int64(Int64(runs[0])),
                )
        tx.commit()
        for i in taken:
            print(
                "sensed",
                String(datasets[i], "@", snapshots[i]),
                "for",
                p.name,
                String(len(runs), " runs from run ", runs[0]) if len(runs)
                > 0 else "no new keys",
            )

    def _schedule_retries(mut self, p: Registered, now: Int64) raises:
        """Gives each failed run that may be retried its time to start."""
        _ = self.db.conn.execute(
            String(
                (
                    "UPDATE orca.runs SET retry_at = $2 + ($3 * power(2, starts"
                    " - 1))::bigint + (id * 7919 % 1000) * $3 / 10000 WHERE"
                    " pipeline = $1 AND status = 'failed' AND "
                ),
                _RETRIED,
                " AND retry_at IS NULL AND starts BETWEEN 1 AND $4",
            ),
            Params()
            .text(p.name)
            .int64(now)
            .int64(self.retry_seconds)
            .int64(Int64(p.run_retries)),
        )

    def _dead(self, stale_param: StaticString) -> String:
        """SQL true for a running run whose process died: its heartbeat is
        older than the stale limit, the parameter `stale_param`, or it has
        none and was started longer ago than the start grace."""
        return String(
            "((heartbeat IS NOT NULL AND heartbeat <= now() -",
            " make_interval(secs => ",
            stale_param,
            ")) OR (heartbeat IS NULL AND (started IS NULL OR started <= now()",
            " - make_interval(secs => ",
            self.start_grace,
            "))))",
        )

    def _start(mut self, p: Registered, now: Int64) raises:
        """Starts queued runs, retries that are due and runs whose process
        died, up to `max_runs` live at once."""
        var dead = self._dead("$2")
        if (self.runner or self.ec2) and not p.image:
            print(
                "orca: pipeline",
                p.name,
                "has no image (orca register --image), so its runs wait",
            )
            return
        var tx = self.db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        # A run asked to cancel whose process died is cancelled.
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'cancelled', ended = now(),",
                " heartbeat = NULL WHERE pipeline = $1 AND status = 'running'",
                " AND cancel_requested AND ",
                dead,
            ),
            Params().text(p.name).float64(self.stale),
        )
        # A run whose process died and has no retries left has failed.
        _ = tx.execute(
            String(
                "UPDATE orca.runs SET status = 'failed', ended = now() WHERE",
                " pipeline = $1 AND status = 'running' AND ",
                _RETRIED,
                " AND ",
                dead,
                " AND starts > $3",
            ),
            Params()
            .text(p.name)
            .float64(self.stale)
            .int64(Int64(p.run_retries)),
        )
        var live = Int(
            tx.query(
                String(
                    "SELECT count(*) AS n FROM orca.runs WHERE pipeline = $1",
                    " AND status = 'running' AND NOT ",
                    dead,
                ),
                Params().text(p.name).float64(self.stale),
            )
            .row(0)
            .int64("n")
        )
        var free = p.max_runs - live
        var need = 0
        if self.runner:
            need = self.runner.value().cpu_of(p.workers)
        elif self.ec2:
            need = self.ec2.value().cpu_of(
                p.workers > 0, p.head_type, p.worker_type, p.workers
            )
        if self.budget > 0 and free > 0:
            # Across pipelines: one start at a time weighs the budget.
            _ = tx.execute(
                "SELECT pg_advisory_xact_lock($1)",
                Params().int64(_BUDGET_LOCK),
            )
            var used = Int(
                tx.query(
                    String(
                        "SELECT coalesce(sum(cpu_millis), 0) AS n FROM",
                        " orca.runs WHERE status = 'running' AND NOT ",
                        self._dead("$1"),
                    ),
                    Params().float64(self.stale),
                )
                .row(0)
                .int64("n")
            )
            free = min(free, (self.budget - used) // need if need > 0 else free)
        var ids = List[Int64]()
        var new_code = List[Bool]()
        var starts = List[Int]()
        if free > 0:
            var rows = tx.query(
                String(
                    (
                        "SELECT id, allow_new_code FROM orca.runs WHERE"
                        " pipeline = $1 AND (status"
                    ),
                    " = 'queued' OR (status = 'failed' AND ",
                    _RETRIED,
                    " AND retry_at <= $4) OR (status = 'running' AND ",
                    _RETRIED,
                    " AND ",
                    dead,
                    " AND starts <= $3)) ORDER BY id LIMIT $5",
                ),
                Params()
                .text(p.name)
                .float64(self.stale)
                .int64(Int64(p.run_retries))
                .int64(now)
                .int64(Int64(free)),
            )
            for r in rows:
                ids.append(r.int64("id"))
                new_code.append(r.bool("allow_new_code"))
            for id in ids:
                starts.append(
                    Int(
                        tx.query(
                            (
                                "UPDATE orca.runs SET status = 'running', owner"
                                " = 'scheduler', heartbeat = NULL, started ="
                                " now(), ended = NULL, starts = starts + 1,"
                                " retry_at = NULL, cpu_millis = $2 WHERE id ="
                                " $1 RETURNING starts"
                            ),
                            Params().int64(id).int64(Int64(need)),
                        )
                        .row(0)
                        .int64("starts")
                    )
                )
        tx.commit()
        for i in range(len(ids)):
            var id = ids[i]
            if self.runner:
                try:
                    self.runner.value().start(
                        RunSpec(
                            Int(id),
                            starts[i],
                            p.name,
                            p.image,
                            p.binary,
                            p.workers,
                            new_code[i],
                        )
                    )
                    print("started", p.name, "run", id, "on its cluster")
                except e:
                    print("orca: run", id, "of", p.name, "can't start:", e)
                    _ = self.db.conn.execute(
                        (
                            "UPDATE orca.runs SET status = 'failed', ended ="
                            " now() WHERE id = $1 AND status = 'running' AND"
                            " heartbeat IS NULL"
                        ),
                        Params().int64(id),
                    )
                continue
            if self.ec2:
                try:
                    self.ec2.value().start(
                        self.db,
                        Ec2Spec(
                            Int(id),
                            starts[i],
                            p.name,
                            p.image,
                            p.binary,
                            p.workers > 0,
                            p.head_type,
                            p.worker_type,
                            p.workers,
                            p.spot,
                            new_code[i],
                            p.min_workers,
                        ),
                    )
                    print("started", p.name, "run", id, "on its instances")
                except e:
                    # Retried as a run whose process failed is.
                    print("orca: run", id, "of", p.name, "can't start:", e)
                    _ = self.db.conn.execute(
                        (
                            "UPDATE orca.runs SET status = 'failed', ended ="
                            " now() WHERE id = $1 AND status = 'running' AND"
                            " heartbeat IS NULL"
                        ),
                        Params().int64(id),
                    )
                continue
            var log = String()
            if self.logs:
                log = String(run_dir(p.name, Int(id)), "/driver.log")
                var stage = stage_of(log)
                makedirs(
                    String(stage[byte = : stage.rfind("/")]), exist_ok=True
                )
            var argv: List[String] = [p.binary, "resume", String(id)]
            if new_code[i]:
                argv.append("--allow-new-code")
            var pid = spawn(argv, stage_of(log) if log else log)
            if is_remote(log):
                self._driver_logs[pid] = log
            print("started", p.name, "run", id, "pid", pid)

    def backfill(mut self, pipeline: String, keys: String) raises -> Int:
        """Queues a run for each partition in `keys` that has none; see
        `backfill`."""
        var ps = registered(self.db, pipeline)
        if len(ps) == 0:
            raise Error("orca: pipeline ", pipeline, " isn't registered")
        return backfill(self.db, ps[0], keys)
