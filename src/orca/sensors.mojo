"""Runs started by datasets: sensors, and rebuilds downstream of a dataset.

```
orca rebuild raw.events 2024-03           every pipeline downstream, in order
```

**Sensors.** A pipeline declared `sensor=True` runs when a dataset it reads
(and doesn't write itself) gets a snapshot. The scheduler learns of one in
two ways (`orca.scheduler`): from `orca.commits`, for a snapshot a
succeeded run of another pipeline committed, and by polling `<binary>
sense` every `ORCA_SENSE_SECONDS` (default 60), for a snapshot something
other than orca committed. `orca.sensed` holds a row per pipeline and
snapshot seen, with the run it started, inserted in the transaction that
queues the run, so a snapshot starts one run however many ticks (or
schedulers) see it. The snapshots a tick sees for one pipeline start one
run, over all the partitions they hold.

A sensor counts from when it's registered as one: the snapshots its
datasets have then are recorded as seen without a run, and commits from
before then are ignored. A backfill or a rebuild covers what came before.

**Partitions.** A snapshot an orca run committed holds the partition of
the step that committed it, or the run's partitions for a gather step. One
committed by something else holds what its summary's `orca.partitions`
says, and without that the downstream pipeline runs the partition its
schedule's next fire would fill now, or, unscheduled, the one now falls in.
`map_keys` moves keys between kinds by containment: a day is the month it's
in, and a month is each of its days. Keys before a pipeline's first
partition are dropped.

**Rebuilds.** `rebuild` queues runs for every pipeline downstream of a
dataset, whether or not it's a sensor, over the keys given, mapped to each
pipeline's kind. A pipeline runs at its longest distance from the dataset
(level 0 reads it), so one that reads two rebuilt datasets runs after both.
The plan is `orca.rebuild_pipelines`; `advance_rebuilds`, on every tick,
queues a level's runs once every run before it has succeeded, and stops at
one that failed or was cancelled, which resuming the run from the UI
continues. A rebuild's commits don't start sensors in pipelines the rebuild
runs itself.
"""

from postgres import Params

from orca.day import Day
from orca.key import parse_keys
from orca.lineage import flows
from orca.live import live_runs
from orca.month import Month, days_in
from orca.state import State
from orca.warehouse import Sensed


def key_kind(key: StringSlice) -> String:
    """`monthly` for a `YYYY-MM` key, `daily` for `YYYY-MM-DD`, or empty."""
    if key.byte_length() == 7:
        return "monthly"
    if key.byte_length() == 10:
        return "daily"
    return ""


def split_keys(s: StringSlice) -> List[String]:
    """Comma-separated keys, without blanks."""
    var out = List[String]()
    for part in s.split(","):
        var k = String(part.strip())
        if k:
            out.append(k^)
    return out^


def map_keys(
    keys: List[String], kind: String, start: String = ""
) raises -> List[String]:
    """`keys`, of either kind, as keys of `kind`: a day as its month, a
    month as each of its days. Those before `start` are dropped, and the
    rest are sorted, once each."""
    var out = List[String]()
    for k in keys:
        var have = key_kind(k)
        var mapped = List[String]()
        if have == "monthly" and kind == "monthly":
            mapped.append(String(Month.parse(k)))
        elif have == "daily" and kind == "daily":
            mapped.append(String(Day.parse(k)))
        elif have == "daily" and kind == "monthly":
            mapped.append(String(Day.parse(k).month_of()))
        elif have == "monthly" and kind == "daily":
            var m = Month.parse(k)
            for d in range(1, days_in(m.year, m.month) + 1):
                mapped.append(String(Day(m.year, m.month, d)))
        else:
            raise Error("orca: not a partition key: '", k, "'")
        # Keys of one kind order as text.
        for t in mapped:
            if (not start or t >= start) and t not in out:
                out.append(t)
    for i in range(1, len(out)):
        var j = i
        while j > 0 and out[j] < out[j - 1]:
            out.swap_elements(j, j - 1)
            j -= 1
    return out^


def join_keys(keys: List[String]) -> String:
    var out = String()
    for i in range(len(keys)):
        out.write("," if i else "", keys[i])
    return out^


def parse_sense(text: StringSlice) raises -> List[Sensed]:
    """What `<binary> sense` printed, a line per dataset."""
    var out = List[Sensed]()
    for line in text.splitlines():
        var f = line.split()
        if len(f) == 0:
            continue
        if len(f) != 4:
            raise Error("orca: not a sense line: '", line, "'")
        out.append(
            Sensed(
                String(f[0]),
                Int64(Int(f[1])),
                f[2] == "orca",
                String() if f[3] == "-" else String(f[3]),
            )
        )
    return out^


# ── rebuilds ─────────────────────────────────────────────────────────────────


@fieldwise_init
struct Planned(Copyable, Movable):
    """A pipeline a rebuild runs: its level, partitions and run."""

    var pipeline: String
    var level: Int
    var partitions: String
    var run: Int
    """0 until queued."""
    var status: String
    """The run's status, or empty."""


@fieldwise_init
struct Rebuild(Copyable, Movable):
    var id: Int
    var dataset: String
    var partitions: String
    var status: String
    var created: String
    var ended: String
    var plan: List[Planned]


def _levels(mut db: State, dataset: String) raises -> Dict[String, Int]:
    """Each pipeline downstream of `dataset`, at its longest distance."""
    var readers = Dict[String, List[String]]()
    var writes = Dict[String, List[String]]()
    var names = List[String]()
    for f in flows(db):
        if f.pipeline not in names:
            names.append(f.pipeline)
        if f.writes:
            if f.pipeline not in writes:
                writes[f.pipeline] = List[String]()
            writes[f.pipeline].append(f.dataset)
        else:
            if f.dataset not in readers:
                readers[f.dataset] = List[String]()
            readers[f.dataset].append(f.pipeline)
    var level = Dict[String, Int]()
    for p in readers.get(dataset, List[String]()):
        level[p] = 0
    # Relax along pipeline edges; registration refused cycles, so this
    # settles within one pass per pipeline.
    for _ in range(len(names) + 1):
        var moved = False
        var at = List[String]()
        for e in level.items():
            at.append(e.key)
        for p in at:
            for ds in writes.get(p, List[String]()):
                for q in readers.get(ds, List[String]()):
                    if q != p and level.get(q, -1) < level[p] + 1:
                        level[q] = level[p] + 1
                        moved = True
        if not moved:
            break
    return level^


def rebuild(mut db: State, dataset: String, keys: String) raises -> Int:
    """Plans a rebuild downstream of `dataset` over `keys` (`KEY[..KEY][,...]`
    of either kind) and returns its ID; `advance_rebuilds` queues its runs.
    Raises, saying why, if there's nothing to rebuild."""
    var given = List[String]()
    var parts = split_keys(keys)
    if len(parts) == 0:
        raise Error("orca: a rebuild needs partitions")
    if key_kind(parts[0].split("..")[0]) == "daily":
        for k in parse_keys[Day](keys):
            given.append(String(k))
    else:
        for k in parse_keys[Month](keys):
            given.append(String(k))
    var level = _levels(db, dataset)
    if len(level) == 0:
        raise Error("orca: no registered pipeline reads ", dataset)
    var plan = List[Planned]()
    var rows = db.conn.query(
        (
            "SELECT name, binary_path IS NOT NULL AS has_binary,"
            " coalesce(describe->'partitions'->>'kind', 'monthly') AS kind,"
            " coalesce(describe->'partitions'->>'start', '') AS start FROM"
            " orca.pipelines ORDER BY name"
        ),
        Params(),
    )
    for r in rows:
        var name = r.text("name")
        if name not in level:
            continue
        if not r.bool("has_binary"):
            raise Error(
                "orca: pipeline ",
                name,
                " is downstream of ",
                dataset,
                " but isn't registered with a binary (orca register)",
            )
        var ks = map_keys(given, r.text("kind"), r.text("start"))
        if len(ks) > 0:
            plan.append(Planned(name, level[name], join_keys(ks), 0, ""))
    if len(plan) == 0:
        raise Error(
            "orca: every partition given is before the first of each",
            " pipeline downstream of ",
            dataset,
        )
    var tx = db.conn.begin()
    var id = (
        tx.query(
            (
                "INSERT INTO orca.rebuilds (dataset, partitions, status) VALUES"
                " ($1, $2, 'running') RETURNING id"
            ),
            Params().text(dataset).text(join_keys(given)),
        )
        .row(0)
        .int64("id")
    )
    for p in plan:
        _ = tx.execute(
            (
                "INSERT INTO orca.rebuild_pipelines (rebuild, pipeline, level,"
                " partitions) VALUES ($1, $2, $3, $4)"
            ),
            Params()
            .int64(id)
            .text(p.pipeline)
            .int64(Int64(p.level))
            .text(p.partitions),
        )
    tx.commit()
    return Int(id)


def rebuilds(
    mut db: State, id: Int = 0, dataset: String = "", limit: Int = 20
) raises -> List[Rebuild]:
    """Rebuilds, newest first: the one `id` names, or those of `dataset`, or
    all; each with its plan in run order."""
    var out = List[Rebuild]()
    var rows = db.conn.query(
        (
            "SELECT id, dataset, partitions, status, to_char(created AT TIME"
            " ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created,"
            " coalesce(to_char(ended AT TIME ZONE 'UTC',"
            " 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '') AS ended FROM"
            " orca.rebuilds WHERE ($1 = 0 OR id = $1) AND ($2 = '' OR dataset"
            " = $2) ORDER BY id DESC LIMIT $3"
        ),
        Params().int64(Int64(id)).text(dataset).int64(Int64(limit)),
    )
    for r in rows:
        var rid = r.int64("id")
        var plan = List[Planned]()
        for p in db.conn.query(
            (
                "SELECT rp.pipeline, rp.level, rp.partitions, coalesce(rp.run,"
                " 0) AS run, coalesce(x.status, '') AS status FROM"
                " orca.rebuild_pipelines rp LEFT JOIN orca.runs x ON x.id ="
                " rp.run WHERE rp.rebuild = $1 ORDER BY rp.level, rp.pipeline"
            ),
            Params().int64(rid),
        ):
            plan.append(
                Planned(
                    p.text("pipeline"),
                    Int(p.int64("level")),
                    p.text("partitions"),
                    Int(p.int64("run")),
                    p.text("status"),
                )
            )
        out.append(
            Rebuild(
                Int(rid),
                r.text("dataset"),
                r.text("partitions"),
                r.text("status"),
                r.text("created"),
                r.text("ended"),
                plan^,
            )
        )
    return out^


def advance_rebuilds(mut db: State) raises:
    """Queues the next level of each rebuild whose levels before it have
    all succeeded, and brings each rebuild's status up to date."""
    var ids = List[Int64]()
    for r in db.conn.query(
        (
            "SELECT id FROM orca.rebuilds WHERE status IN ('running',"
            " 'failed') ORDER BY id"
        ),
        Params(),
    ):
        ids.append(r.int64("id"))
    for id in ids:
        var tx = db.conn.begin()
        var still = tx.query(
            (
                "SELECT id FROM orca.rebuilds WHERE id = $1 AND status IN"
                " ('running', 'failed') FOR UPDATE"
            ),
            Params().int64(id),
        )
        if len(still) == 0:
            tx.commit()
            continue
        var rows = tx.query(
            (
                "SELECT rp.pipeline, rp.level, rp.partitions, rp.run IS NULL AS"
                " unqueued, coalesce(x.status, '') AS status FROM"
                " orca.rebuild_pipelines rp LEFT JOIN orca.runs x ON x.id ="
                " rp.run WHERE rp.rebuild = $1 ORDER BY rp.level, rp.pipeline"
            ),
            Params().int64(id),
        )
        var pipeline = List[String]()
        var level = List[Int]()
        var keys = List[String]()
        var unqueued = List[Bool]()
        var status = List[String]()
        for r in rows:
            pipeline.append(r.text("pipeline"))
            level.append(Int(r.int64("level")))
            keys.append(r.text("partitions"))
            unqueued.append(r.bool("unqueued"))
            status.append(r.text("status"))
        var now_status = String("succeeded")
        var i = 0
        while i < len(pipeline):
            var j = i
            while j < len(pipeline) and level[j] == level[i]:
                j += 1
            var queue = List[Int]()
            var waiting = False
            var stopped = False
            for k in range(i, j):
                if unqueued[k]:
                    queue.append(k)
                elif status[k] == "failed" or status[k] == "cancelled":
                    stopped = True
                elif status[k] != "succeeded":
                    waiting = True
            for k in queue:
                # A live run of one of its keys runs first: the level's run
                # waits for it instead of running beside it.
                _ = tx.query(
                    (
                        "SELECT name FROM orca.pipelines WHERE name = $1 FOR"
                        " UPDATE"
                    ),
                    Params().text(pipeline[k]),
                )
                var live = live_runs(tx, pipeline[k])
                var busy = False
                for key in split_keys(keys[k]):
                    if key in live:
                        busy = True
                if busy:
                    waiting = True
                    continue
                var run = (
                    tx.query(
                        (
                            "INSERT INTO orca.runs (pipeline, registry_hash,"
                            " partitions, trigger, status, rebuild) SELECT"
                            " name, registry_hash, $2, 'rebuild', 'queued', $3"
                            " FROM orca.pipelines WHERE name = $1 RETURNING id"
                        ),
                        Params().text(pipeline[k]).text(keys[k]).int64(id),
                    )
                    .row(0)
                    .int64("id")
                )
                _ = tx.execute(
                    (
                        "UPDATE orca.rebuild_pipelines SET run = $3 WHERE"
                        " rebuild = $1 AND pipeline = $2"
                    ),
                    Params().int64(id).text(pipeline[k]).int64(run),
                )
                print(
                    "rebuild",
                    id,
                    "queued",
                    pipeline[k],
                    "run",
                    run,
                    "for",
                    keys[k],
                )
            if stopped:
                now_status = "failed"
                break
            if waiting or len(queue) > 0:
                now_status = "running"
                break
            i = j
        _ = tx.execute(
            (
                "UPDATE orca.rebuilds SET status = $2, ended = CASE WHEN $2 ="
                " 'succeeded' THEN now() END WHERE id = $1"
            ),
            Params().int64(id).text(now_status),
        )
        tx.commit()
