"""Queueing runs over keys: backfills, the keys a pipeline has done or
holds, and what a rerun or an append refuses.
"""

from std.os.path import exists
from postgres import (
    Params,
    Transaction,
)
from orca.day import Day
from orca.key import parse_keys
from orca.live import live_runs
from orca.month import Month
from orca.name import (
    Name,
    parse_names,
    sort_texts,
)
from orca.proc import (
    binary_seconds,
    capture,
)
from orca.state import State
from orca.registered import Registered


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


def parse_keys_output(
    binary: String, text: String
) raises -> Tuple[Int64, List[String]]:
    """What `<binary> keys` printed: its keys snapshot, then its keys."""
    var lines = text.splitlines()
    if len(lines) == 0:
        raise Error("orca: ", binary, " keys printed nothing")
    var keys = List[String]()
    for i in range(1, len(lines)):
        if lines[i]:
            keys.append(String(lines[i]))
    return (Int64(Int(lines[0])), keys^)


def current_keys(
    p: Registered, timeout: Float64 = -1
) raises -> Tuple[Int64, List[String]]:
    """A keyed pipeline's keys snapshot and keys, as `<binary> keys`
    prints them, killed past `timeout` seconds (by default
    `ORCA_BINARY_SECONDS`)."""
    var got = capture(
        [p.binary, "keys"], timeout if timeout >= 0 else binary_seconds()
    )
    if got[0] != 0:
        raise Error("orca: ", p.binary, " keys exited ", got[0], ": ", got[1])
    return parse_keys_output(p.binary, got[1])


def keep_keys(
    mut db: State, pipeline: String, keys: Tuple[Int64, List[String]]
) raises:
    """Records the keys `<binary> keys` printed, in `orca.keys`."""
    var text = String()
    for i in range(len(keys[1])):
        text.write("\n" if i else "", keys[1][i])
    _ = db.conn.execute(
        (
            "INSERT INTO orca.keys (pipeline, snapshot_id, keys) VALUES ($1,"
            " $2, $3) ON CONFLICT (pipeline) DO UPDATE SET snapshot_id ="
            " EXCLUDED.snapshot_id, keys = EXCLUDED.keys, read_at = now()"
        ),
        Params().text(pipeline).int64(keys[0]).text(text),
    )


@fieldwise_init
struct KeptKeys(Copyable, Movable):
    """A pipeline's keys as `orca.keys` has them."""

    var snapshot: Int64
    var keys: List[String]
    var read_at: String
    var age: Float64
    """Seconds since they were read."""


def kept_keys(mut db: State, pipeline: String) raises -> Optional[KeptKeys]:
    var rows = db.conn.query(
        (
            "SELECT snapshot_id, keys, to_char(read_at AT TIME ZONE 'UTC',"
            ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS at, extract(epoch FROM'
            " now() - read_at)::float8 AS age FROM orca.keys WHERE pipeline ="
            " $1"
        ),
        Params().text(pipeline),
    )
    if len(rows) == 0:
        return None
    var r = rows.row(0)
    var keys = List[String]()
    for k in r.text("keys").split("\n"):
        if k:
            keys.append(String(k))
    return KeptKeys(
        r.int64("snapshot_id"), keys^, r.text("at"), r.float64("age")
    )


def file_names(path: String) raises -> String:
    """The names in the file at `path`, a line each, as `NAME,NAME,...`
    (`@file:<path>`)."""
    var text: String
    with open(path, "r") as f:
        text = f.read()
    var joined = String()
    var at = 0
    for line in text.splitlines():
        at += 1
        var n = line.strip()
        if n:
            # The error names the line, not its text: a file's other
            # contents stay out of the message.
            try:
                _ = Name.parse(n)
            except:
                raise Error("orca: line ", at, " of ", path, " isn't a name")
            joined.write("," if joined else "", n)
    if not joined:
        raise Error("orca: no names in ", path)
    return joined^


def kind_mismatch(mut db: State, describe: String) raises -> String:
    """How the pipeline `describe` gives would make a sensor run keys of the
    other kind, a keyed sensor reading a dataset a time-partitioned
    pipeline writes or the other way round, or empty if it wouldn't.

    A keyed sensor's own keys dataset needs no mapping, whoever writes it:
    a new snapshot of it offers the keys it holds, which are names already.
    So a dated pipeline may write the keys a keyed sensor runs."""
    var rows = db.conn.query(
        (
            "WITH d AS (SELECT name, describe FROM orca.pipelines WHERE name"
            " <> $1::jsonb->>'pipeline' UNION ALL SELECT $1::jsonb->>"
            "'pipeline', $1::jsonb), io AS (SELECT DISTINCT d.name,"
            " coalesce(d.describe->'partitions'->>'kind', 'monthly') AS"
            " pkind, coalesce((d.describe->>'sensor')::boolean, false) AS"
            " sensor, k.kind, x.ds, coalesce(d.describe->'partitions'->>"
            "'dataset', '') AS keys FROM d, jsonb_array_elements(d.describe->"
            "'steps') AS s(st), (VALUES ('reads'), ('writes')) AS k(kind),"
            " jsonb_array_elements_text(coalesce(s.st->k.kind, '[]')) AS"
            " x(ds) UNION SELECT d.name, 'keyed', coalesce((d.describe->>"
            "'sensor')::boolean, false), 'reads', d.describe->'partitions'->>"
            "'dataset', d.describe->'partitions'->>'dataset' FROM d WHERE"
            " d.describe->'partitions'->>'dataset' IS NOT NULL) SELECT w.name"
            " AS writer, w.pkind AS wkind, r.name AS reader, r.pkind AS rkind,"
            " w.ds FROM io w JOIN io r ON r.ds = w.ds AND r.kind = 'reads'"
            " WHERE w.kind = 'writes' AND w.name <> r.name AND r.sensor AND"
            " (w.pkind = 'keyed') <> (r.pkind = 'keyed') AND NOT (r.pkind ="
            " 'keyed' AND r.ds = r.keys) AND $1::jsonb->>'pipeline' IN"
            " (w.name, r.name) ORDER BY 1, 3, 5 LIMIT 1"
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
        var list = file_names(String(keys[byte=6:])) if keys.startswith(
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
    " false END UNION ALL SELECT k.key FROM k WHERE k.key IN (SELECT"
    " unnest(string_to_array(r.partitions, ',')) FROM orca.runs r JOIN p ON"
    " r.registry_hash = p.registry_hash WHERE NOT EXISTS (SELECT 1 FROM m)"
    " AND r.pipeline = $1 AND r.status = 'succeeded')"
)
"""Of the comma-separated keys `$2`, those done for pipeline `$1`: every
mapped step has succeeded for the key in a run of the pipeline's current
code. A pipeline with no mapped step has a key done when a run of its
current code over it succeeded. The two are separate halves, each empty
when the other applies. The first looks each key's steps up by index (the
`CASE` keeps Postgres from hashing every step of every pipeline instead);
the second joins the keys to its runs' keys, read once, rather than
scanning the runs again for each key (1.2 s for 1,500 keys over 30,000
runs when it was a `CASE` branch too, 10 ms now)."""


@fieldwise_init
struct Queued(Movable):
    """What a backfill or a keyed sensor queued, and the keys it left."""

    var runs: List[Int]
    var done: List[String]
    """Keys left because they're done."""
    var live: List[String]
    """Keys left because a live run has them, that run in `live_runs`."""
    var live_runs: List[Int]
    var keys: List[String]
    """The keys queued (or, in a preview, that would be), sorted."""
    var batches: Int
    """How many runs those keys make, `keys_per_run` to a run; a preview
    counts them without queueing any."""

    def __init__(out self):
        self.runs = List[Int]()
        self.done = List[String]()
        self.live = List[String]()
        self.live_runs = List[Int]()
        self.keys = List[String]()
        self.batches = 0

    def skipped(self) -> List[String]:
        """A line for each reason keys were left, or none."""
        var out = List[String]()
        if len(self.done) > 0:
            var line = String("skipped ", len(self.done), " done: ")
            _list(line, self.done)
            out.append(line^)
        if len(self.live) > 0:
            var line = String("skipped ", len(self.live), " in live runs: ")
            var named = List[String]()
            for i in range(len(self.live)):
                named.append(
                    String(self.live[i], " (run ", self.live_runs[i], ")")
                )
            _list(line, named)
            out.append(line^)
        return out^


def _list(mut line: String, items: List[String]):
    """The first ten of `items`, comma-separated, and how many more."""
    for i in range(min(len(items), 10)):
        line.write(", " if i else "", items[i])
    if len(items) > 10:
        line.write(", and ", len(items) - 10, " more")


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
    mut db: State,
    p: Registered,
    keys: List[String],
    trigger: String,
    rerun: Bool = False,
) raises -> Queued:
    """Queues runs of `p` over those of `keys` that aren't done and aren't
    in a live run, sorted, `keys_per_run` to a run; returns the runs, and
    the keys it left and why. With
    `rerun`, keys that are done are queued too, but never one in a live
    run. Under a lock on the pipeline's row, so two backfills don't both
    queue a key."""
    var tx = db.conn.begin()
    var got = _queue(tx, p, keys, trigger, rerun)
    tx.commit()
    return got^


def _queue(
    mut tx: Transaction,
    p: Registered,
    keys: List[String],
    trigger: String,
    rerun: Bool = False,
    dry: Bool = False,
) raises -> Queued:
    """`queue_keys` in `tx`; with `dry`, what it would queue, queueing
    nothing."""
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    var got = Queued()
    var live = live_runs(tx, p.name)
    var joined = String()
    for i in range(len(keys)):
        joined.write("," if i else "", keys[i])
    var done = Dict[String, Bool]()
    if not rerun:
        for r in tx.query(DONE_SQL, Params().text(p.name).text(joined)):
            done[r.text("key")] = True
    var seen = Dict[String, Bool]()
    var todo = List[String]()
    for k in keys:
        if k in seen:
            continue
        seen[k] = True  # each once
        var in_run = live.get(k)
        if in_run:
            got.live.append(k)
            got.live_runs.append(in_run.value())
        elif k in done:
            got.done.append(k)
        else:
            todo.append(k)
    sort_texts(todo)
    got.keys = todo.copy()
    ref runs = got.runs
    var i = 0
    while i < len(todo):
        var batch = String()
        var j = i
        while j < len(todo) and j < i + p.keys_per_run:
            batch.write("," if j > i else "", todo[j])
            j += 1
        got.batches += 1
        if dry:
            i = j
            continue
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
    return got^


comptime APPENDING_SQL = (
    "SELECT x.st->>'name' AS step, w.ds FROM orca.pipelines p,"
    " jsonb_array_elements(p.describe->'steps') WITH ORDINALITY AS x(st, n),"
    " jsonb_array_elements_text(coalesce(x.st->'writes', '[]'::jsonb)) AS"
    " w(ds) WHERE p.name = $1 AND x.n > $2 AND NOT coalesce(x.st->'replaces',"
    " '[]'::jsonb) ? w.ds ORDER BY x.n, w.ds"
)
"""`step` and `ds` for each dataset a step of pipeline `$1` after its
first `$2` declares in `writes=` and not in `replaces=`."""


def appending_writes(
    mut db: State, pipeline: String, from_step: Int = 0
) raises -> List[String]:
    """`step <name> writes <dataset>` for each dataset a step of `pipeline`
    writes without replacing it (in `writes=`, not `replaces=`), from its
    `from_step`th step on (0, the first): what a new run over a key that
    already has rows appends to again. A pipeline registered before
    `replaces=` existed appends to all it writes."""
    var out = List[String]()
    for r in db.conn.query(
        APPENDING_SQL, Params().text(pipeline).int64(Int64(from_step))
    ):
        out.append(String("step ", r.text("step"), " writes ", r.text("ds")))
    return out^


def append_again(
    pipeline: String, what: String, writes: List[String], how: String
) -> String:
    """Why a rerun of `what` is refused: it appends again (`writes`, from
    `appending_writes`). `how` says how to rerun anyway."""
    var line = String(
        "orca: rerunning ",
        what,
        " appends again to what ",
        pipeline,
        " appended to before, duplicating those rows: ",
    )
    _list(line, writes)
    line.write(
        ". Declare a dataset in its step's replaces= to have a new run",
        " replace its partitions (orca.tables), or ",
        how,
    )
    return line^


comptime ALLOW_APPEND_HOW = "pass --allow-append to append anyway"


def rerun_refusal(mut db: State, p: Registered, keys: String) raises -> String:
    """Why `orca backfill --rerun` of `keys` is refused without
    `--allow-append`, or empty: some are done, and the pipeline's steps
    append to datasets (`appending_writes`), so a new run of them would
    leave their rows twice."""
    var writes = appending_writes(db, p.name)
    if len(writes) == 0:
        return ""
    var done = done_keys(db, p.name, partition_keys(p, keys))
    if len(done) == 0:
        return ""
    var names = List[String]()
    for k in done.keys():
        names.append(k)
    sort_texts(names)
    var what = String()
    _list(what, names)
    what.write(" (done)")
    return append_again(p.name, what, writes, ALLOW_APPEND_HOW)


def backfill(
    mut db: State,
    p: Registered,
    keys: String,
    rerun: Bool = False,
    allow_append: Bool = False,
) raises -> Queued:
    """Queues runs for the keys in `keys` that aren't done and aren't in a
    live run, `keys_per_run` to a run; returns the runs it queued and the
    keys it left. With `rerun` (`orca backfill --rerun`) the done ones are
    queued again too, as for a day whose source has changed since; unless
    `allow_append`, that's refused for a pipeline whose steps append to
    what they write (`rerun_refusal`)."""
    if rerun and not allow_append:
        var why = rerun_refusal(db, p, keys)
        if why:
            raise Error(why)
    return queue_keys(db, p, partition_keys(p, keys), "backfill", rerun)


def preview_backfill(
    mut db: State,
    p: Registered,
    keys: String,
    rerun: Bool = False,
    allow_append: Bool = False,
) raises -> Queued:
    """What `backfill` would queue now, queueing nothing: the keys, how many
    runs they make, and the keys it would leave and why; or its refusal."""
    if rerun and not allow_append:
        var why = rerun_refusal(db, p, keys)
        if why:
            raise Error(why)
    var texts = partition_keys(p, keys)
    var tx = db.conn.begin()
    var got = _queue(tx, p, texts, "backfill", rerun, dry=True)
    tx.rollback()
    return got^


def _last_line(path: String) -> String:
    """The last line with text in the file at `path`, at most 300 bytes of
    it, or empty if there's none or it can't be read."""
    if not path or not exists(path):
        return ""
    try:
        var text: String
        with open(path, "r") as f:
            text = f.read()
        var lines = text.splitlines()
        for i in range(len(lines) - 1, -1, -1):
            var line = String(lines[i].strip())
            if line and not line.startswith("--- start "):
                var n = line.byte_length()
                if n > 300:
                    # Cut where a character starts.
                    n = 300
                    while n > 0 and (line.as_bytes()[n] & 0xC0) == 0x80:
                        n -= 1
                    return String(line[byte=:n])
                return line^
    except:
        pass
    return ""
