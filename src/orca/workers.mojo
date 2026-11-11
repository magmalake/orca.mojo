"""Remote workers, the server's side: worker tokens, claims, and the store
calls a worker's runs make over `orca ui` (`orca.http_store`).

```
orca worker-token create --pool bgent [--days N] [--label L]   prints the token once
orca worker-token list                                          id, pool, label, worker, last seen
orca worker-token revoke <id>
```

A pipeline placed `worker:<pool>` (`placement` in orca.toml, or `orca
register --placement worker:<pool>`) has its runs started by a worker of
that pool (`orca worker`, `orca.worker`), never by the scheduler, which
keeps them queued, `held` "waiting for a worker of pool <pool>", requeues
one whose heartbeat went stale (a worker that died, or a laptop that went
to sleep) as it would start any dead run again, and fails one that has no
retries left. A run whose worker is gone (not seen for half the stale
limit, or seen since without it) is queued again whatever its trigger. A
pipeline with `fills = "all"` has its fill runs queued the same way, held
for a worker, at most `fill_max_runs` of them queued or running across the
pool (`orca.fill`).

**Tokens.** A worker token is an API token (`orca.tokens`: `orca_` and 64
hex digits, kept only as its SHA-256), in `orca.workers` rather than
`orca.api_tokens`, scoped to one pool: it can claim that pool's runs and
make the store calls of the runs it claimed, and nothing else; a user's
token gets nothing here, and a worker token gets nothing elsewhere. It
lasts `--days` (default and at most `ORCA_WORKER_TOKEN_DAYS`, 365). A
revoked token stays listed, as revoked, and is answered 401.

**The API** (`orca.api.worker`): `/api/worker/claim`, the worker's
heartbeat and its claims; `/api/worker/release`, a run it won't start;
`/api/worker/exited`, a run's process ended; `/api/worker/store/<call>`,
`store_call`, a `RunStore` call of a run it claimed.

A claim takes, under its pipeline's lock, a queued run whose pipeline isn't
paused and has fewer than `max_runs` live runs (a fill run: fewer than
`fill_max_runs` running fill runs, inside its `fill_window`, with fills on),
its other runs before its fill runs (the oldest of each pipeline's, so one
pipeline's queue at its cap never holds up another's), and starts it as the
scheduler starts a run: `running`, owner `worker`, no heartbeat yet (the
run's process claims it, within the start grace), one more start. Each
claim is a row of `orca.audit` (user `worker:<label>`, source `worker`).
**Versions.** A run needs its pipeline's image (`--image`, `image` in
orca.toml) to be the worker's (`ORCA_IMAGE`), both empty included: a worker
of another image isn't given it, and it stays queued, held saying why. The
worker checks the spec again before it starts anything (`orca.worker`).
"""

from std.os import getenv

from json import Value, loads
from postgres import Params

from orca.admission import (
    PressureRow,
    Report,
    alert_stuck,
    pressure_rows,
    record as record_pressure,
    resolve_stuck,
    row_json,
    worker_host,
)
from orca.control import WORKER_PREFIX, pool_of, pool_problem
from orca.http_store import (
    STORE_PATH,
    Body,
    bytes_of_hex,
    pins_json,
    prior_json,
    run_info_json,
    steps_json,
)
from orca.civil import now
from orca.json import quote
from orca.live import dead_sql, stale_limit, start_grace
from orca.cgroup import growth_pct_of, oom_text, raised_mb, raised_request
from orca.recommend import RunRequest
from orca.resources import Grant, Request, Room, admit
from orca.registered import (
    recorded_cpus,
    recorded_memory_gb,
    registered,
    run_request,
)
from orca.state import PgStore, State, beat_run
from orca.store import FINAL, TableSnapshot
from orca.tokens import (
    Issued,
    clean_label,
    is_token,
    new_token,
    token_hash,
)
from orca.usage import marks_of, record as record_usage

comptime WORKER_OWNER = "worker"
"""A run's owner once a worker claimed it, until its process does."""
comptime DEFAULT_WORKER_TOKEN_DAYS = 365


# ── pools and placement ──────────────────────────────────────────────────────


def version_refusal(needed: String, have: String) -> String:
    """Why a worker of image `have` can't run a run whose pipeline is image
    `needed`, or empty when they're the same."""
    if needed == have:
        return ""
    return String(
        "the run needs image ",
        needed if needed else "(none)",
        " and the worker is ",
        have if have else "(none: ORCA_IMAGE isn't set)",
    )


# ── tokens ───────────────────────────────────────────────────────────────────


def worker_token_days(asked: Int = 0) raises -> Int:
    """`asked` if from 1 to `ORCA_WORKER_TOKEN_DAYS` (default 365, at most
    3650), else that most."""
    var most: Int
    try:
        most = Int(
            getenv("ORCA_WORKER_TOKEN_DAYS", String(DEFAULT_WORKER_TOKEN_DAYS))
        )
    except:
        raise Error("orca: ORCA_WORKER_TOKEN_DAYS isn't a count of days")
    if most < 1 or most > 3650:
        raise Error("orca: ORCA_WORKER_TOKEN_DAYS is 1 to 3650, not ", most)
    return asked if asked >= 1 and asked < most else most


def create_worker_token(
    mut db: State, pool: String, label: String, days: Int, by: String
) raises -> Issued:
    """A new token for a worker of `pool`, which only this answer holds."""
    var why = pool_problem(pool)
    if why:
        raise Error("orca: ", why)
    var token = new_token()
    var expires = (
        db.conn.query(
            (
                "INSERT INTO orca.workers (token_hash, pool, label, created_by,"
                " expires) VALUES ($1, $2, $3, $4, now() + make_interval(days"
                " => $5)) RETURNING to_char(expires AT TIME ZONE 'UTC',"
                ' \'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS expires'
            ),
            Params()
            .text(token_hash(token))
            .text(pool)
            .text(clean_label(label if label else pool))
            .text(by)
            .int32(Int32(days)),
        )
        .row(0)
        .text("expires")
    )
    return Issued(token^, expires^)


def revoke_worker_token(mut db: State, id: Int) raises -> Bool:
    """Revokes worker token `id`; False if it's no live one."""
    return (
        db.conn.execute(
            (
                "UPDATE orca.workers SET revoked = now() WHERE id = $1 AND"
                " revoked IS NULL"
            ),
            Params().int64(Int64(id)),
        )
        == 1
    )


@fieldwise_init
struct WorkerAuth(Copyable, Movable):
    """The worker a request's token is."""

    var id: Int
    var pool: String
    var label: String

    def placement(self) -> String:
        return String(WORKER_PREFIX, self.pool)

    def user(self) -> String:
        """Who it is in `orca.audit`."""
        return String(WORKER_PREFIX, self.label)


def worker_of(mut db: State, token: String) raises -> Optional[WorkerAuth]:
    """The live worker token `token` is, marked seen; None for any other."""
    if not is_token(token):
        return None
    var rows = db.conn.query(
        (
            "UPDATE orca.workers SET last_seen = now() WHERE token_hash = $1"
            " AND revoked IS NULL AND expires > now() RETURNING id, pool, label"
        ),
        Params().text(token_hash(token)),
    )
    if len(rows) != 1:
        return None
    var r = rows.row(0)
    return WorkerAuth(Int(r.int64("id")), r.text("pool"), r.text("label"))


@fieldwise_init
struct WorkerRow(Copyable, Movable):
    """A worker token, and what its worker last said of itself."""

    var id: Int
    var pool: String
    var label: String
    var state: String
    """`live`, `revoked` or `expired`."""
    var name: String
    """The worker's host name, once it has claimed; else empty."""
    var image: String
    var last_seen: String
    var seen_ago: Int
    """Seconds since it was last seen; -1 if never."""
    var cpus: Int
    var memory_mb: Int
    var running: String
    """The runs it's running, comma-separated, as it said."""
    var expires: String
    var pressure: Optional[PressureRow]
    """Its host's newest memory pressure reading (`orca.admission`), if
    it sent one."""


def worker_rows(mut db: State) raises -> List[WorkerRow]:
    """Every worker token, newest first."""
    comptime ts = '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\''
    var out = List[WorkerRow]()
    var pressure = Dict[String, PressureRow]()
    for p in pressure_rows(db):
        pressure[p.host] = p.copy()
    for r in db.conn.query(
        String(
            "SELECT id, pool, label, CASE WHEN revoked IS NOT NULL THEN",
            " 'revoked' WHEN expires <= now() THEN 'expired' ELSE 'live' END",
            " AS state, coalesce(name, '') AS name, coalesce(image, '') AS",
            " image, coalesce(to_char(last_seen AT TIME ZONE 'UTC', ",
            ts,
            "), '') AS last_seen, coalesce(extract(epoch FROM now() -",
            " last_seen)::bigint, -1) AS ago, coalesce(cpus, 0) AS cpus,",
            " coalesce(memory_mb, 0) AS mem, coalesce(running, '') AS running,",
            " to_char(expires AT TIME ZONE 'UTC', ",
            ts,
            ") AS expires FROM orca.workers ORDER BY id DESC",
        )
    ):
        out.append(
            WorkerRow(
                Int(r.int64("id")),
                r.text("pool"),
                r.text("label"),
                r.text("state"),
                r.text("name"),
                r.text("image"),
                r.text("last_seen"),
                Int(r.int64("ago")),
                Int(r.int64("cpus")),
                Int(r.int64("mem")),
                r.text("running"),
                r.text("expires"),
                pressure.get(worker_host(Int(r.int64("id")))),
            )
        )
    return out^


def ran_on(mut db: State, run: Int) raises -> String:
    """The worker a run's latest start was claimed by, as `<name> (token
    <id>, <label>)`, or empty for a run no worker claimed."""
    var rows = db.conn.query(
        (
            "SELECT w.id, w.label, coalesce(w.name, '') AS name FROM orca.runs"
            " r JOIN orca.workers w ON w.id = r.worker WHERE r.id = $1 AND"
            " r.placement LIKE 'worker:%'"
        ),
        Params().int64(Int64(run)),
    )
    if len(rows) == 0:
        return ""
    var r = rows.row(0)
    return String(
        r.text("name") if r.text("name") else String("a worker"),
        " (token ",
        r.int64("id"),
        ", ",
        r.text("label"),
        ")",
    )


# ── claims ───────────────────────────────────────────────────────────────────


@fieldwise_init
struct WorkerRun(Copyable, Movable):
    """What a worker is given to run: a start of a run, and how."""

    var run: Int
    var start: Int
    var pipeline: String
    var binary: String
    var registry_hash: String
    var partitions: String
    var allow_new_code: Bool
    var image: String
    """The image it needs the worker to be (`version_refusal`)."""
    var env: List[Tuple[String, String]]
    """The pipeline's own variables (`env` in orca.toml)."""
    var secrets: List[Tuple[String, String]]
    """Its secrets: the variable and the ID the worker reads it by, with its
    own `ORCA_SECRET_FETCH`."""
    var grant: Grant
    """What this start was granted of the worker's host (`orca.resources`);
    none for a pipeline without a request, or a worker that didn't say
    what it can grant."""


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


def worker_run_json(w: WorkerRun) -> String:
    return (
        Body()
        .int("run", Int64(w.run))
        .int("start", Int64(w.start))
        .text("pipeline", w.pipeline)
        .text("binary", w.binary)
        .text("registry_hash", w.registry_hash)
        .text("partitions", w.partitions)
        .bool("allow_new_code", w.allow_new_code)
        .text("image", w.image)
        .raw("env", _pairs_json(w.env, "name", "value"))
        .raw("secrets", _pairs_json(w.secrets, "name", "id"))
        .int("grant_cpus", Int64(w.grant.cpus))
        .int("grant_memory_mb", Int64(w.grant.memory_mb))
        .finish()
    )


def worker_run_of(v: Value) raises -> WorkerRun:
    return WorkerRun(
        Int(v["run"].int_value()),
        Int(v["start"].int_value()),
        v["pipeline"].string_value(),
        v["binary"].string_value(),
        v["registry_hash"].string_value(),
        v["partitions"].string_value(),
        v["allow_new_code"].bool_value(),
        v["image"].string_value(),
        _pairs_of(v["env"], "name", "value"),
        _pairs_of(v["secrets"], "name", "id"),
        Grant(
            Int(v["grant_cpus"].int_value()) if String("grant_cpus")
            in v else 0,
            Int(v["grant_memory_mb"].int_value()) if String("grant_memory_mb")
            in v else 0,
        ),
    )


def _seen(mut db: State, w: WorkerAuth, v: Value) raises:
    """Keeps what the worker says of itself, for Ops."""
    var cpus = Int64(0)
    var mem = Int64(0)
    if String("cpus") in v and v["cpus"].is_int():
        cpus = v["cpus"].int_value()
    if String("memory_mb") in v and v["memory_mb"].is_int():
        mem = v["memory_mb"].int_value()
    _ = db.conn.execute(
        (
            "UPDATE orca.workers SET name = nullif($2, ''), image ="
            " nullif($3, ''), cpus = nullif($4, 0), memory_mb = nullif($5, 0),"
            " running = nullif($6, '') WHERE id = $1"
        ),
        Params()
        .int64(Int64(w.id))
        .text(text_field(v, "name"))
        .text(text_field(v, "image"))
        .int32(Int32(cpus))
        .int64(mem)
        .text(text_field(v, "running")),
    )
    if String("pressure") in v and v["pressure"].is_object():
        _pressure_seen(db, w, v, Report.from_json(v["pressure"]))


def _pressure_seen(mut db: State, w: WorkerAuth, v: Value, r: Report) raises:
    """Keeps a worker's memory pressure (`orca.admission`), says on its
    pool's queued runs that they wait for it while it claims none, and
    alerts once a hold has nothing of orca's to wait for."""
    record_pressure(db, worker_host(w.id), r)
    var name = text_field(v, "name")
    var who = String("worker ", name) if name else w.user()
    var suffix = String(" on ", who)
    if r.held:
        _ = db.conn.execute(
            (
                "UPDATE orca.runs r SET held = $2 FROM orca.pipelines p WHERE"
                " p.name = r.pipeline AND p.placement = $1 AND r.status ="
                " 'queued' AND r.trigger <> 'fill' AND (r.held IS NULL OR"
                " starts_with(r.held, 'held: waiting for a worker of pool ')"
                " OR (starts_with(r.held, 'waiting: memory pressure ') AND"
                " right(r.held, length($3)) = $3)) AND r.held IS DISTINCT FROM"
                " $2"
            ),
            Params()
            .text(w.placement())
            .text(String(r.held, suffix))
            .text(suffix),
        )
        if r.stuck:
            alert_stuck(db, who, r)
        return
    _ = db.conn.execute(
        (
            "UPDATE orca.runs r SET held = $2 FROM orca.pipelines p WHERE"
            " p.name = r.pipeline AND p.placement = $1 AND r.status = 'queued'"
            " AND starts_with(r.held, 'waiting: memory pressure ') AND"
            " right(r.held, length($3)) = $3"
        ),
        Params()
        .text(w.placement())
        .text(String("held: waiting for a worker of pool ", w.pool))
        .text(suffix),
    )
    resolve_stuck(db, who, r)


def text_field(v: Value, key: String) raises -> String:
    """A field of a worker's JSON as text: a number's digits, empty for
    none or null."""
    if key not in v or v[key].is_null():
        return ""
    if v[key].is_int():
        return String(v[key].int_value())
    return v[key].string_value()


def _num(v: Value, key: String) raises -> Int64:
    if key not in v:
        raise Error("orca: no ", key, " in the request")
    return v[key].int_value()


def claim(mut db: State, w: WorkerAuth, v: Value) raises -> String:
    """`/api/worker/claim`'s answer: `{"run": <spec>|null, "refused": ...}`."""
    _seen(db, w, v)
    var slots = Int(v["slots"].int_value()) if String("slots") in v else 0
    if slots <= 0:
        return String('{"run": null}')
    var image = text_field(v, "image")
    var name = text_field(v, "name")
    # What the worker's host can grant (orca.resources), from a worker that
    # says; -1 from one that doesn't, whose runs aren't admitted.
    var room_cpus = -1
    var room_mb = -1
    if String("room_cpus") in v and String("room_memory_mb") in v:
        room_cpus = Int(v["room_cpus"].int_value())
        room_mb = Int(v["room_memory_mb"].int_value())
    var stale = stale_limit()
    var dead = dead_sql("$2", start_grace(stale))
    var refused = String()
    # The oldest queued run of each of the pool's pipelines, and of its fill
    # runs: the others of a pipeline would be refused for the same reasons
    # (its cap, image or request), and a long queue of one pipeline at its
    # max_runs mustn't hide the runs of the others behind it. The other
    # runs first, then the fill runs, each oldest first.
    var candidates = db.conn.query(
        (
            "SELECT id, pipeline, trigger FROM (SELECT DISTINCT ON"
            " (r.pipeline, r.trigger = 'fill') r.id, r.pipeline, r.trigger"
            " FROM orca.runs r JOIN orca.pipelines p ON p.name = r.pipeline"
            " WHERE r.status = 'queued' AND p.placement = $1 AND NOT p.paused"
            " AND p.binary_path IS NOT NULL AND (r.trigger <> 'fill' OR"
            " p.fills = 'all') ORDER BY r.pipeline, r.trigger = 'fill', r.id)"
            " AS c ORDER BY trigger = 'fill', id"
        ),
        Params().text(w.placement()),
    )
    var hour = Int((now() % 86400) // 3600)
    for c in candidates:
        var id = c.int64("id")
        var ps = registered(db, c.text("pipeline"))
        if len(ps) == 0:
            continue
        ref p = ps[0]
        var fill = c.text("trigger") == "fill"
        if fill and not p.fill.in_window(hour):
            continue  # it waits for its fill_window, as a local one does
        var why = version_refusal(p.image, image)
        if why:
            refused = why
            _ = db.conn.execute(
                (
                    "UPDATE orca.runs SET held = $2 WHERE pipeline = $1 AND"
                    " status = 'queued' AND (trigger = 'fill') = $3"
                ),
                Params()
                .text(p.name)
                .text(String("held: ", w.user(), " can't run it: ", why))
                .bool(fill),
            )
            continue
        # Its request (orca.resources), `auto` learned from its runs
        # (orca.recommend), from a worker that says what it can grant.
        var sized = RunRequest(Request(0, 0, 0, 0), "", 0, "")
        if room_cpus >= 0:
            sized = run_request(db, p)
        var tx = db.conn.begin()
        _ = tx.query(
            "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
            Params().text(p.name),
        )
        # Its fill runs and its others are capped apart, as on the local
        # runner: `fill_max_runs` (the scheduler queues no more than that,
        # and this holds it if it was lowered since), `max_runs`.
        var which = String(" AND trigger = 'fill'") if fill else String(
            " AND trigger <> 'fill'"
        )
        var live = Int(
            tx.query(
                String(
                    "SELECT count(*) AS n FROM orca.runs WHERE pipeline = $1",
                    " AND status = 'running' AND NOT ",
                    dead,
                    which,
                ),
                Params().text(p.name).float64(stale),
            )
            .row(0)
            .int64("n")
        )
        if live >= (p.fill.max_runs if fill else p.max_runs):
            tx.rollback()
            continue
        var request = sized.request
        if request.memory_min_mb > 0:
            # At least what an out-of-memory kill raised it to.
            request.memory_min_mb = raised_request(
                request.memory_min_mb,
                request.memory_max_mb,
                p.memory_raised_mb,
            )
        var grant = Grant.none()
        var admitted = request.any() and room_cpus >= 0
        if admitted:
            var g = tx.query(
                String(
                    "SELECT coalesce(sum(grant_cpus), 0) AS cpus,",
                    " coalesce(sum(grant_memory_mb), 0) AS mb FROM orca.runs",
                    " WHERE worker = $1 AND status = 'running' AND NOT ",
                    dead,
                ),
                Params().int64(Int64(w.id)).float64(stale),
            ).row(0)
            var fit = admit(
                request,
                Room(room_cpus, room_mb, room_cpus, room_mb),
                Int(g.int64("cpus")),
                Int(g.int64("mb")),
                1,
                String("worker ", name) if name else w.user(),
            )
            if len(fit[0]) == 0:
                tx.rollback()
                refused = fit[1]
                _ = db.conn.execute(
                    (
                        "UPDATE orca.runs SET held = $2 WHERE pipeline = $1"
                        " AND status = 'queued' AND (trigger = 'fill') = $3"
                    ),
                    Params().text(p.name).text(fit[1]).bool(fill),
                )
                continue
            grant = fit[0][0]
        var got = tx.query(
            (
                "UPDATE orca.runs SET status = 'running', owner = $2, heartbeat"
                " = NULL, started = clock_timestamp(), ended = NULL, starts ="
                " starts + 1,"
                " retry_at = NULL, error = NULL, waiting_since = NULL,"
                " cpu_millis = 0, placement = $3, held = NULL, worker = $4,"
                " request_cpus = nullif($5, ''), request_memory_gb ="
                " nullif($6, ''), grant_cpus = nullif($7, 0), grant_memory_mb"
                " = nullif($8, 0), request_source = nullif($9, ''),"
                " request_runs = nullif($10, 0) WHERE id = $1 AND status ="
                " 'queued' RETURNING starts, allow_new_code, partitions"
            ),
            Params()
            .int64(id)
            .text(WORKER_OWNER)
            .text(w.placement())
            .int64(Int64(w.id))
            .text(recorded_cpus(p, sized) if admitted else String())
            .text(recorded_memory_gb(p, sized) if admitted else String())
            .int64(Int64(grant.cpus))
            .int64(Int64(grant.memory_mb))
            .text(sized.source)
            .int64(Int64(sized.runs)),
        )
        if len(got) != 1:
            tx.rollback()
            continue
        var r = got.row(0)
        var spec = WorkerRun(
            Int(id),
            Int(r.int64("starts")),
            p.name,
            p.binary,
            p.registry_hash,
            r.text("partitions"),
            r.bool("allow_new_code"),
            p.image,
            p.env.copy(),
            p.secrets.copy(),
            grant,
        )
        _ = tx.execute(
            (
                "INSERT INTO orca.audit (username, action, run, detail,"
                " source) VALUES ($1, '/api/worker/claim', $2, $3, 'worker')"
            ),
            Params()
            .text(w.user())
            .int64(id)
            .text(
                String(
                    "start ",
                    spec.start,
                    " on ",
                    name if name else String("a worker"),
                    ", pool ",
                    w.pool,
                )
            ),
        )
        tx.commit()
        return String('{"run": ', worker_run_json(spec), "}")
    if refused:
        return String('{"run": null, "refused": ', quote(refused), "}")
    return String('{"run": null}')


def release(mut db: State, w: WorkerAuth, v: Value) raises -> String:
    """`/api/worker/release`: puts back a run the worker was given and won't
    start (its spec isn't for this worker's image), held saying why."""
    var why = text_field(v, "why")
    var n = db.conn.execute(
        (
            "UPDATE orca.runs SET status = 'queued', owner = NULL, heartbeat ="
            " NULL, starts = starts - 1, worker = NULL, held = $4 WHERE id ="
            " $1 AND status = 'running' AND owner = $5 AND starts = $2 AND"
            " worker = $3"
        ),
        Params()
        .int64(_num(v, "run"))
        .int64(_num(v, "start"))
        .int64(Int64(w.id))
        .text(String("held: ", w.user(), " refused it: ", why))
        .text(WORKER_OWNER),
    )
    return String('{"released": ', "true" if n == 1 else "false", "}")


def _contained_exit(mut db: State, w: WorkerAuth, v: Value) raises:
    """A worker's run's cgroup (`orca.cgroup`), at its process's end: its
    limit on the attempts it made, and after an out-of-memory kill, while
    the start is still this worker's, those left running failed saying so
    and the pipeline's memory raised, up to its request's maximum; killed
    at that maximum, the run fails too, unretried."""
    var run = _num(v, "run")
    var start = _num(v, "start")
    var limit = Int(_num(v, "memory_limit_mb"))
    var kill = (
        Int(_num(v, "memory_max_mb")) if String("memory_max_mb") in v else limit
    )
    var rows = db.conn.query(
        (
            "SELECT r.pipeline, coalesce(r.grant_memory_mb, 0) AS grant_mb,"
            " coalesce(p.memory_gb, '') AS memory_gb, r.status = 'running' AS"
            " running FROM orca.runs r JOIN orca.pipelines p ON p.name ="
            " r.pipeline WHERE r.id = $1 AND r.worker = $2 AND r.starts = $3"
        ),
        Params().int64(run).int64(Int64(w.id)).int64(start),
    )
    if len(rows) != 1:
        return
    var r = rows.row(0)
    # Stall times from a worker that measures them (an older one doesn't).
    var stall = (
        _num(v, "memory_stall_ms") if String("memory_stall_ms") in v else -1
    )
    var stall_full = (
        _num(v, "memory_stall_full_ms") if String("memory_stall_full_ms")
        in v else -1
    )
    _ = db.conn.execute(
        (
            "UPDATE orca.attempts a SET memory_limit_mb = $3, grant_memory_mb"
            " = nullif($4, 0), memory_stall_ms = nullif($5, -1),"
            " memory_stall_full_ms = nullif($6, -1) FROM orca.steps st WHERE"
            " st.id = a.step AND st.run = $1 AND a.host LIKE '%:' || $2 AND"
            " a.memory_limit_mb IS NULL"
        ),
        Params()
        .int64(run)
        .text(String(_num(v, "pid")))
        .int64(Int64(limit))
        .int64(r.int64("grant_mb"))
        .int64(stall)
        .int64(stall_full),
    )
    if _num(v, "oom") != 1 or not r.bool("running"):
        return
    var ceiling = limit
    if r.text("memory_gb"):
        ceiling = Request.of("", r.text("memory_gb")).memory_max_mb
    var g = getenv("ORCA_RUN_MEMORY_GROWTH")
    var next = raised_mb(limit, growth_pct_of(g) if g else 150, ceiling)
    # `held` from a worker that knows it (an older one doesn't send it).
    var held = String("held") in v and _num(v, "held") == 1
    var why = oom_text(limit, kill, next, held)
    _ = db.interrupt(Int(run), why)
    _ = db.conn.execute(
        (
            "UPDATE orca.pipelines SET memory_raised_mb ="
            " greatest(coalesce(memory_raised_mb, 0), $2), memory_raised_run"
            " = $3 WHERE name = $1"
        ),
        Params().text(r.text("pipeline")).int64(Int64(next)).int64(run),
    )
    print("run", run, "on", w.user(), why)
    # At the most it may ask for, a retry would be killed again: the run
    # fails now, with the kill as its error.
    if next <= limit:
        _ = db.conn.execute(
            (
                "UPDATE orca.runs SET status = 'failed', ended = now(),"
                " heartbeat = NULL, retry_at = NULL, error = $4,"
                " unretried_start = starts WHERE id = $1"
                " AND status = 'running' AND worker = $2 AND starts = $3"
            ),
            Params().int64(run).int64(Int64(w.id)).int64(start).text(why),
        )


def exited(mut db: State, w: WorkerAuth, v: Value) raises -> String:
    """`/api/worker/exited`: fails a run whose process ended before it
    claimed it, as the scheduler's reap does for a local run, and records
    what the process's attempts used from the worker's samples."""
    var run = _num(v, "run")
    var why = String(
        "its process on ",
        text_field(v, "name") if text_field(v, "name") else String("a worker"),
        " exited ",
        _num(v, "code"),
        " before claiming it",
    )
    var last = text_field(v, "last")
    if last:
        why.write(": ", last)
    var n = db.conn.execute(
        (
            "UPDATE orca.runs SET status = 'failed', ended = now(), heartbeat ="
            " NULL, error = $4 WHERE id = $1 AND status = 'running' AND owner"
            " = $5 AND starts = $2 AND worker = $3"
        ),
        Params()
        .int64(run)
        .int64(_num(v, "start"))
        .int64(Int64(w.id))
        .text(why)
        .text(WORKER_OWNER),
    )
    var used = 0
    if String("marks") in v and String("pid") in v:
        used = record_usage(
            db,
            Int(run),
            Int(_num(v, "pid")),
            marks_of(v["marks"]),
            Int(_num(v, "max_rss")) if String("max_rss") in v else -1,
            worker=w.id,
            start=Int(_num(v, "start")),
        )
    if String("memory_limit_mb") in v and String("pid") in v:
        _contained_exit(db, w, v)
    return String(
        '{"failed": ', "true" if n == 1 else "false", ', "usage": ', used, "}"
    )


# ── store calls ──────────────────────────────────────────────────────────────


def _run_of(mut db: State, w: WorkerAuth, run: Int64) raises -> String:
    """The pipeline of `run` if this worker claimed its latest start; else
    raises, final."""
    var rows = db.conn.query(
        (
            "SELECT pipeline FROM orca.runs WHERE id = $1 AND worker = $2 AND"
            " placement = $3"
        ),
        Params().int64(run).int64(Int64(w.id)).text(w.placement()),
    )
    if len(rows) != 1:
        raise Error(
            FINAL,
            "orca: run ",
            run,
            " isn't one ",
            w.user(),
            " claimed; stopping",
        )
    return rows.row(0).text("pipeline")


def _check_attempt(mut db: State, run: Int64, attempt_id: Int64) raises:
    if (
        len(
            db.conn.query(
                (
                    "SELECT 1 FROM orca.attempts a JOIN orca.steps s ON s.id ="
                    " a.step WHERE a.id = $1 AND s.run = $2"
                ),
                Params().int64(attempt_id).int64(run),
            )
        )
        != 1
    ):
        raise Error(
            FINAL, "orca: attempt ", attempt_id, " isn't run ", run, "'s"
        )


def store_call(
    mut db: State, url: String, w: WorkerAuth, call: String, v: Value
) raises -> String:
    """A worker's run's `RunStore` call `call`, with the fields `v`, made on
    `PgStore`; the answer's JSON. Raises what the call raised."""
    var run = _num(v, "run")
    var pipeline = _run_of(db, w, run)
    var store = PgStore(
        State(url),
        url,
        Int(run),
        text_field(v, "owner"),
        text_field(v, "registry_hash"),
    )
    if call == "run_info":
        return String('{"run": ', run_info_json(store.run_info(Int(run))), "}")
    if call == "claim":
        var stale = (
            v["stale"].float_value() if String("stale") in v else stale_limit()
        )
        store.claim(Int(run), text_field(v, "owner"), stale)
        return "{}"
    if call == "version":
        if text_field(v, "pipeline") != pipeline:
            raise Error(FINAL, "orca: run ", run, " is of pipeline ", pipeline)
        var n = store.version(
            pipeline,
            text_field(v, "registry_hash"),
            v["describe"].raw_json(),
            text_field(v, "image"),
        )
        return String('{"version": ', n, "}")
    if call == "ran_version":
        store.ran_version(Int(run), Int(_num(v, "version")))
        return "{}"
    if call == "interrupt":
        var n = store.interrupt(Int(run), text_field(v, "why"))
        return String('{"n": ', n, "}")
    if call == "prior":
        return String('{"steps": ', prior_json(store.prior(Int(run))), "}")
    if call == "steps":
        return String('{"steps": ', steps_json(store.steps(Int(run))), "}")
    if call == "start_attempt":
        var input = bytes_of_hex(text_field(v, "input"))
        var n = store.start_attempt_once(
            text_field(v, "step"),
            text_field(v, "partition"),
            Int(_num(v, "ord")),
            Span(input),
        )
        return String(
            '{"attempt": ', n, ', "attempt_id": ', store.attempt_id(), "}"
        )
    if call == "finish_attempt":
        var output = bytes_of_hex(text_field(v, "output"))
        store.finish_attempt_once(
            text_field(v, "step"),
            text_field(v, "partition"),
            Int(_num(v, "attempt")),
            text_field(v, "status"),
            Span(output),
            text_field(v, "error"),
        )
        return "{}"
    if call == "skip":
        store.skip_once(
            text_field(v, "step"),
            text_field(v, "partition"),
            Int(_num(v, "ord")),
            text_field(v, "reason"),
        )
        return "{}"
    if call == "pinned_reads":
        var pins = store.pinned_reads(
            text_field(v, "step"), text_field(v, "partition")
        )
        return String('{"pins": ', pins_json(pins), "}")
    if call == "run_token":
        return String('{"token": ', quote(store.run_token()), "}")
    if call == "record":
        var attempt = _num(v, "attempt_id")
        _check_attempt(db, run, attempt)
        store.at_attempt(attempt)
        store.record(
            v["commit"].bool_value(),
            TableSnapshot(
                text_field(v, "table"),
                _num(v, "snapshot"),
                text_field(v, "dataset"),
            ),
        )
        return "{}"
    if call == "set_log":
        var attempt = _num(v, "attempt_id")
        _check_attempt(db, run, attempt)
        store.at_attempt(attempt)
        store.set_log("", "", 0, text_field(v, "path"))
        return "{}"
    if call == "fence":
        store.fence()
        return "{}"
    if call == "finish_run":
        store.finish_run(text_field(v, "status"))
        return "{}"
    if call == "defer_run":
        store.defer_run(_num(v, "until"))
        return "{}"
    if call == "cancel_requested":
        var c = store.cancel_requested()
        return String('{"cancel": ', "true" if c else "false", "}")
    if call == "beat":
        var why = beat_run(
            store.db.conn,
            Int(run),
            text_field(v, "owner"),
            text_field(v, "timeouts"),
        )
        return String('{"stop": ', quote(why), "}")
    raise Error("orca: no store call ", call)


# ── Ops ──────────────────────────────────────────────────────────────────────


def workers_json(rows: List[WorkerRow]) -> String:
    var out = String('{"workers": [')
    for i in range(len(rows)):
        ref r = rows[i]
        out.write(", " if i else "")
        out.write(
            Body()
            .int("id", Int64(r.id))
            .text("pool", r.pool)
            .text("label", r.label)
            .text("state", r.state)
            .text("name", r.name)
            .text("image", r.image)
            .text("last_seen", r.last_seen)
            .int("seen_seconds_ago", Int64(r.seen_ago))
            .int("cpus", Int64(r.cpus))
            .int("memory_mb", Int64(r.memory_mb))
            .text("running", r.running)
            .text("expires", r.expires)
            .raw("memory_pressure", row_json(r.pressure))
            .finish()
        )
    out.write("]}")
    return out^
