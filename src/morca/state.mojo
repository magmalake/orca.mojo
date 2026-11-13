"""morca's state in Postgres: pipelines, runs, steps and attempts.

Everything lives in the `morca` schema, which `State.migrate` creates and
brings up to date; migrations are SQL in `_migrations`, applied in order
under a transaction-scoped advisory lock, so two processes starting at once
don't both apply one.

**One writer per run.** The process running a run is its `owner` and keeps
`runs.heartbeat` fresh from a thread (`morca.heartbeat`). `State.claim` takes
a run only if its heartbeat is older than the stale limit, or cleared, which
`finish_run` does when a run ends. Every attempt checks in the same
transaction that this process still owns the run, so a process that lost its
run to a resume stops at its next step instead of racing it. The same check
stops a run that was asked to cancel (`runs.cancel_requested`, which `morca
ui` sets) before its next attempt; an attempt already going finishes and is
recorded.

**Each change is one transaction.** Starting an attempt marks its step
running and inserts the attempt; finishing it records the output or the
error on both. A process killed in between leaves an attempt `running`,
which `interrupt` marks failed when the run is resumed.
"""

from std.ffi import external_call
from std.os import getenv
from std.sys import stderr

from postgres import Connection, Params, Transaction

from morca.heartbeat import Beater, Heartbeat
from morca.migrations import (
    data_version_sql,
    _migrations,
    migration_floor,
    known_migrations,
    schema_ahead,
)
from morca.db.live import stale_limit
from morca.handover import run_outdated, set_outdated
from morca.store import (
    CANCELLED,
    DEFERRED,
    FINAL,
    FAILED,
    INTERRUPTED,
    Backoff,
    RUNNING,
    SKIPPED,
    SUCCEEDED,
    RunInfo,
    RunResult,
    RunStore,
    status_list,
    StepInfo,
    StepRun,
    TableSnapshot,
    Usage,
    process_owner,
)


comptime _MIGRATION_LOCK = 0x6F726361  # "orca"


def live_older(
    mut tx: Transaction, floor: Int
) raises -> List[Tuple[Int, String, Int]]:
    """The scheduler instances (docs/overlap.md) still going, seen within
    the stale limit and not ended, that know fewer than `floor` migrations:
    each one's id, host and how many it knows. None where `orca.schedulers`
    doesn't exist yet."""
    var out = List[Tuple[Int, String, Int]]()
    var t = tx.query(
        "SELECT to_regclass('orca.schedulers') IS NOT NULL AS ok"
    ).row(0)
    if not t.bool("ok"):
        return out^
    for r in tx.query(
        (
            "SELECT id, host, known FROM orca.schedulers WHERE ended IS NULL"
            " AND last_seen > now() - make_interval(secs => $1) AND known < $2"
            " ORDER BY id"
        ),
        Params().float64(stale_limit()).int64(Int64(floor)),
    ):
        out.append((Int(r.int64("id")), r.text("host"), Int(r.int64("known"))))
    return out^


def _refuse_overlap(mut tx: Transaction, floor: Int) raises:
    """Raises if a scheduler instance that knows fewer than `floor`
    migrations is still going: migrating would raise the schema's floor
    past it, and it would go on over a schema it can't run on. Such a
    deploy stops the old scheduler first (docs/overlap.md#migrations)."""
    var old = live_older(tx, floor)
    if len(old) == 0:
        return
    raise Error(
        "morca: this binary's migrations need a binary that knows at least ",
        floor,
        ", and scheduler instance ",
        old[0][0],
        " on ",
        old[0][1],
        " knows ",
        old[0][2],
        " and is still going; stop it, and wait for its runs, before",
        " deploying this one (docs/overlap.md#migrations)",
    )


struct State(Movable):
    """A connection to morca's database."""

    var conn: Connection

    def __init__(out self, url: String) raises:
        if url.startswith("sqlite:"):
            raise Error(
                "morca: MORCA_DATABASE_URL is a SQLite file, which only a",
                " pipeline binary's run, resume and status take; the",
                " scheduler, the UI and morca's other commands need Postgres",
            )
        self.conn = Connection(url)

    def migrate(mut self) raises:
        """Creates the schema, or applies the migrations it hasn't had."""
        var tx = self.conn.begin()
        # IF NOT EXISTS on an existing object is a NOTICE, which libpq prints.
        _ = tx.execute("SET LOCAL client_min_messages = warning")
        _ = tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            Params().int64(_MIGRATION_LOCK),
        )
        _ = tx.execute("CREATE SCHEMA IF NOT EXISTS orca")
        # A row per migration applied, with the fewest migrations a binary
        # must know to run on the schema it left (`min_binary`, null on the
        # rows of binaries before it, which this fills in for those it knows).
        _ = tx.execute(
            "CREATE TABLE IF NOT EXISTS orca.schema_version (version int NOT"
            " NULL)"
        )
        _ = tx.execute(
            "ALTER TABLE orca.schema_version ADD COLUMN IF NOT EXISTS"
            " min_binary int"
        )
        var migrations = _migrations()
        var known = known_migrations()
        var unfloored = List[Int]()
        for r in tx.query(
            (
                "SELECT version FROM orca.schema_version WHERE min_binary IS"
                " NULL AND version BETWEEN 1 AND $1"
            ),
            Params().int64(Int64(known)),
        ):
            unfloored.append(Int(r.int64("version")))
        for v in unfloored:
            _ = tx.execute(
                (
                    "UPDATE orca.schema_version SET min_binary = $2 WHERE"
                    " version = $1"
                ),
                Params().int64(Int64(v)).int64(Int64(migration_floor(v))),
            )
        var row = tx.query(
            "SELECT coalesce(max(version), 0) AS v, CASE WHEN bool_or("
            "min_binary IS NULL) THEN -1 ELSE coalesce(max(min_binary), 0)"
            " END AS floor FROM orca.schema_version"
        ).row(0)
        var have = Int(row.int64("v"))
        var raises_to = migration_floor(known)
        if have < known and raises_to > Int(row.int64("floor")):
            _refuse_overlap(tx, raises_to)
        if schema_ahead(have, Int(row.int64("floor")), known):
            print(
                "morca: the database is at schema version ",
                have,
                ", ahead of this binary's ",
                known,
                "; its migrations since are additive, so running on it",
                sep="",
                file=stderr,
            )
            tx.commit()
            return
        for v in range(have, known):
            for stmt in migrations[v]:
                _ = tx.execute(stmt)
            _ = tx.execute(
                (
                    "INSERT INTO orca.schema_version (version, min_binary)"
                    " VALUES ($1, $2)"
                ),
                Params()
                .int64(Int64(v + 1))
                .int64(Int64(migration_floor(v + 1))),
            )
        tx.commit()

    def register(
        mut self, pipeline: String, registry_hash: String, describe: String
    ) raises:
        """Records a pipeline, or updates it to this binary's DAG."""
        _ = self.conn.execute(
            (
                "INSERT INTO orca.pipelines (name, registry_hash, describe)"
                " VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE SET"
                " registry_hash = EXCLUDED.registry_hash, describe ="
                " EXCLUDED.describe, last_seen = now()"
            ),
            Params().text(pipeline).text(registry_hash).jsonb(describe),
        )
        _ = self.conn.execute(
            "UPDATE orca.pipelines SET version = $2 WHERE name = $1",
            Params()
            .text(pipeline)
            .int64(
                Int64(
                    self.version(
                        pipeline, registry_hash, describe, getenv("MORCA_IMAGE")
                    )
                )
            ),
        )

    def scheduled_hash(mut self, pipeline: String) raises -> String:
        """The registry hash `morca register` gave `pipeline`, or empty if it
        wasn't registered with a binary."""
        var res = self.conn.query(
            (
                "SELECT registry_hash FROM orca.pipelines WHERE name = $1 AND"
                " binary_path IS NOT NULL"
            ),
            Params().text(pipeline),
        )
        return res.row(0).text("registry_hash") if len(res) > 0 else String()

    def version(
        mut self,
        pipeline: String,
        registry_hash: String,
        describe: String,
        image: String,
    ) raises -> Int:
        """The version of `pipeline` that code `registry_hash`, from the
        source `describe` (a pipeline binary's) names, in `image` is;
        recorded the first time it's seen."""
        return Int(
            self.conn.query(
                (
                    String(
                        "INSERT INTO orca.pipeline_versions (pipeline,",
                        " registry_hash, source, commit_sha, dirty, image,",
                        " data_version) SELECT $1, $2,",
                        " coalesce(d->'source'->>'repo', ''),",
                        " coalesce(d->'source'->>'commit', ''),",
                        " coalesce((d->'source'->>'dirty')::boolean, false),",
                        " $4, ",
                        data_version_sql("d"),
                        " FROM (SELECT $3::jsonb AS d) AS x ON CONFLICT",
                        " (pipeline, registry_hash, source, commit_sha, dirty,",
                        " image) DO UPDATE SET last_seen = now(), data_version",
                        " = excluded.data_version RETURNING id",
                    )
                ),
                Params()
                .text(pipeline)
                .text(registry_hash)
                .text(describe)
                .text(image),
            )
            .row(0)
            .int64("id")
        )

    def ran_version(mut self, run: Int, version: Int) raises:
        """Records that `run` is running `version`'s code."""
        _ = self.conn.execute(
            "UPDATE orca.runs SET version = $2 WHERE id = $1",
            Params().int64(Int64(run)).int64(Int64(version)),
        )

    def create_run(
        mut self,
        pipeline: String,
        registry_hash: String,
        partitions: String,
        trigger: String,
        owner: String,
    ) raises -> Int:
        """A new run, already claimed by `owner`."""
        return Int(
            self.conn.query(
                (
                    "INSERT INTO orca.runs (pipeline, registry_hash,"
                    " partitions, trigger, status, owner, heartbeat, started)"
                    " VALUES ($1, $2, $3, $4, 'running', $5, now(), now())"
                    " RETURNING id"
                ),
                Params()
                .text(pipeline)
                .text(registry_hash)
                .text(partitions)
                .text(trigger)
                .text(owner),
            )
            .row(0)
            .int64("id")
        )

    def run_info(mut self, run: Int) raises -> RunInfo:
        var res = self.conn.query(
            (
                "SELECT pipeline, registry_hash, partitions, status,"
                " coalesce(owner, '') AS owner, coalesce(error, '') AS error"
                " FROM orca.runs WHERE id = $1"
            ),
            Params().int64(Int64(run)),
        )
        if len(res) == 0:
            raise Error("morca: no run ", run)
        var r = res.row(0)
        return RunInfo(
            run,
            r.text("pipeline"),
            r.text("registry_hash"),
            r.text("partitions"),
            r.text("status"),
            r.text("owner"),
            r.text("error"),
        )

    def claim(mut self, run: Int, owner: String, stale_seconds: Float64) raises:
        """Makes `owner` the run's writer, if no live process holds it.

        A run can be claimed while it's `queued`, unfinished (`running` with
        a stale or cleared heartbeat; the scheduler starts a run with none)
        or `failed`. Raises otherwise, saying why.
        """
        var res = self.conn.query(
            (
                "UPDATE orca.runs SET status = 'running', owner = $2, heartbeat"
                " = now(), ended = NULL, error = NULL WHERE id = $1 AND status"
                " IN ('queued', 'running', 'failed', 'deferred') AND (heartbeat"
                " IS NULL OR heartbeat < now() - make_interval(secs => $3))"
                " RETURNING id"
            ),
            Params().int64(Int64(run)).text(owner).float64(stale_seconds),
        )
        if len(res) == 1:
            return
        var info = self.run_info(run)
        if (
            info.status != RUNNING
            and info.status != FAILED
            and info.status != "queued"
            and info.status != DEFERRED
        ):
            raise Error("morca: run ", run, " is ", info.status)
        raise Error(
            "morca: run ",
            run,
            " is held by ",
            info.owner,
            ", whose heartbeat is fresher than ",
            stale_seconds,
            " s",
        )

    def interrupt(mut self, run: Int, why: String = "") raises -> Int:
        """Marks attempts left `running` by a process that died as failed,
        and returns how many there were. `why` is their error and their
        steps', such as a crash's (`morca.crash`); empty, it's
        `INTERRUPTED`."""
        var tx = self.conn.begin()
        var n = tx.execute(
            (
                "UPDATE orca.attempts a SET status = 'failed', ended = now(),"
                " error = $2 FROM orca.steps s WHERE a.step = s.id AND s.run ="
                " $1 AND a.status = 'running'"
            ),
            Params().int64(Int64(run)).text(why if why else INTERRUPTED),
        )
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = 'failed', error = $2"
                " WHERE run = $1 AND status = 'running'"
            ),
            Params()
            .int64(Int64(run))
            .text(why if why else String("interrupted")),
        )
        tx.commit()
        return n

    def prior(mut self, run: Int) raises -> RunResult:
        """The run's succeeded steps, with their outputs, for resuming it."""
        var res = RunResult()
        var rows = self.conn.query(
            (
                "SELECT step, partition, output FROM orca.steps WHERE run = $1"
                " AND status = 'succeeded'"
            ),
            Params().int64(Int64(run)),
        )
        for r in rows:
            res.steps.append(
                StepRun(
                    r.text("step"),
                    r.text("partition"),
                    SUCCEEDED,
                    r.bytea("output"),
                    "",
                )
            )
        return res^

    def finish_run(mut self, run: Int, owner: String, status: String) raises:
        """Ends the run and clears its heartbeat, so it can be resumed at
        once if it failed."""
        _ = self.conn.execute(
            (
                "UPDATE orca.runs SET status = $2, ended = now(), heartbeat ="
                " NULL WHERE id = $1 AND owner = $3"
            ),
            Params().int64(Int64(run)).text(status).text(owner),
        )

    def defer_run(mut self, run: Int, owner: String, until: Int64) raises:
        """Ends the run `deferred`, to be started again at `until` (on the
        scheduler's clock) by the scheduler, whatever started it; that start
        isn't counted against its retries."""
        _ = self.conn.execute(
            (
                "UPDATE orca.runs SET status = 'deferred', ended = now(),"
                " heartbeat = NULL, retry_at = $3, deferrals = deferrals + 1"
                " WHERE id = $1 AND owner = $2"
            ),
            Params().int64(Int64(run)).text(owner).int64(until),
        )

    def cancel_requested(mut self, run: Int) raises -> Bool:
        return (
            self.conn.query(
                "SELECT cancel_requested FROM orca.runs WHERE id = $1",
                Params().int64(Int64(run)),
            )
            .row(0)
            .bool("cancel_requested")
        )

    def runs(
        mut self, limit: Int = 20, before: Int = 0, status: String = ""
    ) raises -> List[RunInfo]:
        """The latest runs, newest first; those under `before` if it isn't
        0, and with `status` (one or several, comma-separated) one of
        those."""
        var out = List[RunInfo]()
        var rows = self.conn.query(
            (
                "SELECT id, pipeline, registry_hash, partitions, status,"
                " coalesce(owner, '') AS owner, coalesce(error, '') AS error"
                " FROM orca.runs WHERE ($2 = 0 OR id < $2) AND ($3 = '' OR"
                " status = ANY(string_to_array($3, ','))) ORDER BY id DESC"
                " LIMIT $1"
            ),
            Params()
            .int64(Int64(limit))
            .int64(Int64(before))
            .text(String(",").join(status_list(status))),
        )
        for r in rows:
            out.append(
                RunInfo(
                    Int(r.int64("id")),
                    r.text("pipeline"),
                    r.text("registry_hash"),
                    r.text("partitions"),
                    r.text("status"),
                    r.text("owner"),
                    r.text("error"),
                )
            )
        return out^

    def steps(mut self, run: Int) raises -> List[StepInfo]:
        """A run's steps in run order, with their attempt counts."""
        var out = List[StepInfo]()
        var rows = self.conn.query(
            (
                "SELECT s.step, s.partition, s.status, coalesce(s.error, '') AS"
                " error, (SELECT count(*) FROM orca.attempts a WHERE a.step ="
                " s.id) AS attempts, coalesce(l.log, '') AS log,"
                " coalesce(l.peak_rss_bytes, -1) AS peak,"
                " coalesce(l.cpu_seconds, -1) AS cpu, coalesce(l.peak_cpus,"
                " -1) AS cpus FROM orca.steps s LEFT JOIN LATERAL (SELECT"
                " a.log, a.peak_rss_bytes, a.cpu_seconds, a.peak_cpus FROM"
                " orca.attempts a WHERE a.step = s.id ORDER BY a.attempt DESC"
                " LIMIT 1) l ON true WHERE s.run = $1 ORDER BY s.ord, s.id"
            ),
            Params().int64(Int64(run)),
        )
        for r in rows:
            out.append(
                StepInfo(
                    r.text("step"),
                    r.text("partition"),
                    r.text("status"),
                    Int(r.int64("attempts")),
                    r.text("error"),
                    r.text("log"),
                    Usage(
                        Int(r.int64("peak")),
                        r.float64("cpu"),
                        r.float64("cpus"),
                    ),
                )
            )
        return out^


struct DbRetry(Movable):
    """Retries of one call to Postgres from a run's process: `Backoff`,
    then a new connection, so a restart or failover of the server doesn't
    stop the run."""

    var backoff: Backoff

    def __init__(out self):
        self.backoff = Backoff("Postgres")

    def again(mut self, e: Error, mut db: State, url: String) raises:
        self.backoff.again(e)
        try:
            db = State(url)
        except:
            pass  # the next try fails too, and comes back here


comptime _WATCH_SQL = (
    "SELECT 'cancel' AS why, 0::bigint AS attempt, '' AS step, '' AS"
    " partition, 0::float8 AS secs FROM orca.runs WHERE id = $1 AND owner ="
    " $2 AND status = 'running' AND cancel_requested AND cancel_at <= now()"
    " UNION ALL SELECT 'timeout', a.id, s.step, s.partition, t.secs FROM"
    " orca.attempts a JOIN orca.steps s ON s.id = a.step JOIN orca.runs r ON"
    " r.id = s.run JOIN (SELECT split_part(x, '=', 1) AS step,"
    " split_part(x, '=', 2)::float8 AS secs FROM unnest(string_to_array($3,"
    " ',')) AS x) AS t ON t.step = s.step WHERE s.run = $1 AND r.owner = $2"
    " AND r.status = 'running' AND a.status = 'running' AND a.started <= now()"
    " - make_interval(secs => t.secs) LIMIT 1"
)
"""A reason to stop the run's process now, if there is one: a cancel whose
time has come, or an attempt past its step's timeout."""


def _stop(
    mut conn: Connection, run: Int, owner: String, timeouts: String
) raises -> String:
    """Ends the run as the watchdog finds it should end, and returns why,
    or empty if it shouldn't."""
    var rows = conn.query(
        _WATCH_SQL, Params().int64(Int64(run)).text(owner).text(timeouts)
    )
    if len(rows) == 0:
        return ""
    var r = rows.row(0)
    var tx = conn.begin()
    var why: String
    if r.text("why") == "timeout":
        var step = r.text("step")
        if r.text("partition"):
            step.write(" ", r.text("partition"))
        why = String(
            "timed out after ",
            Int(r.float64("secs")),
            " s (timeout_seconds), so its process was stopped",
        )
        var n = tx.execute(
            (
                "UPDATE orca.attempts SET status = 'failed', ended = now(),"
                " error = $2 WHERE id = $1 AND status = 'running'"
            ),
            Params().int64(r.int64("attempt")).text(why),
        )
        if n != 1:
            tx.rollback()
            return ""  # it ended as we looked
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = 'failed', error = $2 WHERE id ="
                " (SELECT step FROM orca.attempts WHERE id = $1)"
            ),
            Params().int64(r.int64("attempt")).text(why),
        )
        why = String("step ", step, " ", why)
        _ = tx.execute(
            (
                "UPDATE orca.runs SET status = 'failed', ended = now(),"
                " heartbeat = NULL, error = $3 WHERE id = $1 AND owner = $2"
            ),
            Params().int64(Int64(run)).text(owner).text(why),
        )
    else:
        why = String(
            "cancelled: its step hadn't finished when the cancel's",
            " MORCA_CANCEL_KILL_SECONDS ran out, so its process was stopped",
        )
        _ = tx.execute(
            (
                "UPDATE orca.attempts a SET status = 'failed', ended = now(),"
                " error = $2 FROM orca.steps s WHERE a.step = s.id AND s.run ="
                " $1 AND a.status = 'running'"
            ),
            Params().int64(Int64(run)).text(why),
        )
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = 'failed', error = $2 WHERE run"
                " = $1 AND status = 'running'"
            ),
            Params().int64(Int64(run)).text(why),
        )
        _ = tx.execute(
            (
                "UPDATE orca.runs SET status = 'cancelled', ended = now(),"
                " heartbeat = NULL WHERE id = $1 AND owner = $2"
            ),
            Params().int64(Int64(run)).text(owner),
        )
    _ = tx.execute(
        (
            "INSERT INTO orca.audit (username, action, run, detail) VALUES"
            " ('orca', 'stop', $1, $2)"
        ),
        Params().int64(Int64(run)).text(String(owner, ": ", why)),
    )
    tx.commit()
    return why^


def beat_run(
    mut conn: Connection, run: Int, owner: String, timeouts: String
) raises -> String:
    """One beat of `run`'s heartbeat as `owner`, and the watchdog's look at
    it (`morca.heartbeat`): why the run's process must stop now, a cancel
    whose time has come or an attempt past its step's timeout, which is
    then recorded, or empty. `PgBeat`'s, and `morca ui`'s for a worker's run
    (`orca.workers`), whose process then stops itself."""
    _ = conn.execute(
        (
            "UPDATE orca.runs SET heartbeat = now() WHERE id = $1 AND"
            " owner = $2 AND status = 'running'"
        ),
        Params().int64(Int64(run)).text(owner),
    )
    return _stop(conn, run, owner, timeouts)


struct PgBeat(Beater):
    """Refreshes a run's heartbeat in Postgres, on a connection of its own,
    and asks whether the run is outdated (`morca.outdated`)."""

    var url: String
    var run: Int
    var owner: String
    var timeouts: String
    """`step=seconds` for each step with a timeout, comma-separated."""
    var _conn: Optional[Connection]

    def __init__(
        out self, url: String, run: Int, owner: String, timeouts: String = ""
    ):
        self.url = url
        self.run = run
        self.owner = owner
        self.timeouts = timeouts
        self._conn = None

    def beat(mut self) raises:
        if not self._conn:
            self._conn = Connection(self.url)
        var why = beat_run(
            self._conn.value(), self.run, self.owner, self.timeouts
        )
        if why:
            print("run", self.run, why, flush=True)
            external_call["_exit", NoneType](Int32(1))
        # Whether a newer version leads, for `morca.outdated()` (#397). Not
        # knowing leaves the last answer; it never stops the beat.
        try:
            var o = run_outdated(self._conn.value(), self.run)
            set_outdated(o.outdated, o.mine, o.desired, o.why)
        except:
            pass

    def reset(mut self):
        self._conn = None


struct PgStore(Movable, RunStore):
    """morca's runs in Postgres, `State` behind the `RunStore` trait.

    The calls for the owned run (attempts, skips, reads and commits, its
    end) are retried for `MORCA_DB_RETRY_SECONDS` on a new connection
    (`DbRetry`), except when the run was lost or cancelled, so a blip of
    the server doesn't throw away a step that finished. The others are made
    once, before the run starts or after it ends."""

    var db: State
    var url: String
    var run: Int
    var owner: String
    var registry_hash: String
    var _attempt_id: Int64
    """The `attempts` row of the attempt last started."""
    var _heartbeat: Optional[Heartbeat[PgBeat]]

    def __init__(out self, url: String) raises:
        """Connects to `url`; `own` a run before recording attempts."""
        self.db = State(url)
        self.url = url
        self.run = 0
        self.owner = String()
        self.registry_hash = String()
        self._attempt_id = 0
        self._heartbeat = None

    def __init__(
        out self,
        var db: State,
        url: String,
        run: Int,
        owner: String,
        registry_hash: String,
    ):
        """Over `db`, already owning `run`."""
        self.db = db^
        self.url = url
        self.run = run
        self.owner = owner
        self.registry_hash = registry_hash
        self._attempt_id = 0
        self._heartbeat = None

    # --- RunStore: State's calls, for the pipeline binary ---

    def migrate(mut self) raises:
        self.db.migrate()

    def register(
        mut self, pipeline: String, registry_hash: String, describe: String
    ) raises:
        self.db.register(pipeline, registry_hash, describe)

    def scheduled_hash(mut self, pipeline: String) raises -> String:
        return self.db.scheduled_hash(pipeline)

    def version(
        mut self,
        pipeline: String,
        registry_hash: String,
        describe: String,
        image: String,
    ) raises -> Int:
        return self.db.version(pipeline, registry_hash, describe, image)

    def create_run(
        mut self,
        pipeline: String,
        registry_hash: String,
        partitions: String,
        trigger: String,
        owner: String,
    ) raises -> Int:
        return self.db.create_run(
            pipeline, registry_hash, partitions, trigger, owner
        )

    def claim(mut self, run: Int, owner: String, stale_seconds: Float64) raises:
        self.db.claim(run, owner, stale_seconds)

    def ran_version(mut self, run: Int, version: Int) raises:
        self.db.ran_version(run, version)

    def interrupt(mut self, run: Int, why: String = "") raises -> Int:
        return self.db.interrupt(run, why)

    def prior(mut self, run: Int) raises -> RunResult:
        return self.db.prior(run)

    def run_info(mut self, run: Int) raises -> RunInfo:
        return self.db.run_info(run)

    def runs(
        mut self, limit: Int, before: Int = 0, status: String = ""
    ) raises -> List[RunInfo]:
        return self.db.runs(limit, before, status)

    def steps(mut self, run: Int) raises -> List[StepInfo]:
        return self.db.steps(run)

    def own(mut self, run: Int, owner: String, registry_hash: String) raises:
        self.run = run
        self.owner = owner
        self.registry_hash = registry_hash

    def attempt_id(self) -> Int64:
        """The `attempts` row of the attempt last started: what a worker's
        run is told, to name it in its later calls (`orca.workers`)."""
        return self._attempt_id

    def at_attempt(mut self, id: Int64):
        """Points `record` and `set_log` at the `attempts` row `id`."""
        self._attempt_id = id

    def start_attempt_once(
        mut self,
        step: String,
        partition: String,
        ord: Int,
        input: Span[UInt8, _],
    ) raises -> Int:
        """`start_attempt` without its retries, its errors as they are
        (`FINAL` for a lost or cancelled run): for `morca ui`, whose worker
        retries the call itself (`morca.http_store`)."""
        return self._start_attempt(step, partition, ord, input)

    def finish_attempt_once(
        mut self,
        step: String,
        partition: String,
        attempt: Int,
        status: String,
        output: Span[UInt8, _],
        error: String,
    ) raises:
        """`finish_attempt` without its retries, as `start_attempt_once`."""
        self._finish_attempt(step, partition, attempt, status, output, error)

    def skip_once(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        """`skip` without its retries, as `start_attempt_once`."""
        self._skip(step, partition, ord, reason)

    def start_heartbeat(
        mut self, interval: Float64, timeouts: String = ""
    ) raises:
        self._heartbeat = Heartbeat(
            PgBeat(self.url, self.run, self.owner, timeouts), interval
        )

    def stop_heartbeat(mut self):
        self._heartbeat = None

    # --- Store: the owned run's attempts ---

    @staticmethod
    def _still_owner(
        mut tx: Transaction, run: Int, owner: String, starting: Bool
    ) raises:
        """Raises unless this process still owns the run, or, when a step is
        `starting`, if the run was asked to cancel."""
        var res = tx.query(
            (
                "UPDATE orca.runs SET heartbeat = now() WHERE id = $1 AND owner"
                " = $2 AND status = 'running' RETURNING cancel_requested"
            ),
            Params().int64(Int64(run)).text(owner),
        )
        if len(res) != 1:
            raise Error(
                FINAL, "morca: lost run ", run, " to another process; stopping"
            )
        if starting and res.row(0).bool("cancel_requested"):
            raise Error(FINAL, "morca: run ", run, " was cancelled; stopping")

    def start_attempt(
        mut self,
        step: String,
        partition: String,
        ord: Int,
        input: Span[UInt8, _],
    ) raises -> Int:
        var r = DbRetry()
        while True:
            try:
                return self._start_attempt(step, partition, ord, input)
            except e:
                r.again(e, self.db, self.url)

    def _start_attempt(
        mut self,
        step: String,
        partition: String,
        ord: Int,
        input: Span[UInt8, _],
    ) raises -> Int:
        var tx = self.db.conn.begin()
        Self._still_owner(tx, self.run, self.owner, starting=True)
        var id = (
            tx.query(
                (
                    "INSERT INTO orca.steps (run, step, partition, ord, status,"
                    " input) VALUES ($1, $2, $3, $4, 'running', $5) ON CONFLICT"
                    " (run, step, partition) DO UPDATE SET status = 'running',"
                    " input = EXCLUDED.input, error = NULL RETURNING id"
                ),
                Params()
                .int64(Int64(self.run))
                .text(step)
                .text(partition)
                .int32(Int32(ord))
                .bytea(input),
            )
            .row(0)
            .int64("id")
        )
        var n = (
            tx.query(
                (
                    "SELECT coalesce(max(attempt), 0) + 1 AS n FROM"
                    " orca.attempts WHERE step = $1"
                ),
                Params().int64(id),
            )
            .row(0)
            .int64("n")
        )
        self._attempt_id = (
            tx.query(
                (
                    "INSERT INTO orca.attempts (step, attempt, status, host,"
                    " registry_hash) VALUES ($1, $2, 'running', $3, $4)"
                    " RETURNING id"
                ),
                Params()
                .int64(id)
                .int64(n)
                .text(self.owner)
                .text(self.registry_hash),
            )
            .row(0)
            .int64("id")
        )
        tx.commit()
        return Int(n)

    def pinned_reads(
        mut self, step: String, partition: String
    ) raises -> Dict[String, Int64]:
        var r = DbRetry()
        while True:
            try:
                return self._pinned_reads(step, partition)
            except e:
                r.again(e, self.db, self.url)

    def run_token(mut self) raises -> String:
        var r = DbRetry()
        while True:
            try:
                var rows = self.db.conn.query(
                    (
                        "SELECT coalesce(token::text, '') AS token FROM"
                        " orca.runs WHERE id = $1"
                    ),
                    Params().int64(Int64(self.run)),
                )
                if len(rows) == 0:
                    return String()
                return rows.row(0).text("token")
            except e:
                r.again(e, self.db, self.url)

    def _pinned_reads(
        mut self, step: String, partition: String
    ) raises -> Dict[String, Int64]:
        var out = Dict[String, Int64]()
        var rows = self.db.conn.query(
            (
                "SELECT r.table_name, r.snapshot_id FROM orca.reads r JOIN"
                " orca.attempts a ON a.id = r.attempt JOIN orca.steps s ON s.id"
                " = a.step WHERE s.run = $1 AND s.step = $2 AND s.partition ="
                " $3 ORDER BY a.attempt DESC"
            ),
            Params().int64(Int64(self.run)).text(step).text(partition),
        )
        # Newest first, so the first attempt's snapshot is the one left.
        for r in rows:
            out[r.text("table_name")] = r.int64("snapshot_id")
        return out^

    def record(mut self, commit: Bool, t: TableSnapshot) raises:
        # Not owner-checked: what an attempt read or committed is so even
        # if the run was lost meanwhile, and a commit must stay on record.
        var r = DbRetry()
        while True:
            try:
                _ = self.db.conn.execute(
                    String(
                        "INSERT INTO orca.",
                        "commits" if commit else "reads",
                        " (attempt, table_name, snapshot_id, dataset) VALUES",
                        " ($1, $2, $3, nullif($4, ''))",
                    ),
                    Params()
                    .int64(self._attempt_id)
                    .text(t.table)
                    .int64(t.snapshot_id)
                    .text(t.dataset),
                )
                return
            except e:
                r.again(e, self.db, self.url)

    def fence(mut self) raises:
        # On the store's connection; an unreachable database is waited for
        # while the heartbeat could still be fresh, then the commit is off.
        var stale: Float64
        try:
            stale = Float64(getenv("MORCA_STALE_SECONDS", "30"))
        except:
            stale = 30.0
        var r = DbRetry()
        r.backoff.budget = stale / 2
        while True:
            try:
                var row = self.db.conn.query(
                    (
                        "SELECT coalesce(owner, '') AS owner, status,"
                        " coalesce(heartbeat > now() - make_interval(secs =>"
                        " $2), false) AS fresh FROM orca.runs WHERE id = $1"
                    ),
                    Params().int64(Int64(self.run)).float64(stale / 2),
                ).row(0)
                if (
                    row.text("owner") != self.owner
                    or row.text("status") != RUNNING
                ):
                    raise Error(
                        "morca: not committing: run ",
                        self.run,
                        " is ",
                        row.text("status"),
                        " and held by ",
                        row.text("owner") if row.text("owner") else "nobody",
                        ", not this process (",
                        self.owner,
                        ")",
                    )
                if not row.bool("fresh"):
                    raise Error(
                        "morca: not committing: run ",
                        self.run,
                        "'s heartbeat is older than ",
                        stale / 2,
                        " s, so another process may take it over",
                    )
                return
            except e:
                if String(e).startswith("morca: not committing"):
                    raise e^
                try:
                    r.again(e, self.db, self.url)
                except e2:
                    raise Error("morca: not committing: ", e2)

    def set_log(
        mut self, step: String, partition: String, attempt: Int, path: String
    ) raises:
        var r = DbRetry()
        while True:
            try:
                _ = self.db.conn.execute(
                    "UPDATE orca.attempts SET log = $2 WHERE id = $1",
                    Params().int64(self._attempt_id).text(path),
                )
                return
            except e:
                r.again(e, self.db, self.url)

    def finish_attempt(
        mut self,
        step: String,
        partition: String,
        attempt: Int,
        status: String,
        output: Span[UInt8, _],
        error: String,
    ) raises:
        # Retried whole: it sets the attempt's and the step's rows, so
        # doing it twice leaves what doing it once does.
        var r = DbRetry()
        while True:
            try:
                self._finish_attempt(
                    step, partition, attempt, status, output, error
                )
                return
            except e:
                r.again(e, self.db, self.url)

    def _finish_attempt(
        mut self,
        step: String,
        partition: String,
        attempt: Int,
        status: String,
        output: Span[UInt8, _],
        error: String,
    ) raises:
        var tx = self.db.conn.begin()
        Self._still_owner(tx, self.run, self.owner, starting=False)
        var key = Params().int64(Int64(self.run)).text(step).text(partition)
        var id = (
            tx.query(
                (
                    "SELECT id FROM orca.steps WHERE run = $1 AND step = $2 AND"
                    " partition = $3"
                ),
                key,
            )
            .row(0)
            .int64("id")
        )
        var err = Params().int64(id).int64(Int64(attempt)).text(status)
        err = err^.null() if not error else err^.text(error)
        _ = tx.execute(
            (
                "UPDATE orca.attempts SET status = $3, ended = now(), error ="
                " $4 WHERE step = $1 AND attempt = $2"
            ),
            err,
        )
        var p = Params().int64(id).text(status)
        p = p^.bytea(output) if status == SUCCEEDED else p^.null()
        p = p^.null() if not error else p^.text(error)
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = $2, output = $3, error = $4"
                " WHERE id = $1"
            ),
            p,
        )
        tx.commit()

    def skip(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        var r = DbRetry()
        while True:
            try:
                self._skip(step, partition, ord, reason)
                return
            except e:
                r.again(e, self.db, self.url)

    def finish_run(mut self, status: String) raises:
        """`State.finish_run` for this run, retried as the attempts are."""
        var r = DbRetry()
        while True:
            try:
                self.db.finish_run(self.run, self.owner, status)
                return
            except e:
                r.again(e, self.db, self.url)

    def defer_run(mut self, until: Int64) raises:
        """`State.defer_run` for this run, retried as the attempts are."""
        var r = DbRetry()
        while True:
            try:
                self.db.defer_run(self.run, self.owner, until)
                return
            except e:
                r.again(e, self.db, self.url)

    def cancel_requested(mut self) raises -> Bool:
        var r = DbRetry()
        while True:
            try:
                return self.db.cancel_requested(self.run)
            except e:
                r.again(e, self.db, self.url)

    def _skip(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        var tx = self.db.conn.begin()
        Self._still_owner(tx, self.run, self.owner, starting=True)
        _ = tx.execute(
            (
                "INSERT INTO orca.steps (run, step, partition, ord, status,"
                " error) VALUES ($1, $2, $3, $4, 'skipped', $5) ON CONFLICT"
                " (run, step, partition) DO UPDATE SET status = 'skipped',"
                " error = EXCLUDED.error, output = NULL"
            ),
            Params()
            .int64(Int64(self.run))
            .text(step)
            .text(partition)
            .int32(Int32(ord))
            .text(reason),
        )
        tx.commit()
