"""orca's state in Postgres: pipelines, runs, steps and attempts.

Everything lives in the `orca` schema, which `State.migrate` creates and
brings up to date; migrations are SQL in `_migrations`, applied in order
under a transaction-scoped advisory lock, so two processes starting at once
don't both apply one.

**One writer per run.** The process running a run is its `owner` and keeps
`runs.heartbeat` fresh from a thread (`orca.heartbeat`). `State.claim` takes
a run only if its heartbeat is older than the stale limit, or cleared, which
`finish_run` does when a run ends. Every attempt checks in the same
transaction that this process still owns the run, so a process that lost its
run to a resume stops at its next step instead of racing it. The same check
stops a run that was asked to cancel (`runs.cancel_requested`, which `orca
ui` sets) before its next attempt; an attempt already going finishes and is
recorded.

**Each change is one transaction.** Starting an attempt marks its step
running and inserts the attempt; finishing it records the output or the
error on both. A process killed in between leaves an attempt `running`,
which `interrupt` marks failed when the run is resumed.
"""

from std.ffi import external_call

from postgres import Connection, Params, Transaction

from orca.ctx import Recorder
from orca.pipeline import FAILED, SUCCEEDED, SKIPPED, RunResult, StepRun
from orca.store import Store

comptime RUNNING = "running"
comptime CANCELLED = "cancelled"


def _migrations() -> List[List[String]]:
    """Each migration is a list of statements; its version is its index + 1."""
    return [
        [
            """CREATE TABLE orca.pipelines (
            name text PRIMARY KEY,
            registry_hash text NOT NULL,
            describe jsonb NOT NULL,
            first_seen timestamptz NOT NULL DEFAULT now(),
            last_seen timestamptz NOT NULL DEFAULT now())""",
            """CREATE TABLE orca.runs (
            id bigserial PRIMARY KEY,
            pipeline text NOT NULL REFERENCES orca.pipelines (name),
            registry_hash text NOT NULL,
            partitions text NOT NULL,
            trigger text NOT NULL,
            status text NOT NULL,
            owner text,
            heartbeat timestamptz,
            created timestamptz NOT NULL DEFAULT now(),
            started timestamptz,
            ended timestamptz)""",
            """CREATE TABLE orca.steps (
            id bigserial PRIMARY KEY,
            run bigint NOT NULL REFERENCES orca.runs (id),
            step text NOT NULL,
            partition text NOT NULL,
            ord int NOT NULL,
            status text NOT NULL,
            input bytea,
            output bytea,
            error text,
            UNIQUE (run, step, partition))""",
            """CREATE TABLE orca.attempts (
            id bigserial PRIMARY KEY,
            step bigint NOT NULL REFERENCES orca.steps (id),
            attempt int NOT NULL,
            status text NOT NULL,
            host text NOT NULL,
            registry_hash text NOT NULL,
            started timestamptz NOT NULL DEFAULT now(),
            ended timestamptz,
            error text,
            log text,
            UNIQUE (step, attempt))""",
            """CREATE TABLE orca.reads (
            attempt bigint NOT NULL REFERENCES orca.attempts (id),
            table_name text NOT NULL,
            snapshot_id bigint NOT NULL)""",
            """CREATE TABLE orca.commits (
            attempt bigint NOT NULL REFERENCES orca.attempts (id),
            table_name text NOT NULL,
            snapshot_id bigint NOT NULL)""",
        ],
        # The scheduler: a pipeline's binary and since when it's scheduled
        # (both on the scheduler's clock, which ORCA_NOW can fix), how often
        # the scheduler started a run and when it retries it, and fires.
        [
            "ALTER TABLE orca.pipelines ADD COLUMN binary_path text",
            "ALTER TABLE orca.pipelines ADD COLUMN since bigint",
            "ALTER TABLE orca.runs ADD COLUMN starts int NOT NULL DEFAULT 0",
            "ALTER TABLE orca.runs ADD COLUMN retry_at bigint",
            "CREATE INDEX runs_pipeline_status ON orca.runs (pipeline, status)",
            """CREATE TABLE orca.fires (
            pipeline text NOT NULL REFERENCES orca.pipelines (name),
            fire_at bigint NOT NULL,
            partition text NOT NULL,
            run bigint REFERENCES orca.runs (id),
            created timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (pipeline, fire_at))""",
        ],
        # The UI's actions: a cancel asked for, a resume that may run new
        # code, and the steps a rerun copied from the run it reruns.
        [
            (
                "ALTER TABLE orca.runs ADD COLUMN cancel_requested boolean NOT"
                " NULL DEFAULT false"
            ),
            (
                "ALTER TABLE orca.runs ADD COLUMN allow_new_code boolean NOT"
                " NULL DEFAULT false"
            ),
            (
                "ALTER TABLE orca.steps ADD COLUMN copied_from bigint"
                " REFERENCES orca.steps (id)"
            ),
        ],
        # Datasets: the warehouse name of a table read or committed, null
        # for a table outside the warehouse.
        [
            "ALTER TABLE orca.reads ADD COLUMN dataset text",
            "ALTER TABLE orca.commits ADD COLUMN dataset text",
        ],
        # Lineage walks: a snapshot to the attempts that read or made it,
        # and an attempt to what it read and made.
        [
            "CREATE INDEX reads_snapshot ON orca.reads (dataset, snapshot_id)",
            (
                "CREATE INDEX commits_snapshot ON orca.commits (dataset,"
                " snapshot_id)"
            ),
            "CREATE INDEX reads_attempt ON orca.reads (attempt)",
            "CREATE INDEX commits_attempt ON orca.commits (attempt)",
        ],
    ]


comptime _MIGRATION_LOCK = 0x6F726361  # "orca"


def process_owner() -> String:
    """`host:pid`, naming this process as a run's owner."""
    var buf = List[UInt8](length=256, fill=0)
    _ = external_call["gethostname", Int32](buf.unsafe_ptr(), Int(255))
    var n = 0
    while n < 255 and buf[n] != 0:
        n += 1
    var host = String(from_utf8_lossy=Span(buf)[:n])
    return String(host, ":", external_call["getpid", Int32]())


@fieldwise_init
struct RunInfo(Copyable, Movable):
    var id: Int
    var pipeline: String
    var registry_hash: String
    var partitions: String
    var status: String
    var owner: String


@fieldwise_init
struct StepInfo(Copyable, Movable):
    var step: String
    var partition: String
    var status: String
    var attempts: Int
    var error: String
    var log: String
    """The latest attempt's log file, or empty."""


struct State(Movable):
    """A connection to orca's database."""

    var conn: Connection

    def __init__(out self, url: String) raises:
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
        _ = tx.execute(
            "CREATE TABLE IF NOT EXISTS orca.schema_version (version int NOT"
            " NULL)"
        )
        var have = Int(
            tx.query(
                "SELECT coalesce(max(version), 0) AS v FROM orca.schema_version"
            )
            .row(0)
            .int64("v")
        )
        var migrations = _migrations()
        if have > len(migrations):
            raise Error(
                "orca: the database is at schema version ",
                have,
                ", newer than this binary's ",
                len(migrations),
            )
        for v in range(have, len(migrations)):
            for stmt in migrations[v]:
                _ = tx.execute(stmt)
            _ = tx.execute(
                "INSERT INTO orca.schema_version VALUES ($1)",
                Params().int64(Int64(v + 1)),
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
                " coalesce(owner, '') AS owner FROM orca.runs WHERE id = $1"
            ),
            Params().int64(Int64(run)),
        )
        if len(res) == 0:
            raise Error("orca: no run ", run)
        var r = res.row(0)
        return RunInfo(
            run,
            r.text("pipeline"),
            r.text("registry_hash"),
            r.text("partitions"),
            r.text("status"),
            r.text("owner"),
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
                " = now(), ended = NULL WHERE id = $1 AND status IN ('queued',"
                " 'running', 'failed') AND (heartbeat IS NULL OR heartbeat <"
                " now() - make_interval(secs => $3)) RETURNING id"
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
        ):
            raise Error("orca: run ", run, " is ", info.status)
        raise Error(
            "orca: run ",
            run,
            " is held by ",
            info.owner,
            ", whose heartbeat is fresher than ",
            stale_seconds,
            " s",
        )

    def interrupt(mut self, run: Int) raises -> Int:
        """Marks attempts left `running` by a process that died as failed,
        and returns how many there were."""
        var tx = self.conn.begin()
        var n = tx.execute(
            (
                "UPDATE orca.attempts a SET status = 'failed', ended = now(),"
                " error = 'interrupted: the process running it stopped' FROM"
                " orca.steps s WHERE a.step = s.id AND s.run = $1 AND a.status"
                " = 'running'"
            ),
            Params().int64(Int64(run)),
        )
        _ = tx.execute(
            (
                "UPDATE orca.steps SET status = 'failed', error = 'interrupted'"
                " WHERE run = $1 AND status = 'running'"
            ),
            Params().int64(Int64(run)),
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

    def cancel_requested(mut self, run: Int) raises -> Bool:
        return (
            self.conn.query(
                "SELECT cancel_requested FROM orca.runs WHERE id = $1",
                Params().int64(Int64(run)),
            )
            .row(0)
            .bool("cancel_requested")
        )

    def runs(mut self, limit: Int = 20) raises -> List[RunInfo]:
        """The latest runs, newest first."""
        var out = List[RunInfo]()
        var rows = self.conn.query(
            (
                "SELECT id, pipeline, registry_hash, partitions, status,"
                " coalesce(owner, '') AS owner FROM orca.runs ORDER BY id DESC"
                " LIMIT $1"
            ),
            Params().int64(Int64(limit)),
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
                " s.id) AS attempts, coalesce((SELECT a.log FROM orca.attempts"
                " a WHERE a.step = s.id ORDER BY a.attempt DESC LIMIT 1), '')"
                " AS log FROM orca.steps s WHERE s.run = $1 ORDER BY s.ord,"
                " s.id"
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
                )
            )
        return out^


struct PgStore(Movable, Store):
    """Records a run's attempts in Postgres, as its owner."""

    var db: State
    var url: String
    var run: Int
    var owner: String
    var registry_hash: String
    var _attempt_id: Int64
    """The `attempts` row of the attempt last started."""

    def __init__(
        out self,
        var db: State,
        url: String,
        run: Int,
        owner: String,
        registry_hash: String,
    ):
        self.db = db^
        self.url = url
        self.run = run
        self.owner = owner
        self.registry_hash = registry_hash
        self._attempt_id = 0

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
            raise Error("orca: lost run ", run, " to another process; stopping")
        if starting and res.row(0).bool("cancel_requested"):
            raise Error("orca: run ", run, " was cancelled; stopping")

    def start_attempt(
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

    def recorder(mut self) raises -> Recorder:
        return Recorder(self.url, self._attempt_id)

    def set_log(
        mut self, step: String, partition: String, attempt: Int, path: String
    ) raises:
        _ = self.db.conn.execute(
            "UPDATE orca.attempts SET log = $2 WHERE id = $1",
            Params().int64(self._attempt_id).text(path),
        )

    def finish_attempt(
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
