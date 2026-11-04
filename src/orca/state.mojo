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

from std.os import getenv
from std.time import perf_counter_ns, sleep

from postgres import Connection, Params, Transaction

from orca.heartbeat import Beater, Heartbeat
from orca.store import (
    CANCELLED,
    FAILED,
    RUNNING,
    SKIPPED,
    SUCCEEDED,
    RunInfo,
    RunResult,
    RunStore,
    StepInfo,
    StepRun,
    TableSnapshot,
    process_owner,
)


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
        # Sensors and rebuilds (orca.sensors): since when a pipeline is a
        # sensor, the snapshots each sensor has seen and the run each
        # started, and rebuilds with their plans and runs.
        [
            "ALTER TABLE orca.pipelines ADD COLUMN sensing_since timestamptz",
            """CREATE TABLE orca.sensed (
            pipeline text NOT NULL REFERENCES orca.pipelines (name),
            dataset text NOT NULL,
            snapshot_id bigint NOT NULL,
            run bigint REFERENCES orca.runs (id),
            created timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (pipeline, dataset, snapshot_id))""",
            "CREATE INDEX sensed_run ON orca.sensed (run)",
            """CREATE TABLE orca.rebuilds (
            id bigserial PRIMARY KEY,
            dataset text NOT NULL,
            partitions text NOT NULL,
            status text NOT NULL,
            created timestamptz NOT NULL DEFAULT now(),
            ended timestamptz)""",
            """CREATE TABLE orca.rebuild_pipelines (
            rebuild bigint NOT NULL REFERENCES orca.rebuilds (id),
            pipeline text NOT NULL REFERENCES orca.pipelines (name),
            level int NOT NULL,
            partitions text NOT NULL,
            run bigint REFERENCES orca.runs (id),
            PRIMARY KEY (rebuild, pipeline))""",
            (
                "ALTER TABLE orca.runs ADD COLUMN rebuild bigint REFERENCES"
                " orca.rebuilds (id)"
            ),
        ],
        # Keys: what's done is a step's status by its partition key.
        ["CREATE INDEX steps_partition ON orca.steps (partition, step)"],
        # Runs on Kubernetes (orca.kube): the image a pipeline's pods run,
        # and the CPU a run's pods asked for, in thousandths of a vCPU.
        [
            "ALTER TABLE orca.pipelines ADD COLUMN image text",
            "ALTER TABLE orca.runs ADD COLUMN cpu_millis int",
        ],
        # Login to orca ui (orca.auth): users, their sessions, and what each
        # did. pgcrypto, a trusted extension, hashes the passwords.
        [
            "CREATE EXTENSION IF NOT EXISTS pgcrypto",
            """CREATE TABLE orca.users (
            name text PRIMARY KEY,
            password_hash text NOT NULL,
            created timestamptz NOT NULL DEFAULT now())""",
            """CREATE TABLE orca.sessions (
            token_hash bytea PRIMARY KEY,
            username text NOT NULL REFERENCES orca.users (name) ON DELETE
                CASCADE,
            created timestamptz NOT NULL DEFAULT now(),
            expires timestamptz NOT NULL)""",
            """CREATE TABLE orca.audit (
            id bigserial PRIMARY KEY,
            at timestamptz NOT NULL DEFAULT now(),
            username text NOT NULL,
            action text NOT NULL,
            run bigint,
            detail text NOT NULL)""",
            "CREATE INDEX audit_run ON orca.audit (run)",
        ],
        # The ec2 runner (orca.ec2): a pipeline's instance types, its
        # workers' count and whether they're Spot, as registered.
        [
            "ALTER TABLE orca.pipelines ADD COLUMN head_type text",
            "ALTER TABLE orca.pipelines ADD COLUMN worker_type text",
            "ALTER TABLE orca.pipelines ADD COLUMN ec2_workers int",
            "ALTER TABLE orca.pipelines ADD COLUMN spot boolean",
        ],
        # Versions (orca.source): each registry hash, source commit and
        # image a pipeline was registered or run with, the pipeline's
        # latest, and the one each run ran.
        [
            """CREATE TABLE orca.pipeline_versions (
            id bigserial PRIMARY KEY,
            pipeline text NOT NULL REFERENCES orca.pipelines (name),
            registry_hash text NOT NULL,
            source text NOT NULL,
            commit_sha text NOT NULL,
            dirty boolean NOT NULL,
            image text NOT NULL,
            first_seen timestamptz NOT NULL DEFAULT now(),
            last_seen timestamptz NOT NULL DEFAULT now(),
            UNIQUE (pipeline, registry_hash, source, commit_sha, dirty,
                image))""",
            (
                "ALTER TABLE orca.pipelines ADD COLUMN version bigint"
                " REFERENCES orca.pipeline_versions (id)"
            ),
            (
                "ALTER TABLE orca.runs ADD COLUMN version bigint REFERENCES"
                " orca.pipeline_versions (id)"
            ),
        ],
        # The instances a run ran on (orca.ec2): what each was, when it
        # started and ended, and why it ended.
        [
            """CREATE TABLE orca.instances (
            id text PRIMARY KEY,
            run bigint NOT NULL REFERENCES orca.runs (id),
            start int NOT NULL,
            role text NOT NULL,
            instance_type text NOT NULL,
            market text NOT NULL,
            launched timestamptz NOT NULL DEFAULT now(),
            ended timestamptz,
            why text)""",
            "CREATE INDEX instances_run ON orca.instances (run)",
            (
                "CREATE INDEX instances_live ON orca.instances (run) WHERE"
                " ended IS NULL"
            ),
            # Where its functions are in its source (orca.locate), as
            # registration found them beside the binary.
            "ALTER TABLE orca.pipeline_versions ADD COLUMN locations jsonb",
        ],
        # Autoscaling on EC2: a pipeline's fewest workers (`--workers
        # A..B`; null, a fixed count), each instance's private address, and
        # when the scheduler drained a worker.
        [
            "ALTER TABLE orca.pipelines ADD COLUMN ec2_min_workers int",
            "ALTER TABLE orca.instances ADD COLUMN address text",
            "ALTER TABLE orca.instances ADD COLUMN drained timestamptz",
        ],
        # Secrets a pipeline's runs get (`--secret NAME=ID`): environment
        # variable to Secrets Manager secret ID.
        [
            "ALTER TABLE orca.pipelines ADD COLUMN secrets jsonb",
        ],
        # Why a run ended without running, or stopped where no step could
        # say: code its binary refuses, a process that exited before it
        # claimed the run, a cluster that couldn't start.
        [
            "ALTER TABLE orca.runs ADD COLUMN error text",
        ],
        # `orca pause`: the scheduler starts none of a paused pipeline's
        # runs, which stay queued (or due to retry) until `orca unpause`.
        [
            (
                "ALTER TABLE orca.pipelines ADD COLUMN paused boolean NOT NULL"
                " DEFAULT false"
            ),
        ],
        # Sensing that went wrong: why a snapshot was recorded as seen
        # without a run (its keys aren't partitions), and the error of a
        # sensor's last pass that failed, cleared by one that doesn't.
        [
            "ALTER TABLE orca.sensed ADD COLUMN error text",
            "ALTER TABLE orca.pipelines ADD COLUMN sense_error text",
            "ALTER TABLE orca.pipelines ADD COLUMN sense_error_at timestamptz",
        ],
    ]


comptime _MIGRATION_LOCK = 0x6F726361  # "orca"


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
        _ = self.conn.execute(
            "UPDATE orca.pipelines SET version = $2 WHERE name = $1",
            Params()
            .text(pipeline)
            .int64(
                Int64(
                    self.version(
                        pipeline, registry_hash, describe, getenv("ORCA_IMAGE")
                    )
                )
            ),
        )

    def scheduled_hash(mut self, pipeline: String) raises -> String:
        """The registry hash `orca register` gave `pipeline`, or empty if it
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
                    "INSERT INTO orca.pipeline_versions (pipeline,"
                    " registry_hash, source, commit_sha, dirty, image) SELECT"
                    " $1, $2, coalesce(d->'source'->>'repo', ''),"
                    " coalesce(d->'source'->>'commit', ''),"
                    " coalesce((d->'source'->>'dirty')::boolean, false), $4"
                    " FROM (SELECT $3::jsonb AS d) AS x ON CONFLICT (pipeline,"
                    " registry_hash, source, commit_sha, dirty, image) DO"
                    " UPDATE SET last_seen = now() RETURNING id"
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
            raise Error("orca: no run ", run)
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
                " IN ('queued', 'running', 'failed') AND (heartbeat IS NULL OR"
                " heartbeat < now() - make_interval(secs => $3)) RETURNING id"
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
                " coalesce(owner, '') AS owner, coalesce(error, '') AS error"
                " FROM orca.runs ORDER BY id DESC LIMIT $1"
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


comptime FINAL = "\x1f"
"""Starts the message of an error a retry can't help: the run was lost to
another process, or cancelled. `DbRetry` raises it at once, without it."""


def db_retry_seconds() -> Float64:
    """`ORCA_DB_RETRY_SECONDS`: how long a run's process keeps retrying a
    write to Postgres that failed, by default the stale limit
    (`ORCA_STALE_SECONDS`, 30), past which another process may take the run
    over anyway."""
    try:
        return Float64(
            getenv("ORCA_DB_RETRY_SECONDS", getenv("ORCA_STALE_SECONDS", "30"))
        )
    except:
        return 30.0


struct DbRetry(Movable):
    """Retries of one call to Postgres from a run's process.

    `again(e, ...)` is called with what the call raised: it raises again
    if the error is final (`FINAL`) or the time is up, else it waits, a
    quarter of a second doubling to 4 s, and makes a new connection, so a
    restart or failover of the server doesn't stop the run."""

    var started: Int
    var wait: Float64
    var budget: Float64

    def __init__(out self):
        self.started = perf_counter_ns()
        self.wait = 0.25
        self.budget = db_retry_seconds()

    def again(mut self, e: Error, mut db: State, url: String) raises:
        var msg = String(e)
        if msg.startswith(FINAL):
            raise Error(String(msg[byte = FINAL.byte_length() :]))
        var spent = Float64(perf_counter_ns() - self.started) / 1e9
        if spent + self.wait > self.budget:
            raise Error(msg)
        print(
            "orca: Postgres failed (",
            msg,
            "); retrying in ",
            self.wait,
            " s",
            sep="",
        )
        sleep(self.wait)
        self.wait = min(self.wait * 2, 4.0)
        try:
            db = State(url)
        except:
            pass  # the next try fails too, and comes back here


struct PgBeat(Beater):
    """Refreshes a run's heartbeat in Postgres, on a connection of its own."""

    var url: String
    var run: Int
    var owner: String
    var _conn: Optional[Connection]

    def __init__(out self, url: String, run: Int, owner: String):
        self.url = url
        self.run = run
        self.owner = owner
        self._conn = None

    def beat(mut self) raises:
        if not self._conn:
            self._conn = Connection(self.url)
        _ = self._conn.value().execute(
            (
                "UPDATE orca.runs SET heartbeat = now() WHERE id = $1 AND"
                " owner = $2 AND status = 'running'"
            ),
            Params().int64(Int64(self.run)).text(self.owner),
        )

    def reset(mut self):
        self._conn = None


struct PgStore(Movable, RunStore):
    """orca's runs in Postgres, `State` behind the `RunStore` trait.

    The calls for the owned run (attempts, skips, reads and commits, its
    end) are retried for `ORCA_DB_RETRY_SECONDS` on a new connection
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

    def interrupt(mut self, run: Int) raises -> Int:
        return self.db.interrupt(run)

    def prior(mut self, run: Int) raises -> RunResult:
        return self.db.prior(run)

    def run_info(mut self, run: Int) raises -> RunInfo:
        return self.db.run_info(run)

    def runs(mut self, limit: Int) raises -> List[RunInfo]:
        return self.db.runs(limit)

    def steps(mut self, run: Int) raises -> List[StepInfo]:
        return self.db.steps(run)

    def own(mut self, run: Int, owner: String, registry_hash: String) raises:
        self.run = run
        self.owner = owner
        self.registry_hash = registry_hash

    def start_heartbeat(mut self, interval: Float64) raises:
        self._heartbeat = Heartbeat(
            PgBeat(self.url, self.run, self.owner), interval
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
                FINAL, "orca: lost run ", run, " to another process; stopping"
            )
        if starting and res.row(0).bool("cancel_requested"):
            raise Error(FINAL, "orca: run ", run, " was cancelled; stopping")

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
