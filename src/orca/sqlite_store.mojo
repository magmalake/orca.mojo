"""orca's runs in a SQLite file: `SqliteStore`, for single-node orca.

A pipeline binary takes `ORCA_DATABASE_URL=sqlite:<path>` (or
`sqlite://<path>`, so `sqlite:///var/orca/runs.db` is an absolute path):
`run`, `resume` and `status` then keep pipelines, runs, steps, attempts,
reads and commits in that file, through sqlite-mojo, with nothing to
operate. A run killed anywhere is resumed from the file as from Postgres,
by the same `RunStore` code path. The scheduler, the UI and orca's other
commands stay on Postgres (`orca.state.State` refuses a SQLite URL).

The tables are Postgres's, under the same names without the `orca.` schema,
holding what a pipeline binary writes: steps' inputs and outputs are hex
text, times are Unix seconds (`julianday` based, to the millisecond).

**Concurrency.** The file is in WAL mode, so readers (`status`, `sqlite3`)
don't block the writer and the writer doesn't block them. There is one
writer at a time: each change is one `BEGIN IMMEDIATE` transaction, which
takes the file's write lock up front, and a writer that finds it taken
waits up to a second (`busy_timeout`), then fails; the owned run's writes
are retried, reopening the file, for `ORCA_DB_RETRY_SECONDS`, as `PgStore`
reconnects. That serialises several runs' processes on one machine, which
is fine for steps that take seconds and rows that take microseconds; it
isn't meant for hundreds of runs at once. Claims are a single guarded
`UPDATE`, so two resumes racing for a run can't both win, and every attempt
checks ownership in its own transaction, as on Postgres. The file must be
on a local disk: SQLite's locks don't hold over NFS and most network file
systems, so two machines must not share one.

`synchronous = FULL`: a commit is on disk before it returns, so a power
loss, not only a killed process, keeps every recorded attempt.
"""

from std.ffi import external_call
from std.os import getenv

from sqlite import Database, Statement

from orca.heartbeat import Beater, Heartbeat
from orca.store import (
    DEFERRED,
    FAILED,
    FINAL,
    INTERRUPTED,
    RUNNING,
    SKIPPED,
    SUCCEEDED,
    Backoff,
    RunInfo,
    RunResult,
    RunStore,
    StepInfo,
    StepRun,
    TableSnapshot,
    Usage,
)

comptime _NOW = "((julianday('now') - 2440587.5) * 86400.0)"
"""Unix seconds, to the millisecond, in SQL."""

comptime _BUSY_MS = 1000
"""How long a write waits for another writer's lock before it fails (and,
for the owned run, is retried on the file opened again)."""


def sqlite_path(url: String) -> String:
    """The file of `sqlite:<path>` or `sqlite://<path>`, or empty if `url`
    isn't a SQLite URL."""
    if not url.startswith("sqlite:"):
        return String()
    var rest = String(url[byte=7:])
    if rest.startswith("//"):
        return String(rest[byte=2:])
    return rest^


def _migrations() -> List[List[String]]:
    """Each migration is a list of statements; its version is its index + 1."""
    return [
        [
            """CREATE TABLE pipelines (
            name TEXT PRIMARY KEY,
            registry_hash TEXT NOT NULL,
            describe TEXT NOT NULL,
            binary_path TEXT,
            version INTEGER,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL)""",
            """CREATE TABLE pipeline_versions (
            id INTEGER PRIMARY KEY,
            pipeline TEXT NOT NULL REFERENCES pipelines (name),
            registry_hash TEXT NOT NULL,
            source TEXT NOT NULL,
            commit_sha TEXT NOT NULL,
            dirty INTEGER NOT NULL,
            image TEXT NOT NULL,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL,
            UNIQUE (pipeline, registry_hash, source, commit_sha, dirty,
                image))""",
            # AUTOINCREMENT: a run's number is in the summaries of the
            # snapshots it commits, so it's never given out twice.
            """CREATE TABLE runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline TEXT NOT NULL,
            registry_hash TEXT NOT NULL,
            partitions TEXT NOT NULL,
            trigger TEXT NOT NULL,
            status TEXT NOT NULL,
            owner TEXT,
            heartbeat REAL,
            created REAL NOT NULL,
            started REAL,
            ended REAL,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            version INTEGER REFERENCES pipeline_versions (id),
            error TEXT)""",
            """CREATE TABLE steps (
            id INTEGER PRIMARY KEY,
            run INTEGER NOT NULL REFERENCES runs (id),
            step TEXT NOT NULL,
            partition TEXT NOT NULL,
            ord INTEGER NOT NULL,
            status TEXT NOT NULL,
            input TEXT,
            output TEXT,
            error TEXT,
            UNIQUE (run, step, partition))""",
            """CREATE TABLE attempts (
            id INTEGER PRIMARY KEY,
            step INTEGER NOT NULL REFERENCES steps (id),
            attempt INTEGER NOT NULL,
            status TEXT NOT NULL,
            host TEXT NOT NULL,
            registry_hash TEXT NOT NULL,
            started REAL NOT NULL,
            ended REAL,
            error TEXT,
            log TEXT,
            UNIQUE (step, attempt))""",
            """CREATE TABLE reads (
            attempt INTEGER NOT NULL REFERENCES attempts (id),
            table_name TEXT NOT NULL,
            snapshot_id INTEGER NOT NULL,
            dataset TEXT)""",
            """CREATE TABLE commits (
            attempt INTEGER NOT NULL REFERENCES attempts (id),
            table_name TEXT NOT NULL,
            snapshot_id INTEGER NOT NULL,
            dataset TEXT)""",
            "CREATE INDEX reads_attempt ON reads (attempt)",
            "CREATE INDEX commits_attempt ON commits (attempt)",
            "CREATE INDEX steps_run ON steps (run, ord)",
        ],
        # A run a step deferred (#66): when it may go on, and how many
        # times it was, as Postgres's migration 18 keeps them.
        [
            "ALTER TABLE runs ADD COLUMN retry_at INTEGER",
            "ALTER TABLE runs ADD COLUMN deferrals INTEGER NOT NULL DEFAULT 0",
        ],
        # What each attempt's processes used, as Postgres's migration 35
        # keeps it (orca.usage); a run in a file has no scheduler or worker
        # to measure it, so they stay null unless copied in.
        [
            "ALTER TABLE attempts ADD COLUMN peak_rss_bytes INTEGER",
            "ALTER TABLE attempts ADD COLUMN cpu_seconds REAL",
            "ALTER TABLE attempts ADD COLUMN peak_cpus REAL",
        ],
    ]


def _open(path: String) raises -> Database:
    var db = Database(path)
    db.execute(String("PRAGMA busy_timeout = ", _BUSY_MS))
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA synchronous = FULL")
    db.execute("PRAGMA foreign_keys = ON")
    return db^


comptime _HEX = "0123456789abcdef"


def _hex(b: Span[UInt8, _]) -> String:
    var out = String(capacity_bytes=2 * len(b))
    for x in b:
        out.write(_HEX[byte=Int(x >> 4)], _HEX[byte=Int(x & 15)])
    return out^


def _nibble(c: UInt8) raises -> UInt8:
    if c >= 48 and c <= 57:
        return c - 48
    if c >= 97 and c <= 102:
        return c - 87
    raise Error("orca: not hex in the SQLite store")


def _unhex(s: String) raises -> List[UInt8]:
    var b = s.as_bytes()
    var out = List[UInt8](capacity=len(b) // 2)
    for i in range(0, len(b) - 1, 2):
        out.append(_nibble(b[i]) << 4 | _nibble(b[i + 1]))
    return out^


def _rollback(db: Database):
    try:
        db.execute("ROLLBACK")
    except:
        pass


struct SqliteBeat(Beater):
    """Refreshes a run's heartbeat in the file, on a connection of its own."""

    var path: String
    var run: Int
    var owner: String
    var timeouts: String
    """`step=seconds` for each step with a timeout, comma-separated."""
    var _db: Optional[Database]

    def __init__(
        out self, path: String, run: Int, owner: String, timeouts: String = ""
    ):
        self.path = path
        self.run = run
        self.owner = owner
        self.timeouts = timeouts
        self._db = None

    def beat(mut self) raises:
        if not self._db:
            self._db = _open(self.path)
        var st = self._db.value().prepare(
            String(
                "UPDATE runs SET heartbeat = ",
                _NOW,
                " WHERE id = ?1 AND owner = ?2 AND status = 'running'",
            )
        )
        st.bind_int(1, self.run)
        st.bind_text(2, self.owner)
        _ = st.step()
        # The watchdog (orca.heartbeat): an attempt past its step's
        # timeout stops the process. There's no cancel to time out here:
        # `orca cancel` is the scheduler's, on Postgres.
        var why = self._timed_out()
        if why:
            print("run", self.run, why, flush=True)
            external_call["_exit", NoneType](Int32(1))

    def _timed_out(mut self) raises -> String:
        """Fails the attempt past its step's timeout, its step and the run,
        in one transaction, and says why; empty if there's none."""
        if not self.timeouts:
            return ""
        ref db = self._db.value()
        for pair in self.timeouts.split(","):
            var eq = pair.find("=")
            if eq <= 0:
                continue
            var step = String(pair[byte=:eq])
            var secs = Float64(String(pair[byte = eq + 1 :]))
            var st = db.prepare(
                String(
                    "SELECT a.id, s.partition FROM attempts a JOIN steps s ON",
                    " s.id = a.step JOIN runs r ON r.id = s.run WHERE s.run =",
                    " ?1 AND s.step = ?2 AND r.owner = ?3 AND r.status =",
                    " 'running' AND a.status = 'running' AND a.started <= ",
                    _NOW,
                    " - ?4 LIMIT 1",
                )
            )
            st.bind_int(1, self.run)
            st.bind_text(2, step)
            st.bind_text(3, self.owner)
            st.bind_float(4, secs)
            var got = st.step()
            if not got:
                continue
            var attempt = got.value().int_val(0)
            var partition = got.value().text_val(1)
            var why = String(
                "timed out after ",
                Int(secs),
                " s (timeout_seconds), so its process was stopped",
            )
            db.execute("BEGIN IMMEDIATE")
            try:
                var a = db.prepare(
                    String(
                        "UPDATE attempts SET status = 'failed', ended = ",
                        _NOW,
                        ", error = ?2 WHERE id = ?1 AND status = 'running'",
                    )
                )
                a.bind_int(1, attempt)
                a.bind_text(2, why)
                _ = a.step()
                if db.changes() != 1:
                    _rollback(db)
                    return ""  # it ended as we looked
                var s = db.prepare(
                    "UPDATE steps SET status = 'failed', error = ?2 WHERE id ="
                    " (SELECT step FROM attempts WHERE id = ?1)"
                )
                s.bind_int(1, attempt)
                s.bind_text(2, why)
                _ = s.step()
                var named = String("step ", step)
                if partition:
                    named.write(" ", partition)
                var run_why = String(named, " ", why)
                var r = db.prepare(
                    String(
                        "UPDATE runs SET status = 'failed', ended = ",
                        _NOW,
                        ", heartbeat = NULL, error = ?3 WHERE id = ?1 AND",
                        " owner = ?2",
                    )
                )
                r.bind_int(1, self.run)
                r.bind_text(2, self.owner)
                r.bind_text(3, run_why)
                _ = r.step()
                db.execute("COMMIT")
                return run_why^
            except e:
                _rollback(db)
                raise e^
        return ""

    def reset(mut self):
        self._db = None


struct SqliteStore(Movable, RunStore):
    """orca's runs in a SQLite file, as their owner."""

    var path: String
    var db: Database
    var run: Int
    var owner: String
    var registry_hash: String
    var _attempt_id: Int
    """The `attempts` row of the attempt last started."""
    var _heartbeat: Optional[Heartbeat[SqliteBeat]]

    def __init__(out self, path: String) raises:
        """Opens the file at `path`, creating it if there's none; `own` a
        run before recording attempts."""
        if not path:
            raise Error("orca: no file in the SQLite URL")
        self.path = path
        self.db = _open(path)
        self.run = 0
        self.owner = String()
        self.registry_hash = String()
        self._attempt_id = 0
        self._heartbeat = None

    def _reopen(mut self):
        try:
            self.db = _open(self.path)
        except:
            pass  # the next try fails too, and comes back here

    def _stmt(self, sql: String) raises -> Statement:
        return self.db.prepare(sql)

    # --- RunStore ---

    def migrate(mut self) raises:
        # BEGIN IMMEDIATE takes the write lock: two processes starting at
        # once apply each migration once.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER"
                " NOT NULL)"
            )
            var have = (
                self._stmt(
                    "SELECT coalesce(max(version), 0) FROM schema_version"
                )
                .step()
                .value()
                .int_val(0)
            )
            var migrations = _migrations()
            if have > len(migrations):
                raise Error(
                    "orca: the SQLite file is at schema version ",
                    have,
                    ", newer than this binary's ",
                    len(migrations),
                )
            for v in range(have, len(migrations)):
                for stmt in migrations[v]:
                    self.db.execute(stmt)
                self.db.execute(
                    String("INSERT INTO schema_version VALUES (", v + 1, ")")
                )
            self.db.execute("COMMIT")
        except e:
            _rollback(self.db)
            raise e^

    def register(
        mut self, pipeline: String, registry_hash: String, describe: String
    ) raises:
        var st = self._stmt(
            String(
                "INSERT INTO pipelines (name, registry_hash, describe,",
                " first_seen, last_seen) VALUES (?1, ?2, ?3, ",
                _NOW,
                ", ",
                _NOW,
                ") ON CONFLICT (name) DO UPDATE SET registry_hash =",
                " excluded.registry_hash, describe = excluded.describe,",
                " last_seen = excluded.last_seen",
            )
        )
        st.bind_text(1, pipeline)
        st.bind_text(2, registry_hash)
        st.bind_text(3, describe)
        _ = st.step()
        var v = self.version(
            pipeline, registry_hash, describe, getenv("ORCA_IMAGE")
        )
        var up = self._stmt("UPDATE pipelines SET version = ?2 WHERE name = ?1")
        up.bind_text(1, pipeline)
        up.bind_int(2, v)
        _ = up.step()

    def scheduled_hash(mut self, pipeline: String) raises -> String:
        var st = self._stmt(
            "SELECT registry_hash FROM pipelines WHERE name = ?1 AND"
            " binary_path IS NOT NULL"
        )
        st.bind_text(1, pipeline)
        var r = st.step()
        return r.value().text_val(0) if r else String()

    def version(
        mut self,
        pipeline: String,
        registry_hash: String,
        describe: String,
        image: String,
    ) raises -> Int:
        var st = self._stmt(
            String(
                "INSERT INTO pipeline_versions (pipeline, registry_hash,",
                " source, commit_sha, dirty, image, first_seen, last_seen)",
                " VALUES (?1, ?2, coalesce(json_extract(?3,",
                " '$.source.repo'), ''), coalesce(json_extract(?3,",
                " '$.source.commit'), ''), coalesce(json_extract(?3,",
                " '$.source.dirty'), 0), ?4, ",
                _NOW,
                ", ",
                _NOW,
                ") ON CONFLICT (pipeline, registry_hash, source, commit_sha,",
                " dirty, image) DO UPDATE SET last_seen = excluded.last_seen",
                " RETURNING id",
            )
        )
        st.bind_text(1, pipeline)
        st.bind_text(2, registry_hash)
        st.bind_text(3, describe)
        st.bind_text(4, image)
        return st.step().value().int_val(0)

    def create_run(
        mut self,
        pipeline: String,
        registry_hash: String,
        partitions: String,
        trigger: String,
        owner: String,
    ) raises -> Int:
        var st = self._stmt(
            String(
                "INSERT INTO runs (pipeline, registry_hash, partitions,",
                " trigger, status, owner, heartbeat, created, started) VALUES",
                " (?1, ?2, ?3, ?4, 'running', ?5, ",
                _NOW,
                ", ",
                _NOW,
                ", ",
                _NOW,
                ") RETURNING id",
            )
        )
        st.bind_text(1, pipeline)
        st.bind_text(2, registry_hash)
        st.bind_text(3, partitions)
        st.bind_text(4, trigger)
        st.bind_text(5, owner)
        return st.step().value().int_val(0)

    def claim(mut self, run: Int, owner: String, stale_seconds: Float64) raises:
        var st = self._stmt(
            String(
                "UPDATE runs SET status = 'running', owner = ?2, heartbeat = ",
                _NOW,
                ", ended = NULL, error = NULL WHERE id = ?1 AND status IN",
                (
                    " ('queued', 'running', 'failed', 'deferred') AND"
                    " (heartbeat IS NULL OR"
                ),
                " heartbeat < ",
                _NOW,
                " - ?3) RETURNING id",
            )
        )
        st.bind_int(1, run)
        st.bind_text(2, owner)
        st.bind_float(3, stale_seconds)
        if st.step():
            return
        var info = self.run_info(run)
        if (
            info.status != RUNNING
            and info.status != FAILED
            and info.status != DEFERRED
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

    def ran_version(mut self, run: Int, version: Int) raises:
        var st = self._stmt("UPDATE runs SET version = ?2 WHERE id = ?1")
        st.bind_int(1, run)
        st.bind_int(2, version)
        _ = st.step()

    def interrupt(mut self, run: Int, why: String = "") raises -> Int:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            var st = self._stmt(
                String(
                    "UPDATE attempts SET status = 'failed', ended = ",
                    _NOW,
                    ", error = ?2 WHERE status = 'running' AND step IN",
                    " (SELECT id FROM steps WHERE run = ?1)",
                )
            )
            st.bind_int(1, run)
            st.bind_text(2, why if why else INTERRUPTED)
            _ = st.step()
            var n = self.db.changes()
            var steps = self._stmt(
                "UPDATE steps SET status = 'failed', error = ?2"
                " WHERE run = ?1 AND status = 'running'"
            )
            steps.bind_int(1, run)
            steps.bind_text(2, why if why else String("interrupted"))
            _ = steps.step()
            self.db.execute("COMMIT")
            return n
        except e:
            _rollback(self.db)
            raise e^

    def prior(mut self, run: Int) raises -> RunResult:
        var res = RunResult()
        var st = self._stmt(
            "SELECT step, partition, coalesce(output, '') FROM steps WHERE"
            " run = ?1 AND status = 'succeeded' ORDER BY id"
        )
        st.bind_int(1, run)
        while True:
            var r = st.step()
            if not r:
                break
            ref row = r.value()
            res.steps.append(
                StepRun(
                    row.text_val(0),
                    row.text_val(1),
                    SUCCEEDED,
                    _unhex(row.text_val(2)),
                    "",
                )
            )
        return res^

    def _run_infos(mut self, mut st: Statement) raises -> List[RunInfo]:
        var out = List[RunInfo]()
        while True:
            var r = st.step()
            if not r:
                break
            ref row = r.value()
            out.append(
                RunInfo(
                    row.int_val(0),
                    row.text_val(1),
                    row.text_val(2),
                    row.text_val(3),
                    row.text_val(4),
                    row.text_val(5),
                    row.text_val(6),
                )
            )
        return out^

    comptime _RUN_COLUMNS = (
        "SELECT id, pipeline, registry_hash, partitions, status,"
        " coalesce(owner, ''), coalesce(error, '') FROM runs"
    )

    def run_info(mut self, run: Int) raises -> RunInfo:
        var st = self._stmt(String(Self._RUN_COLUMNS, " WHERE id = ?1"))
        st.bind_int(1, run)
        var got = self._run_infos(st)
        if len(got) == 0:
            raise Error("orca: no run ", run)
        return got[0].copy()

    def runs(mut self, limit: Int) raises -> List[RunInfo]:
        var st = self._stmt(
            String(Self._RUN_COLUMNS, " ORDER BY id DESC LIMIT ?1")
        )
        st.bind_int(1, limit)
        return self._run_infos(st)

    def steps(mut self, run: Int) raises -> List[StepInfo]:
        var out = List[StepInfo]()
        var st = self._stmt(
            "SELECT s.step, s.partition, s.status, coalesce(s.error, ''),"
            " (SELECT count(*) FROM attempts a WHERE a.step = s.id),"
            " coalesce(l.log, ''), coalesce(l.peak_rss_bytes, -1),"
            " coalesce(l.cpu_seconds, -1.0), coalesce(l.peak_cpus, -1.0)"
            " FROM steps s LEFT JOIN attempts l ON l.id = (SELECT a.id FROM"
            " attempts a WHERE a.step = s.id ORDER BY a.attempt DESC LIMIT 1)"
            " WHERE s.run = ?1 ORDER BY s.ord, s.id"
        )
        st.bind_int(1, run)
        while True:
            var r = st.step()
            if not r:
                break
            ref row = r.value()
            out.append(
                StepInfo(
                    row.text_val(0),
                    row.text_val(1),
                    row.text_val(2),
                    row.int_val(4),
                    row.text_val(3),
                    row.text_val(5),
                    Usage(row.int_val(6), row.float_val(7), row.float_val(8)),
                )
            )
        return out^

    def own(mut self, run: Int, owner: String, registry_hash: String) raises:
        self.run = run
        self.owner = owner
        self.registry_hash = registry_hash

    def start_heartbeat(
        mut self, interval: Float64, timeouts: String = ""
    ) raises:
        self._heartbeat = Heartbeat(
            SqliteBeat(self.path, self.run, self.owner, timeouts), interval
        )

    def stop_heartbeat(mut self):
        self._heartbeat = None

    def finish_run(mut self, status: String) raises:
        var r = Backoff("SQLite")
        while True:
            try:
                var st = self._stmt(
                    String(
                        "UPDATE runs SET status = ?2, ended = ",
                        _NOW,
                        ", heartbeat = NULL WHERE id = ?1 AND owner = ?3",
                    )
                )
                st.bind_int(1, self.run)
                st.bind_text(2, status)
                st.bind_text(3, self.owner)
                _ = st.step()
                return
            except e:
                r.again(e)
                self._reopen()

    def defer_run(mut self, until: Int64) raises:
        var r = Backoff("SQLite")
        while True:
            try:
                var st = self._stmt(
                    String(
                        "UPDATE runs SET status = 'deferred', ended = ",
                        _NOW,
                        ", heartbeat = NULL, retry_at = ?3, deferrals =",
                        " deferrals + 1 WHERE id = ?1 AND owner = ?2",
                    )
                )
                st.bind_int(1, self.run)
                st.bind_text(2, self.owner)
                st.bind_int(3, Int(until))
                _ = st.step()
                return
            except e:
                r.again(e)
                self._reopen()

    def cancel_requested(mut self) raises -> Bool:
        var r = Backoff("SQLite")
        while True:
            try:
                var st = self._stmt(
                    "SELECT cancel_requested FROM runs WHERE id = ?1"
                )
                st.bind_int(1, self.run)
                return st.step().value().int_val(0) != 0
            except e:
                r.again(e)
                self._reopen()

    # --- Store: the owned run's attempts, each change one transaction ---

    def _still_owner(mut self, starting: Bool) raises:
        """Raises unless this process still owns the run, or, when a step is
        `starting`, if the run was asked to cancel. In the transaction."""
        var st = self._stmt(
            String(
                "UPDATE runs SET heartbeat = ",
                _NOW,
                " WHERE id = ?1 AND owner = ?2 AND status = 'running'",
                " RETURNING cancel_requested",
            )
        )
        st.bind_int(1, self.run)
        st.bind_text(2, self.owner)
        var r = st.step()
        if not r:
            raise Error(
                FINAL,
                "orca: lost run ",
                self.run,
                " to another process; stopping",
            )
        if starting and r.value().int_val(0) != 0:
            raise Error(
                FINAL, "orca: run ", self.run, " was cancelled; stopping"
            )

    def start_attempt(
        mut self,
        step: String,
        partition: String,
        ord: Int,
        input: Span[UInt8, _],
    ) raises -> Int:
        var r = Backoff("SQLite")
        var hex_input = _hex(input)
        while True:
            try:
                return self._start_attempt(step, partition, ord, hex_input)
            except e:
                r.again(e)
                self._reopen()

    def _start_attempt(
        mut self, step: String, partition: String, ord: Int, input: String
    ) raises -> Int:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._still_owner(starting=True)
            var up = self._stmt(
                "INSERT INTO steps (run, step, partition, ord, status, input)"
                " VALUES (?1, ?2, ?3, ?4, 'running', ?5) ON CONFLICT (run,"
                " step, partition) DO UPDATE SET status = 'running', input ="
                " excluded.input, error = NULL RETURNING id"
            )
            up.bind_int(1, self.run)
            up.bind_text(2, step)
            up.bind_text(3, partition)
            up.bind_int(4, ord)
            up.bind_text(5, input)
            var id = up.step().value().int_val(0)
            var nq = self._stmt(
                "SELECT coalesce(max(attempt), 0) + 1 FROM attempts WHERE step"
                " = ?1"
            )
            nq.bind_int(1, id)
            var n = nq.step().value().int_val(0)
            var ins = self._stmt(
                String(
                    "INSERT INTO attempts (step, attempt, status, host,",
                    " registry_hash, started) VALUES (?1, ?2, 'running', ?3,",
                    " ?4, ",
                    _NOW,
                    ") RETURNING id",
                )
            )
            ins.bind_int(1, id)
            ins.bind_int(2, n)
            ins.bind_text(3, self.owner)
            ins.bind_text(4, self.registry_hash)
            var attempt_id = ins.step().value().int_val(0)
            self.db.execute("COMMIT")
            self._attempt_id = attempt_id
            return n
        except e:
            _rollback(self.db)
            raise e^

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
        var r = Backoff("SQLite")
        var hex_output = _hex(output)
        while True:
            try:
                self._finish_attempt(
                    step, partition, attempt, status, hex_output, error
                )
                return
            except e:
                r.again(e)
                self._reopen()

    def _finish_attempt(
        mut self,
        step: String,
        partition: String,
        attempt: Int,
        status: String,
        output: String,
        error: String,
    ) raises:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._still_owner(starting=False)
            var q = self._stmt(
                "SELECT id FROM steps WHERE run = ?1 AND step = ?2 AND"
                " partition = ?3"
            )
            q.bind_int(1, self.run)
            q.bind_text(2, step)
            q.bind_text(3, partition)
            var id = q.step().value().int_val(0)
            var a = self._stmt(
                String(
                    "UPDATE attempts SET status = ?3, ended = ",
                    _NOW,
                    ", error = nullif(?4, '') WHERE step = ?1 AND attempt = ?2",
                )
            )
            a.bind_int(1, id)
            a.bind_int(2, attempt)
            a.bind_text(3, status)
            a.bind_text(4, error)
            _ = a.step()
            var s = self._stmt(
                "UPDATE steps SET status = ?2, output = ?3, error = nullif(?4,"
                " '') WHERE id = ?1"
            )
            s.bind_int(1, id)
            s.bind_text(2, status)
            if status == SUCCEEDED:
                s.bind_text(3, output)
            else:
                s.bind_null(3)
            s.bind_text(4, error)
            _ = s.step()
            self.db.execute("COMMIT")
        except e:
            _rollback(self.db)
            raise e^

    def skip(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        var r = Backoff("SQLite")
        while True:
            try:
                self._skip(step, partition, ord, reason)
                return
            except e:
                r.again(e)
                self._reopen()

    def _skip(
        mut self, step: String, partition: String, ord: Int, reason: String
    ) raises:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._still_owner(starting=True)
            var st = self._stmt(
                "INSERT INTO steps (run, step, partition, ord, status, error)"
                " VALUES (?1, ?2, ?3, ?4, 'skipped', ?5) ON CONFLICT (run,"
                " step, partition) DO UPDATE SET status = 'skipped', error ="
                " excluded.error, output = NULL"
            )
            st.bind_int(1, self.run)
            st.bind_text(2, step)
            st.bind_text(3, partition)
            st.bind_int(4, ord)
            st.bind_text(5, reason)
            _ = st.step()
            self.db.execute("COMMIT")
        except e:
            _rollback(self.db)
            raise e^

    def pinned_reads(
        mut self, step: String, partition: String
    ) raises -> Dict[String, Int64]:
        var r = Backoff("SQLite")
        while True:
            try:
                var out = Dict[String, Int64]()
                var st = self._stmt(
                    "SELECT r.table_name, r.snapshot_id FROM reads r JOIN"
                    " attempts a ON a.id = r.attempt JOIN steps s ON s.id ="
                    " a.step WHERE s.run = ?1 AND s.step = ?2 AND s.partition"
                    " = ?3 ORDER BY a.attempt DESC"
                )
                st.bind_int(1, self.run)
                st.bind_text(2, step)
                st.bind_text(3, partition)
                # Newest first, so the first attempt's snapshot is the one
                # left.
                while True:
                    var row = st.step()
                    if not row:
                        break
                    out[row.value().text_val(0)] = Int64(row.value().int_val(1))
                return out^
            except e:
                r.again(e)
                self._reopen()

    def record(mut self, commit: Bool, t: TableSnapshot) raises:
        # Not owner-checked, as on Postgres: what an attempt read or
        # committed is so even if the run was lost meanwhile.
        var r = Backoff("SQLite")
        while True:
            try:
                var st = self._stmt(
                    String(
                        "INSERT INTO ",
                        "commits" if commit else "reads",
                        " (attempt, table_name, snapshot_id, dataset) VALUES",
                        " (?1, ?2, ?3, nullif(?4, ''))",
                    )
                )
                st.bind_int(1, self._attempt_id)
                st.bind_text(2, t.table)
                st.bind_int(3, Int(t.snapshot_id))
                st.bind_text(4, t.dataset)
                _ = st.step()
                return
            except e:
                r.again(e)
                self._reopen()

    def fence(mut self) raises:
        # As on Postgres: the owner, and a heartbeat younger than half the
        # stale limit; a file that stays locked or unreadable is waited for
        # while the heartbeat could still be fresh, then the commit is off.
        var stale: Float64
        try:
            stale = Float64(getenv("ORCA_STALE_SECONDS", "30"))
        except:
            stale = 30.0
        var r = Backoff("SQLite")
        r.budget = stale / 2
        while True:
            try:
                var st = self._stmt(
                    String(
                        "SELECT coalesce(owner, ''), status, coalesce(",
                        "heartbeat > ",
                        _NOW,
                        " - ?2, 0) FROM runs WHERE id = ?1",
                    )
                )
                st.bind_int(1, self.run)
                st.bind_float(2, stale / 2)
                var got = st.step()
                if not got:
                    raise Error(
                        "orca: not committing: run ", self.run, " is gone"
                    )
                ref row = got.value()
                var owner = row.text_val(0)
                var status = row.text_val(1)
                var fresh = row.int_val(2) != 0
                if owner != self.owner or status != RUNNING:
                    raise Error(
                        "orca: not committing: run ",
                        self.run,
                        " is ",
                        status,
                        " and held by ",
                        owner if owner else "nobody",
                        ", not this process (",
                        self.owner,
                        ")",
                    )
                if not fresh:
                    raise Error(
                        "orca: not committing: run ",
                        self.run,
                        "'s heartbeat is older than ",
                        stale / 2,
                        " s, so another process may take it over",
                    )
                return
            except e:
                if String(e).startswith("orca: not committing"):
                    raise e^
                try:
                    r.again(e)
                except e2:
                    raise Error("orca: not committing: ", e2)
                self._reopen()

    def set_log(
        mut self, step: String, partition: String, attempt: Int, path: String
    ) raises:
        var r = Backoff("SQLite")
        while True:
            try:
                var st = self._stmt(
                    "UPDATE attempts SET log = ?2 WHERE id = ?1"
                )
                st.bind_int(1, self._attempt_id)
                st.bind_text(2, path)
                _ = st.step()
                return
            except e:
                r.again(e)
                self._reopen()
