"""`orca register`: recording a pipeline binary and how its runs run
(`register_pipeline`).
"""

from std.os.path import exists
from postgres import Params
from orca.config import (
    ProjectConfig,
    RegisterFlags,
    max_runs_auto,
    parse_keys_per_run,
    parse_max_runs,
    parse_worker_procs,
    parse_workers,
    resolve,
)
from orca.fill import (
    fill_refusal,
    parse_fill_budget,
    parse_fill_max_runs,
)
from orca.proc import (
    absolute,
    binary_seconds,
    capture,
)
from orca.secrets import secrets_json
from orca.sensors import parse_sense
from orca.scheduler import (
    Scheduler,
    _RETRIED,
)
from orca.registered import (
    registered,
    repeated_fill,
)
from orca.queueing import (
    backfill,
    dataset_cycle,
    kind_mismatch,
)


def register_pipeline(
    mut self: Scheduler,
    binary: String,
    now: Int64,
    flags: RegisterFlags = RegisterFlags(),
    file: ProjectConfig = ProjectConfig(),
) raises -> String:
    """Records the pipeline `binary` describes, with its path; its
    schedule counts from `now` the first time, and a sensor from now the
    first time it's one, with the snapshots its datasets have now seen.
    Queued runs move to the new code, and so do failed ones waiting for
    a retry with no step done. How its runs run is `flags` over what
    `file`, the project's orca.toml (`orca.config`), says for it: for a
    pipeline the file names, exactly that, a setting neither gives
    going back to its default; for one it doesn't, the flags given,
    each left out keeping what was registered (`--workers MIN..MAX`
    scales an EC2 run's workers, `--worker-procs` is the dista worker
    processes per EC2 worker instance, `--secret`s replace the
    pipeline's, `--placement` is `control`, `ec2` or `default`).
    Returns the pipeline's name."""
    var path = absolute(binary)
    var got = capture([path, "describe"], binary_seconds())
    if got[0] != 0 or not got[1].startswith('{"pipeline": '):
        raise Error("orca: ", path, " describe exited ", got[0], ": ", got[1])
    var cycle = dataset_cycle(self.db, got[1])
    if cycle:
        raise Error("orca: ", path, " can't be registered: ", cycle)
    var mismatch = kind_mismatch(self.db, got[1])
    if mismatch:
        raise Error("orca: ", path, " can't be registered: ", mismatch)
    var info = self.db.conn.query(
        (
            "SELECT $1::jsonb->>'pipeline' AS name,"
            " coalesce(($1::jsonb->>'sensor')::boolean, false) AS"
            " sensor, NOT EXISTS (SELECT 1 FROM orca.pipelines WHERE name"
            " = $1::jsonb->>'pipeline' AND sensing_since IS NOT NULL) AS"
            " new_sensor, coalesce($1::jsonb->'partitions'->>'kind',"
            " 'monthly') AS kind, coalesce($1::jsonb->'partitions'->>"
            "'start', '') AS start"
        ),
        Params().text(got[1]),
    ).row(0)
    var sensor = info.bool("sensor")
    var r = resolve(info.text("name"), flags, file)
    var workers = 0
    var min_workers = -1
    if r.text("workers"):
        var w = parse_workers(r.text("workers"))
        min_workers = w[0]
        workers = w[1]
    var worker_procs = -1
    if r.text("worker_procs"):
        worker_procs = parse_worker_procs(r.text("worker_procs"))
    var max_runs = -1
    var auto_runs = False
    if r.text("max_runs"):
        max_runs = parse_max_runs(r.text("max_runs"))
        auto_runs = max_runs_auto(r.text("max_runs"))
    # Fills (orca.fill): only from the file.
    if r.text("fills") == "all":
        var why = fill_refusal(
            info.text("kind"), info.text("start"), sensor, r.name
        )
        if why:
            var at = file.where(r.name, "fills", r.values["fills"].copy())
            raise Error(
                "orca: ",
                at,
                ' fills = "all" can\'t be registered: ',
                why,
                '. Set fills = "since_registration" for it in its',
                " own table",
            )
    var keys_per_run = -1
    if r.text("keys_per_run"):
        keys_per_run = parse_keys_per_run(r.text("keys_per_run"))
    var fill_max_runs = -1
    if r.text("fill_max_runs"):
        fill_max_runs = parse_fill_max_runs(r.text("fill_max_runs"))
    var fill_budget = -1.0
    if r.text("fill_budget_per_day"):
        fill_budget = parse_fill_budget(r.text("fill_budget_per_day"))
    var seen = String()
    if sensor:
        var sensed = capture([path, "sense"], binary_seconds())
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
    # $14: the file names it, so what it and the flags say is all of
    # it; otherwise a value not given keeps what was registered.
    var res = self.db.conn.query(
        (
            "INSERT INTO orca.pipelines (name, registry_hash, describe,"
            " binary_path, since, sensing_since, image, head_type,"
            " worker_type, ec2_workers, spot, ec2_min_workers, secrets,"
            " worker_procs, placement, max_runs, env, config, fills,"
            " fill_order, fill_max_runs, fill_budget_per_day, fill_window,"
            " cpus, memory_gb, fill_lag, learn, max_runs_auto, keys_per_run)"
            " SELECT"
            " d->>'pipeline',"
            " d->>'registry_hash',"
            " d, $2, $3, CASE WHEN $4 THEN now() END, nullif($5, ''),"
            " nullif($6, ''), nullif($7, ''), nullif($8, 0), $9,"
            " nullif($10, -1), nullif($11, '')::jsonb, nullif($12, -1),"
            " nullif(nullif($13, ''), 'default'), nullif($15, -1),"
            " nullif($16, '')::jsonb, $17::jsonb, nullif($18, ''),"
            " nullif($19, ''), nullif($20, -1), nullif($21, -1::float8),"
            " nullif($22, ''), nullif($23, ''), nullif($24, ''),"
            " nullif($25, ''), nullif($26, '')::boolean, CASE WHEN $15 > 0"
            " THEN $27 END, nullif($28, -1) FROM"
            " (SELECT $1::jsonb AS d) AS x ON CONFLICT (name) DO UPDATE SET"
            " registry_hash = EXCLUDED.registry_hash, describe ="
            " EXCLUDED.describe, binary_path = EXCLUDED.binary_path, since"
            " = coalesce(orca.pipelines.since, EXCLUDED.since),"
            " sensing_since = CASE WHEN $4 THEN"
            " coalesce(orca.pipelines.sensing_since, now()) END, image ="
            " coalesce(EXCLUDED.image, orca.pipelines.image), head_type ="
            " CASE WHEN $14 THEN EXCLUDED.head_type ELSE"
            " coalesce(EXCLUDED.head_type, orca.pipelines.head_type) END,"
            " worker_type = CASE WHEN $14 THEN EXCLUDED.worker_type ELSE"
            " coalesce(EXCLUDED.worker_type, orca.pipelines.worker_type)"
            " END, ec2_workers = CASE WHEN $14 THEN EXCLUDED.ec2_workers"
            " ELSE coalesce(EXCLUDED.ec2_workers,"
            " orca.pipelines.ec2_workers) END, ec2_min_workers = CASE WHEN"
            " $14 OR $8 > 0 THEN EXCLUDED.ec2_min_workers ELSE"
            " orca.pipelines.ec2_min_workers END, secrets = CASE WHEN $14"
            " THEN EXCLUDED.secrets ELSE coalesce(EXCLUDED.secrets,"
            " orca.pipelines.secrets) END, worker_procs = CASE WHEN $14"
            " THEN EXCLUDED.worker_procs WHEN $12 >= 0 THEN $12 ELSE"
            " orca.pipelines.worker_procs END, placement = CASE WHEN $14"
            " THEN EXCLUDED.placement WHEN $13 = '' THEN"
            " orca.pipelines.placement ELSE nullif($13, 'default') END,"
            " max_runs = CASE WHEN $14 OR $15 > 0 THEN EXCLUDED.max_runs"
            " ELSE orca.pipelines.max_runs END, max_runs_auto = CASE WHEN"
            " $14 OR $15 > 0 THEN EXCLUDED.max_runs_auto ELSE"
            " orca.pipelines.max_runs_auto END, env = CASE WHEN $14 THEN"
            " EXCLUDED.env ELSE orca.pipelines.env END, config = CASE WHEN"
            " $14 THEN EXCLUDED.config ELSE jsonb_build_object('sources',"
            " coalesce(orca.pipelines.config->'sources', '{}'::jsonb) ||"
            " coalesce(EXCLUDED.config->'sources', '{}'::jsonb)) END, spot"
            " = CASE WHEN $6 <> '' OR $7 <> '' OR $8 > 0 OR $9 THEN $9 ELSE"
            " orca.pipelines.spot END, fills = CASE WHEN $14 THEN"
            " EXCLUDED.fills ELSE orca.pipelines.fills END, fill_order ="
            " CASE WHEN $14 THEN EXCLUDED.fill_order ELSE"
            " orca.pipelines.fill_order END, fill_max_runs = CASE WHEN $14"
            " THEN EXCLUDED.fill_max_runs ELSE orca.pipelines.fill_max_runs"
            " END, fill_budget_per_day = CASE WHEN $14 THEN"
            " EXCLUDED.fill_budget_per_day ELSE"
            " orca.pipelines.fill_budget_per_day END, fill_window = CASE"
            " WHEN $14 THEN EXCLUDED.fill_window ELSE"
            " orca.pipelines.fill_window END, fill_lag = CASE WHEN $14 THEN"
            " EXCLUDED.fill_lag ELSE orca.pipelines.fill_lag END, learn ="
            " CASE WHEN $14 THEN EXCLUDED.learn ELSE orca.pipelines.learn"
            " END, keys_per_run = CASE WHEN $14 THEN EXCLUDED.keys_per_run"
            " ELSE orca.pipelines.keys_per_run END, cpus ="
            " CASE WHEN $14 OR $23"
            " <> '' THEN EXCLUDED.cpus ELSE orca.pipelines.cpus END,"
            " memory_gb = CASE WHEN $14 OR $24 <> '' THEN"
            " EXCLUDED.memory_gb ELSE orca.pipelines.memory_gb END,"
            # A new request forgets what out-of-memory kills raised.
            " memory_raised_mb = CASE WHEN (CASE WHEN $14 OR $24 <> ''"
            " THEN EXCLUDED.memory_gb ELSE orca.pipelines.memory_gb END)"
            " IS DISTINCT FROM orca.pipelines.memory_gb THEN NULL ELSE"
            " orca.pipelines.memory_raised_mb END,"
            " last_seen = now() RETURNING name, registry_hash,"
            " coalesce(image, '') AS image"
        ),
        Params()
        .text(got[1])
        .text(path)
        .int64(now)
        .bool(sensor)
        .text(flags.image)
        .text(r.text("head_type"))
        .text(r.text("worker_type"))
        .int64(Int64(workers))
        .bool(flags.spot)
        .int64(Int64(min_workers))
        .text(secrets_json(r.secret_specs()))
        .int64(Int64(worker_procs))
        .text(r.text("placement"))
        .bool(r.in_file)
        .int64(Int64(max_runs))
        .text(r.env_json())
        .text(r.config_json(file))
        .text(r.text("fills"))
        .text(r.text("fill_order"))
        .int64(Int64(fill_max_runs))
        .float64(fill_budget)
        .text(r.text("fill_window"))
        .text(r.text("cpus"))
        .text(r.text("memory_gb"))
        .text(r.text("fill_lag"))
        .text(r.text("learn"))
        .bool(auto_runs)
        .int64(Int64(keys_per_run)),
    )
    var name = res.row(0).text("name")
    var version = self.db.version(
        name,
        res.row(0).text("registry_hash"),
        got[1],
        res.row(0).text("image"),
    )
    # The data version before and after (#297), for the notice below:
    # a first registration has no before.
    var dv = self.db.conn.query(
        (
            "SELECT p.version IS NOT NULL AS was, coalesce((SELECT"
            " data_version FROM orca.pipeline_versions WHERE id ="
            " p.version), '') AS old, coalesce((SELECT data_version FROM"
            " orca.pipeline_versions WHERE id = $2), '') AS new FROM"
            " orca.pipelines p WHERE p.name = $1"
        ),
        Params().text(name).int64(Int64(version)),
    )
    var data_changed = dv.row(0).bool("was") and dv.row(0).text(
        "old"
    ) != dv.row(0).text("new")
    var data_version = dv.row(0).text("new")
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
    for q in registered(self.db, name):
        if data_changed:
            var what = String(
                "its steps' version= is now ",
                data_version if data_version else "none",
            )
            if q.fill.all:
                print(
                    "orca:",
                    q.name,
                    String(what, ":"),
                    "its fill runs its partitions again, within its",
                    "fill limits",
                )
            else:
                print(
                    "orca:",
                    q.name,
                    String(what, ":"),
                    "its keys are no longer done; regenerate its history",
                    "with `orca backfill",
                    q.name,
                    '<range>` (or fills = "all")',
                )
        var twice = repeated_fill(q, now)
        if twice:
            print("orca: warning:", twice)
        if q.fill.all and q.fill.budget < 0:
            print(
                "orca: warning:",
                q.name,
                "fills its range with no fill_budget_per_day: its fill",
                "runs start only where they cost nothing (placement",
                "control, or the local runner); on ec2 or kubernetes",
                "they wait for a budget",
            )
    # A retry of a run with steps done would run the new code over the
    # old code's outputs, which only a resume that says so may do.
    _ = self.db.conn.execute(
        String(
            "UPDATE orca.runs r SET registry_hash = $2 WHERE pipeline =",
            " $1 AND registry_hash <> $2 AND (status = 'queued' OR",
            " ((status = 'deferred' OR (status = 'failed' AND ",
            _RETRIED,
            " AND starts - deferrals BETWEEN 1 AND (SELECT",
            " coalesce((describe->>'run_retries')::bigint, 3) FROM",
            " orca.pipelines WHERE name = $1))) AND NOT EXISTS (SELECT 1",
            " FROM orca.steps s WHERE s.run = r.id AND s.status =",
            " 'succeeded')))",
        ),
        Params().text(name).text(res.row(0).text("registry_hash")),
    )
    return name^
