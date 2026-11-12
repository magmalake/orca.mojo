"""Starting runs (`orca.scheduler`): due retries, dead runs and queued ones,
within `max_runs`, memory pressure, grants and the vCPU budget; and a
control-placed run's process.
"""

from std.os import makedirs
from postgres import Params
from orca.cgroup import (
    Cgroups,
    raised_request,
)
from orca.control import (
    CONTROL,
    control_stage,
    fetch_secret,
    pool_of,
    pressure_reason,
    run_env,
    secrets_env,
    slots_reason,
)
from orca.fill import (
    FillStatus,
    fill_status,
)
from orca.system import system_now
from orca.batches import mark_batch
from orca.live import behind_sql, hold_behind
from orca.kube import (
    RunSpec,
    cpu_millis,
)
from orca.logs import (
    is_remote,
    run_dir,
    stage_of,
)
from orca.proc import spawn
from orca.sample import (
    metrics_location,
    sample_seconds,
)
from orca.resources import (
    Grant,
    Request,
    admit,
    resolve_auto,
    run_env as grant_env,
)
from orca.pressure import pressure_env
from orca.recommend import (
    DEFAULT,
    RunRequest,
)
from orca.usage import Meter
from orca.scheduler import (
    AUTO_ONE_AT_A_TIME,
    Scheduler,
    _BUDGET_LOCK,
    _CONTROL_LOCK,
    _ControlProc,
    _HOST_LOCK,
    _RETRIED,
)
from orca.registered import (
    Registered,
    _ec2_spec,
    fill_pipeline,
    recorded_cpus,
    recorded_memory_gb,
    run_request,
)
from orca.pool_runs import (
    _fill_for_workers,
    _requeue_for_workers,
    _requeue_orphans,
)


def _start(
    mut self: Scheduler, p: Registered, now: Int64, fill: Bool = False
) raises:
    """Starts queued runs, retries that are due and runs whose process
    died, up to `max_runs` live at once; none while `p` is paused, when
    they wait (runs already going go on). A control-placed pipeline's
    runs start here (`orca.control`), within the control instance's
    limits; a queued run those hold, or that's placed on a runner this
    scheduler hasn't, is `held` with the reason.

    With `fill`, the same for its fill runs (trigger `fill`), up to
    `fill_max_runs` beside the others: due retries first, then new
    runs over the partitions `orca.fill.fill_status` gives, within its
    window and budget."""
    var pool = pool_of(p.placement)
    if pool:
        # A remote worker of the pool starts them (orca.workers).
        if fill:
            _fill_for_workers(self, p, now, pool)
        else:
            _requeue_for_workers(self, p, now, pool)
        return
    var dead = self._dead("$2")
    # Which runs this pass is for: its fill runs, or all the others.
    var which = String(" AND trigger = 'fill'") if fill else String(
        " AND trigger <> 'fill'"
    )
    var control = p.placement == CONTROL
    var placed = String(CONTROL) if control else (
        String("ec2") if self.ec2 else (
            String("kubernetes") if self.runner else String("local")
        )
    )
    var hold = String()
    if p.placement == "ec2" and not self.ec2:
        hold = String(
            "held: placed on ec2, but this scheduler's runner",
            " (ORCA_RUNNER) is ",
            placed,
        )
    var pressure = String()
    if control:
        try:
            pressure = pressure_reason(system_now(self.db))
        except e:
            print("orca: can't read the control instance's samples:", e)
    if not control and not hold and (self.runner or self.ec2) and not p.image:
        if fill:
            return  # said once, by the pass for its other runs
        print(
            "orca: pipeline",
            p.name,
            "has no image (orca register --image), so its runs wait",
        )
        return
    # On EC2 with the warm pool, the keys of the instances its runs may
    # take, and whether some are still on their way to the pool from a
    # run that just ended: then nothing starts this tick, so the next
    # run takes them rather than new ones.
    var keys = List[String]()
    var settling = False
    if self.ec2 and not control:
        keys = self.ec2.value().pool_keys(_ec2_spec(p, 0, 0, False))
        settling = self.ec2.value().settling(self.db, keys)
    # Its request on this host (orca.resources), `auto` learned from its
    # runs (orca.recommend): resolved here, once for the starts this
    # pass makes, each granted from it for its life.
    var sized = RunRequest(Request(0, 0, 0, 0), "", 0, "")
    if control or placed == "local":
        sized = run_request(self.db, p, self.room.cpus)
    var tx = self.db.conn.begin()
    _ = tx.query(
        "SELECT name FROM orca.pipelines WHERE name = $1 FOR UPDATE",
        Params().text(p.name),
    )
    # A run of a scheduler instance that's gone, whose heartbeat is
    # stale, is queued again whatever its trigger, as a dead worker's
    # is; one its live instance still holds isn't dead
    # (orca.live.held_sql), so nothing below ends or restarts it.
    var lost = List[Tuple[Int, String]]()
    var local = control or placed == "local"
    if not fill:
        lost = _requeue_orphans(self, tx, p)
        self._end_dead(tx, p, dead)
    var live = Int(
        tx.query(
            String(
                "SELECT count(*) AS n FROM orca.runs WHERE pipeline = $1",
                " AND status = 'running' AND NOT ",
                dead,
                which,
            ),
            Params().text(p.name).float64(self.stale),
        )
        .row(0)
        .int64("n")
    )
    var free = 0 if p.paused or settling or hold else p.max_runs - live
    var status = Optional[FillStatus](None)
    if fill:
        status = fill_status(
            tx,
            fill_pipeline(
                p,
                placed,
                Int(self.ec2.value().pool_seconds) if self.ec2 else 0,
            ),
            now,
            p.run_retries,
        )
        free = 0 if settling or hold else status.value().slots
    var need = 0
    if control:
        need = 0  # no instance: nothing of the budget
    elif self.runner:
        need = self.runner.value().cpu_of(p.workers)
    elif self.ec2:
        need = self.ec2.value().cpu_of(
            p.workers > 0, p.head_type, p.worker_type, p.workers
        )
    if control and free > 0:
        if pressure:
            hold = pressure
            free = 0
        else:
            # Across pipelines: one start at a time counts the slots.
            _ = tx.execute(
                "SELECT pg_advisory_xact_lock($1)",
                Params().int64(_CONTROL_LOCK),
            )
            var used = Int(
                tx.query(
                    String(
                        "SELECT count(*) AS n FROM orca.runs WHERE status",
                        " = 'running' AND placement = 'control' AND NOT ",
                        self._dead("$1"),
                    ),
                    Params().float64(self.stale),
                )
                .row(0)
                .int64("n")
            )
            if used >= self.limits.max_runs:
                hold = slots_reason(used, self.limits.max_runs)
                free = 0
            else:
                free = min(free, self.limits.max_runs - used)
    # Memory pressure (orca.pressure): on this host, no start while it's
    # over the limit, but one at a time once a hold has lasted its
    # grace with none of orca's runs going.
    if (control or placed == "local") and free > 0 and not hold:
        var allowed = self.admission.allows(self._idle())
        if allowed == 0:
            hold = self.admission.hold
            free = 0
        elif allowed > 0:
            free = min(free, allowed)
    # A request (orca.resources): on this host, a start only if its
    # minimum fits what the running runs weren't granted, each with its
    # grant. A pipeline without one is started as ever.
    var request = sized.request
    if request.memory_min_mb > 0:
        # At least what an out-of-memory kill raised it to.
        request.memory_min_mb = raised_request(
            request.memory_min_mb,
            request.memory_max_mb,
            p.memory_raised_mb,
        )
    # max_runs = "auto" (#266): how many follows the host's room, which
    # admission below weighs; with no request to weigh yet (nothing
    # declared, nothing learned), one run at a time.
    if (
        ((p.max_runs_auto and not fill) or (p.fill.max_runs_auto and fill))
        and local
        and free > 0
        and not request.any()
    ):
        if live > 0:
            free = 0
            if not hold:
                hold = String(AUTO_ONE_AT_A_TIME)
        else:
            free = 1
    var grants = List[Grant]()
    var admitted = request.any() and (control or placed == "local")
    if admitted and free > 0:
        # Across pipelines: one start at a time weighs the grants.
        _ = tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            Params().int64(_HOST_LOCK),
        )
        var g = tx.query(
            String(
                "SELECT coalesce(sum(grant_cpus), 0) AS cpus,",
                " coalesce(sum(grant_memory_mb), 0) AS mb FROM orca.runs",
                " WHERE status = 'running' AND placement IN ('local',",
                " 'control') AND NOT ",
                self._dead("$1"),
            ),
            Params().float64(self.stale),
        ).row(0)
        var got = admit(
            request,
            self.room,
            Int(g.int64("cpus")),
            Int(g.int64("mb")),
            free,
        )
        grants = got[0].copy()
        if len(grants) < free:
            # The ones that don't start say why; those that do, not.
            hold = got[1]
        free = len(grants)
    # Its queued runs say why they wait, or no longer do; one behind an
    # older live run of its keys, which run.
    _ = tx.execute(
        String(
            "UPDATE orca.runs SET held = nullif($2, '') WHERE pipeline = $1",
            " AND status = 'queued' AND held IS DISTINCT FROM nullif($2,",
            " '') AND NOT ",
            behind_sql(),
            which,
        ),
        Params().text(p.name).text(hold),
    )
    hold_behind(tx, p.name)
    if self.budget > 0 and free > 0 and not control:
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
        # The pool's idle instances count too, but those a run of `p`
        # would take are its own: k runs fit if the budget holds the
        # others and k runs' worth or those, whichever is more.
        var others = 0
        var mine = 0
        if len(keys) > 0:
            var b = keys[1] if len(keys) > 1 else keys[0]
            var pr = tx.query(
                (
                    "SELECT coalesce(sum(i.cpu_millis), 0) AS n,"
                    " coalesce(sum(i.cpu_millis) FILTER (WHERE i.pool_key"
                    " IN ($1, $2)), 0) AS mine FROM orca.instances i JOIN"
                    " orca.runs r ON r.id = i.run WHERE i.ended IS NULL AND"
                    " i.pool_key IS NOT NULL AND NOT (r.status = 'running'"
                    " AND r.starts = i.start)"
                ),
                Params().text(keys[0]).text(b),
            ).row(0)
            mine = Int(pr.int64("mine"))
            others = Int(pr.int64("n")) - mine
        var room = self.budget - used - others
        if room < mine:
            free = 0
        else:
            free = min(free, room // need if need > 0 else free)
    var ids = List[Int64]()
    var new_code = List[Bool]()
    var starts = List[Int]()
    if free > 0:
        var rows = tx.query(
            String(
                (
                    "SELECT id, allow_new_code FROM orca.runs WHERE"
                    " pipeline = $1 AND ((status"
                ),
                " = 'queued' AND NOT ",
                behind_sql(),
                ") OR (status = 'deferred' AND retry_at <= $4) OR",
                " (status = 'failed' AND ",
                _RETRIED,
                " AND retry_at <= $4) OR (status = 'running' AND ",
                _RETRIED,
                " AND ",
                dead,
                " AND starts - deferrals <= $3))",
                which,
                " ORDER BY id LIMIT $5",
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
        if status:
            # New fill runs, over the partitions nothing has filled.
            ref next = status.value().next_keys
            var filled = List[Int64]()
            for i in range(min(free - len(ids), len(next))):
                ids.append(
                    tx.query(
                        (
                            "INSERT INTO orca.runs (pipeline,"
                            " registry_hash, partitions, trigger, status)"
                            " VALUES ($1, $2, $3, 'fill', 'queued')"
                            " RETURNING id"
                        ),
                        Params()
                        .text(p.name)
                        .text(p.registry_hash)
                        .text(next[i]),
                    )
                    .row(0)
                    .int64("id")
                )
                new_code.append(False)
                filled.append(ids[len(ids) - 1])
                print("fill", p.name, next[i], "run", ids[len(ids) - 1])
            mark_batch(tx, filled)
        for i in range(len(ids)):
            var id = ids[i]
            var grant = grants[i] if admitted else Grant.none()
            # `started` is the clock's now, not now(): that is when this
            # transaction began, before it waited on the locks above and
            # counted the slots and the budget, so a run that waited for
            # another to end could be recorded as started before it did.
            starts.append(
                Int(
                    tx.query(
                        (
                            "UPDATE orca.runs SET status = 'running', owner"
                            " = 'scheduler', heartbeat = NULL, started ="
                            " clock_timestamp(), ended = NULL, starts ="
                            " starts + 1,"
                            " retry_at = NULL, error = NULL, waiting_since"
                            " = NULL, cpu_millis = $2, placement = $3, held"
                            " = NULL, request_cpus = nullif($4, ''),"
                            " request_memory_gb = nullif($5, ''),"
                            " grant_cpus = nullif($6, 0), grant_memory_mb ="
                            " nullif($7, 0), memory_limit_mb = NULL,"
                            " memory_max_mb = NULL, memory_bytes = NULL,"
                            " instance = nullif($8, 0), request_source ="
                            " nullif($9, ''), request_runs = nullif($10,"
                            " 0) WHERE id = $1 RETURNING starts"
                        ),
                        Params()
                        .int64(id)
                        .int64(Int64(need))
                        .text(placed)
                        .text(recorded_cpus(p, sized) if admitted else String())
                        .text(
                            recorded_memory_gb(
                                p, sized
                            ) if admitted else String()
                        )
                        .int64(Int64(grant.cpus))
                        .int64(Int64(grant.memory_mb))
                        .int64(Int64(self.instance if local else 0))
                        .text(sized.source)
                        .int64(Int64(sized.runs)),
                    )
                    .row(0)
                    .int64("starts")
                )
            )
    tx.commit()
    for l in lost:
        print(
            String("requeued run ", l[0], " of ", p.name),
            ": ",
            l[1],
            sep="",
        )
    for i in range(len(ids)):
        var id = ids[i]
        var grant = grants[i] if admitted else Grant.none()
        if admitted:
            print(
                "granted",
                p.name,
                "run",
                id,
                grant.cpus,
                "CPUs and",
                grant.memory_mb,
                "MB (0: not asked for)",
                sized.text(),
            )
        elif sized.source == DEFAULT:
            print("run", id, "of", p.name, "requests", sized.text())
        if control:
            _start_control(self, p, Int(id), starts[i], new_code[i], grant)
            continue
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
                        resolve_auto(p.env),
                    )
                )
                print("started", p.name, "run", id, "on its cluster")
            except e:
                print("orca: run", id, "of", p.name, "can't start:", e)
                _ = self.db.conn.execute(
                    (
                        "UPDATE orca.runs SET status = 'failed', ended ="
                        " now(), error = $2 WHERE id = $1 AND status ="
                        " 'running' AND heartbeat IS NULL"
                    ),
                    Params().int64(id).text(String("can't start: ", e)),
                )
            continue
        if self.ec2:
            try:
                self.ec2.value().start(
                    self.db, _ec2_spec(p, Int(id), starts[i], new_code[i])
                )
                print("started", p.name, "run", id, "on its instances")
            except e:
                # Retried as a run whose process failed is.
                print("orca: run", id, "of", p.name, "can't start:", e)
                _ = self.db.conn.execute(
                    (
                        "UPDATE orca.runs SET status = 'failed', ended ="
                        " now(), error = $2 WHERE id = $1 AND status ="
                        " 'running' AND heartbeat IS NULL"
                    ),
                    Params().int64(id).text(String("can't start: ", e)),
                )
            continue
        # Its own variables, from orca.toml, its grant
        # (orca.resources), and its secrets, read now and named in
        # ORCA_SECRETS for masking; the rest it inherits.
        var env = grant_env(p.env, grant, pressure_env(self.psi))
        try:
            var secrets = List[Tuple[String, String]]()
            for s in p.secrets:
                secrets.append((s[0], fetch_secret(s[1])))
            env.extend(secrets_env(secrets))
        except e:
            # Retried as a run whose process failed is.
            print("orca: run", id, "of", p.name, "can't start:", e)
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.runs SET status = 'failed', ended ="
                    " now(), error = $2 WHERE id = $1 AND status ="
                    " 'running' AND heartbeat IS NULL"
                ),
                Params().int64(id).text(String("can't start: ", e)),
            )
            continue
        var log = String()
        var stage = String()
        # A run with secrets prints what it likes: its output is
        # staged outside ORCA_LOGS and appended to its driver.log,
        # masked, by keep-log once it ends, as a control-placed run's.
        var kept = len(p.secrets) > 0
        if self.logs:
            log = String(run_dir(p.name, Int(id)), "/driver.log")
            stage = control_stage(
                p.name, Int(id), starts[i]
            ) if kept else stage_of(log)
            makedirs(String(stage[byte = : stage.rfind("/")]), exist_ok=True)
        var argv: List[String] = [p.binary, "resume", String(id)]
        if new_code[i]:
            argv.append("--allow-new-code")
        if log:
            if kept:
                # keep-log heads it with the start.
                with open(stage, "w") as f:
                    f.write("")
            else:
                # Each start's output after a line of its own, as on
                # the other runners: a crash's dump is looked for after
                # the last.
                with open(stage, "a") as f:
                    f.write("--- start ", starts[i], "\n")
        var cg = self._contain(p, Int(id), starts[i], grant, False)
        var pid = spawn(argv, stage, env=env, cgroup=cg.path)
        if cg.path:
            self._contained[pid] = cg^
        self._meters[pid] = Meter()
        self._spawned[pid] = (Int(id), starts[i], stage)
        if log and kept:
            self._kept[pid] = _ControlProc(
                Int(id), p.binary, stage, log, starts[i], env^, ""
            )
        elif is_remote(log):
            self._driver_logs[pid] = log
        print("started", p.name, "run", id, "pid", pid)
        if log and sample_seconds() > 0:
            # The machine's metrics while the run's process lives
            # (orca.sample); it ends once that process has.
            try:
                _ = spawn(
                    [
                        p.binary,
                        "sample",
                        "--out",
                        metrics_location(p.name, Int(id), "local"),
                        "--pid",
                        String(pid),
                    ]
                )
            except e:
                print("orca: no sampler for run", id, ":", e)


def _start_control(
    mut self: Scheduler,
    p: Registered,
    id: Int,
    start: Int,
    new_code: Bool,
    grant: Grant = Grant.none(),
):
    """Starts start `start` of `p`'s run `id` on this host
    (`orca.control`): its binary's `resume`, in a process group of its
    own, at a lower CPU priority, first to go if memory runs out, with
    its secrets read now and given to it alone, and its output staged
    for `keep-log`. A run that can't start fails, and is retried as a
    run whose process failed is."""
    var c: _ControlProc
    var pid: Int
    var cg = self._contain(p, id, start, grant, True)
    try:
        var secrets = List[Tuple[String, String]]()
        for s in p.secrets:
            secrets.append((s[0], fetch_secret(s[1])))
        var extra = run_env(self.limits, p.image, secrets)
        extra.extend(pressure_env(self.psi))
        var env = grant_env(p.env, grant, extra)
        var location = String()
        var stage = String()
        if self.logs:
            location = String(run_dir(p.name, id), "/driver.log")
            stage = control_stage(p.name, id, start)
            makedirs(String(stage[byte = : stage.rfind("/")]), exist_ok=True)
            with open(stage, "w") as f:
                f.write("")
        var argv: List[String] = [p.binary, "resume", String(id)]
        if new_code:
            argv.append("--allow-new-code")
        pid = spawn(
            argv,
            stage,
            env=env,
            nice=self.limits.nice,
            group=True,
            first_to_go=True,
            cgroup=cg.path,
        )
        c = _ControlProc(id, p.binary, stage, location, start, env^, "")
    except e:
        print("orca: run", id, "of", p.name, "can't start:", e)
        if cg.path and not Cgroups.remove(cg.path):
            self._unswept.append(cg.path)
        try:
            _ = self.db.conn.execute(
                (
                    "UPDATE orca.runs SET status = 'failed', ended ="
                    " now(), error = $2 WHERE id = $1 AND status ="
                    " 'running' AND heartbeat IS NULL"
                ),
                Params().int64(Int64(id)).text(String("can't start: ", e)),
            )
        except e2:
            print("orca: can't record run", id, "failing:", e2)
        return
    self._spawned[pid] = (id, start, c.stage)
    self._meters[pid] = Meter()
    if cg.path:
        self._contained[pid] = cg^
    var sampled = c.location != ""
    self._control[pid] = c^
    print("started", p.name, "run", id, "pid", pid, "on the control host")
    if sampled and sample_seconds() > 0:
        try:
            _ = spawn(
                [
                    p.binary,
                    "sample",
                    "--out",
                    metrics_location(p.name, id, CONTROL),
                    "--pid",
                    String(pid),
                ],
                nice=self.limits.nice,
            )
        except e:
            print("orca: no sampler for run", id, ":", e)
