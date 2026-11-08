# The runner: runs that survive a scheduler restart

Design note for #232. Today every local and control-placed run is a child
process of `orca scheduler`. In the Hetzner stack the scheduler is a
container, so restarting it (each deploy, a crash, an `orca-stack apply`)
kills every run in flight. `orca-stack apply` refuses while runs are going,
or with `--force` kills them, and their retries start over.

Remote workers (#173, #221) already show the way. A worker's runs belong to
the worker, not to the scheduler: Postgres holds their state, and the
scheduler requeues a dead worker's runs by their heartbeats. This note gives
runs on the control host the same independence: a long-lived `orca runner`
owns them. The scheduler decides what starts. The runner starts it.

```text
orca scheduler ── decides: counts, control slots, pressure, grants
    │  UPDATE runs SET status = 'running', owner = 'runner', grant ...
    ▼
Postgres  ◀───────────── the runs' own state, as today (PgStore)
    ▲
    │  claims assigned runs, keeps its orca.workers row fresh
orca runner (pool "local") ── spawns <binary> resume <run>: cgroup, nice,
                              sampler, keep-log; reports each exit
```

## Decisions

### The worker protocol, over Postgres

The issue offers two ways to connect the scheduler and the runner: a
Postgres claim, or the runner as a worker of a reserved pool over the
worker API. This takes the worker protocol (its rows, its rules and its
functions) without the HTTP hop:

- **The runner is a worker of the reserved pool `local`.** It has a row in
  `orca.workers` like any worker. It keeps that row's `last_seen` and
  `running` fresh on every poll, claims runs, and reports each process's
  end through the same server functions the worker API serves:
  `orca.workers.claim`'s sibling for assigned runs, `exited` and `release`.
  Its runs are requeued by the same rule as a dead worker's
  (`_requeue_lost`). On the client side it is `orca.worker.Worker`, the
  same loop that spawns, contains, samples, reaps and keeps logs, with
  control-host semantics added.
- **The runner calls those functions on its own Postgres connection**, not
  over HTTPS. Three reasons. First, the UI restarts on every deploy (until
  #231) and is one thread (#221's long-poll note, #229's latency). A runner
  that needed it for claims would stall on every deploy, and runs whose
  state went through it would go stale during a restart longer than the
  stale limit. That is the failure this change exists to remove. Second,
  the runner sits on the control host, where runs already get
  `ORCA_DATABASE_URL`: it is no more trusted than the scheduler or the runs
  it starts. Third, it is one hop fewer.
- **The runs write to Postgres directly**, as local runs do today
  (`PgStore`), not through `orca.http_store`. The runner passes its own
  environment on (`ORCA_DATABASE_URL`, `ORCA_WAREHOUSE`, `ORCA_LOGS`) and
  sets no `ORCA_STATE_URL` or `ORCA_WORKER_PID`.

A runner on another machine is not a new thing to build: it is a remote
worker.

### The scheduler decides, the runner starts

The scheduler keeps all of `_start`'s admission for local and control runs,
unchanged: `max_runs`, `fill_max_runs`, the control slots
(`ORCA_CONTROL_MAX_RUNS`), the control host's samples (`pressure_reason`),
memory pressure (#225), grants under the host lock (#208), and the order
these are checked in. The claim's own admission (a worker's room and
grants) isn't used for pool `local`. There is one writer of scheduling
decisions, as #231 requires.

What changes is the last step. Where it would fork, the scheduler marks the
run `running` with owner `runner`, no worker, no heartbeat, and its
placement and grant recorded, exactly as it does before a fork today.
`owner = 'runner'` is the assignment. The runner's claim
(`orca.workers.claim_assigned`) takes assigned runs `FOR UPDATE SKIP
LOCKED`, sets `owner = 'worker'` and `worker` to its row, and audits the
claim as a worker claim is. The spec it gets is a `WorkerRun` plus what the
control host needs: the placement, the request's memory maximum and what
an out-of-memory kill raised the pipeline to.

No run waits for a runner that isn't there. While no runner of pool `local`
has been seen for half the stale limit, the scheduler assigns nothing, and
its queued runs are `held: waiting for the runner (ORCA_LOCAL_RUNNER=runner)`.
A run assigned and not claimed within the start grace goes back to `queued`
with its start given back.

### The adoption rule

A run keeps doing what it does today: a heartbeat row, steps writing their
state, its process claiming it as `host:pid`. A scheduler that starts, or
the one that's running on each tick, needs nothing from the process that
started it. For a runner's run (`worker` set, placement `local` or
`control`):

- **left alone** while its heartbeat is fresh, or while its runner has been
  seen within half the stale limit and lists it in `running` (the runner
  then deals with it: see below);
- **requeued**, whatever its trigger, when its heartbeat is stale and its
  runner is gone or no longer lists it. This is `_requeue_lost`'s rule for
  a dead worker, the same SQL with the placement widened. A start the run's
  process never made is given back. One it made counts and is bounded by
  `run_retries`, so a run that kills its runner can't loop.

So the SQL that counts live runs (`max_runs`, control slots, grants) counts
a runner's run whose runner still holds it, and the dead-run paths
(`_end_dead`, the restart of dead runs) skip it. For a scheduler's own
children (`ORCA_LOCAL_RUNNER=process`) nothing changes: their owner is
alive exactly while they're in `_spawned`, and a wedged one is killed
before its run starts again.

A stale heartbeat with a live runner means a wedged process or a database
the run can't reach. The scheduler can't kill another container's process,
so the runner does what the scheduler does for its children: a run whose
heartbeat has been stale for the stale limit is killed (its process group),
its exit reported, and it leaves `running`. Then the rule above requeues
it.

### What lives in the runner

Everything the scheduler does to a process today, for local and control
runs, moves with the fork:

- **cgroups (#226).** `orca runner` calls `Cgroups.setup`, makes each run's
  cgroup at `start_limit_mb` of its grant, request maximum, raised memory
  and, for control runs, `ORCA_CONTROL_MEMORY_MB`, and records the limit on
  the run. It records `memory_bytes` every 5 s, reports an OOM kill with
  `exited` (which fails the attempts with `oom_text` and raises the
  pipeline's memory, as for a worker), and removes the cgroup at the reap.
  In the stack the runner's container is then the one with `CAP_SYS_ADMIN`
  for the cgroup remount, which takes it away from the scheduler.
- **The control placement.** `nice` `ORCA_CONTROL_NICE`, its own process
  group, `oom_score_adj` 1000, `ORCA_CONTROL_WORKERS` (`run_env`), and,
  for an uncontained run, the kill over `ORCA_CONTROL_MEMORY_MB` from its
  tree's resident memory, reported with `exited` as `over`, which fails its
  attempts saying so.
- **Memory pressure (#225).** The scheduler still reads the host's PSI and
  holds starts by it. Its "none of orca's runs going" check for the grace
  counts the runner's runs from Postgres instead of its children. The
  runner passes `ORCA_MEMORY_PRESSURE_FILE` to runs and doesn't hold claims
  itself: the scheduler already did.
- **Grants (#208).** Decided by the scheduler, carried in the spec, given to
  the run as `ORCA_GRANT_*`.
- **Usage, samples and logs.** Each run's process tree is metered and its
  peaks sent with `exited`, `<binary> sample` writes the Machines card's
  metrics, and the output is staged under `--dir` (`$TMPDIR/orca-runner`)
  and appended to `driver.log` by `keep-log`, masked, which also fails the
  attempts a crash left running. That is the worker's path for every run;
  the scheduler used it only for control runs and runs with secrets.

### Logs and stage directories

Nothing has to be shared between the scheduler and the runner beyond
Postgres. The runner needs the same `ORCA_LOGS` (Object Storage, or a
volume the UI also reads, as today), `ORCA_WAREHOUSE`, `ORCA_SECRET_FETCH`
and the image's pipeline binaries. A run's stage file is read only by the
runner's own `keep-log`, so the stage directory needs to outlive only the
runner's process, not the scheduler's: a volume of the runner's container,
so a restarted runner can keep what its predecessor staged (a next step).

### `ORCA_RUNNER` and `ORCA_LOCAL_RUNNER`

`ORCA_RUNNER` keeps its meaning: `local` (runs on this host, the default),
`kubernetes` or `ec2`. The new `ORCA_LOCAL_RUNNER` says who forks the runs
that run on this host, local and control-placed alike: `process` (the
scheduler, as today, the default until the runner is proven) or `runner`.
With `ORCA_RUNNER=ec2`, control-placed runs go to the runner too.

The pool name `local` is reserved: `orca worker-token create --pool local`
and `--placement worker:local` are refused, so no remote worker can claim
the control host's runs.

### Migration

No schema change: the runner's row is an `orca.workers` row (a random token
hash nobody holds, `created_by` `orca runner`), runs use existing columns,
and `owner = 'runner'` is a new value no older code acts on. An older
scheduler sees such a run as one it started that never claimed its
heartbeat, dead after the start grace, and starts it again. So rolling back
is setting `ORCA_LOCAL_RUNNER=process` (or an older image) and restarting
the scheduler; runs the runner has going finish and record themselves.

Rollout: run `orca runner` beside the scheduler with
`ORCA_LOCAL_RUNNER=process` (it idles), then switch the scheduler to
`runner`. Runs already going as the old scheduler's children are left to
finish. Once proven, `runner` becomes the default in the stack.

## What happens when

- **The scheduler restarts.** The runner's runs go on. The new scheduler
  finds them `running` with fresh heartbeats, held by a runner seen within
  half the stale limit, and leaves them alone. Nothing is assigned while it
  is down; queued runs wait. Its first tick counts the runner's runs
  against `max_runs`, the control slots and the grants as before.
- **The UI restarts.** Nothing changes for local and control runs: neither
  the runner nor its runs talk to it.
- **The runner restarts.** In a container its runs die with it. Their
  heartbeats go stale, the runner's row isn't seen (or is seen, by the new
  runner, without them), and they are requeued whatever their trigger,
  their starts counted: the dead-worker rule. A drain avoids that: `orca
  runner` stops claiming and waits for its runs, up to a timeout, then
  exits; the scheduler holds new runs for the runner meanwhile (a next
  step, together with the stack's runner container). On a bare host the
  runs outlive the runner (next case).
- **Both restart.** As the runner's restart: the scheduler doesn't need
  the old one's memory, and the requeue is Postgres's.
- **A run outlives its runner** (a bare host, or a runner killed while its
  runs' processes weren't). Its heartbeat stays fresh, so it is left alone,
  finishes and records itself once, as a resumed process would. What's
  lost is the runner's bookkeeping for that process: `keep-log` (its
  output stays in the stage file), its usage samples, an OOM read as such
  (it reads as a crash), and its cgroup, removed by the next runner's sweep
  once empty. A runner that adopts such processes (the run's `host:pid`
  and the stage file name the process and its output) is a next step.
- **The runner wedges** (alive, not polling). Its row goes unseen, so its
  runs are requeued only once their heartbeats go stale too, as for a
  worker that went to sleep.

## The stack (#230, #231)

The Hetzner stack needs a `runner` container: the same image and env file
as the scheduler, `orca runner` as its command, the cgroup flags
(`--cgroupns=private --cap-add SYS_ADMIN --security-opt
apparmor=unconfined` and the remount entrypoint) moved over from the
scheduler, and a volume for its stage directory. The scheduler gets
`ORCA_LOCAL_RUNNER=runner` and loses those flags. `orca-stack apply` then
replaces the scheduler and the UI freely, and the runner only when its
image or settings changed (#230), with a drain. That change is left for a
follow-up PR, so this one doesn't touch `deploy/hetzner/` while #230 and
#231 are being built there.
