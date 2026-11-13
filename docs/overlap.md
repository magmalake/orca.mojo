# Overlapping schedulers: deploys that don't stop runs

Design note for #232. Today every local and control-placed run is a child
process of `morca scheduler`. In the Hetzner stack the scheduler is a
container, so restarting it (each deploy, a crash, an `morca-stack apply`)
kills every run in flight. `morca-stack apply` refuses while runs are going,
or with `--force` kills them, and their retries start over.

An earlier version of PR #242 moved the runs into a separate long-lived
`morca runner` container. This version doesn't. Instead a deploy runs two
schedulers, of the old image and the new one, side by side on one Postgres
for as long as the old one's runs take:

```text
      versioned, replaced each deploy                persistent
┌──────────────────────────────────────┐   ┌──────────────────────────┐
│ image N:   UI + morca + pipelines     │   │ Postgres                 │
│   morca scheduler   (draining)        │──▶│ warehouse                │
│     └ its runs, until they end       │   │ logs (object storage)    │
│ image N+1: UI + morca + pipelines     │   │                          │
│   morca scheduler   (leading)         │──▶│                          │
│     └ every new run                  │   │                          │
└──────────────────────────────────────┘   └──────────────────────────┘
```

The system is two parts. The **versioned unit** is the image: the UI, morca
and the user's pipeline binaries, built and replaced together. The
**persistent tier** is Postgres, the warehouse and the logs in object
storage. Nothing in the versioned unit has to outlive its image, so no part
of morca needs a lifetime of its own (the runner did). A run belongs to the
image that started it and ends under it.

## Decisions

### `MORCA_OVERLAP`, off by default

`MORCA_OVERLAP=on` makes each `morca scheduler` a scheduler instance. With it
`off`, the default, nothing changes: no instance rows, runs start with no
instance, and every rule below has nothing to apply to. Deploys keep
pausing and waiting, as now, until overlap is proven in the stack; then
the stack turns it on, and later it becomes the default. A flag, rather
than a version check, because the first deploy that turns it on still
replaces a scheduler that doesn't know how to drain.

`morca tick` is never an instance: it's one pass, for tests and by hand.

### One leader, by a session advisory lock

Each instance has a row in a new table, `orca.schedulers`: host, pid,
image (`MORCA_IMAGE`), how many migrations its binary knows, `last_seen`,
role (`waiting`, `leading`, `draining`, `ended`), and `running`, the ids of
the runs whose processes are its children. It keeps the row fresh on every
tick.

The leader holds the session advisory lock `orca.instances.LEAD_LOCK`. The
rule is **the newest live instance leads**:

- a new instance joins `waiting` and, on each tick, takes the lock if it's
  free;
- an instance that sees a newer live instance that isn't draining, or that
  is asked to (`morca scheduler drain <id>`), gives the lock up and drains;
  it never leads again;
- an instance that dies loses the lock with its connection, so the next
  one takes it within a tick. An instance whose connection breaks and
  comes back tries again, unless a newer one has taken it.

Only the leader decides: fires, sensors, retries, fills, starts, alerts,
the watchdog, expiry, the fleet's samples, the memory pressure row and
Kubernetes and EC2 sweeps. Any instance, leading or not, supervises the
processes it started: reaps them and records their exits, samples their
usage, records their cgroups' memory, kills a control run over its memory
cap or a process whose heartbeat went stale, and keeps their logs.

The lock isn't what keeps two starts of one run apart: every start already
takes the pipeline's row `FOR UPDATE` and counts in SQL, so two schedulers
were always safe to run at once (the scheduler's doc says so). The lock
keeps everything else a scheduler does single, and makes "who decides"
something a deploy can wait for.

A lease row with an expiry was the other choice. The advisory lock needs
no clock and no expiry to tune, and is released by Postgres the moment its
holder's connection goes.

### Ownership and the adoption rule

Each local or control-placed run an instance starts records it
(`runs.instance`). The rule from the earlier #242, with "held by instance
X" in place of "held by the runner" (`morca._internal.db.live.held_sql`, part of
`dead_sql`):

- **held**, so live, while its instance has been seen within the stale
  limit, hasn't ended and lists it in `running`, whatever its heartbeat.
  The leader counts such runs against `max_runs`, `fill_max_runs`, the
  control slots, the grants under the host lock and the memory pressure
  grace's "none of morca's runs going", and its dead-run paths (`_end_dead`,
  the restart of dead runs, the backfill's live keys) skip them;
- **dead as before** when its instance is live but no longer lists it: the
  instance reaped it and recorded its exit, so a UI run fails "interrupted"
  and a scheduled one is started again, as for a single scheduler;
- **queued again**, whatever its trigger, when its instance is gone (ended,
  or not seen for the stale limit) and its heartbeat is stale
  (`Scheduler._requeue_orphans`, audited as `/api/scheduler/requeue`). Its
  start counts, and past `run_retries` it is left to `_end_dead`, so a run
  that kills its scheduler can't loop. A run whose heartbeat is still fresh
  (its process outlived its scheduler) is left to finish.

The full stale limit, not half of it as for remote workers: a run is
requeued only once its instance and its heartbeat have both gone quiet,
so a slow tick never makes a live instance's wedged run look orphaned.

An instance kills a child whose heartbeat went stale, as the scheduler did,
but now as a pass of its own on each tick over its children
(`Scheduler._kill_wedged`), since a held run never matches `dead_sql`.
Its run is dead once the process is reaped and unlisted, one tick later
than before.

`orca.workers` (the earlier #242's home for the runner) isn't reused: a
scheduler row there would show on the Pools page and its staleness would
raise "lost workers" alerts. Nor is the reserved pool `local`: nothing
claims runs any more.

### The drain

An instance that drains starts nothing, keeps supervising its children and
the `keep-log`s their ends started, and once none is left ends its row
(`role = 'ended'`, `ended` set) and exits 0, however long that takes: a
run may last months, and morca never stops one for a deploy (#397). With
`MORCA_DRAIN_SECONDS` set, it kills what is left after that many seconds
(each control run's process group too), ends its row and exits; the
leader queues those runs again once their heartbeats are stale. Unset, the
default since #397, there is no limit. An ended instance is gone at once,
so nothing waits for its row to go stale.

### Outdated runs

A draining instance's runs, and any run whose pipeline has since been
registered with other code, are **outdated**, and can find out
(`morca._internal.handover`, #397). Morca tells them and does nothing else: it
doesn't cancel, kill or queue them again. Their code decides.

- **Where the flag comes from.** Each run's heartbeat thread already beats
  `orca.runs` on its own connection every fifth of the stale limit. On the
  same beat it now asks whether the run is outdated (`outdated_sql`): its
  scheduler instance drains (`draining_since` or `drain_asked` on its row,
  while it lists the run), or its pipeline's registered version
  (`orca.pipelines.version`) is other code than the one the run recorded
  (`orca.runs.version`). A worker's run gets the same answer in the reply
  to its beat (`/api/worker/store/beat`). The answer goes into an atomic
  in the run's process, so `morca.outdated()` costs a load and can be
  called between any two units of work. No new column: both conditions
  are already in the schema.
- **The code version** is the version row `morca register` and each run
  already record (`orca.pipeline_versions`, `morca._internal.source`): the source
  commit the binary was built from (`-D MORCA_COMMIT`), whether its tree
  was dirty, and the registry hash. Not the registry hash alone (what the
  snapshot summaries carry as `orca.code`): it covers the DAG, the steps'
  names, functions and datasets, not what a step's body does, so a fix to
  a poll loop wouldn't change it. Not the image either: a registration's
  image is where remote runs go (`--image`), a run's the one it ran in, so
  the two differ for the same code. A build without a commit is told apart
  by its registry hash, and during a deploy by its scheduler draining.
- **The SDK.** `morca.outdated() -> Bool`; `morca.code_version()`, the
  run's version (the commit's first 12 characters, `+dirty` after a dirty
  tree's, or `code <registry hash>` without a commit);
  `morca.desired_code_version()`, the version its pipeline is registered
  with now, what a new run would start with; `morca.outdated_why()`, the
  reason as the UI says it. All are false or empty until the first beat,
  and for a run without a heartbeat (in memory, over SQLite).
- **Shown.** A running run's page (Status card, "Version"), its JSON
  (`run.code_version`, `run.desired_code_version`, `run.outdated`, also on
  every running row of the pipeline's runs) and `morca status <pipeline>`
  mark it `outdated (newer version <commit> is leading)`, or `outdated
  (its scheduler drains: scheduler instance N takes over)` when the code is
  the same (a deploy of new settings, say).

A long poll loop hands over like this:

```mojo
from morca import Ctx, Name, outdated, outdated_why

def follow(mut ctx: Ctx, key: Name) raises -> Int:
    var polls = 0
    while not outdated():
        poll_once(key)        # one unit of work, saved before the next
        polls += 1
    ctx.log(String("handing over: ", outdated_why()))
    return polls              # the step succeeds; the next run starts new code
```

A run that never asks runs on to its end under the old image, its
scheduler draining beside the new one all that time.

### Registering after the handover

`morca register` writes the pipelines' binaries and code. Under an instance
that still leads, the new image's paths and registry hashes would be
started by the old image. So a deploy registers only once the new instance
leads: `morca scheduler wait-lead` waits for the newest live instance to
lead, and `morca register` itself, with `MORCA_OVERLAP=on`, waits for that
(`MORCA_REGISTER_WAIT_SECONDS`, 120) and refuses after. With no instance
live it doesn't wait.

The old instance's runs keep their code. One that fails after the new code
is registered is handled as today: a retry of steps done by other code
fails, saying to resume it with `--allow-new-code`.

### Migrations

The new instance migrates the schema when it starts, while the old one
still runs on it. That is the rollback rule of #234 again, a binary running
on a schema newer than it knows, so it uses the same floors:

- an additive migration (`_migrations`' rules) keeps the old binary
  working, and the two overlap;
- a migration in `_floor_raisers` raises the schema's floor past the old
  binary. Then they must not overlap: the deploy stops the old scheduler
  first, pausing and waiting for its runs as today.

Two guards make that hold:

- `morca scheduler can-overlap`, run from the new image before it starts
  anything, exits 0 (`yes`) when a live instance leads and every live
  instance knows at least this binary's floor (`migration_floor` of the
  migrations it knows), and 1 (`no: <why>`) otherwise: a floor it would
  raise past, or no live leading instance (none going, or one with
  `MORCA_OVERLAP` off, which keeps no row). It doesn't migrate. `deploy.sh`
  overlaps on `yes` and pauses and waits on `no`;
- `State.migrate` refuses to apply migrations that raise the floor while a
  live instance (seen within the stale limit, not ended) knows fewer than
  the new floor, whatever command migrates. Once the old scheduler has been
  stopped for the stale limit, it goes ahead.

Migration 42 itself (`orca.schedulers`, `runs.instance`) is additive. The
binaries before it start runs without touching `instance`; a value left
from an earlier start names an instance that doesn't list the run, so it
holds nothing.

### Two behaviours decided with the user

- **A draining instance never takes the lead back**, even when the new one
  dies before leading. Until a newer instance runs (the container's
  restart, or a rollback), nothing starts. A lead that could move back
  would let an old image start runs after the new one had registered.
- **A gone instance's runs are queued again whatever their trigger**,
  manual and UI runs too, rather than failing "interrupted: its process
  stopped" as a lone scheduler's would. The deploy or a crash stopped them,
  not their code, as for a dead worker's runs.

## What happens when

- **A deploy, overlapping.** The new instance starts, migrates (additive),
  joins and waits. The old one sees it on its next tick, gives up the lead
  and drains; the new one takes the lead on its next. The deploy waits for
  that (`wait-lead`), registers, and returns. New runs start under the new
  image; the old image's runs see they're outdated and finish under it
  when their code decides, counted by the new leader, and the old
  scheduler exits. A reaper on the server then removes its container.
- **A deploy while an old scheduler still drains.** It overlaps again,
  unless the new binary raises the floor past any live instance: the
  scheduler it replaces drains too, under a name of its own. Several old
  instances can drain at once, each supervising its own runs; the newest
  leads.
- **A deploy that raises the floor.** `can-overlap` says no; the deploy
  pauses, waits for the runs, stops the old scheduler and starts the new
  one, as today.
- **The new instance crashes while the old one drains.** The old one
  doesn't come back to lead. Nothing starts until a newer instance runs:
  the container's restart, or a rollback, which is another deploy.
- **The old instance crashes while it drains.** Its runs die with its
  container. They're queued again by the leader once its row and their
  heartbeats are stale, whatever their trigger.
- **The leader crashes, no overlap going.** Its restart joins as a new
  instance and leads at once; the old row's runs are queued again once
  stale, as above.
- **Postgres restarts.** Each instance reconnects keeping its children
  (`Scheduler.reconnect`); the lead, lost with the old connection, goes to
  the newest instance again on the next tick.
- **The UI restarts.** Nothing changes for runs: they don't talk to it.

## The stack (#246)

The Hetzner change is #246, stacked on #244 (stack #245), so this PR
doesn't touch `deploy/hetzner/`. With `OVERLAP=1` in stack.conf (off by
default) the containers get `MORCA_OVERLAP=on`, and:

- `deploy.sh` asks `morca-stack can-overlap --tag <tag>`, which runs the new
  image's `morca scheduler can-overlap`; on `no` it quiesces and replaces as
  today, saying why;
- on `yes`, `morca-stack apply --overlap` starts the new scheduler as
  `scheduler-next` beside the old one, sets the old one's restart policy to
  `no` (a restart after its drain would be the newest instance and take
  the lead back), renames it `scheduler-old-<UTC time>` and the new one
  `scheduler`, and waits for `morca scheduler wait-lead`;
- the deploy registers the pipelines, then `morca-stack drain-old` asks
  every old instance to drain and returns (#397): it leaves each a reaper
  (`morca-stack reap-old`) that removes its container once it has exited.
  `drain-old --wait` waits instead, and `--timeout` stops one after that;
- an old scheduler still draining doesn't stop the next deploy from
  overlapping: each gets its own name, so none shadows another. `morca-stack
  check` and `status` report each as `scheduler-old-<time>: draining N
  run(s) (outdated)`, as information;
- an old scheduler or a `scheduler-next` left behind that isn't running any
  more holds no runs: the next `morca-stack apply` removes it first,
  naming its image and how long ago it exited (#352);
- `morca-stack status` lists the instances (`morca scheduler status`).

### Deploys that don't overlap

The pause-and-wait path (`morca-stack quiesce`) still waits for the
running runs, the old schedulers' included. It can't only pause new
starts: it's taken when the schedulers can't overlap, and then the
scheduler is replaced by stopping its container, whose child processes
are every local and control run, so not waiting kills them. And when the
reason is a migration that raises the floor, the old binary's runs can't
go on over the new schema at all (`State.migrate` refuses while an older
instance is live). So it is left as it was; overlapping, the default once
#390 lands, is the path that doesn't wait.

### Not in this slice

- The required global drain limit (#390: `DRAIN_SECONDS` in hetzner.env,
  a hard error when missing): until then `MORCA_DRAIN_SECONDS` is
  optional and unset means no limit.
- Concurrency keys (#397: one `mojo_live` across versions), and the limit
  on how long a run waits for an older version's run.

Each container stages its own runs' output in the shared stage volume
under names of their own (run and start), so nothing collides.

## Status (PR #242)

Built: migration 42, `orca.instances` (join, the lead, the drain, status,
`can-overlap`, `wait-lead`), `MORCA_OVERLAP`, `MORCA_DRAIN_SECONDS`, the
adoption rule (`held_sql`, `stale_sql`, `_requeue_orphans`), the stale-kill
pass, `register`'s wait and `migrate`'s floor guard. Removed from the
earlier #242: `morca runner`, `MORCA_LOCAL_RUNNER`, the assignment and its
claim, the hold while no runner is seen, and the reserved pool. Tested by
`tests/adopt.sh` (the rule over made-up rows) and `tests/overlap.sh` (three
real instances on one Postgres: a drain with runs in flight, `max_runs`
across both, no double start or requeue of the draining one's runs, a
drain that runs out and its requeue, register's wait, and the floor cases).

Next: the stack (#246, above), the instances on Ops' System page, and making
`MORCA_OVERLAP=on` the default once proven.

#397's first slice: outdated runs (above, `morca._internal.handover`), no drain
limit by default, and deploys that don't wait for the old scheduler's runs
(`drain-old` returns, old schedulers named by time, check's `info` line).
Tested by `tests/outdated.sh` (three real instances: a run that hands
over, one that ignores it and succeeds once it returns, nothing killed,
the drain-only case) and `tests/hetzner.sh` (a deploy that returns while
the old scheduler drains, a second deploy while it does, the reaper).
