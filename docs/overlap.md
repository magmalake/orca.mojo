# Overlapping schedulers: deploys that don't stop runs

Design note for #232. Today every local and control-placed run is a child
process of `orca scheduler`. In the Hetzner stack the scheduler is a
container, so restarting it (each deploy, a crash, an `orca-stack apply`)
kills every run in flight. `orca-stack apply` refuses while runs are going,
or with `--force` kills them, and their retries start over.

An earlier version of PR #242 moved the runs into a separate long-lived
`orca runner` container. This version doesn't. Instead a deploy runs two
schedulers, of the old image and the new one, side by side on one Postgres
for as long as the old one's runs take:

```text
      versioned, replaced each deploy                persistent
┌──────────────────────────────────────┐   ┌──────────────────────────┐
│ image N:   UI + orca + pipelines     │   │ Postgres                 │
│   orca scheduler   (draining)        │──▶│ warehouse                │
│     └ its runs, until they end       │   │ logs (object storage)    │
│ image N+1: UI + orca + pipelines     │   │                          │
│   orca scheduler   (leading)         │──▶│                          │
│     └ every new run                  │   │                          │
└──────────────────────────────────────┘   └──────────────────────────┘
```

The system is two parts. The **versioned unit** is the image: the UI, orca
and the user's pipeline binaries, built and replaced together. The
**persistent tier** is Postgres, the warehouse and the logs in object
storage. Nothing in the versioned unit has to outlive its image, so no part
of orca needs a lifetime of its own (the runner did). A run belongs to the
image that started it and ends under it.

## Decisions

### `ORCA_OVERLAP`, off by default

`ORCA_OVERLAP=on` makes each `orca scheduler` a scheduler instance. With it
`off`, the default, nothing changes: no instance rows, runs start with no
instance, and every rule below has nothing to apply to. Deploys keep
pausing and waiting, as now, until overlap is proven in the stack; then
the stack turns it on, and later it becomes the default. A flag, rather
than a version check, because the first deploy that turns it on still
replaces a scheduler that doesn't know how to drain.

`orca tick` is never an instance: it's one pass, for tests and by hand.

### One leader, by a session advisory lock

Each instance has a row in a new table, `orca.schedulers`: host, pid,
image (`ORCA_IMAGE`), how many migrations its binary knows, `last_seen`,
role (`waiting`, `leading`, `draining`, `ended`), and `running`, the ids of
the runs whose processes are its children. It keeps the row fresh on every
tick.

The leader holds the session advisory lock `orca.instances.LEAD_LOCK`. The
rule is **the newest live instance leads**:

- a new instance joins `waiting` and, on each tick, takes the lock if it's
  free;
- an instance that sees a newer live instance that isn't draining, or that
  is asked to (`orca scheduler drain <id>`), gives the lock up and drains;
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
X" in place of "held by the runner" (`orca.live.held_sql`, part of
`dead_sql`):

- **held**, so live, while its instance has been seen within the stale
  limit, hasn't ended and lists it in `running`, whatever its heartbeat.
  The leader counts such runs against `max_runs`, `fill_max_runs`, the
  control slots, the grants under the host lock and the memory pressure
  grace's "none of orca's runs going", and its dead-run paths (`_end_dead`,
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
(`role = 'ended'`, `ended` set) and exits 0. After `ORCA_DRAIN_SECONDS`
(3600, the same as `orca-stack quiesce`'s default timeout) it kills what is
left (each control run's process group too), ends its row and exits; the
leader queues those runs again once their heartbeats are stale. An ended
instance is gone at once, so nothing waits for its row to go stale.

### Registering after the handover

`orca register` writes the pipelines' binaries and code. Under an instance
that still leads, the new image's paths and registry hashes would be
started by the old image. So a deploy registers only once the new instance
leads: `orca scheduler wait-lead` waits for the newest live instance to
lead, and `orca register` itself, with `ORCA_OVERLAP=on`, waits for that
(`ORCA_REGISTER_WAIT_SECONDS`, 120) and refuses after. With no instance
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

- `orca scheduler can-overlap`, run from the new image before it starts
  anything, exits 0 (`yes`) when a live instance leads and every live
  instance knows at least this binary's floor (`migration_floor` of the
  migrations it knows), and 1 (`no: <why>`) otherwise: a floor it would
  raise past, or no live leading instance (none going, or one with
  `ORCA_OVERLAP` off, which keeps no row). It doesn't migrate. `deploy.sh`
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
  that (`wait-lead`) and registers. New runs start under the new image;
  the old image's runs finish under it, counted by the new leader, and the
  old scheduler exits. The deploy then removes its container.
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
default) the containers get `ORCA_OVERLAP=on`, and:

- `deploy.sh` asks `orca-stack can-overlap --tag <tag>`, which runs the new
  image's `orca scheduler can-overlap`; on `no` it quiesces and replaces as
  today, saying why;
- on `yes`, `orca-stack apply --overlap` starts the new scheduler as
  `scheduler-next` beside the old one, sets the old one's restart policy to
  `no` (a restart after its drain would be the newest instance and take
  the lead back), renames it `scheduler-old` and the new one `scheduler`,
  and waits for `orca scheduler wait-lead`;
- the deploy registers the pipelines, then `orca-stack drain-old` asks the
  old instance to drain, waits for its container to exit (bounded by the
  deploy's `--timeout`, then stopped), and removes it;
- a `scheduler-old` or `scheduler-next` left behind that isn't running any
  more (one that exited when `drain-old` didn't follow) holds no runs: the
  next `orca-stack apply` removes it first, naming its image and how long
  ago it exited, and `can-overlap` doesn't count it (#352). A running one
  stays until `drain-old`, and `can-overlap` says no and `orca-stack check`
  fails while it's there;
- `orca-stack status` lists the instances (`orca scheduler status`).

Each container stages its own runs' output in the shared stage volume
under names of their own (run and start), so nothing collides.

## Status (PR #242)

Built: migration 42, `orca.instances` (join, the lead, the drain, status,
`can-overlap`, `wait-lead`), `ORCA_OVERLAP`, `ORCA_DRAIN_SECONDS`, the
adoption rule (`held_sql`, `stale_sql`, `_requeue_orphans`), the stale-kill
pass, `register`'s wait and `migrate`'s floor guard. Removed from the
earlier #242: `orca runner`, `ORCA_LOCAL_RUNNER`, the assignment and its
claim, the hold while no runner is seen, and the reserved pool. Tested by
`tests/adopt.sh` (the rule over made-up rows) and `tests/overlap.sh` (three
real instances on one Postgres: a drain with runs in flight, `max_runs`
across both, no double start or requeue of the draining one's runs, a
drain that runs out and its requeue, register's wait, and the floor cases).

Next: the stack (#246, above), the instances on Ops' System page, and making
`ORCA_OVERLAP=on` the default once proven.
