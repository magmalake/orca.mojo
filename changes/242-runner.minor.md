- **Runs that survive a scheduler restart: `orca runner` (#232).** With
  `ORCA_LOCAL_RUNNER=runner` the scheduler no longer forks local and
  control-placed runs. It decides as before (`max_runs`, control slots,
  memory pressure, grants) and assigns each start (`owner = 'runner'`); a
  long-lived `orca runner`, a worker of the reserved pool `local` that
  speaks the worker protocol over Postgres rather than HTTPS, claims and
  starts it with the control host's rules (its cgroup at its limit, the
  control placement's nice, process group and memory cap, samples, usage,
  `keep-log`) and reports its end as a worker does. A restarted scheduler
  needs nothing from the old one: a run its runner holds (seen within half
  the stale limit, listing it) is left alone and counts as live, whatever
  its heartbeat; once the runner is gone, or seen without it, and its
  heartbeat is stale, it's queued again whatever its trigger, by the
  dead-worker rule, audited. While no runner is seen, runs wait, held
  "waiting for the runner". `ORCA_LOCAL_RUNNER=process`, today's behaviour,
  stays the default. The pool name `local` is reserved. No migration.
  Design: docs/runner.md.
