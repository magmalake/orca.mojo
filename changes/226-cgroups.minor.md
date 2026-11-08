- **Each local, control-placed and worker run gets a cgroup with a memory
  limit, and an out-of-memory kill retries bigger** (#201). Where orca can
  make cgroup v2 cgroups (`orca scheduler`, `orca worker`), each start gets
  `orca-run-<run>-<start>-<pid>` beside the leaf `orca` the scheduler moves
  itself into: `memory.high` at its grant, else its request's maximum, else
  `ORCA_RUN_MEMORY_MB` (default what the host can grant; on the control
  placement at most `ORCA_CONTROL_MEMORY_MB`), and `memory.max`
  `ORCA_RUN_MEMORY_MARGIN` (25) percent above, with `memory.oom.group` on.
  The process is placed in it between `fork` and `exec` (the child writes
  `0` to a `cgroup.procs` descriptor the parent opened). At the reap,
  `memory.events`' `oom_kill` (or a stale-heartbeat kill while held at the
  limit) fails the attempts `out of memory at <limit>: ...` instead of
  signal 9, and raises `orca.pipelines.memory_raised_mb` to
  `ORCA_RUN_MEMORY_GROWTH` (1.5) times the limit, up to `memory_gb`'s
  maximum: admission makes it the request's minimum for the retry and the
  pipeline's later runs; registering a different `memory_gb` resets it.
  Cgroups are emptied (`cgroup.kill`) and removed when runs end, and a
  scheduler that starts removes those a dead one left. The run's page shows
  Memory limit, the Steps table each attempt's limit beside its peak, the
  attempt page its limit and grant, and Ops › Control a Runs card of the
  runs going with their cgroup's memory against their limit (`runs` in
  `/api/system`); the JSON has `memory_limit`, `memory_limit_mb` and
  `grant_memory_mb`. Migration 39. Where cgroups aren't available (macOS,
  cgroup v1, no delegation, `ORCA_CGROUPS=0`) the scheduler says why once
  and runs as before. On the Hetzner server, `orca-stack configure
  CGROUPS=1` gives the scheduler's container a private cgroup namespace and
  a writable `/sys/fs/cgroup` (off by default; docs/hetzner.md).
