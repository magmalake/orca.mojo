- **Admission by memory pressure.** On the local runner, the control host
  and each remote worker, no new run starts while Linux's pressure stall
  information reads `some avg10` over `ORCA_ADMIT_MEMORY_PRESSURE` (10%; 0
  turns it off); runs already going go on. A waiting run says why, as the
  grants' reasons do: `waiting: memory pressure 23% (limit 10%)` on its
  page, its pipeline's **Waiting**, the home and runs pages and the JSON
  (`... on worker <name>` for a worker's pool). The hold ends under half
  the limit, or after 30 s at most it. A hold that outlasts
  `ORCA_ADMIT_PRESSURE_GRACE_SECONDS` (600) with none of orca's runs going
  is logged, raises the new `memory-pressure` alert once, and lets runs
  start one at a time. The file read is the nearest cgroup's
  `memory.pressure` with a memory limit (a container run with `--memory`),
  else the host's `/proc/pressure/memory`, or `ORCA_MEMORY_PRESSURE_FILE`;
  without PSI (macOS, `psi=0`) nothing changes and the log says so once.
  `orca.pressure`, `orca.admission`; migration 40. docs/orca-toml.md,
  "Memory pressure"; `tests/pressure.sh`.
- **The pressure, shown.** Ops › Control has a Memory pressure card
  (`memory_pressure` in `/api/system`), `/api/workers`
  each worker's, `/metrics` the gauges `orca_memory_pressure_some_avg10`
  (and `_avg60`, `_avg300`, `full_*`) and
  `orca_admission_memory_pressure_held` by `host`, and `orca status` a line.
- **Runs get `ORCA_MEMORY_PRESSURE_FILE`**, the PSI file their host's
  pressure is read from (now reserved in `env`), and `orca.memory_pressure()`
  reads it, so a step can lower its own parallelism while it's high.
