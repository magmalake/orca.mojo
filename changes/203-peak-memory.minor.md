- **Each run's peak memory and CPU** (#200). The scheduler samples each
  local and control-placed run's process tree every tick (one pass over
  `/proc` for all of them: resident bytes, and utime + stime with what the
  tree waited for), and `orca worker` its runs' every poll; `reap` now
  uses `wait4`, whose `ru_maxrss` and CPU time close each process's
  samples. Each attempt the process made gets its share by time:
  `peak_rss_bytes`, `cpu_seconds` and `peak_cpus` on `orca.attempts`
  (migration 35; SQLite files get the columns too), null where nothing
  measured it (EC2, Kubernetes, `<binary> run`, and macOS's peak cores).
  A worker sends its samples with `/api/worker/exited` (`pid`, `max_rss`,
  `marks`). The run's page has Peak memory and CPU used, the Steps table
  and Runs tables a Peak column, an attempt's page its own; the pipeline's
  Overview has a **Resources** card, the p50 and max of its last 20
  measured runs (`usage` in its JSON), what #171's requests will be read
  from. A run, step and attempt have `usage` in the JSON, `orca status
  <pipeline>` prints each run's and `<binary> status <run>` each step's.
  `orca.usage`, `docs/ui.md#what-a-run-uses`, `tests/usage.sh` (a step
  holding 200 MiB beside a child holding 100, each burning a core, on the
  local runner, the control host and a worker).
