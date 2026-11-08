- **Runs sized to their share of the machine** (part of #171). A pipeline
  can ask for `cpus = "1..16"` and `memory_gb = "2..24"` in orca.toml
  (`[defaults]` or its own table; a single number is both ends) or with
  `orca register --cpus` and `--memory-gb`; `orca config check` and
  register refuse bad ranges, and `orca config show` gives each with its
  source. On the local runner and for control-placed runs the scheduler
  then starts a run only if its minimum fits what this host can grant (its
  cgroup limits, else `/proc/meminfo` and its cores; `sysctl` on macOS;
  `ORCA_HOST_CPUS`, `ORCA_HOST_MEMORY_MB` over them) less a reserve
  (`ORCA_RESERVE_CPUS`, 1; `ORCA_RESERVE_MEMORY_MB`, 1536) less what the
  running runs were granted; otherwise it waits, held saying why
  (`waiting: needs 2 GB, 1.3 GB free of grants`) on its page, the
  pipeline's Progress card and in their JSON (`held`, `waiting`). A run
  that starts gets up to its maximum of what's left, fixed for that start,
  in `ORCA_GRANT_CPUS` and `ORCA_GRANT_MEMORY_MB`, recorded on the run
  (migration 36) and shown on its page beside its peak (`request`, `grant`
  in its JSON). `orca worker` admits the runs it takes against its own host
  the same way. `DISTA_WORKER_SLOTS = "auto"` becomes the granted CPUs over
  the run's local workers; other `"auto"` values reach the run as they are,
  for `orca.auto_count` to size from the grant. A pipeline without a
  request runs exactly as before. `orca.resources`,
  `docs/orca-toml.md#requests-and-grants`, `tests/grants.sh`. Also: `orca
  worker` reported its memory in KB as MB on Linux.
