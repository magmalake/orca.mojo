- **A Hetzner deploy whose only failure is an unhealthy new UI or app no
  longer leaves the pipelines paused.** `deploy.sh` (and `--rollback`)
  registers a scheduler it replaced, unpauses what it paused, prints
  `orca-stack status` and fails naming the containers that weren't replaced
  (the old ones keep serving; `orca-stack apply` retries them). A scheduler
  that didn't start, a failed registration or a failed backup still leave
  them paused.
- **`orca-stack quiesce` no longer waits for runs on remote workers.** It,
  `apply`'s refusal and `status` count only the runs the scheduler's
  processes hold (placement not `worker:<pool>`), which a scheduler restart
  kills; the runs on workers go on through it and are listed as left
  running on workers. `rescale.sh`, which takes the UI down with the
  server, waits for them too (`quiesce --workers`).
