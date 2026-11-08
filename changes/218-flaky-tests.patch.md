- **A log or samples file an EC2 instance keeps is never left empty.**
  `put-log` and `keep-log` (and the Kubernetes runner's log copies) wrote a
  local `ORCA_LOGS` file in place, truncating it first, so an instance
  terminated mid-copy, or a page read at that moment, could find the file
  empty or half written; it is now written beside it and renamed over it.
  An instance's last upload also waits for one still going, which could
  otherwise land after it with older lines.
- **A run's `started` is when the scheduler started it.** It was the time the
  tick's transaction began, before the start waited on locks and counted
  slots and the vCPU budget, so a run that correctly waited for another to
  end could look as if it had run beside it (the runs list, the timeline,
  and `tests/kind.sh`'s budget check).
