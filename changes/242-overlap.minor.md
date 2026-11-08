- **Deploys that don't stop runs: overlapping schedulers (#232).** With
  `ORCA_OVERLAP=on` each `orca scheduler` is an instance with a row in the
  new `orca.schedulers`, and the newest live one leads (a session advisory
  lock) and alone decides. An older instance that sees a newer one, or is
  asked to (`orca scheduler drain <id>`), starts nothing more, supervises
  the runs it started until they end, then exits; after
  `ORCA_DRAIN_SECONDS` (3600) it kills what's left. Each local and
  control-placed run records its instance (`runs.instance`), and while
  that instance lists it the run is live: counted against `max_runs`, the
  control slots, the grants and memory pressure, and left alone. Once the
  instance is gone and the run's heartbeat stale, the leader queues it
  again whatever its trigger, audited. `orca scheduler status` lists the
  instances, `orca scheduler can-overlap` tells a deploy whether the new
  binary may start beside the old one (not when its migrations raise the
  floor past it, which `migrate` then refuses), `orca scheduler wait-lead`
  waits for the handover, and `orca register` waits for it too.
  `ORCA_OVERLAP=off` stays the default. Migration 42, additive. Design:
  docs/overlap.md.
