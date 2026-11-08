- **Alerts have their own page, Ops › Alerts.** What `orca notify` pushes
  to the phone is now in the UI too, at `/ops/alerts`, the first entry of
  the Ops menu (`/ops` opens it rather than Control). It lists every alert
  newest first, 50 to a page: when, its kind (run failed, watchdog trip,
  fill stalled, disk, worker lost, memory pressure, backup failed, and each
  "resolved"), its title and message as sent, links to the run, pipelines,
  watchdog rule, worker or Control, whether ntfy took it (sent, waiting,
  failed and retrying with the error, or replaced before it went), and for
  alerts that end whether each is still open, tinted red until it
  resolves. `?kind=` and `?state=open|resolved` filter it, it pages like
  the runs list, the newest page refreshes every 30 s, and every column
  has a "?". `/api/alerts` is the same as JSON, and `orca alerts [--kind K]
  [--open|--resolved] [--limit N] [--json]` prints it, also `--remote`.
  Each alert sent or resolved is a row of the new `orca.alerts` (migration
  41, additive, so an older binary still runs on the schema; seeded from
  what `orca.notifications` held, kept 90 days); sending is unchanged.
