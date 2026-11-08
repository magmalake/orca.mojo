- **How far a sensor is behind what it follows.** A dated sensor that runs
  when another pipeline's dataset gets a snapshot (the census's mojo_repos
  and mojo_activity follow gharchive) has **Following** on its Progress
  card: `toy (up.days): 3 of 9 days done; 6 behind: 3 running or queued, 1
  failed and 2 not queued yet`, the first eight of each linked to their
  month on Coverage. What it follows has is every partition of the
  datasets it reads that orca knows of: the snapshots other pipelines'
  succeeded attempts committed (`orca.commits`) and the runs its own
  sensed snapshots started (`orca.sensed`), mapped to its kind and range.
  Coverage marks those days it hasn't done, failed or started as
  **behind** (amber, in the legend with a line saying what it means). The
  page's JSON and `/api/pipelines/<name>` have `follow` (null for other
  pipelines). One query for the keys, then the calendar's three.
  `orca.follow`, `docs/ui.md`, `tests/calendar.sh`.
