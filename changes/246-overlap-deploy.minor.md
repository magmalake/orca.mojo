- **Hetzner deploys that don't pause: overlapping schedulers (#232).**
  With `orca-stack configure OVERLAP=1` (off by default) the containers
  get `ORCA_OVERLAP=on`, and `deploy.sh` asks `orca-stack can-overlap`,
  the new image's `orca scheduler can-overlap`, before replacing the
  scheduler. On yes it pauses nothing: `orca-stack apply --overlap` starts
  the new scheduler beside the old one, keeps the old one from restarting,
  renames it `scheduler-old` and waits for the new one to lead; the
  pipelines are registered from the new one; and `orca-stack drain-old`
  asks the old one to drain, waits for its runs (up to `--timeout`, then
  stops it) and removes it. On no (a migration that raises the floor,
  overlap off, or an old scheduler still draining) the deploy pauses and
  waits as before. docs/hetzner.md, docs/overlap.md.
