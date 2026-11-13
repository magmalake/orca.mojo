- **Overlapping schedulers on by default, with a required drain limit.**
  Upgrading needs `DRAIN_SECONDS=<seconds>` in hetzner.env (how long an old scheduler may drain before it is stopped): deploys refuse without it, and `OVERLAP=0` opts out of overlap (#411)
