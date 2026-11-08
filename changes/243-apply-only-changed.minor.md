- **`orca-stack apply` starts again only the containers whose image or
  settings changed.** Each container is labelled with a hash of what it's
  started with (`orca.stack.spec`: the image's id, its env file, its flags,
  mounts and `CGROUPS`), and apply keeps the ones that match, printing
  `ui: replaced (image ac0c766 -> b2b9558)` or `console: unchanged` for
  each. It refuses over running runs only when the scheduler would be
  replaced. `orca-stack plan` says what an apply would do without doing it,
  `apply --all` replaces everything as before, and `apply --only <name>`
  one container. Postgres is still never restarted. The first apply with
  this `orca-stack` replaces every container once (they have no label yet).
- **`deploy.sh` pauses the pipelines only when the scheduler is replaced.**
  It asks `orca-stack plan` first: a census-only deploy (`./deploy.sh <the
  orca tag that runs> console=<sha>`) backs up and replaces the console,
  with nothing paused, killed or registered; a deploy of what already runs
  does nothing. A new orca tag still pauses, waits, backs up, applies,
  registers and unpauses. `deploy.sh --all` replaces every container.
