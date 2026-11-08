- **`deploy.sh` registers the pipelines on every deploy, and a restore
  puts back what the deploy paused.** After the apply and before it
  unpauses anything, `deploy.sh` runs `orca-stack register` (a failure
  leaves the pipelines paused and says how to go on). Each backup writes
  `<time>.paused` beside its dump, the pipelines paused by hand and the ones
  a deploy paused; `orca-stack restore` unpauses the latter after restoring,
  leaving the hand-paused ones paused, and `list-backups` says which dumps
  have one.
