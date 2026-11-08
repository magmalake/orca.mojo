- **An older orca runs on a newer schema whose migrations since are
  additive.** Each migration has a compatibility floor, the fewest
  migrations a binary must know to keep working on its result, recorded in
  `orca.schema_version.min_binary`. A binary behind the database now starts
  when the recorded floor is within what it knows, logging `orca: the
  database is at schema version 41, ahead of this binary's 40; its
  migrations since are additive, so running on it`, and refuses only past a
  floor-raising migration (so far 24, the warm pool's new key on
  `orca.instances`). AGENTS.md has the rule for writing migrations that
  keep rollbacks possible.
- **`deploy.sh` backs up before it deploys, and `deploy.sh --rollback` goes
  back.** Each deploy runs `orca-stack backup-now` after quiescing and
  before applying (stopping if it fails, unless `--no-backup`) and prints
  the previous tag, the new one and the dump's key. `orca-stack` keeps the
  tags each apply replaced (`orca-stack tags --previous`, `ORCA_PREV_TAG`),
  and `orca-stack rollback` applies them again and registers the pipelines.
  docs/hetzner.md has a Rolling back runbook.
