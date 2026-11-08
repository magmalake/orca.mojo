- **Runs on the local runner get their secrets.** A pipeline's
  `secrets` (orca.toml, or `--secret`) were read for control-placed runs,
  on EC2 and by `orca worker`, but the default local runner gave its runs
  only `env`, so a `GITHUB_TOKEN` run called GitHub anonymously. The
  scheduler now reads each secret as it starts a local run and gives the
  values to that run alone, named in `ORCA_SECRETS` for masking; a secret
  it can't read fails the run with `can't start: can't read secret <id>`,
  retried as other start failures are. A local run with secrets stages its
  output outside `ORCA_LOGS`, as a control-placed run does, and `keep-log`
  appends it to `driver.log` masked once the run ends (so its driver.log
  fills at the end rather than live). The Kubernetes runner still gives
  its pods no secrets. `tests/secrets.sh` has a local-runner case.
