- **`orca register` with no arguments works.** It printed the usage instead
  of registering every `[pipelines.<name>]` of the file, as the docs say (and
  `deploy/hetzner` runs it so): only a call with a flag or a path got through.
  `tests/config.sh` now registers bare, from `./orca.toml` and from
  `ORCA_CONFIG`.
