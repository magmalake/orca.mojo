- **`deploy.sh` brings the server's settings in line with `hetzner.env` (#257).**
  A setting added to `hetzner.env` after `up.sh` made the server (say
  `POSTHOG_KEY`) never reached it: only `up.sh` wrote `settings.env`, the
  secret files and the stack settings. Both now run `lib.sh`'s
  `sync_settings` (`--rollback` too), before the plan: each file is written
  only if it differs (mode 600, over ssh's standard input, `orca-stack
  sync`), and the output names what changed, never a value: the
  `settings.env` keys added, removed or changed, a secret file as changed,
  a stack setting old -> new. A setting emptied in `hetzner.env` is removed.
  The plan then replaces the containers whose env file changed, a changed
  scheduler quiescing as for a new tag. The tags stay `deploy.sh`'s, and
  `CGROUPS`, `OVERLAP` and `HEALTH_TIMEOUT` are left alone. An edit by hand
  to `/etc/orca/settings.env` is now undone by the next deploy: put it in
  `EXTRA_ENV_FILE`.
