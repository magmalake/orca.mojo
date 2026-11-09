- **`/healthz` answers 503 `draining` once `orca ui` starts draining.**
  From the moment SIGTERM or SIGINT starts a drain, `/healthz` says
  `draining` with a 503 without asking the database, so a health checker
  or proxy that asks over a connection it kept alive stops routing to the
  UI that's going away (docs/ui.md#stopping).
