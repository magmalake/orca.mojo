- **`orca-stack apply` replaces the UI and the apps without downtime.** The
  new container starts as `<name>-next` on the `orca` network, gets the
  alias `<name>` only once it's healthy (the UI: 200 on `/healthz`, and its
  worker API listener answering if `ORCA_WORKER_ADDR` is set; an app: 200
  on its check path, `/` by default, which the census console answers
  only once it has read the lake), is checked again, and only then is the
  old one stopped (`docker stop -t 10`), removed and the new one renamed.
  cloudflared resolves the name per connection, so readers and remote
  workers see no gap. A new container that isn't healthy within
  `HEALTH_TIMEOUT` (default 300 s; `--health-timeout`) is removed, the old
  one keeps serving, and the apply exits non-zero saying why after
  applying the rest; `deploy.sh` still registers a scheduler it replaced.
  `orca-stack app NAME PORT CHECK` sets an app's check path. The
  scheduler, sampler and tunnel are replaced as before.
- **`orca ui` answers `/healthz`**: 200 `ok` once the database answers a
  query, 503 if it doesn't, with no login needed.
