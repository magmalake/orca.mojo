- **Lists in pages, and pages that keep up by themselves.** `/runs`, a
  pipeline's Runs tab and a dataset's snapshots page by id (keyset), not
  by offset: `?before=<id>`, `?after=<id>` and `?limit=` (50 on `/runs`,
  20 on the tab, at most 500; `?n=` still works), with « Newest, ← Newer
  and Older → links that keep the filters, and `1–50 of 1,234` from an
  indexed count. A page stays put while new runs arrive. The JSON keeps
  its fields and first page, and adds `limit`, `after`, `older` and the
  cursors `next`, `prev`, `next_url`, `prev_url` (`runs_page` on a
  pipeline, `snapshots_page` on a dataset, which used to stop silently at
  50). Migration 37 adds `runs_pipeline_id` on `orca.runs (pipeline, id
  DESC)`; `RunStore.runs` takes a `before` cursor in every store, and
  `<pipeline> status --before <run>` pages on the command line. Runs
  tables refresh from `/runs/table` (every 5 s while one of their runs is
  queued or running, the newest page every 30 s so new runs appear at its
  top, older pages not once they're idle), as do the home page's
  pipelines (`/home/pipelines`), an attempt's facts, Ops Control (30 s),
  Workers (10 s) and the watchdog (30 s); a refresh keeps the row at the
  top of the window in place when rows arrive above it, and leaves a field
  being typed in alone. `docs/ui.md#lists-of-runs-in-pages`,
  `docs/ui.md#live-pages`, `tests/ui.sh`, `tests/store_suite.mojo`.
