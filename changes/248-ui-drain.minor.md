- **`orca ui` drains on SIGTERM.** SIGTERM or SIGINT closes the listening
  socket, lets the requests in flight finish and exits 0 once they have,
  within `ORCA_UI_DRAIN_SECONDS` (default 8, under `docker stop`'s 10 s), so
  a deploy that retires the old UI no longer cuts a page off mid-answer or
  waits out the grace period for a kill (docs/ui.md#stopping).
