- **The web layer in modules, behind one router.** `orca ui`'s 8,600-line
  `orca.ui` is four packages: `orca.web` (the server, a route table with a
  method-aware router in place of `serve()`'s chain of `if
  path.startswith(...)`, sessions and API tokens as a wrapper around the
  routes), `orca.api` (the actions, the worker API with its own route
  table, and the JSON writers), `orca.ui` (a module per page) and
  `orca.views` (the typed rows both draw from; the run and pipeline pages
  read a `RunView` and a `PipelineView` once for their HTML and their
  JSON). Pages, JSON and the worker API answer as before, byte for byte;
  only a malformed path's 404 can say it differently (`/runs/x/bogus` is
  "nothing at /runs/x/bogus", not "no run x"). The
  `orca.ui_style`, `orca.ui_viz`, `orca.ui_coverage`, `orca.terms` and
  `orca.step_cards` modules are `orca.ui.style`, `.viz`, `.coverage`,
  `.terms` and `.step_cards`. DESIGN.md, "Web layer", has the module map.
- **`ORCA_WORKER_ADDR`: the worker API on a listener of its own.** Set to a
  `host:port`, `orca ui` also serves `/api/worker/...` there, from a thread
  of its own, so a slow page can't hold up a worker's claims, heartbeats
  and store calls; everything else there is 404, and the UI's port keeps
  answering the worker API too. Unset, nothing changes.
  `docs/workers.md#a-listener-of-its-own`, `tests/ui.sh`.
