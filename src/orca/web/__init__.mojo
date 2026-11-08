"""orca's web server: one router over the pages (`orca.ui`) and the JSON API
(`orca.api`), who each request is from, and the listeners.

| Module | |
|---|---|
| `orca.web.server` | `orca ui`: the handler, its steps, the listeners |
| `orca.web.router` | how a path finds its route; `Call`, what a handler gets |
| `orca.web.routes` | the route table: every path and what answers it |
| `orca.web.auth` | sessions, API tokens, same-origin posts, `/login`, `/logout` |
"""

from orca.web.server import DEFAULT_ADDR, act, read_page, serve_ui
