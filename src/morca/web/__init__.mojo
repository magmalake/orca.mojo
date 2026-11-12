"""morca's web server: one router over the pages (`morca.ui`) and the JSON API
(`morca.api`), who each request is from, and the listeners.

| Module | |
|---|---|
| `morca.web.server` | `morca ui`: the handler, its steps, the listeners |
| `morca.web.router` | how a path finds its route; `Call`, what a handler gets |
| `morca.web.routes` | the route table: every path and what answers it |
| `morca.web.auth` | sessions, API tokens, same-origin posts, `/login`, `/logout` |
"""

from morca.web.server import DEFAULT_ADDR, act, read_page, serve_ui
