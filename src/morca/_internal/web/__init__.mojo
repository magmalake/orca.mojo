"""morca's web server: one router over the pages (`morca._internal.ui`) and the JSON API
(`morca._internal.api`), who each request is from, and the listeners.

| Module | |
|---|---|
| `morca._internal.web.server` | `morca ui`: the handler, its steps, the listeners |
| `morca._internal.web.router` | how a path finds its route; `Call`, what a handler gets |
| `morca._internal.web.routes` | the route table: every path and what answers it |
| `morca._internal.web.auth` | sessions, API tokens, same-origin posts, `/login`, `/logout` |
"""

from morca._internal.web.server import DEFAULT_ADDR, act, read_page, serve_ui
