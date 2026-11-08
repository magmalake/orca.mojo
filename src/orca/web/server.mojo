"""`orca ui`'s server: one handler that looks each request up in the route
table (`orca.web.routes`), checks who it's from (`orca.web.auth`) and
answers it as its route's kind says.

```
ORCA_DATABASE_URL=postgresql:///orca ./build/orca ui     # 127.0.0.1:8270
```

A request goes through, in order:

1. The stylesheet, and the worker API (`POST /api/worker/...`,
   `orca.api.worker`, its own token), answered before anything else.
2. The route. A `POST` that's no action, or another method than `GET`,
   `HEAD` or `POST`, is 405.
3. A `POST` whose `Origin` (or `Referer`) isn't the UI's own is refused
   (403), unless it's a script's with no `Origin` (`orca.web.auth`).
4. The database, per request; `/api/login` is answered here, before any
   token is looked at.
5. Who it's from: an API token or a session. A token that's no use is 401.
6. By the route's kind (`orca.web.router`): logging in or out as the
   handler says; a page with who's logged in (or a way to log in) in its
   header; an action, once login is on only with a session, audited as its
   user's.

The UI holds no state: its actions are rows the scheduler acts on, and it
can run beside the scheduler or anywhere else that reaches the database.
`ORCA_UI_ADDR` (`host:port`) moves it; it binds to localhost unless told
otherwise. `ORCA_WORKER_ADDR` serves the worker API on a listener of its
own as well (`orca.api.worker`).
"""

from flare.http import Handler, HttpServer, Request, Response, ServerConfig
from flare.net import SocketAddr

from orca.api import worker as worker_api
from orca.auth import audit, login_required
from orca.html import Html, query_param, url_encode
from orca.json import quote
from orca.state import State
from orca.ui.coverage import COVERAGE_STYLE
from orca.ui.layout import (
    AUTH_SLOT,
    HERE_SLOT,
    USER_SLOT,
    bad_request,
    not_found,
    page_html,
)
from orca.ui.style import STYLE
from orca.ui.viz import VIZ_STYLE
from orca.web.auth import (
    API_LOGIN,
    client,
    content_type,
    cross_origin,
    json_error,
    json_form,
    login_needed,
    same_origin,
    scripted,
    sign_in,
    token_refused,
)
from orca.web.page import Page, respond
from orca.web.router import ACTION, AUTH, Call, PAGE, Route, answer, find
from orca.web.routes import routes

comptime DEFAULT_ADDR = "127.0.0.1:8270"
comptime STYLESHEET = "/static/orca.css"


def _path_of(url: String) -> String:
    var q = url.find("?")
    return String(url[byte=:q]) if q >= 0 else url


def _not_allowed() -> Response:
    var resp = Response(status=405, reason="Method Not Allowed")
    try:
        resp.headers.set("Allow", "GET, HEAD")
    except:
        pass
    return resp^


def _with_user(
    var page: Page,
    json: Bool,
    user: String,
    required: Bool,
    here: String = "/",
) -> Response:
    """The page, with who's logged in (or a way to log in, back to `here`)
    in its header."""
    var slot: String
    if user:
        var h = Html()
        h.raw('<span class="top__user">')
        h.text(user)
        h.raw(
            ' <form method="post" action="/logout"><button type="submit">',
            "Log out</button></form></span>",
        )
        slot = h^.finish()
    elif required:
        slot = String('<a class="top__user" href="/login">Log in</a>')
    else:
        slot = String()
    page.html = page.html.replace(USER_SLOT, slot)
    page.html = page.html.replace(
        String('<body class="', AUTH_SLOT, '">'),
        '<body class="anon">' if required and not user else "<body>",
    )
    page.html = page.html.replace(HERE_SLOT, url_encode(here))
    return respond(page, json)


def _run_of(location: String, path: String) -> Int:
    """The run an action made or acted on, from where it sent the browser or
    its own path; 0 for none."""
    for p in [location, path]:
        if p.startswith("/runs/"):
            var rest = p[byte=6:]
            var end = rest.find("/")
            try:
                return Int(rest[byte=:end] if end >= 0 else rest)
            except:
                pass
    return 0


def _error(e: Error, json: Bool) -> Response:
    """A 500: what went wrong, as the page or its JSON."""
    var h = Html()
    h.raw("<h1>Error</h1><pre>")
    h.text(e)
    h.raw("</pre>")
    var page = Page(
        500,
        String('{"error": ', quote(String(e)), "}"),
        page_html("Error", "", h),
    )
    var resp = respond(page, json)
    resp.reason = "Internal Server Error"
    return resp^


def _stylesheet() raises -> Response:
    """`/static/orca.css`: the pages' rules and the views'."""
    var resp = Response(
        status=200,
        reason="OK",
        body=List[UInt8](String(STYLE, VIZ_STYLE, COVERAGE_STYLE).as_bytes()),
    )
    resp.headers.set("Content-Type", "text/css; charset=utf-8")
    return resp^


struct Ui(Copyable, Handler):
    """The pages, the API and the actions, over the database at `url`."""

    var url: String
    var table: List[Route]

    def __init__(out self, url: String):
        self.url = url
        self.table = routes()

    def serve(self, req: Request) raises -> Response:
        var path = _path_of(req.url)
        var post = req.method == "POST"
        if path == STYLESHEET:
            return _stylesheet()
        if post and path.startswith("/api/worker/"):
            return worker_api.worker_response(self.url, req)
        var found = find(self.table, req.method, path)
        if not found.route and req.method != "GET" and req.method != "HEAD":
            return _not_allowed()

        var by_script = scripted(req, path)
        var json = (
            "application/json" in req.headers.get("Accept")
            or query_param(req.url, "format") == "json"
            or by_script
            or path.startswith("/api/")
        )
        if (
            post
            and not (by_script and not req.headers.get("Origin"))
            and not same_origin(req)
        ):
            return respond(cross_origin(), json)
        if path == API_LOGIN and not by_script:
            return respond(json_error(400, "a login for a token is JSON"), True)

        try:
            var db = State(self.url)
            var call = Call(req.url, path)
            call.body = String(from_utf8_lossy=Span(req.body))
            call.form = call.body
            call.address = client(req)
            if path == API_LOGIN:
                return respond(found.route.value().handler(db, call), True)
            if not sign_in(db, req, call):
                return respond(token_refused(), True)
            var required = login_required(db)
            if post and content_type(req) == "application/json":
                try:
                    call.form = json_form(call.body)
                except e:
                    return respond(bad_request(String(e)), json)

            if not found.route:
                var miss = found.miss if found.miss else String(
                    "nothing at ", path
                )
                return _with_user(
                    not_found(miss), json, call.user, required, req.url
                )
            ref route = found.route.value()
            call.params = found.params.copy()
            if route.kind == AUTH:
                return respond(route.handler(db, call), json)
            if route.kind == PAGE:
                var page = route.handler(db, call)
                var here = page.here if page.here else req.url
                return _with_user(page^, json, call.user, required, here)
            # An action.
            if required and not call.user:
                return login_needed(req, json)
            var page = route.handler(db, call)
            if page.status == 303 and call.user:
                audit(
                    db,
                    call.user,
                    path,
                    _run_of(page.location, path),
                    call.form,
                    call.source,
                )
            return _with_user(page^, json, call.user, required)
        except e:
            return _error(e, json)


def read_page(mut db: State, url: String) raises -> Page:
    """What `GET url` answers, for `orca` commands that print a page's JSON
    over the database (`orca.remote`)."""
    var got = answer(routes(), db, Call(url, _path_of(url)), PAGE)
    return got.take()


def act(mut db: State, path: String, form: String) raises -> Optional[Page]:
    """The action `path` with the fields of `form`, as the UI's button does
    it, or None if `path` names none (`orca.remote`). Not audited: the
    command acts as whoever can reach the database."""
    var call = Call(path, path)
    call.form = form
    call.body = form
    return answer(routes(), db, call^, ACTION)


def serve_ui(url: String, addr: String, worker_addr: String = "") raises:
    """Serves the pages on `addr` until the process is stopped, and the
    worker API on `worker_addr` too, if one is given (`orca.api.worker`)."""
    var cfg = ServerConfig()
    cfg.idle_timeout_ms = 30_000
    cfg.write_timeout_ms = 30_000
    var server = HttpServer.bind(SocketAddr.parse(addr), cfg^)
    if worker_addr:
        worker_api.listen(url, worker_addr)
        print("orca ui: the worker API on http://", worker_addr, sep="")
    print("orca ui on http://", addr, sep="")
    server.serve(Ui(url))
