"""The worker API: what `orca worker` (`orca.worker`) and the runs it starts
(`orca.http_store.HttpStore`) call, each a `POST` of JSON with
`Authorization: Bearer <worker token>`. What each call does is
`orca.workers`; this is how a request reaches it.

| Path | |
|---|---|
| `/api/worker/claim` | the worker's heartbeat, and with `slots` > 0 a queued run of its pool to start: its spec, or `null` |
| `/api/worker/release` | a run it was given and won't start: queued again, held saying why |
| `/api/worker/exited` | a run's process ended, with its samples |
| `/api/worker/store/<call>` | a `RunStore` call of a run this worker claimed (`orca.state.PgStore`) |

Every call is checked the same way first: the token is a live worker
token (401), the body a JSON object (400), and a `pool` it names the
token's (403). A user's API token gets nothing here, and a worker token
nothing elsewhere.

**Its own listener.** By default `orca ui` answers these with its pages.
With `ORCA_WORKER_ADDR` (`host:port`) it also serves them on that address,
from a thread of their own, so a slow page can't hold up a worker's claims
and heartbeats or its runs' store calls: point `orca worker --remote` at
it. Everything else there is 404.
"""

from std.memory.alloc import unsafe_alloc

from flare.http import Handler, HttpServer, Request, Response, ServerConfig
from flare.net import SocketAddr
from json import Value, loads
from threads import AtomicFlag, WorkerPool
from threads.ffi import OpaquePtr, opaque_ptr

from orca.http_store import STORE_PATH
from orca.json import quote
from orca.state import State
from orca.store import FINAL
from orca.web.auth import bearer
from orca.web.page import Page, respond
from orca.workers import (
    WorkerAuth,
    claim,
    exited,
    release,
    store_call,
    text_field,
    worker_of,
)

comptime PREFIX = "/api/worker/"


@fieldwise_init
struct WorkerCall(Movable):
    """A worker's request as its handler sees it: the database's URL (a
    store call opens its own `PgStore`), the worker, its JSON, and for a
    store call which one."""

    var url: String
    var worker: WorkerAuth
    var body: Value
    var call: String


comptime WorkerEndpoint = def(mut State, WorkerCall) thin raises -> Page


@fieldwise_init
struct WorkerRoute(Copyable, Movable):
    var path: StaticString
    """The path, or with `prefix` what it starts with."""
    var prefix: Bool
    var handler: WorkerEndpoint


def routes() -> List[WorkerRoute]:
    return [
        WorkerRoute("/api/worker/claim", False, _claim),
        WorkerRoute("/api/worker/release", False, _release),
        WorkerRoute("/api/worker/exited", False, _exited),
        WorkerRoute(STORE_PATH, True, _store),
    ]


def _answer(status: Int, var json: String) -> Page:
    return Page(status, json^, String())


def _error(status: Int, why: String) -> Page:
    return _answer(status, String('{"error": ', quote(why), "}"))


def _claim(mut db: State, w: WorkerCall) raises -> Page:
    return _answer(200, claim(db, w.worker, w.body))


def _release(mut db: State, w: WorkerCall) raises -> Page:
    return _answer(200, release(db, w.worker, w.body))


def _exited(mut db: State, w: WorkerCall) raises -> Page:
    return _answer(200, exited(db, w.worker, w.body))


def _store(mut db: State, w: WorkerCall) raises -> Page:
    """A store call; one refused is final for the run (403 if it isn't the
    worker's any more, 409 if it's lost or cancelled)."""
    try:
        return _answer(200, store_call(db, w.url, w.worker, w.call, w.body))
    except e:
        var msg = String(e)
        if msg.startswith(FINAL):
            var bare = String(msg[byte = FINAL.byte_length() :])
            var status = 403 if " claimed; stopping" in bare else 409
            return _error(status, bare)
        if msg.startswith("orca: not committing"):
            return _error(409, msg)
        return _error(500, msg)


def _body(text: String) -> Optional[Value]:
    """The request's JSON object, or None for anything else."""
    try:
        var v = loads(text if text.strip() else "{}")
        if v.is_object():
            return v^
    except:
        pass
    return None


def answer(url: String, path: String, token: String, body: String) -> Page:
    """A worker's request to `path` (under `/api/worker/`), with the token
    `token`, over the database at `url`."""
    try:
        var db = State(url)
        var w = worker_of(db, token)
        if not w:
            return _error(
                401,
                (
                    "orca: this worker token is unknown, expired or revoked"
                    " (orca worker-token create)"
                ),
            )
        var v = _body(body)
        if not v:
            return _error(400, "orca: a worker's request is a JSON object")
        var pool = text_field(v.value(), "pool")
        if pool and pool != w.value().pool:
            return _error(
                403,
                String(
                    "orca: this worker token is for pool ",
                    w.value().pool,
                    ", not ",
                    pool,
                ),
            )
        for r in routes():
            var call = String()
            if r.prefix and path.startswith(r.path):
                call = String(path[byte = r.path.byte_length() :])
            elif path != r.path:
                continue
            var wc = WorkerCall(
                url=url, worker=w.value().copy(), body=v.take(), call=call^
            )
            return r.handler(db, wc)
        return _error(404, String("orca: nothing at ", path))
    except e:
        return _error(500, String(e))


def worker_response(url: String, req: Request) -> Response:
    """The response to a worker's `req`, which the UI's server and the
    worker listener both send."""
    var q = req.url.find("?")
    var path = String(req.url[byte=:q]) if q >= 0 else req.url
    var page = answer(
        url,
        path,
        bearer(req.headers.get("Authorization")),
        String(from_utf8_lossy=Span(req.body)),
    )
    if page.status == 401:
        page.headers.append(("WWW-Authenticate", 'Bearer realm="orca"'))
    return respond(page, True)


# ── its own listener ─────────────────────────────────────────────────────────


struct WorkerServer(Copyable, Handler):
    """The worker listener (`ORCA_WORKER_ADDR`): the worker API, and 404
    for anything else."""

    var url: String

    def __init__(out self, url: String):
        self.url = url

    def serve(self, req: Request) raises -> Response:
        if req.method == "POST" and req.url.startswith(PREFIX):
            return worker_response(self.url, req)
        return respond(_error(404, String("orca: nothing at ", req.url)), True)


struct _Listening(Movable):
    var server: HttpServer
    var url: String

    def __init__(out self, var server: HttpServer, url: String):
        self.server = server^
        self.url = url


def _listen(worker: Int, ctx: OpaquePtr, stop: AtomicFlag) -> None:
    ref l = Pointer[_Listening, MutAnyOrigin](unsafe_from_address=Int(ctx))[]
    try:
        l.server.serve(WorkerServer(l.url))
    except e:
        print("orca ui: the worker listener stopped:", e)


struct _Listener(Movable):
    var _state: List[_Listening]
    """One element, which the thread holds by address: a list's elements
    stay where they are when the list is moved."""
    var _pool: WorkerPool

    def __init__(out self, url: String, addr: String) raises:
        var cfg = ServerConfig()
        cfg.idle_timeout_ms = 30_000
        cfg.write_timeout_ms = 30_000
        self._state = List[_Listening]()
        self._state.append(
            _Listening(HttpServer.bind(SocketAddr.parse(addr), cfg^), url)
        )
        self._pool = WorkerPool.start[_listen](
            1, opaque_ptr(Int(Pointer(to=self._state[0])))
        )


def listen(url: String, addr: String) raises:
    """Binds `addr` and serves the worker API there from a thread of its
    own until the process ends. The listener is never dropped: dropping a
    `WorkerPool` joins its thread, and nothing ends a server's loop but the
    process ending."""
    var l = unsafe_alloc[_Listener](1)
    l.unsafe_write(_Listener(url, addr))
