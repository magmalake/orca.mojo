"""The `orca` command against a deployed `orca ui`, over HTTPS, as a user of
it: log in once, then act from anywhere the UI can be reached.

```
orca login https://orca.mlake.dev     # asks for a name and a password
orca --remote orca.mlake.dev backfill gharchive 2025-01-02..2025-01-31
orca --remote orca.mlake.dev pause gharchive
orca --remote orca.mlake.dev status gharchive
orca logout https://orca.mlake.dev    # revokes the token
```

`--remote <url>` (or `--url`), else `ORCA_REMOTE` (or `ORCA_URL`), picks
the UI; without one every command goes to `ORCA_DATABASE_URL`, as it always
has. A bare host is `https://<host>`. Plain `http` is refused but to
`127.0.0.1`, `localhost` and `[::1]`, where nothing is on the wire: a token,
like a password, never crosses a network in the clear.

`orca login` posts the name and password to `/api/login`, which checks
them as `/login` does, under the same limit on failures, and answers an API
token (`orca.tokens`). The password is read without echo from the terminal,
or as a line of standard input when that isn't one (`--user` names the
user then). The token is kept in `ORCA_CREDENTIALS`, by default
`~/.config/orca/credentials` (`$XDG_CONFIG_HOME/orca` if set), a line per
UI: its URL, the user, when the token expires, and the token. The file is
written with mode 600 in a directory made with mode 700, and one that
others can read is refused, as ssh refuses a key.

The commands that can go to a UI are `backfill` (`--rerun`, and
`--preview`, which queues nothing), `start`, `cancel` (a pipeline's keys,
or one run), `pause` and `unpause`, `resume` and `rerun` of a run,
`rebuild`, and the reads `status` and `lineage`. An action is a `POST` of
JSON to the path the UI's own button posts to, with `Authorization: Bearer
<token>`, so it's the same code, the same checks and the same row of
`orca.audit` as the button's, from `cli`. Reads are the pages' JSON,
which needs no login, and they send no token. Each prints what the command
prints over the database. The token is never in a URL, an argument or a
message.
"""

from std.ffi import c_int, c_uint, external_call
from std.os import getenv, makedirs, remove, stat
from std.os.path import dirname, exists
from std.sys import stderr

from json import Value, loads
from objectstore.http import Header, HttpClient, Response

from orca.civil import now, parse_time
from orca.html import url_encode
from orca.json import quote
from orca.lineage import SnapshotRef
from orca.pressure import pct_text
from orca.scheduler import file_names
from orca.state import State
from orca.ui.parts import took_text
from orca.usage import usage_of, usage_text
from orca.watchdog import status_lines
from orca.web.server import act, read_page


def is_remote_command(cmd: String) -> Bool:
    """Whether `--remote` can run `cmd`; the others need the database."""
    return (
        cmd == "backfill"
        or cmd == "start"
        or cmd == "cancel"
        or cmd == "pause"
        or cmd == "unpause"
        or cmd == "resume"
        or cmd == "rerun"
        or cmd == "rebuild"
        or cmd == "status"
        or cmd == "lineage"
        or cmd == "watchdog"
    )


# ── where to ─────────────────────────────────────────────────────────────────


def _local_host(host: String) -> Bool:
    return host == "127.0.0.1" or host == "localhost" or host == "[::1]"


def remote_url(given: String) raises -> String:
    """`given` as a UI's URL, `scheme://host[:port]` in lower case without a
    default port, or raises why it's refused: anything but `https`, but
    `http` to this machine; a path, query, fragment or user in it."""
    var u = String(given.strip())
    if not u:
        raise Error("orca: no UI to go to (--remote <url>, or ORCA_REMOTE)")
    if u.find("://") < 0:
        u = String("https://", u)
    var sep = u.find("://")
    var scheme = String(u[byte=:sep]).lower()
    var rest = String(u[byte = sep + 3 :])
    if rest.endswith("/"):
        var trimmed = String(rest[byte = : rest.byte_length() - 1])
        rest = trimmed^
    for c in rest.as_bytes():
        if (
            c <= 0x20
            or c >= 0x7F
            or c == UInt8(ord("/"))
            or c == UInt8(ord("?"))
            or c == UInt8(ord("#"))
            or c == UInt8(ord("@"))
            or c == UInt8(ord("\\"))
        ):
            raise Error(
                "orca: a UI's URL is scheme://host[:port], with no path,",
                " query or user: ",
                given,
            )
    var authority = rest.lower()
    if not authority or authority.startswith(":"):
        raise Error("orca: no host in ", given)
    # The host is what comes before the port: after `]` for an IPv6 one.
    var host = authority
    var close = authority.find("]")
    var colon = authority.rfind(":")
    if colon > close:
        var port = String(authority[byte = colon + 1 :])
        host = String(authority[byte=:colon])
        try:
            var p = Int(port)
            if p < 1 or p > 65535:
                raise Error()
        except:
            raise Error("orca: not a port: ", port)
        if (scheme == "https" and port == "443") or (
            scheme == "http" and port == "80"
        ):
            authority = host
    if scheme == "http":
        if not _local_host(host):
            raise Error(
                "orca: ",
                given,
                " is plain http, which would send the password and the",
                " token in the clear; use https (http is only for",
                " 127.0.0.1 and localhost)",
            )
    elif scheme != "https":
        raise Error("orca: a UI is reached over https, not ", scheme)
    return String(scheme, "://", authority)


def remote_target(mut args: List[String]) raises -> String:
    """The UI the command line picks, `--remote <url>` (or `--url`) right
    after `orca`, which this removes from `args`, else `ORCA_REMOTE` (or
    `ORCA_URL`); empty for none."""
    var given = String()
    if len(args) >= 2:
        var a = args[1]
        if a == "--remote" or a == "--url":
            if len(args) < 3:
                raise Error("orca: ", a, " needs the UI's URL")
            given = args[2]
            _ = args.pop(1)
            _ = args.pop(1)
        elif a.startswith("--remote=") or a.startswith("--url="):
            given = String(a[byte = a.find("=") + 1 :])
            _ = args.pop(1)
    if not given:
        given = getenv("ORCA_REMOTE", getenv("ORCA_URL"))
    return remote_url(given) if given else String()


# ── the credentials file ─────────────────────────────────────────────────────


@fieldwise_init
struct Credential(Copyable, Movable):
    var url: String
    var user: String
    var expires: String
    var token: String


def credentials_path() raises -> String:
    """`ORCA_CREDENTIALS`, else `orca/credentials` in `XDG_CONFIG_HOME`, else
    in `~/.config`."""
    var p = getenv("ORCA_CREDENTIALS")
    if p:
        return p
    var config = getenv("XDG_CONFIG_HOME")
    if not config:
        var home = getenv("HOME")
        if not home:
            raise Error("orca: no HOME for the credentials (ORCA_CREDENTIALS)")
        config = String(home, "/.config")
    return String(config, "/orca/credentials")


def _mode(path: String) raises -> Int:
    return Int(stat(path).st_mode) & 0o777


def read_credentials(path: String) raises -> List[Credential]:
    """The logins kept in `path`, none if there's no file. Raises if others
    may read it."""
    var out = List[Credential]()
    if not exists(path):
        return out^
    var mode = _mode(path)
    if mode & 0o077 != 0:
        raise Error(
            "orca: ",
            path,
            " can be read by others (mode ",
            oct(mode),
            "); chmod 600 it, or remove it and orca login again",
        )
    var text: String
    with open(path, "r") as f:
        text = f.read()
    # Not `splitlines`, which takes the tab between fields for a line's end.
    for line in text.split("\n"):
        if not line or line.startswith("#"):
            continue
        var f = line.split("\t")
        if len(f) != 4:
            raise Error("orca: ", path, " has a line that isn't a login")
        out.append(
            Credential(String(f[0]), String(f[1]), String(f[2]), String(f[3]))
        )
    return out^


def write_credentials(path: String, creds: List[Credential]) raises:
    """Writes `creds` to `path` with mode 600, in a directory made with mode
    700 if there's none: a new file, created under umask 077 and moved into
    place, so the token is never in a file others could read."""
    var dir = dirname(path)
    if dir:
        makedirs(dir, mode=0o700, exist_ok=True)
    var text = String(
        "# orca login's API tokens (orca.remote): URL, user, expires, token."
        "\n# Keep this file to yourself; orca logout <url> revokes one.\n"
    )
    for c in creds:
        text.write(c.url, "\t", c.user, "\t", c.expires, "\t", c.token, "\n")
    var tmp = String(path, ".", Int(external_call["getpid", c_int]()), ".tmp")
    var old = external_call["umask", c_uint](c_uint(0o077))
    try:
        with open(tmp, "w") as f:
            f.write(text)
    finally:
        _ = external_call["umask", c_uint](old)
    if _mode(tmp) != 0o600:
        remove(tmp)
        raise Error("orca: couldn't make ", tmp, " mode 600")
    var final = path
    if (
        external_call["rename", c_int](
            tmp.as_c_string_span(), final.as_c_string_span()
        )
        != 0
    ):
        remove(tmp)
        raise Error("orca: couldn't write ", path)


def find_credential(creds: List[Credential], url: String) -> Int:
    """The index of `url`'s login in `creds`, or -1."""
    for i in range(len(creds)):
        if creds[i].url == url:
            return i
    return -1


# ── HTTP ─────────────────────────────────────────────────────────────────────


def _refusal(v: Value) -> String:
    """The `error` of a UI's JSON answer, as orca's own errors say it."""
    try:
        var why = v["error"].string_value()
        return why if why.startswith("orca") else String("orca: ", why)
    except:
        return ""


struct Remote(Movable):
    """A UI at `url`, as `user` with `token` (empty for reads)."""

    var url: String
    var token: String
    var http: HttpClient

    def __init__(out self, url: String, token: String = ""):
        self.url = url
        self.token = token
        self.http = HttpClient()
        # Never retried: a POST the UI may have acted on isn't sent twice,
        # and the client follows no redirect, which could take the token
        # somewhere else.
        self.http.follow_redirects = False
        self.http.verify_tls = True

    def _headers(self, json_body: Bool) -> List[Header]:
        """A read sends no token: the pages need none, and the token
        travels only with what it's for."""
        var h: List[Header] = [
            Header("accept", "application/json"),
            Header("user-agent", "orca-cli"),
        ]
        if json_body:
            h.append(Header("content-type", "application/json"))
        if json_body and self.token:
            h.append(Header("authorization", String("Bearer ", self.token)))
        return h^

    def _answer(self, resp: Response) raises -> String:
        """The answer's JSON if it's a 200, else raises what it says."""
        var text = String(from_utf8_lossy=Span(resp.body))
        if resp.status == 200:
            return text^
        if resp.status == 401 and self.token:
            raise Error(
                "orca: ",
                self.url,
                " refused the login: the token has expired or been revoked;",
                " orca login ",
                self.url,
                " again",
            )
        var why = String()
        try:
            why = _refusal(loads(text))
        except:
            pass
        if resp.status == 401 and not why:
            why = String("orca: log in first: orca login ", self.url)
        if why:
            raise Error(why)
        raise Error(
            "orca: ",
            self.url,
            " answered ",
            resp.status,
            ": ",
            resp.body_excerpt(200),
        )

    def get(self, path: String) raises -> String:
        var resp: Response
        try:
            resp = self.http.get(String(self.url, path), self._headers(False))
        except e:
            raise Error("orca: can't reach ", self.url, ": ", e)
        return self._answer(resp)

    def post(self, path: String, body: String) raises -> String:
        var resp: Response
        try:
            resp = self.http.request(
                "POST",
                String(self.url, path),
                self._headers(True),
                body.as_bytes(),
            )
        except e:
            raise Error("orca: can't reach ", self.url, ": ", e)
        return self._answer(resp)


def _json_object(fields: List[Tuple[String, String]]) -> String:
    var out = String("{")
    for i in range(len(fields)):
        out.write(", " if i else "", quote(fields[i][0]), ": ")
        out.write(quote(fields[i][1]))
    out.write("}")
    return out^


def _form(fields: List[Tuple[String, String]]) -> String:
    var form = String()
    for f in fields:
        form.write("&" if form else "", url_encode(f[0]), "=", url_encode(f[1]))
    return form^


struct Api(Movable):
    """What the commands here read and do: a UI's JSON (`remote`), or the
    same pages and actions over the database, in this process."""

    var remote: Optional[Remote]
    var db_url: String

    def __init__(out self, var remote: Remote):
        self.remote = remote^
        self.db_url = ""

    def __init__(out self, db_url: String):
        self.remote = None
        self.db_url = db_url

    def get(mut self, path: String) raises -> String:
        """The JSON `GET path` answers (`path` has its query)."""
        if self.remote:
            var sep = "&" if "?" in path else "?"
            return self.remote.value().get(String(path, sep, "format=json"))
        var db = State(self.db_url)
        db.migrate()
        var page = read_page(db, path)
        if page.status != 200:
            raise Error(_refusal(loads(page.json)))
        return page.json

    def post(
        mut self, path: String, fields: List[Tuple[String, String]]
    ) raises -> String:
        """The JSON of the action `path` with `fields`, or raises why it was
        refused."""
        if self.remote:
            return self.remote.value().post(path, _json_object(fields))
        var db = State(self.db_url)
        db.migrate()
        var done = act(db, path, _form(fields))
        if not done:
            raise Error("orca: nothing at ", path)
        ref page = done.value()
        if page.status != 303:
            raise Error(_refusal(loads(page.json)))
        return page.json


# ── logging in and out ───────────────────────────────────────────────────────


def _isatty() -> Bool:
    return external_call["isatty", c_int](c_int(0)) == 1


def _shell(cmd: String) -> Int:
    var c = List[UInt8](cmd.as_bytes())
    c.append(0)
    return Int(external_call["system", c_int](c.unsafe_ptr()))


def _read_password() raises -> String:
    """A line of standard input: typed at the terminal without echo, or
    piped in."""
    if not _isatty():
        return String(input().rstrip("\r\n"))
    print("Password: ", end="", file=stderr, flush=True)
    _ = _shell("stty -echo")
    var password: String
    try:
        password = String(input().rstrip("\r\n"))
    finally:
        _ = _shell("stty echo")
        print(file=stderr)
    return password^


def _hostname() -> String:
    var buf = List[UInt8](length=256, fill=0)
    if external_call["gethostname", c_int](buf.unsafe_ptr(), Int(255)) != 0:
        return "cli"
    var n = 0
    while n < 255 and buf[n] != 0:
        n += 1
    return String(StringSlice(unsafe_from_utf8=Span(buf)[0:n]))


def login_command(args: List[String], target: String) raises -> Int:
    """`orca login [<url>] [--user NAME] [--label L] [--days N]`."""
    var url = target
    var user = String()
    var label = _hostname()
    var days = 0
    var i = 2
    while i < len(args):
        var a = args[i]
        if (a == "--user" or a == "--label" or a == "--days") and i + 1 < len(
            args
        ):
            if a == "--user":
                user = args[i + 1]
            elif a == "--label":
                label = args[i + 1]
            else:
                try:
                    days = Int(args[i + 1])
                except:
                    raise Error("orca: --days is a count of days")
            i += 2
        elif not a.startswith("--") and not url:
            url = remote_url(a)
            i += 1
        else:
            raise Error("orca: unknown argument to login: ", a)
    if not url:
        raise Error("orca: login needs the UI's URL: orca login https://<host>")
    var path = credentials_path()
    var creds = read_credentials(path)
    if not user:
        if not _isatty():
            raise Error(
                "orca: login needs --user NAME when standard input isn't a"
                " terminal"
            )
        print("User: ", end="", file=stderr, flush=True)
        user = String(input().strip())
    var password = _read_password()
    var body = String(
        '{"name": ',
        quote(user),
        ', "password": ',
        quote(password),
        ', "label": ',
        quote(label),
    )
    if days > 0:
        body.write(', "days": ', days)
    body.write("}")
    var v = loads(Remote(url).post("/api/login", body))
    var c = Credential(
        url,
        v["user"].string_value(),
        v["expires"].string_value(),
        v["token"].string_value(),
    )
    var at = find_credential(creds, url)
    if at >= 0:
        creds[at] = c.copy()
    else:
        creds.append(c.copy())
    write_credentials(path, creds)
    print("logged in to", url, "as", c.user, "until", c.expires)
    return 0


def logout_command(args: List[String], target: String) raises -> Int:
    """`orca logout [<url>]`: revokes the token there, and forgets it."""
    var url = target
    if len(args) == 3:
        url = remote_url(args[2])
    elif len(args) > 3:
        raise Error("orca: logout takes one URL")
    var path = credentials_path()
    var creds = read_credentials(path)
    if not url and len(creds) == 1:
        url = creds[0].url
    if not url:
        raise Error("orca: logout from which UI? orca logout https://<host>")
    var at = find_credential(creds, url)
    if at < 0:
        raise Error("orca: not logged in to ", url)
    var note = String()
    try:
        _ = Remote(url, creds[at].token).post("/api/logout", "{}")
    except e:
        # An expired or revoked token is what logging out wants; anything
        # else (the UI unreachable) keeps it, to try again.
        if "expired or been revoked" not in String(e):
            raise e^
        note = " (its token had already expired or been revoked)"
    _ = creds.pop(at)
    write_credentials(path, creds)
    print("logged out of ", url, note, sep="")
    return 0


def remote_api(url: String) raises -> Api:
    """`url`'s UI as the login kept for it, or with no token if there's
    none (reads need none)."""
    var creds = read_credentials(credentials_path())
    var at = find_credential(creds, url)
    return Api(Remote(url, creds[at].token if at >= 0 else String()))


def _need_login(api: Api) raises:
    if api.remote and not api.remote.value().token:
        raise Error(
            "orca: not logged in to ",
            api.remote.value().url,
            "; orca login ",
            api.remote.value().url,
            " first",
        )


# ── the commands ─────────────────────────────────────────────────────────────


def _seg(s: String) -> String:
    return url_encode(s)


def _keys_arg(keys: String) raises -> String:
    """`keys`, with `@file:<path>` read here: the UI can't read this
    machine's files, so their names go to it as a list."""
    if keys.startswith("@file:"):
        return file_names(String(keys[byte=6:]))
    return keys


def _int_of(v: Value, key: String) raises -> Int:
    return Int(v[key].int_value())


def _run_took(r: Value) raises -> String:
    """How long a run ran (`3m 21s`), up to now while it runs; `-` before it
    started."""
    var started = r["started"]
    if not started.is_string() or not started.string_value():
        return "-"
    var ended = r["ended"]
    var end = now()
    if ended.is_string() and ended.string_value():
        end = parse_time(ended.string_value())
    return took_text(Int(end - parse_time(started.string_value())))


def _text(v: Value, key: String) raises -> String:
    """A field of `v` as text: a string as it is, null as `-`."""
    var x = v[key]
    if x.is_null():
        return "-"
    if x.is_string():
        return x.string_value()
    return String(x)


def _status_line(p: Value) raises -> String:
    """A pipeline in one line: whether it's paused, its live runs, its last
    run and its next fire."""
    var line = String(p["name"].string_value(), "  ")
    line.write("paused" if p["paused"].bool_value() else "active")
    line.write(
        "  running ",
        _int_of(p, "running"),
        "  queued ",
        _int_of(p, "queued"),
        "  last ",
    )
    var last = p["last_run"]
    if last.is_null():
        line.write("-")
    else:
        line.write(
            "run ",
            _int_of(last, "id"),
            " ",
            last["status"].string_value(),
            " ",
            last["partitions"].string_value(),
        )
    line.write("  next ")
    var next = p["next_fire"]
    if next.is_null():
        line.write("-")
    else:
        line.write(
            next["at"].string_value(), " ", next["partition"].string_value()
        )
    return line^


def pressure_status(v: Value) raises -> String:
    """`orca status`'s line for the scheduler's host's memory pressure
    (`memory_pressure` in `/`'s JSON, `orca.admission`), or empty for
    none."""
    if String("memory_pressure") not in v or v["memory_pressure"].is_null():
        return ""
    var m = v["memory_pressure"]
    if m["some"].is_null() and m["path"].string_value():
        return String("memory pressure: can't read ", m["path"].string_value())
    if m["some"].is_null():
        return "memory pressure: none to read (no PSI on the scheduler's host)"
    var line = String(
        "memory pressure: ",
        _pct(m["some"]["avg10"].float_or(0.0)),
        "% (avg10; ",
        _pct(m["some"]["avg60"].float_or(0.0)),
        "% avg60): ",
    )
    var limit = m["limit"].float_or(0.0)
    if m["held"].is_string():
        line.write(m["held"].string_value())
        if m["stuck"].bool_or(False):
            line.write(", one at a time with nothing else going")
    elif limit <= 0.0:
        line.write("admission by pressure is off")
    else:
        line.write("starting runs (limit ", _pct(limit), "%)")
    return line^


def _pct(x: Float64) -> String:
    return pct_text(x)


def print_status(json: String, one: Bool) raises:
    """`orca status`'s lines from `/`'s JSON, or with `one` a pipeline's
    page's: its line, any sensing error, and its latest runs."""
    var v = loads(json)
    var ps = List[Value]()
    if one:
        ps.append(v["pipeline"].copy())
    else:
        ps = v["pipelines"].array_items()
    if len(ps) == 0:
        print("no pipelines")
    if not one:
        var pressure = pressure_status(v)
        if pressure:
            print(pressure)
    for p in ps:
        print(_status_line(p))
        var se = p["sense_error"]
        if not se.is_null():
            print(
                "  sensing failed at",
                se["at"].string_value() + ":",
                se["error"].string_value(),
            )
    if not one:
        return
    var runs = v["runs"].array_items()
    print("runs:" if len(runs) > 0 else "runs: none")
    for r in runs:
        var line = String(
            "  ",
            _int_of(r, "id"),
            " ",
            r["status"].string_value(),
            " ",
            r["trigger"].string_value(),
            " ",
            r["partitions"].string_value(),
            " ",
            _run_took(r),
        )
        if (
            r["cancel_requested"].bool_value()
            and r["status"].string_value() == "running"
        ):
            line.write(" (cancelling)")
        if String("usage") in r:
            var used = usage_text(usage_of(r["usage"]))
            if used:
                line.write(" ", used)
        var err = r["error"]
        if err.is_string() and err.string_value():
            line.write(" error: ", err.string_value())
        print(line)


def _print_made(ms: List[Value]) raises:
    """As `orca lineage` prints `Made`s (orca.command)."""
    if len(ms) == 0:
        print("  nothing")
    for m in ms:
        var at = SnapshotRef(
            m["dataset"].string_value(), m["snapshot"].int_value()
        )
        var line = String("  ", _int_of(m, "depth"), " ", at, " by ")
        line.write(m["pipeline"].string_value(), " run ", _int_of(m, "run"))
        line.write(" ", m["step"].string_value())
        if m["partition"].string_value():
            line.write(" ", m["partition"].string_value())
        line.write(" code ", m["code"].string_value())
        var inputs = m["inputs"].array_items()
        if len(inputs) > 0:
            line.write(" from")
            for x in inputs:
                line.write(
                    " ",
                    SnapshotRef(
                        x["dataset"].string_value(), x["snapshot"].int_value()
                    ),
                )
        print(line)


def api_command(mut api: Api, args: List[String]) raises -> Int:
    """Runs `args` (`orca <command> ...`) through `api`, printing what the
    command prints over the database."""
    var cmd = args[1]
    var n = len(args)
    var json_out = False
    var rest = List[String]()
    for i in range(2, n):
        if args[i] == "--json" and (
            cmd == "status" or cmd == "lineage" or cmd == "watchdog"
        ):
            json_out = True
        else:
            rest.append(args[i])
    if cmd == "status" and len(rest) <= 1:
        var one = len(rest) == 1
        var got = api.get(
            String("/pipelines/", _seg(rest[0])) if one else String("/")
        )
        if json_out:
            print(got)
        else:
            print_status(got, one)
        return 0
    if cmd == "watchdog" and len(rest) == 1 and rest[0] == "status":
        var got = api.get("/api/watchdog")
        if json_out:
            print(got)
        else:
            for line in status_lines(got):
                print(line)
        return 0
    if cmd == "lineage" and (len(rest) == 1 or len(rest) == 2):
        var ds = rest[0]
        var id: Int64
        if len(rest) == 2:
            try:
                id = Int64(Int(rest[1]))
            except:
                raise Error("usage: not a snapshot: ", rest[1])
        else:
            var d = loads(api.get(String("/datasets/", _seg(ds))))
            if d["latest"].is_null():
                raise Error("orca: no snapshot of ", ds, " is recorded")
            id = d["latest"].int_value()
        var got = api.get(String("/datasets/", _seg(ds), "/snapshots/", id))
        if json_out:
            print(got)
            return 0
        var v = loads(got)
        print(SnapshotRef(ds, id))
        print("made from:")
        _print_made(v["back"].array_items())
        print("made from it:")
        _print_made(v["forward"].array_items())
        return 0
    _need_login(api)
    if cmd == "backfill":
        var rerun = False
        var preview = False
        var allow_append = False
        var pos = List[String]()
        for a in rest:
            if a == "--rerun":
                rerun = True
            elif a == "--preview":
                preview = True
            elif a == "--allow-append":
                allow_append = True
            elif a.startswith("--"):
                raise Error("usage: unknown option: ", a)
            else:
                pos.append(a)
        if len(pos) != 2:
            raise Error(
                "usage: orca backfill [--rerun [--allow-append]] [--preview]",
                " <pipeline> KEYS",
            )
        var keys = _keys_arg(pos[1])
        if preview:
            var v = loads(
                api.get(
                    String(
                        "/pipelines/",
                        _seg(pos[0]),
                        "?preview=1&partitions=",
                        url_encode(keys),
                        "&rerun=1" if rerun else "",
                        "&allow_append=1" if allow_append else "",
                    )
                )
            )
            if String("preview") not in v:
                raise Error("orca: nothing to preview over '", keys, "'")
            var p = v["preview"]
            if not p["refused"].is_null():
                var why = p["refused"].string_value()
                raise Error(
                    why if why.startswith("orca") else String("orca: ", why)
                )
            print(p["summary"].string_value())
            return 0
        var fields: List[Tuple[String, String]] = [(String("partitions"), keys)]
        if rerun:
            fields.append((String("rerun"), String("1")))
        if allow_append:
            fields.append((String("allow_append"), String("1")))
        var v = loads(
            api.post(String("/pipelines/", _seg(pos[0]), "/backfill"), fields)
        )
        if String("skipped") in v:
            for line in v["skipped"].array_items():
                print(line.string_value(), file=stderr)
        print("queued", _int_of(v, "queued"), "runs")
        return 0
    if cmd == "start" and len(rest) == 2:
        var v = loads(
            api.post(
                String("/pipelines/", _seg(rest[0]), "/runs"),
                [(String("partitions"), _keys_arg(rest[1]))],
            )
        )
        print("queued run", _int_of(v, "run"))
        return 0
    if cmd == "cancel" and len(rest) == 2:
        var v = loads(
            api.post(
                String("/pipelines/", _seg(rest[0]), "/cancel"),
                [(String("partitions"), _keys_arg(rest[1]))],
            )
        )
        print("cancelled", _int_of(v, "cancelled"), "runs")
        return 0
    if cmd == "cancel" and len(rest) == 1:
        var run = _run_arg(rest[0])
        _ = api.post(
            String("/runs/", run, "/cancel"), List[Tuple[String, String]]()
        )
        print("cancelled run", run)
        return 0
    if (cmd == "pause" or cmd == "unpause") and len(rest) == 1:
        _ = api.post(
            String("/pipelines/", _seg(rest[0]), "/", cmd),
            List[Tuple[String, String]](),
        )
        print(cmd + "d", rest[0])
        return 0
    if cmd == "watchdog" and len(rest) >= 1 and rest[0] == "ack":
        var which = (
            String("all") if len(rest) == 1 or rest[1] == "--all" else rest[1]
        )
        _ = api.post(
            String("/watchdog/", _seg(which), "/ack"),
            List[Tuple[String, String]](),
        )
        print("acknowledged", "all trips" if which == "all" else which)
        return 0
    if cmd == "watchdog" and len(rest) == 3 and rest[0] == "snooze":
        _ = api.post(
            String("/watchdog/", _seg(rest[1]), "/snooze"),
            [(String("duration"), rest[2])],
        )
        print("snoozed", rest[1], "for", rest[2])
        return 0
    if cmd == "resume" and (len(rest) == 1 or len(rest) == 2):
        var allow = False
        var run = 0
        for a in rest:
            if a == "--allow-new-code":
                allow = True
            else:
                run = _run_arg(a)
        if run == 0:
            raise Error("usage: orca resume [--allow-new-code] <run>")
        var fields = List[Tuple[String, String]]()
        if allow:
            fields.append((String("allow_new_code"), String("1")))
        try:
            _ = api.post(String("/runs/", run, "/resume"), fields)
        except e:
            var why = String(e)
            if "ran code" in why and not allow:
                why.write(
                    " (orca resume --allow-new-code ",
                    run,
                    " resumes it with the new code)",
                )
            raise Error(why)
        print("resumed run", run)
        return 0
    if cmd == "rerun" and (len(rest) == 2 or len(rest) == 3):
        var allow_append = False
        var pos = List[String]()
        for a in rest:
            if a == "--allow-append":
                allow_append = True
            else:
                pos.append(a)
        if len(pos) != 2:
            raise Error("usage: orca rerun [--allow-append] <run> <step>")
        var run = _run_arg(pos[0])
        var fields: List[Tuple[String, String]] = [(String("step"), pos[1])]
        if allow_append:
            fields.append((String("allow_append"), String("1")))
        var v = loads(api.post(String("/runs/", run, "/rerun"), fields))
        print(
            "queued run",
            _int_of(v, "run"),
            "rerunning run",
            run,
            "from step",
            pos[1],
        )
        return 0
    if cmd == "rebuild" and len(rest) == 2:
        var v = loads(
            api.post(
                String("/datasets/", _seg(rest[0]), "/rebuild"),
                [(String("partitions"), rest[1])],
            )
        )
        var id = _int_of(v, "rebuild")
        print("rebuild", id)
        var r = loads(api.get(String("/rebuilds/", id)))["rebuild"]
        for p in r["plan"].array_items():
            print(
                "  level",
                _int_of(p, "level"),
                p["pipeline"].string_value(),
                p["partitions"].string_value(),
            )
        return 0
    raise Error("usage: ", cmd, " doesn't take those arguments")


def _run_arg(s: String) raises -> Int:
    try:
        var n = Int(s)
        if n > 0:
            return n
    except:
        pass
    raise Error("usage: not a run: ", s)
