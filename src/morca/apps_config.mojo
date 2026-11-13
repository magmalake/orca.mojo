"""Apps beside the UI (#331): an `[apps.<name>]` table in morca.toml says
what the app needs, as `[pipelines.<name>]` does for a pipeline, and the
stack that runs it (`morca-stack` on Hetzner) builds its environment from
that, through `morca config apps`.

    [apps.console]
    port = 8080                       # it listens on $PORT, this
    check = "/healthz"                # answers 200 once it's ready (default /)
    env = { CONSOLE_SEMANTIC = "1" }
    lake = true                       # the warehouse and the stack's S3 keys
    secrets = { POSTHOG_KEY = "orca/posthog-key" }
    memory_gb = 1                     # its memory limit (none by default)

The app's binary is `/opt/morca/bin/<name>` in the image. A stack names the
apps it runs (`APPS=console` in hetzner.env); this file says what each one
gets. An app gets nothing it doesn't ask for: no morca database, no
analytics key unless it names one in `secrets`.
"""

from morca._internal.secrets import secret_problem
from morca.toml import BOOLEAN, FLOAT, INTEGER, STRING, TABLE, TomlDoc, toml_key

comptime APP_KEYS = ["port", "check", "env", "lake", "secrets", "memory_gb"]

comptime LAKE_ENV = [
    "AWS_REGION",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "MORCA_WAREHOUSE",
    "MORCA_PUBLIC_WAREHOUSE",
]
"""What `lake = true` gives an app, which `env` and `secrets` can't set."""

comptime STACK_NAMES = ["scheduler", "ui", "sampler", "cloudflared", "postgres"]
"""The stack's own containers, which an app can't be named."""


@fieldwise_init
struct AppConfig(Copyable, Movable):
    """An `[apps.<name>]` table, checked."""

    var name: String
    var port: Int
    """0 if the file gives none (the stack's APPS must)."""
    var check: String
    var lake: Bool
    var memory_mb: Int
    """0 for no limit."""
    var env: List[Tuple[String, String]]
    var secrets: List[Tuple[String, String]]
    """NAME and secret ID (`orca/...`), never a value."""


@fieldwise_init
struct AppProblem(Copyable, Movable):
    """Why a file's `[apps]` is refused: the path it's about, for its
    `file:line`, the table and the reason."""

    var path: List[String]
    var table: String
    var msg: String


def app_name_problem(name: String) -> String:
    """Why `name` can't be an app's, or empty: it's a container's name and
    a binary's, so lower-case letters, digits, `_` and `-`."""
    if name.byte_length() == 0:
        return "an app's name can't be empty"
    var i = 0
    for b in name.as_bytes():
        var c = Int(b)
        var ok = (
            (c >= ord("a") and c <= ord("z"))
            or (c >= ord("0") and c <= ord("9"))
            or (i > 0 and (c == ord("_") or c == ord("-")))
        )
        if not ok:
            return String(
                "an app's name is lower-case letters, digits, _ and -: ", name
            )
        i += 1
    for s in materialize[STACK_NAMES]():
        if name == s:
            return String(name, " is one of the stack's own containers")
    return ""


def _env_problem(name: String) -> String:
    """Why `name` can't be in an app's `env` or `secrets`, or empty."""
    if name == "PORT":
        return "PORT is set by the stack, from port"
    for r in materialize[LAKE_ENV]():
        if name == r:
            return String(name, " comes with lake = true")
    return ""


def _value_problem(text: String) -> String:
    """An env file holds a value to the end of its line."""
    if "\n" in text or "\r" in text:
        return "a value is one line"
    return ""


def _sub(path: List[String], key: String) -> List[String]:
    var p = path.copy()
    p.append(key)
    return p^


def read_apps(doc: TomlDoc) raises -> List[AppConfig]:
    """The file's `[apps.<name>]` tables, checked (`apps_problem` says
    why one is refused)."""
    var why = apps_problem(doc)
    if why:
        var p = why.value().copy()
        raise Error(p.table, " ", p.msg)
    var out = List[AppConfig]()
    for name in doc.tables(["apps"]):
        var path: List[String] = ["apps", name]
        var a = AppConfig(
            name,
            0,
            "/",
            False,
            0,
            List[Tuple[String, String]](),
            List[Tuple[String, String]](),
        )
        var port = doc.get(_sub(path, "port"))
        if port:
            a.port = Int(port.value().text)
        var check = doc.get(_sub(path, "check"))
        if check:
            a.check = check.value().text
        var lake = doc.get(_sub(path, "lake"))
        a.lake = Bool(lake) and lake.value().text == "true"
        var mem = doc.get(_sub(path, "memory_gb"))
        if mem:
            a.memory_mb = Int(Float64(mem.value().text) * 1024.0 + 0.5)
        for m in ["env", "secrets"]:
            var mp = _sub(path, String(m))
            for k in doc.keys(mp):
                var v = doc.get(_sub(mp, k)).value().text
                if m == "env":
                    a.env.append((k, v))
                else:
                    a.secrets.append((k, v))
        out.append(a^)
    return out^


def apps_problem(doc: TomlDoc) raises -> Optional[AppProblem]:
    """The first thing wrong with the file's `[apps]`, or none."""
    var top: List[String] = ["apps"]
    if not doc.get(top):
        return None
    if not doc.has_table(top):
        return AppProblem(top.copy(), "apps", "is a table of [apps.<name>]")
    for name in doc.keys(top):
        var path: List[String] = ["apps", name]
        var table = String("[apps.", toml_key(name), "]")
        if not doc.has_table(path):
            return AppProblem(
                path.copy(), String("apps.", toml_key(name)), "isn't a table"
            )
        var why = app_name_problem(name)
        if why:
            return AppProblem(path.copy(), table, why)
        for key in doc.keys(path):
            var p = _sub(path, key)
            var v = doc.get(p).value().copy()
            var known = False
            for k in materialize[APP_KEYS]():
                if key == k:
                    known = True
            if not known:
                var all = String()
                for k in materialize[APP_KEYS]():
                    all.write(", " if all else "", k)
                return AppProblem(
                    p.copy(),
                    table,
                    String("has no key ", toml_key(key), " (it has ", all, ")"),
                )
            if key == "port":
                if v.kind != INTEGER or Int(v.text) < 1 or Int(v.text) > 65535:
                    return AppProblem(
                        p.copy(), table, "port is a number from 1 to 65535"
                    )
            elif key == "check":
                if (
                    v.kind != STRING
                    or not v.text.startswith("/")
                    or ":" in v.text
                    or " " in v.text
                    or _value_problem(v.text)
                ):
                    return AppProblem(
                        p.copy(), table, "check is a path, like / or /healthz"
                    )
            elif key == "lake":
                if v.kind != BOOLEAN:
                    return AppProblem(p.copy(), table, "lake is true or false")
            elif key == "memory_gb":
                var n = 0.0
                if v.kind == INTEGER or v.kind == FLOAT:
                    n = Float64(v.text)
                if n <= 0.0 or n > 65536.0:
                    return AppProblem(
                        p.copy(), table, "memory_gb is a number of GB over 0"
                    )
            else:
                if v.kind != TABLE:
                    return AppProblem(
                        p.copy(),
                        table,
                        String(key, ' is a table, { NAME = "..." }'),
                    )
                for k in doc.keys(p):
                    var q = _sub(p, k)
                    var item = doc.get(q).value().copy()
                    var w: String
                    if key == "secrets":
                        if item.kind != STRING:
                            w = String(k, " is a Secrets Manager ID, a string")
                        else:
                            w = secret_problem(String(k, "=", item.text))
                            w = w.replace("--secret's ", "")
                            if not w and (
                                ".." in item.text or "//" in item.text
                            ):
                                w = String(k, ": an ID has no .. or //")
                    elif item.kind == TABLE or item.kind == "array":
                        w = String(k, " is a string, a number or a boolean")
                    else:
                        w = _value_problem(item.text)
                        if not w:
                            w = _env_name_problem(k)
                    if not w:
                        w = _env_problem(k)
                    if w:
                        return AppProblem(q.copy(), table, String(key, ": ", w))
    return None


def _env_name_problem(name: String) -> String:
    """A variable's name: letters, digits and `_`, and not a secret's."""
    var i = 0
    for b in name.as_bytes():
        var c = Int(b)
        var ok = (
            (c >= ord("A") and c <= ord("Z"))
            or (c >= ord("a") and c <= ord("z"))
            or c == ord("_")
            or (i > 0 and c >= ord("0") and c <= ord("9"))
        )
        if not ok:
            return String(
                "a variable's name is letters, digits and _, not ", name
            )
        i += 1
    var upper = name.upper()
    for word in ["PASSWORD", "SECRET", "TOKEN"]:
        if word in upper:
            return String(
                name,
                " looks like a secret: put its ID in secrets, not its value",
                " in env",
            )
    return ""


def apps_text(apps: List[AppConfig]) -> String:
    """`morca config apps`: a line per fact, `NAME FIELD VALUE`, for the
    stack's script; secrets by ID, never a value.

        console port 8080
        console check /healthz
        console lake true
        console memory_mb 1024
        console env CONSOLE_SEMANTIC=1
        console secret POSTHOG_KEY orca/posthog-key
    """
    var out = String()
    for a in apps:
        out.write(a.name, " port ", a.port, "\n")
        out.write(a.name, " check ", a.check, "\n")
        out.write(a.name, " lake ", "true" if a.lake else "false", "\n")
        out.write(a.name, " memory_mb ", a.memory_mb, "\n")
        for e in a.env:
            out.write(a.name, " env ", e[0], "=", e[1], "\n")
        for s in a.secrets:
            out.write(a.name, " secret ", s[0], " ", s[1], "\n")
    return out^
