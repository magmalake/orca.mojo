"""`orca.toml`: one checked-in file for how each of a project's pipelines
runs (#145), read by `orca register` and `orca aws register`.

    [defaults]
    placement = "ec2"
    head_type = "t4g.medium"

    [pipelines.gharchive]
    worker_type = "c7g.xlarge"
    workers = "0..4"             # or a count, 4
    worker_procs = 1             # or "auto", one per vCPU
    max_runs = 2                 # over the code's Pipeline max_runs
    cpus = "1..16"               # a request: MIN..MAX (orca.resources)
    memory_gb = "2..24"
    env = { DISTA_WORKER_SLOTS = "4", GHARCHIVE_PARALLEL = "16" }
    secrets = { GITHUB_TOKEN = "orca/github-token" }   # Secrets Manager IDs
    fills = "all"                # fill the range by itself (orca.fill)
    fill_budget_per_day = 5.0    # and the rest of the fill* keys

    [stack]                      # for `orca aws up`; not read yet

`orca config check [<file>] [--secrets]` checks a file without a database,
for a project's CI: what registering checks, and that orca can size each
instance type (a warning for one its price table hasn't); with `--secrets`,
that each secret can be read (its value is never printed). The pipeline
page's Configuration (`config_html`) is `orca config show`'s.

What stays in the pipeline's code is what it does; this file says where and
how much. A key in `[pipelines.<name>]` beats the same key in `[defaults]`
(`env` and `secrets` are merged name by name), and a flag of `orca register`
beats both, recorded with the registration as an override. A pipeline the
file names is registered with exactly what it and its flags say: a key
taken out of the file goes back to its default. `[defaults]` applies only to
the pipelines the file names; one it doesn't name is registered as before,
each flag left out keeping what was registered.

The registration keeps the file's SHA-256 and its git commit (and whether
it had changes not committed) in `orca.pipelines.config`, with where each
value came from, for `orca config show`. `orca config export` writes the
file back from what's registered.

**Adding a key**: a name in `SCALAR_KEYS` and its check in `_canonical`
(and its types in `_canonical_toml`), which gives the value as text; it is
then read, merged, overridden, recorded, shown and exported like the
others, and only its consumer is new. The `fill*` keys (#155, `orca.fill`)
were added this way, with no flag.
"""

from std.base64 import b64decode
from std.os import getenv
from std.os.path import exists

from json import Value, loads
from objectstore.crypto import sha256_hex
from postgres import Params, Row

from orca.control import parse_placement
from orca.fill import (
    SINCE_REGISTRATION,
    parse_fill_budget,
    parse_fill_max_runs,
    parse_fill_order,
    parse_fill_window,
    parse_fills,
)
from orca.html import Html
from orca.json import quote
from orca.proc import absolute, capture
from orca.resources import canonical_cpus, canonical_memory_gb
from orca.secrets import parse_secrets, secret_problem
from orca.source import is_hex, web_url
from orca.state import State
from orca.ui.terms import key_help
from orca.toml import (
    BOOLEAN,
    FLOAT,
    INTEGER,
    STRING,
    TABLE,
    TomlDoc,
    TomlValue,
    parse_toml,
    toml_key,
    toml_string,
)

comptime CODE_DEFAULT = "code default"
comptime FROM_FILE = "orca.toml"
comptime FROM_FLAG = "flag"
comptime FROM_ENV = "environment"

comptime SCALAR_KEYS = [
    "placement",
    "head_type",
    "worker_type",
    "workers",
    "worker_procs",
    "max_runs",
    "cpus",
    "memory_gb",
    "fills",
    "fill_order",
    "fill_max_runs",
    "fill_budget_per_day",
    "fill_window",
]
"""The keys of `[defaults]` and `[pipelines.<name>]` with one value. The
`fill*` keys (`orca.fill`) are the file's only: no flag sets them."""
comptime MAP_KEYS = ["secrets", "env"]
"""The keys whose value is a table of names."""

comptime _RESERVED_ENV = [
    "ORCA_DATABASE_URL",
    "ORCA_IMAGE",
    "ORCA_SECRETS",
    "ORCA_WORKERS",
    "DISTA_ROLE",
    "DISTA_HEAD",
    "DISTA_ADVERTISE",
    "ORCA_GRANT_CPUS",
    "ORCA_GRANT_MEMORY_MB",
    "ORCA_MEMORY_PRESSURE_FILE",
]
"""Variables orca gives a run itself, which `env` can't set."""


def flag_of(key: String) -> String:
    """The `orca register` flag of `key`: `--head-type` of `head_type`."""
    return String("--", key.replace("_", "-"))


def parse_worker_procs(v: String) raises -> Int:
    """`--worker-procs`'s value: a count of at least 1, or `auto` (returned
    as 0), one process per vCPU of the worker type."""
    if v == "auto":
        return 0
    try:
        var n = Int(v)
        if n >= 1 and n <= 256:
            return n
    except:
        pass
    raise Error("--worker-procs is a count from 1 to 256 or auto, not ", v)


def parse_workers(v: String) raises -> Tuple[Int, Int]:
    """`--workers`' value, `N` or `MIN..MAX`: the fewest (-1 for a fixed
    count) and the most."""
    try:
        var dots = v.find("..")
        if dots >= 0:
            var lo = Int(v[byte=:dots])
            var hi = Int(v[byte = dots + 2 :])
            if lo >= 0 and hi >= max(lo, 1):
                return (lo, hi)
        else:
            var n = Int(v)
            if n >= 1:
                return (-1, n)
    except:
        pass
    raise Error("not a count or a range MIN..MAX: ", v)


def parse_max_runs(v: String) raises -> Int:
    try:
        var n = Int(v)
        if n >= 1 and n <= 10000:
            return n
    except:
        pass
    raise Error("max_runs is a count from 1 to 10000, not ", v)


def _instance_type(v: String) raises -> String:
    var dot = v.find(".")
    var ok = dot > 0 and dot < v.byte_length() - 1
    for b in v.as_bytes():
        var c = Int(b)
        if not (
            (c >= ord("a") and c <= ord("z"))
            or (c >= ord("0") and c <= ord("9"))
            or c == ord(".")
            or c == ord("-")
        ):
            ok = False
    if not ok:
        raise Error("not an instance type: ", v)
    return v


def _canonical(key: String, text: String) raises -> String:
    """`key`'s value `text`, checked, as registration keeps it."""
    if key == "placement":
        return parse_placement(text)
    if key == "head_type" or key == "worker_type":
        return _instance_type(text)
    if key == "workers":
        var w = parse_workers(text)
        if w[0] < 0:
            return String(w[1])
        return String(w[0], "..", w[1])
    if key == "worker_procs":
        var n = parse_worker_procs(text)
        return String("auto") if n == 0 else String(n)
    if key == "max_runs":
        return String(parse_max_runs(text))
    if key == "cpus":
        return canonical_cpus(text)
    if key == "memory_gb":
        return canonical_memory_gb(text)
    if key == "fills":
        return parse_fills(text)
    if key == "fill_order":
        return parse_fill_order(text)
    if key == "fill_max_runs":
        return String(parse_fill_max_runs(text))
    if key == "fill_budget_per_day":
        return String(parse_fill_budget(text))
    if key == "fill_window":
        var w = parse_fill_window(text)
        return String(w[0], "..", w[1])
    raise Error("unknown key ", key)


def canonical_flag(key: String, text: String) raises -> String:
    """A flag's value for `key`, checked, as registration keeps it."""
    return _canonical(key, text)


def _canonical_toml(key: String, v: TomlValue) raises -> String:
    """`key`'s value in the file, checked: the types each takes."""
    if key == "cpus":
        if v.kind != INTEGER and v.kind != STRING:
            raise Error('is a count or a string "MIN..MAX", not ', v.kind)
        return _canonical(key, v.text)
    if key == "memory_gb":
        if v.kind != INTEGER and v.kind != FLOAT and v.kind != STRING:
            raise Error('is GB, a number or a string "MIN..MAX", not ', v.kind)
        return _canonical(key, v.text)
    if key == "fill_max_runs":
        if v.kind != INTEGER:
            raise Error("is a count, not ", v.kind)
        return _canonical(key, v.text)
    if key == "fill_budget_per_day":
        if v.kind != INTEGER and v.kind != FLOAT:
            raise Error("is US dollars a day, a number, not ", v.kind)
        return _canonical(key, v.text)
    if key == "fills" or key == "fill_order" or key == "fill_window":
        if v.kind != STRING:
            raise Error("is a string, not ", v.kind)
        return _canonical(key, v.text)
    var numeric = key == "workers" or key == "worker_procs" or key == "max_runs"
    if v.kind == INTEGER and numeric:
        return _canonical(key, v.text)
    if v.kind == STRING and key != "max_runs":
        return _canonical(key, v.text)
    if numeric and key != "max_runs":
        raise Error("is a count or a string, not ", v.kind)
    if key == "max_runs":
        raise Error("is a count, not ", v.kind)
    raise Error("is a string, not ", v.kind)


def env_name_problem(name: String) -> String:
    """Why `name` can't be a variable in `env`, or empty if it can."""
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
    if i == 0:
        return "a variable's name can't be empty"
    for r in materialize[_RESERVED_ENV]():
        if name == r:
            return String(name, " is set by orca itself")
    var upper = name.upper()
    for word in ["PASSWORD", "SECRET", "TOKEN", "DATABASE_URL"]:
        if word in upper:
            return String(
                name,
                " looks like a secret: put its Secrets Manager ID in",
                " secrets, not its value in env",
            )
    return ""


struct Setting(Copyable, Movable):
    """A value and where it came from: `CODE_DEFAULT`, `FROM_FILE`,
    `FROM_FLAG` or `FROM_ENV`, with a detail such as `[defaults]`, and for
    the file's, the line it's on."""

    var value: String
    var source: String
    var detail: String
    var line: Int
    """The file's line that sets it, from 1; 0 if not from the file, or
    registered before orca kept it."""

    def __init__(
        out self, value: String, source: String, detail: String, line: Int = 0
    ):
        self.value = value
        self.source = source
        self.detail = detail
        self.line = line


def is_scalar_key(key: String) -> Bool:
    for k in materialize[SCALAR_KEYS]():
        if key == k:
            return True
    return False


def is_map_key(key: String) -> Bool:
    return key == "secrets" or key == "env"


struct ProjectConfig(Movable):
    """A project's `orca.toml`, read and checked, or none (`path` empty)."""

    var path: String
    """The file as given, for messages; empty for no file."""
    var hash: String
    """The SHA-256 of its bytes, hex."""
    var commit: String
    """The git commit of the checkout it's in, or empty."""
    var dirty: Bool
    """Whether it differs from that commit."""
    var source: String
    """Where the commit is from (`ORCA_SOURCE`), for a file read outside a git
    checkout, as in the image; empty otherwise."""
    var repo_path: String
    """Its path in its checkout (`git ls-files --full-name`), for a link to
    its lines at the commit; empty outside a checkout."""
    var doc: TomlDoc

    def __init__(out self):
        """No file."""
        self.path = String()
        self.hash = String()
        self.commit = String()
        self.dirty = False
        self.source = String()
        self.repo_path = String()
        self.doc = TomlDoc()

    def __init__(
        out self,
        text: String,
        path: String,
        commit: String = "",
        dirty: Bool = False,
    ) raises:
        """`text`, the file `path` holds, checked: every key is one this
        orca knows, every value of its type."""
        self.path = path
        self.repo_path = String()
        self.hash = sha256_hex(text)
        self.commit = commit
        self.dirty = dirty
        self.source = String()
        try:
            self.doc = parse_toml(text)
        except e:
            raise Error(path, ":", e)
        self._check()

    @staticmethod
    def load(path: String) raises -> ProjectConfig:
        """The file at `path`, with its checkout's commit."""
        if not exists(path):
            raise Error("orca: no ", path)
        var text: String
        with open(path, "r") as f:
            text = f.read()
        var full = absolute(path)
        var slash = full.rfind("/")
        var dir = String(full[byte=:slash]) if slash > 0 else String("/")
        var commit = String()
        var dirty = False
        var repo_path = String()
        # sh keeps git's "fatal: not a git repository" off stderr.
        var base = String("git -C '", dir, "' ")
        try:
            var got = capture(
                ["sh", "-c", String(base, "rev-parse HEAD 2> /dev/null")], 10
            )
            if got[0] == 0:
                commit = String(got[1].strip())
                var st = capture(
                    [
                        "sh",
                        "-c",
                        String(
                            base,
                            "status --porcelain -- '",
                            String(full[byte = slash + 1 :]),
                            "' 2> /dev/null",
                        ),
                    ],
                    10,
                )
                dirty = st[0] != 0 or String(st[1].strip()) != ""
                var at = capture(
                    [
                        "sh",
                        "-c",
                        String(
                            base,
                            "ls-files --full-name -- '",
                            String(full[byte = slash + 1 :]),
                            "' 2> /dev/null",
                        ),
                    ],
                    10,
                )
                if at[0] == 0:
                    repo_path = String(at[1].strip())
        except:
            pass
        var source = String()
        if not commit and getenv("ORCA_COMMIT"):
            # Not in a checkout (the image): what the build recorded.
            commit = getenv("ORCA_COMMIT")
            source = getenv("ORCA_SOURCE")
        var cfg = ProjectConfig(text, path, commit, dirty)
        cfg.source = source
        cfg.repo_path = repo_path
        return cfg^

    @staticmethod
    def from_b64(
        b64: String, path: String, commit: String, dirty: Bool
    ) raises -> ProjectConfig:
        """The file as `orca aws register` sends it: base64."""
        var bytes = b64decode(b64)
        return ProjectConfig(
            String(from_utf8_lossy=Span(bytes)), path, commit, dirty
        )

    def is_file(self) -> Bool:
        return self.path != ""

    def has(self, pipeline: String) -> Bool:
        """Whether the file has `[pipelines.<pipeline>]`."""
        return self.doc.has_table(["pipelines", pipeline])

    def pipelines(self) -> List[String]:
        return self.doc.tables(["pipelines"])

    def where(self, pipeline: String, key: String, s: Setting) -> String:
        """`file:line: [table]` of `key`'s value `s` from this file, for
        a refusal that needs the pipeline (`fills` on a keyed one)."""
        var p: List[String] = ["defaults", key]
        if s.detail != "[defaults]":
            p = ["pipelines", pipeline, key]
        return String(self.path, ":", self._line(p), ": ", s.detail)

    def entries(self, key: String) -> List[Tuple[String, String]]:
        """Each value of `key` in `[defaults]` and the `[pipelines.<name>]`
        tables, with `file:line: [table] key` for a message; for `secrets`
        or `env`, each of its names (`... secrets: NAME`)."""
        var out = List[Tuple[String, String]]()
        var tables = List[List[String]]()
        var labels = List[String]()
        if self.doc.has_table(["defaults"]):
            tables.append(["defaults"])
            labels.append("[defaults]")
        for name in self.pipelines():
            tables.append(["pipelines", name])
            labels.append(String("[pipelines.", toml_key(name), "]"))
        for t in range(len(tables)):
            var p = tables[t].copy()
            p.append(key)
            var v = self.doc.get(p)
            if not v:
                continue
            if is_map_key(key):
                for name in self.doc.keys(p):
                    var q = p.copy()
                    q.append(name)
                    var item = self.doc.get(q).value().copy()
                    out.append(
                        (
                            String(
                                self.path,
                                ":",
                                item.line,
                                ": ",
                                labels[t],
                                " ",
                                key,
                                ": ",
                                name,
                            ),
                            item.text,
                        )
                    )
            else:
                out.append(
                    (
                        String(
                            self.path,
                            ":",
                            v.value().line,
                            ": ",
                            labels[t],
                            " ",
                            key,
                        ),
                        v.value().text,
                    )
                )
        return out^

    def _fail(self, line: Int, table: String, msg: String) raises:
        raise Error(self.path, ":", line, ": ", table, " ", msg)

    def _line(self, path: List[String]) -> Int:
        var v = self.doc.get(path)
        return v.value().line if v else 0

    def _check(self) raises:
        for top in self.doc.keys([]):
            var p: List[String] = [top]
            if not self.doc.has_table(p):
                self._fail(
                    self._line(p),
                    top,
                    (
                        "is a key at the top; the file has [defaults],"
                        " [pipelines.<name>] and [stack]"
                    ),
                )
            if top == "defaults":
                self._check_table(p, "[defaults]")
            elif top == "pipelines":
                for name in self.doc.keys(p):
                    var q: List[String] = ["pipelines", name]
                    if not self.doc.has_table(q):
                        self._fail(
                            self._line(q),
                            String("pipelines.", toml_key(name)),
                            (
                                "isn't a table: a pipeline's settings are"
                                " [pipelines.<name>]"
                            ),
                        )
                    self._check_table(
                        q, String("[pipelines.", toml_key(name), "]")
                    )
            elif top != "stack":
                # [stack] is for `orca aws up`, which doesn't read it yet.
                self._fail(
                    self._line(p),
                    String("[", toml_key(top), "]"),
                    (
                        "isn't a table orca knows: [defaults],"
                        " [pipelines.<name>] or [stack]"
                    ),
                )

    def _check_table(self, path: List[String], table: String) raises:
        for key in self.doc.keys(path):
            var p = path.copy()
            p.append(key)
            var v = self.doc.get(p).value().copy()
            if is_scalar_key(key):
                if v.kind == TABLE:
                    self._fail(v.line, table, String(key, " is a table"))
                try:
                    _ = _canonical_toml(key, v)
                except e:
                    self._fail(v.line, table, String(key, ": ", e))
            elif is_map_key(key):
                if v.kind != TABLE:
                    self._fail(
                        v.line,
                        table,
                        String(key, ' is a table, { NAME = "..." }'),
                    )
                for name in self.doc.keys(p):
                    var q = p.copy()
                    q.append(name)
                    var item = self.doc.get(q).value().copy()
                    var why = self._item_problem(key, name, item)
                    if why:
                        self._fail(item.line, table, String(key, ": ", why))
            else:
                var known = String()
                for k in materialize[SCALAR_KEYS]():
                    known.write(k, ", ")
                known.write("secrets and env")
                self._fail(
                    v.line,
                    table,
                    String(
                        "has no key ", toml_key(key), " (it has ", known, ")"
                    ),
                )

    def _item_problem(
        self, key: String, name: String, item: TomlValue
    ) -> String:
        if key == "secrets":
            if item.kind != STRING:
                return String(name, " is a Secrets Manager ID, a string")
            var why = secret_problem(String(name, "=", item.text))
            return why.replace("--secret's ", "")
        if item.kind == TABLE or item.kind == "array":
            return String(name, " is a string, a number or a boolean")
        return env_name_problem(name)

    def _map(
        self, path: List[String], key: String
    ) -> List[Tuple[String, String]]:
        """The names and values of `key`'s table under `path`."""
        var out = List[Tuple[String, String]]()
        var p = path.copy()
        p.append(key)
        for name in self.doc.keys(p):
            var q = p.copy()
            q.append(name)
            out.append((name, self.doc.get(q).value().text))
        return out^


@fieldwise_init
struct RegisterFlags(Copyable, Movable):
    """What `orca register`'s command line gave."""

    var values: Dict[String, String]
    """A `SCALAR_KEYS` key to its flag's value, checked."""
    var secrets: List[String]
    """`--secret NAME=ID`s, which replace the pipeline's when given."""
    var image: String
    var spot: Bool

    def __init__(out self):
        self.values = Dict[String, String]()
        self.secrets = List[String]()
        self.image = String()
        self.spot = False


def _set(mut list: List[Tuple[String, Setting]], name: String, var s: Setting):
    """`name`'s entry in `list`, replaced or added."""
    for ref e in list:
        if e[0] == name:
            e[1] = s^
            return
    list.append((name, s^))


struct Resolved(Movable):
    """What one pipeline is registered with: its settings from the file and
    the flags, each with where it came from."""

    var name: String
    var in_file: Bool
    """Whether the file names it; if not, flags left out keep what was
    registered, as before orca.toml."""
    var values: Dict[String, Setting]
    var secrets: List[Tuple[String, Setting]]
    var env: List[Tuple[String, Setting]]
    var overrides: List[Tuple[String, String]]
    """The flags given over the file's values, key and value."""

    def __init__(out self, name: String, in_file: Bool):
        self.name = name
        self.in_file = in_file
        self.values = Dict[String, Setting]()
        self.secrets = List[Tuple[String, Setting]]()
        self.env = List[Tuple[String, Setting]]()
        self.overrides = List[Tuple[String, String]]()

    def text(self, key: String) -> String:
        """`key`'s value, or empty if neither the file nor a flag set it."""
        var s = self.values.get(key)
        return s.value().value if s else String()

    def secret_specs(self) -> List[String]:
        var out = List[String]()
        for s in self.secrets:
            out.append(String(s[0], "=", s[1].value))
        return out^

    def env_pairs(self) -> List[Tuple[String, String]]:
        var out = List[Tuple[String, String]]()
        for e in self.env:
            out.append((e[0], e[1].value))
        return out^

    def env_json(self) -> String:
        """`env` as the JSON object Postgres keeps; empty for none."""
        return env_json(self.env_pairs())

    def config_json(self, file: ProjectConfig) -> String:
        """`orca.pipelines.config`: where each value came from, and for a
        pipeline the file names, the file's hash, commit and the flags that
        overrode it."""
        var src = String("{")
        var first = True
        for e in self.values.items():
            src.write("" if first else ", ", _source_json(e.key, e.value))
            first = False
        for e in self.secrets:
            src.write(
                "" if first else ", ",
                _source_json(String("secrets.", e[0]), e[1]),
            )
            first = False
        for e in self.env:
            src.write(
                "" if first else ", ", _source_json(String("env.", e[0]), e[1])
            )
            first = False
        src.write("}")
        var out = String('{"sources": ', src)
        if self.in_file:
            out.write(
                ', "file": ',
                quote(file.path),
                ', "hash": ',
                quote(file.hash),
                ', "commit": ',
                quote(file.commit),
                ', "dirty": ',
                "true" if file.dirty else "false",
            )
            if file.source:
                out.write(', "source": ', quote(file.source))
            if file.repo_path:
                out.write(', "repo_path": ', quote(file.repo_path))
            out.write(', "overrides": {')
            for i in range(len(self.overrides)):
                out.write(
                    "" if i == 0 else ", ",
                    quote(self.overrides[i][0]),
                    ": ",
                    quote(self.overrides[i][1]),
                )
            out.write("}")
        out.write("}")
        return out^


def _source_json(key: String, s: Setting) -> String:
    var out = String(
        quote(key),
        ': {"source": ',
        quote(s.source),
        ', "detail": ',
        quote(s.detail),
    )
    if s.line > 0:
        out.write(', "line": ', s.line)
    out.write("}")
    return out^


def env_json(env: List[Tuple[String, String]]) -> String:
    """Names and values as a JSON object; empty for none."""
    if len(env) == 0:
        return ""
    var out = String("{")
    for i in range(len(env)):
        out.write(
            "" if i == 0 else ", ", quote(env[i][0]), ": ", quote(env[i][1])
        )
    out.write("}")
    return out^


def parse_env(text: String) raises -> List[Tuple[String, String]]:
    """`env_json`'s object back as (NAME, value) pairs."""
    return parse_secrets(text)


def resolve(
    name: String, flags: RegisterFlags, file: ProjectConfig
) raises -> Resolved:
    """`name`'s registration from `file` and `flags`: `[defaults]`, then
    `[pipelines.<name>]`, then the flags."""
    var r = Resolved(name, file.has(name))
    if r.in_file:
        var tables: List[List[String]] = [
            ["defaults"],
            ["pipelines", name],
        ]
        var labels: List[String] = [
            String("[defaults]"),
            String("[pipelines.", toml_key(name), "]"),
        ]
        for t in range(2):
            if not file.doc.has_table(tables[t]):
                continue
            for key in materialize[SCALAR_KEYS]():
                var p = tables[t].copy()
                p.append(String(key))
                var v = file.doc.get(p)
                if v:
                    r.values[String(key)] = Setting(
                        _canonical_toml(String(key), v.value()),
                        FROM_FILE,
                        labels[t],
                        v.value().line,
                    )
            for m in ["secrets", "env"]:
                for kv in file._map(tables[t], m):
                    var p = tables[t].copy()
                    p.append(String(m))
                    p.append(kv[0])
                    var st = Setting(kv[1], FROM_FILE, labels[t], file._line(p))
                    if m == "secrets":
                        _set(r.secrets, kv[0], st^)
                    else:
                        _set(r.env, kv[0], st^)
    for e in flags.values.items():
        if r.in_file:
            r.overrides.append((e.key, e.value))
        r.values[e.key] = Setting(e.value, FROM_FLAG, flag_of(e.key))
    if len(flags.secrets) > 0:
        if r.in_file:
            var joined = String()
            for s in flags.secrets:
                joined.write("," if joined else "", s)
            r.overrides.append(("secrets", joined))
        r.secrets = List[Tuple[String, Setting]]()
        for s in flags.secrets:
            var eq = s.find("=")
            _set(
                r.secrets,
                String(s[byte=:eq]),
                Setting(String(s[byte = eq + 1 :]), FROM_FLAG, "--secret"),
            )
    return r^


# `orca config show` and `orca config export`: from the registrations.


@fieldwise_init
struct _Row(Copyable, Movable):
    var key: String
    var setting: Setting


struct Shown(Movable):
    """One registered pipeline's effective configuration."""

    var name: String
    var file: String
    var hash: String
    var commit: String
    var dirty: Bool
    var source: String
    var repo_path: String
    """The file's path in its checkout, for links to its lines."""
    var overrides: List[Tuple[String, String]]
    var rows: List[_Row]

    def __init__(out self, name: String):
        self.name = name
        self.file = String()
        self.hash = String()
        self.commit = String()
        self.dirty = False
        self.source = String()
        self.repo_path = String()
        self.overrides = List[Tuple[String, String]]()
        self.rows = List[_Row]()

    def add(
        mut self, key: String, value: String, source: String, detail: String
    ):
        self.rows.append(_Row(key, Setting(value, source, detail)))

    def json(self) -> String:
        var out = String('{"pipeline": ', quote(self.name))
        if self.file:
            out.write(
                ', "file": ',
                quote(self.file),
                ', "hash": ',
                quote(self.hash),
                ', "commit": ',
                quote(self.commit),
                ', "dirty": ',
                "true" if self.dirty else "false",
            )
        else:
            out.write(', "file": null')
        out.write(', "overrides": {')
        for i in range(len(self.overrides)):
            out.write(
                "" if i == 0 else ", ",
                quote(self.overrides[i][0]),
                ": ",
                quote(self.overrides[i][1]),
            )
        out.write('}, "settings": [')
        for i in range(len(self.rows)):
            ref r = self.rows[i]
            out.write(
                "" if i == 0 else ", ",
                '{"key": ',
                quote(r.key),
                ', "value": ',
                quote(r.setting.value),
                ', "source": ',
                quote(r.setting.source),
                ', "detail": ',
                quote(r.setting.detail),
                "}",
            )
        out.write("]}")
        return out^

    def text(self) -> String:
        var out = String(self.name)
        if self.file:
            out.write("  registered from ", self.file)
            if self.commit:
                out.write(
                    " @ ",
                    self.commit[byte = : min(12, self.commit.byte_length())],
                )
                if self.dirty:
                    out.write(" (with changes not committed)")
                if self.source:
                    out.write(" of ", self.source)
            out.write(
                ", sha256 ",
                self.hash[byte = : min(12, self.hash.byte_length())],
            )
        else:
            out.write("  not registered from orca.toml")
        out.write("\n")
        if len(self.overrides) > 0:
            out.write("  overrides:")
            for o in self.overrides:
                out.write(
                    " ",
                    flag_of(o[0]) if o[0] != "secrets" else String("--secret"),
                    " ",
                    o[1],
                )
            out.write("\n")
        var width = 0
        var vwidth = 0
        for r in self.rows:
            width = max(width, r.key.byte_length())
            vwidth = max(vwidth, r.setting.value.byte_length())
        vwidth = min(vwidth, 40)
        for r in self.rows:
            out.write("  ", r.key)
            for _ in range(width - r.key.byte_length() + 2):
                out.write(" ")
            out.write(r.setting.value)
            for _ in range(max(vwidth - r.setting.value.byte_length(), 0) + 2):
                out.write(" ")
            out.write(r.setting.source)
            if r.setting.detail:
                out.write(" ", r.setting.detail)
            out.write("\n")
        return out^


comptime _VALUE_TERMS = ["placement", "fills", "fill_order", "spot"]
"""The keys whose values are one of a fixed set (`orca.ui.terms`)."""


def config_html(
    mut h: Html, s: Shown, repo: String = "", code_line: String = ""
):
    """The pipeline page's Configuration (`id="config"`): what `orca config
    show` prints, where it was registered from, and the flags that
    overrode the file. Each key has its help, each value from a fixed set
    its meaning on hover, and From links to where the value is set where
    that's known: an orca.toml line at the registration's commit in `repo`
    (a repository's web address), or for a default from the pipeline's
    code, `code_line`, its declaration."""
    h.raw('<h2 id="config">Configuration</h2><p class="muted">')
    if s.file:
        h.raw("Registered from <code>")
        h.text(s.file)
        h.raw("</code>")
        if s.commit:
            h.raw(" @ <code>")
            h.text(s.commit[byte = : min(12, s.commit.byte_length())])
            h.raw("</code>")
            if s.dirty:
                h.raw(" (with changes not committed)")
            if s.source:
                h.text(" of ", s.source)
        h.raw(", sha256 <code>")
        h.text(s.hash[byte = : min(12, s.hash.byte_length())])
        h.raw("</code>.")
    else:
        h.raw(
            "Not registered from orca.toml: from <code>orca register</code>'s",
            " flags, each left out keeping what was registered.",
        )
    h.raw("</p>")
    if len(s.overrides) > 0:
        h.raw(
            '<p class="warn">Registered with flags over the file, gone at',
            " the next registration without them:",
        )
        for o in s.overrides:
            h.raw(" <code>")
            h.text(
                flag_of(o[0]) if o[0] != "secrets" else String("--secret"),
                " ",
                o[1],
            )
            h.raw("</code>")
        h.raw("</p>")
    # The file at its commit, where a line of it can be linked.
    var file_url = String()
    var web = web_url(s.source) if s.source else repo
    if web and s.repo_path and s.commit and is_hex(s.commit) and not s.dirty:
        if not s.repo_path.startswith("/") and ".." not in s.repo_path:
            file_url = String(web, "/blob/", s.commit, "/", s.repo_path)
    h.raw(
        '<div class="scroll"><table><thead><tr><th data-help="config">Key</th>',
        '<th>Value</th><th data-help="config">From</th></tr></thead><tbody>',
    )
    for i in range(len(s.rows)):
        ref r = s.rows[i]
        h.raw("<tr><td><code>")
        h.text(r.key)
        h.raw("</code>")
        var help = key_help(r.key)
        if help:
            h.help(String("key-", i), r.key, help)
        h.raw("</td><td><code>")
        var termed = False
        for k in materialize[_VALUE_TERMS]():
            termed = termed or r.key == k
        if termed:
            h.term(r.key, r.setting.value)
        else:
            h.text(r.setting.value)
        h.raw("</code></td><td>")
        h.term("from", r.setting.source)
        if r.setting.detail:
            h.text(" ", r.setting.detail)
        if r.setting.source == FROM_FILE and file_url and r.setting.line > 0:
            h.raw(" ")
            h.code(
                String(file_url, "#L", r.setting.line),
                String(s.repo_path, ":", r.setting.line),
                s.commit,
            )
        elif (
            r.setting.source == CODE_DEFAULT
            and code_line
            and (r.setting.detail.startswith("the pipeline's"))
        ):
            h.raw(" ")
            h.code(code_line, "the Pipeline[...] line", "")
        h.raw("</td></tr>")
    h.raw("</tbody></table></div>")


comptime _SHOWN_SQL = (
    "SELECT name, head_type, worker_type, ec2_workers, ec2_min_workers,"
    " worker_procs, placement, max_runs, coalesce(describe->>'max_runs', '1')"
    " AS code_max_runs, describe->>'workers' AS code_workers,"
    " jsonb_array_length(coalesce(describe->'tasks', '[]')) > 0 AS tasks,"
    " coalesce(secrets::text, '') AS secrets, coalesce(env::text, '') AS env,"
    " coalesce(config::text, '') AS config, coalesce(image, '') AS image,"
    " coalesce(spot, false) AS spot, coalesce(fills, '') AS fills,"
    " coalesce(fill_order, '') AS fill_order, coalesce(fill_max_runs, 0) AS"
    " fill_max_runs, coalesce(fill_budget_per_day, -1) AS fill_budget,"
    " coalesce(fill_window, '') AS fill_window, coalesce(cpus, '') AS cpus,"
    " coalesce(memory_gb, '') AS memory_gb FROM orca.pipelines WHERE"
    " binary_path IS"
    " NOT NULL AND ($1 = '' OR name = $1) ORDER BY name"
)


def _source(
    sources: Optional[Value], key: String
) raises -> Tuple[String, String]:
    """Where the registration says `key` came from; a value registered
    before orca kept that could only have come from a flag."""
    if sources:
        var s = sources.value().get(key)
        if s:
            return (
                s.value()["source"].string_value(),
                s.value()["detail"].string_value(),
            )
    return (String(FROM_FLAG), String(""))


def _line_of(sources: Optional[Value], key: String) raises -> Int:
    """The file's line the registration says `key` is set on, or 0."""
    if sources:
        var s = sources.value().get(key)
        if s:
            var l = s.value().get("line")
            if l:
                return Int(l.value().int_value())
    return 0


def _run_env_names() -> List[String]:
    var out = List[String]()
    for n in getenv("ORCA_RUN_ENV").split(","):
        if n.strip():
            out.append(String(n.strip()))
    return out^


def shown(mut db: State, only: String = "") raises -> List[Shown]:
    """The effective configuration of each registered pipeline (or the one
    named), with the source of every value. Values orca's runners take from
    their environment when nothing is registered (`ORCA_EC2_HEAD_TYPE`,
    `ORCA_EC2_WORKER_TYPE`, `ORCA_RUN_ENV`'s variables) are read from this
    process's, which is the scheduler's when run beside it."""
    var out = List[Shown]()
    for r in db.conn.query(_SHOWN_SQL, Params().text(only)):
        var s = Shown(r.text("name"))
        var sources = Optional[Value](None)
        if r.text("config"):
            var c = loads(r.text("config"))
            var f = c.get("file")
            if f and f.value().is_string():
                s.file = f.value().string_value()
                s.hash = c["hash"].string_value()
                s.commit = c["commit"].string_value()
                s.dirty = c["dirty"].bool_value()
                var so = c.get("source")
                if so and so.value().is_string():
                    s.source = so.value().string_value()
                var rp = c.get("repo_path")
                if rp and rp.value().is_string():
                    s.repo_path = rp.value().string_value()
                for kv in c["overrides"].object_items():
                    s.overrides.append((String(kv[0]), kv[1].string_value()))
            var src = c.get("sources")
            if src:
                sources = src.value().copy()
        # placement
        if r.is_null("placement"):
            s.add(
                "placement",
                "default",
                CODE_DEFAULT,
                String(
                    "the scheduler's runner, ", getenv("ORCA_RUNNER", "local")
                ),
            )
        else:
            var w = _source(sources, "placement")
            s.add("placement", r.text("placement"), w[0], w[1])
        for t in range(2):
            var key = String("head_type") if t == 0 else String("worker_type")
            var var_name = String("ORCA_EC2_HEAD_TYPE") if t == 0 else String(
                "ORCA_EC2_WORKER_TYPE"
            )
            if not r.is_null(key):
                var w = _source(sources, key)
                s.add(key, r.text(key), w[0], w[1])
            elif getenv(var_name):
                s.add(key, getenv(var_name), FROM_ENV, var_name)
            else:
                s.add(
                    key,
                    "c7g.large" if t == 0 else "c7g.xlarge",
                    CODE_DEFAULT,
                    "the ec2 runner's",
                )
        if not r.is_null("ec2_workers"):
            var w = _source(sources, "workers")
            var v = String(r.int64("ec2_workers"))
            if not r.is_null("ec2_min_workers"):
                v = String(r.int64("ec2_min_workers"), "..", v)
            var detail = w[1]
            if not r.bool("tasks"):
                detail.write(
                    "; " if detail else "", "unused: the pipeline has no tasks"
                )
            s.add("workers", v, w[0], detail)
        elif not r.bool("tasks"):
            s.add("workers", "0", CODE_DEFAULT, "the pipeline has no tasks")
        elif not r.is_null("code_workers"):
            s.add(
                "workers",
                r.text("code_workers"),
                CODE_DEFAULT,
                "the pipeline's code",
            )
        else:
            s.add("workers", "2", CODE_DEFAULT, "")
        if r.is_null("worker_procs"):
            s.add("worker_procs", "1", CODE_DEFAULT, "")
        else:
            var w = _source(sources, "worker_procs")
            var n = r.int64("worker_procs")
            s.add(
                "worker_procs",
                String("auto") if n == 0 else String(n),
                w[0],
                w[1],
            )
        if r.is_null("max_runs"):
            s.add(
                "max_runs",
                r.text("code_max_runs"),
                CODE_DEFAULT,
                "the pipeline's max_runs",
            )
        else:
            var w = _source(sources, "max_runs")
            s.add("max_runs", String(r.int64("max_runs")), w[0], w[1])
        # A request (orca.resources), only when it has one.
        for key in ["cpus", "memory_gb"]:
            if r.text(key):
                var w = _source(sources, key)
                s.add(key, r.text(key), w[0], w[1])
        _show_fills(s, r, sources)
        if r.text("image"):
            s.add("image", r.text("image"), FROM_FLAG, "--image")
        if r.bool("spot"):
            s.add("spot", "true", FROM_FLAG, "--spot")
        for kv in parse_secrets(r.text("secrets")):
            var key = String("secrets.", kv[0])
            var w = _source(sources, key)
            s.add(key, kv[1], w[0], w[1])
        var env = parse_env(r.text("env"))
        for kv in env:
            var key = String("env.", kv[0])
            var w = _source(sources, key)
            s.add(key, kv[1], w[0], w[1])
        for n in _run_env_names():
            var mine = False
            for kv in env:
                if kv[0] == n:
                    mine = True
            if not mine and getenv(n):
                s.add(String("env.", n), getenv(n), FROM_ENV, "ORCA_RUN_ENV")
        for ref row in s.rows:
            if row.setting.source == FROM_FILE:
                row.setting.line = _line_of(sources, row.key)
        out.append(s^)
    return out^


def _show_fills(mut s: Shown, r: Row, sources: Optional[Value]) raises:
    """The `fill*` keys (`orca.fill`): `fills` always, the others when the
    pipeline fills or sets them."""
    var fills = r.text("fills")
    if fills:
        var w = _source(sources, "fills")
        s.add("fills", fills, w[0], w[1])
    else:
        s.add("fills", SINCE_REGISTRATION, CODE_DEFAULT, "")
    var any = (
        fills == "all"
        or r.text("fill_order") != ""
        or r.int64("fill_max_runs") > 0
        or r.float64("fill_budget") >= 0
        or r.text("fill_window") != ""
    )
    if not any:
        return
    if r.text("fill_order"):
        var w = _source(sources, "fill_order")
        s.add("fill_order", r.text("fill_order"), w[0], w[1])
    else:
        s.add("fill_order", "oldest", CODE_DEFAULT, "")
    if r.int64("fill_max_runs") > 0:
        var w = _source(sources, "fill_max_runs")
        s.add("fill_max_runs", String(r.int64("fill_max_runs")), w[0], w[1])
    else:
        s.add("fill_max_runs", "1", CODE_DEFAULT, "")
    if r.float64("fill_budget") >= 0:
        var w = _source(sources, "fill_budget_per_day")
        s.add(
            "fill_budget_per_day", String(r.float64("fill_budget")), w[0], w[1]
        )
    else:
        s.add(
            "fill_budget_per_day",
            "none",
            CODE_DEFAULT,
            "fill runs on ec2 or kubernetes don't start without one",
        )
    if r.text("fill_window"):
        var w = _source(sources, "fill_window")
        s.add("fill_window", r.text("fill_window"), w[0], w[1])
    else:
        s.add("fill_window", "0..24", CODE_DEFAULT, "all day")


def show_json(items: List[Shown], one: Bool) -> String:
    """`orca config show --json`: the pipeline's object, or for all of them
    `{"pipelines": [...]}`."""
    if one and len(items) == 1:
        return items[0].json()
    var out = String('{"pipelines": [')
    for i in range(len(items)):
        out.write("" if i == 0 else ", ", items[i].json())
    out.write("]}")
    return out^


def _inline(pairs: List[Tuple[String, String]]) -> String:
    var out = String("{ ")
    for i in range(len(pairs)):
        out.write(
            "" if i == 0 else ", ",
            toml_key(pairs[i][0]),
            " = ",
            toml_string(pairs[i][1]),
        )
    out.write(" }")
    return out^


def export_toml(mut db: State) raises -> String:
    """`orca config export`: an `orca.toml` with a `[pipelines.<name>]` for
    each registered pipeline holding what's registered for it beyond the
    defaults, which registering from it again gives back."""
    var out = String(
        "# orca.toml, written by `orca config export` from the registrations\n",
        "# in orca's database. `orca register` reads it (orca.config).\n",
    )
    for r in db.conn.query(_SHOWN_SQL, Params().text("")):
        out.write("\n[pipelines.", toml_key(r.text("name")), "]\n")
        if not r.is_null("placement"):
            out.write("placement = ", toml_string(r.text("placement")), "\n")
        if not r.is_null("head_type"):
            out.write("head_type = ", toml_string(r.text("head_type")), "\n")
        if not r.is_null("worker_type"):
            out.write(
                "worker_type = ", toml_string(r.text("worker_type")), "\n"
            )
        if not r.is_null("ec2_workers"):
            if r.is_null("ec2_min_workers"):
                out.write("workers = ", r.int64("ec2_workers"), "\n")
            else:
                out.write(
                    'workers = "',
                    r.int64("ec2_min_workers"),
                    "..",
                    r.int64("ec2_workers"),
                    '"\n',
                )
        if not r.is_null("worker_procs"):
            var n = r.int64("worker_procs")
            if n == 0:
                out.write('worker_procs = "auto"\n')
            else:
                out.write("worker_procs = ", n, "\n")
        if not r.is_null("max_runs"):
            out.write("max_runs = ", r.int64("max_runs"), "\n")
        for key in ["cpus", "memory_gb"]:
            if r.text(key):
                out.write(key, " = ", toml_string(r.text(key)), "\n")
        if r.text("fills"):
            out.write("fills = ", toml_string(r.text("fills")), "\n")
        if r.text("fill_order"):
            out.write("fill_order = ", toml_string(r.text("fill_order")), "\n")
        if r.int64("fill_max_runs") > 0:
            out.write("fill_max_runs = ", r.int64("fill_max_runs"), "\n")
        if r.float64("fill_budget") >= 0:
            out.write(
                "fill_budget_per_day = ", String(r.float64("fill_budget")), "\n"
            )
        if r.text("fill_window"):
            out.write(
                "fill_window = ", toml_string(r.text("fill_window")), "\n"
            )
        var env = parse_env(r.text("env"))
        if len(env) > 0:
            out.write("env = ", _inline(env), "\n")
        var secrets = parse_secrets(r.text("secrets"))
        if len(secrets) > 0:
            out.write("secrets = ", _inline(secrets), "\n")
    return out^
