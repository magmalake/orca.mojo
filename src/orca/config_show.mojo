"""`orca config show` and `orca config export` (`orca.config`): each
pipeline's settings with where each came from, as text, HTML and JSON, and
the registered settings written back as an orca.toml.
"""

from std.os import getenv
from json import (
    Value,
    loads,
)
from postgres import (
    Params,
    Row,
)
from orca.fill import SINCE_REGISTRATION
from orca.html import Html
from orca.json import quote
from orca.recommend import (
    Recommendation,
    learned,
)
from orca.resources import Request
from orca.secrets import parse_secrets
from orca.source import (
    is_hex,
    web_url,
)
from orca.state import State
from orca.ui.terms import key_help
from orca.toml import (
    toml_key,
    toml_string,
)
from orca.config import (
    CODE_DEFAULT,
    FROM_ENV,
    FROM_FILE,
    FROM_FLAG,
    Setting,
    flag_of,
    max_runs_auto,
    max_runs_text,
    parse_env,
)


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
    var learned: Optional[Recommendation]
    """The request its runs' peaks suggest (`orca.recommend`), applied when
    it asks for `auto` and learns."""
    var auto: Bool
    """Whether its `cpus` or `memory_gb` is `auto`."""
    var learn: Bool
    """Whether it learns (`learn`, true unless set false)."""

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
        self.learned = None
        self.auto = False
        self.learn = True

    def add(
        mut self, key: String, value: String, source: String, detail: String
    ):
        self.rows.append(_Row(key, Setting(value, source, detail)))

    def applied_text(self) -> String:
        """Whether its runs start with what it learned, and why not."""
        if self.learned and self.learned.value().applied:
            return "applied: its request is auto"
        if self.auto and not self.learn:
            return "not applied: learn = false"
        return "shown, not applied: its request isn't auto"

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
        out.write('], "learned": ')
        if self.learned:
            out.write(self.learned.value().json())
        else:
            out.write("null")
        out.write("}")
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
        if self.learned and self.learned.value().runs > 0:
            ref l = self.learned.value()
            out.write("  ", l.text())
            if l.any():
                out.write(" (", l.source, "); ", self.applied_text())
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
    " worker_procs, placement, max_runs, coalesce(max_runs_auto, false) AS"
    " max_runs_auto, coalesce(describe->>'max_runs', '1') AS code_max_runs,"
    " keys_per_run, coalesce(describe->>'keys_per_run', '1') AS"
    " code_keys_per_run,"
    " describe->>'workers' AS code_workers,"
    " jsonb_array_length(coalesce(describe->'tasks', '[]')) > 0 AS tasks,"
    " coalesce(secrets::text, '') AS secrets, coalesce(env::text, '') AS env,"
    " coalesce(config::text, '') AS config, coalesce(image, '') AS image,"
    " coalesce(spot, false) AS spot, coalesce(fills, '') AS fills,"
    " coalesce(fill_order, '') AS fill_order, coalesce(fill_max_runs, 0) AS"
    " fill_max_runs, coalesce(fill_budget_per_day, -1) AS fill_budget,"
    " coalesce(fill_window, '') AS fill_window, coalesce(fill_lag, '') AS"
    " fill_lag, coalesce(cpus, '') AS cpus, coalesce(memory_gb, '') AS"
    " memory_gb, learn FROM orca.pipelines WHERE binary_path IS NOT NULL AND"
    " ($1 = '' OR name = $1) ORDER BY name"
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
            s.add(
                "max_runs",
                max_runs_text(
                    Int(r.int64("max_runs")), r.bool("max_runs_auto")
                ),
                w[0],
                w[1],
            )
        if r.is_null("keys_per_run"):
            s.add(
                "keys_per_run",
                r.text("code_keys_per_run"),
                CODE_DEFAULT,
                "the pipeline's keys_per_run",
            )
        else:
            var w = _source(sources, "keys_per_run")
            s.add("keys_per_run", String(r.int64("keys_per_run")), w[0], w[1])
        # A request (orca.resources), only when it has one.
        for key in ["cpus", "memory_gb"]:
            if r.text(key):
                var w = _source(sources, key)
                s.add(key, r.text(key), w[0], w[1])
        var learn = r.is_null("learn") or r.bool("learn")
        if not learn:
            var w = _source(sources, "learn")
            s.add("learn", "false", w[0], w[1])
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
        var l = learned(db, s.name, r.text("cpus"), r.text("memory_gb"))
        s.auto = Request.of(r.text("cpus"), r.text("memory_gb")).auto()
        s.learn = learn
        l.applied = s.auto and learn and l.any()
        s.learned = l^
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
        or r.text("fill_lag") != ""
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
    if r.text("fill_lag"):
        var w = _source(sources, "fill_lag")
        s.add("fill_lag", r.text("fill_lag"), w[0], w[1])
    else:
        s.add("fill_lag", "0", CODE_DEFAULT, "a partition fills once it ends")


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
            if r.bool("max_runs_auto"):
                var t = max_runs_text(Int(r.int64("max_runs")), True)
                out.write("max_runs = ", toml_string(t), "\n")
            else:
                out.write("max_runs = ", r.int64("max_runs"), "\n")
        if not r.is_null("keys_per_run"):
            out.write("keys_per_run = ", r.int64("keys_per_run"), "\n")
        for key in ["cpus", "memory_gb"]:
            if r.text(key):
                out.write(key, " = ", toml_string(r.text(key)), "\n")
        if not r.is_null("learn") and not r.bool("learn"):
            out.write("learn = false\n")
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
        if r.text("fill_lag"):
            out.write("fill_lag = ", toml_string(r.text("fill_lag")), "\n")
        var env = parse_env(r.text("env"))
        if len(env) > 0:
            out.write("env = ", _inline(env), "\n")
        var secrets = parse_secrets(r.text("secrets"))
        if len(secrets) > 0:
            out.write("secrets = ", _inline(secrets), "\n")
    return out^
