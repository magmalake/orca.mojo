"""The parser and writer of morca.toml (`morca.toml`): what it reads, what it
refuses, and that what it writes reads back."""

from std.testing import TestSuite, assert_equal, assert_false, assert_true

from morca.toml import (
    ARRAY,
    BOOLEAN,
    FLOAT,
    INTEGER,
    STRING,
    TABLE,
    TomlDoc,
    parse_toml,
    path_key,
    toml_key,
    toml_string,
)


def _refused(text: String, why: String) raises:
    """`text` fails to parse, saying `why` (a part of the message)."""
    try:
        _ = parse_toml(text)
    except e:
        if why not in String(e):
            raise Error("refused ", repr(text), " but said: ", e)
        return
    raise Error("accepted: ", repr(text))


def _text(doc: TomlDoc, path: List[String]) raises -> String:
    var v = doc.get(path)
    if not v:
        raise Error("nothing at ", path[len(path) - 1])
    return v.value().text


def test_the_issue_example() raises:
    var doc = parse_toml(
        """# morca.toml
[defaults]
placement = "ec2"            # #144
head_type = "t4g.medium"

[pipelines.gharchive]
worker_type = "c7g.xlarge"
workers = "0..4"
worker_procs = 1
env = { DISTA_WORKER_SLOTS = "4", GHARCHIVE_PARALLEL = "16" }
max_runs = 2                 # overrides the code's default

[pipelines.mojo_repos]
placement = "control"
secrets = { GITHUB_TOKEN = "orca/github-token" }

[stack]
hostname = "orca.mlake.dev"
public_data = false
"""
    )
    assert_equal(doc.keys([]), ["defaults", "pipelines", "stack"])
    assert_equal(doc.tables(["pipelines"]), ["gharchive", "mojo_repos"])
    assert_equal(_text(doc, ["defaults", "placement"]), "ec2")
    var w = doc.get(["pipelines", "gharchive", "worker_procs"]).value().copy()
    assert_equal(w.kind, INTEGER)
    assert_equal(w.as_int(), 1)
    assert_equal(w.line, 9)
    assert_equal(
        doc.keys(["pipelines", "gharchive", "env"]),
        ["DISTA_WORKER_SLOTS", "GHARCHIVE_PARALLEL"],
    )
    assert_equal(
        _text(doc, ["pipelines", "gharchive", "env", "GHARCHIVE_PARALLEL"]),
        "16",
    )
    assert_true(doc.has_table(["pipelines", "mojo_repos", "secrets"]))
    assert_equal(doc.get(["stack", "public_data"]).value().kind, BOOLEAN)
    assert_false(doc.get(["pipelines", "nope"]))


def test_strings_and_keys() raises:
    var doc = parse_toml(
        'a = "tab\\there \\"q\\" \\\\ \\u00e9 \\U0001F600"\n'
        "b = 'C:\\path\\x'\n"
        '"quoted key" = 1\n'
        "'lit.key' = 2\n"
        "c . d = 3\n"
        'e = ""\n'
    )
    assert_equal(_text(doc, ["a"]), 'tab\there "q" \\ é 😀')
    assert_equal(_text(doc, ["b"]), "C:\\path\\x")
    assert_equal(_text(doc, ["quoted key"]), "1")
    assert_equal(_text(doc, ["lit.key"]), "2")
    assert_true(doc.has_table(["c"]))
    assert_equal(_text(doc, ["c", "d"]), "3")
    assert_equal(_text(doc, ["e"]), "")


def test_numbers_arrays_and_tables() raises:
    var doc = parse_toml(
        "i = -1_000\nj = +7\nz = 0\nf = 3.5e-2\ng = -0.0\n"
        "a = [\n  1, # one\n  2,\n]\ns = ['x', \"y\"]\nn = []\n"
        "t = {}\nu = { a.b = 1, c = { d = true } }\n"
        "[x.y]\n[x]\nk = 1\n"
    )
    assert_equal(_text(doc, ["i"]), "-1000")
    assert_equal(doc.get(["i"]).value().as_int(), -1000)
    assert_equal(_text(doc, ["j"]), "+7")
    assert_equal(doc.get(["f"]).value().kind, FLOAT)
    var a = doc.get(["a"]).value().copy()
    assert_equal(a.kind, ARRAY)
    assert_equal(a.items, ["1", "2"])
    assert_equal(doc.get(["s"]).value().item_kinds, [STRING, STRING])
    assert_equal(len(doc.get(["n"]).value().items), 0)
    assert_equal(doc.get(["t"]).value().kind, TABLE)
    assert_equal(_text(doc, ["u", "a", "b"]), "1")
    assert_equal(_text(doc, ["u", "c", "d"]), "true")
    assert_equal(_text(doc, ["x", "k"]), "1")
    assert_true(doc.has_table(["x", "y"]))
    # CRLF, a byte order mark and a file without a final newline.
    var bom: List[UInt8] = [0xEF, 0xBB, 0xBF]
    var text = String(unsafe_from_utf8=Span(bom))
    text.write("a = 1\r\n[b]\r\nc = 'd'")
    var crlf = parse_toml(text)
    assert_equal(_text(crlf, ["b", "c"]), "d")


def test_what_toml_refuses() raises:
    _refused("a = 1\na = 2\n", "defined twice")
    _refused("[a]\n[a]\n", "defined twice")
    _refused("a = 1\n[a]\n", "is a value")
    _refused("a = 1\na.b = 2\n", "is a value")
    _refused("[a.b]\nc = 1\n[a]\nb.d = 2\n", "a dotted key can't add")
    _refused("t = {a = 1}\n[t]\n", "defined twice")
    _refused("t = {a = 1}\nt.b = 2\n", "can't add")
    _refused("t = {a = 1,}\n", "can't end with a comma")
    _refused("a = 1 b = 2\n", "end of the line")
    _refused("a = \n", "expected a value")
    _refused("= 1\n", "expected a key")
    _refused("a = 01\n", "leading zero")
    _refused("a = 1__0\n", "between digits")
    _refused("a = 1.\n", "after its point")
    _refused('a = "x\n', "closing quote")
    _refused("a = 'x\n", "closing quote")
    _refused('a = "\\q"\n', "unknown escape")
    _refused('a = "\\uD800"\n', "Unicode scalar")
    _refused('a = "\x01"\n', "control character")
    _refused("a = [1, 2\n", "closing")
    _refused("a = [1 2]\n", "expected ','")
    _refused("a = 1\rb = 2\n", "carriage return")
    _refused("a = 1234567890123456789\n", "too large")
    _refused("a = TRUE\n", "expected a value")


def test_the_subset_says_what_it_leaves_out() raises:
    _refused('a = """x"""\n', "multi-line strings")
    _refused("a = '''x'''\n", "multi-line strings")
    _refused("a = 1979-05-27\n", "dates and times")
    _refused("a = 07:32:00\n", "dates and times")
    _refused("a = 0xff\n", "hexadecimal")
    _refused("a = inf\n", "inf and nan")
    _refused("a = -nan\n", "inf and nan")
    _refused("a = [[1]]\n", "arrays of arrays")
    _refused("a = [1, {b = 1}]\n", "mix inline tables")
    _refused("a = [{b = 1}, 2]\n", "mix inline tables")
    _refused('"a\\u0001" = 1\n', "control character")


def test_arrays_of_inline_tables() raises:
    """`[{...}, ...]`: an `ARRAY` of indices, each element a table under
    its index (morca #267's `machines`)."""
    var doc = parse_toml(
        String(
            "[pools.lab]\n",
            "machines = [\n",
            '  { type = "cax31", wait = 600 },  # first\n',
            '  { type = "cpx32", where = { zone = "a" } },\n',
            "]\n",
            'locations = ["hel1", "nbg1"]\n',
            "empty = []\n",
        )
    )
    var m = doc.get(["pools", "lab", "machines"]).value().copy()
    assert_equal(m.kind, ARRAY)
    assert_equal(m.items, ["0", "1"])
    assert_equal(m.item_kinds, [TABLE, TABLE])
    assert_true(doc.is_table_array(m))
    assert_equal(_text(doc, ["pools", "lab", "machines", "0", "type"]), "cax31")
    assert_equal(_text(doc, ["pools", "lab", "machines", "0", "wait"]), "600")
    assert_equal(
        _text(doc, ["pools", "lab", "machines", "1", "where", "zone"]), "a"
    )
    assert_false(Bool(doc.get(["pools", "lab", "machines", "1", "wait"])))
    assert_equal(doc.keys(["pools", "lab", "machines"]), ["0", "1"])
    assert_false(doc.is_table_array(doc.get(["pools", "lab", "empty"]).value()))
    # It reads back from its dump.
    var again = parse_toml(doc.dump())
    assert_equal(len(again.entries), len(doc.entries))
    for e in doc.entries:
        var v = again.get(e.path)
        assert_true(Bool(v), String("lost ", e.path[len(e.path) - 1]))
        assert_equal(v.value().text, e.value.text)
        assert_equal(v.value().items, e.value.items)
    _refused("a = [{b = 1}, {b = 1, b = 2}]\n", "defined twice")
    _refused("a = [{b = 1}\n", "closing")
    _refused("a = [{b = 1}]\na.c = 2\n", "is a value")


def test_arrays_of_table_headers() raises:
    """`[[x]]` headers, and `[[x.y]]` adding to the last `[[x]]`."""
    var doc = parse_toml(
        String(
            "top = 1\n",
            "[[version]]\n",
            'version = "1.1.0"\n',
            "[[version.step]]\n",
            'id = "a"\n',
            "[[version.step]]\n",
            'id = "b"\n',
            "[[version]]\n",
            'version = "1.2.0"\n',
            "[[version.step]]\n",
            'id = "c"\n',
            "[after]\n",
            "x = 2\n",
        )
    )
    var v = doc.get(["version"])
    assert_true(v.value().kind == "array")
    assert_equal(len(v.value().items), 2)
    assert_equal(doc.get(["version", "0", "version"]).value().text, "1.1.0")
    assert_equal(len(doc.get(["version", "0", "step"]).value().items), 2)
    assert_equal(doc.get(["version", "0", "step", "1", "id"]).value().text, "b")
    assert_equal(doc.get(["version", "1", "step", "0", "id"]).value().text, "c")
    assert_equal(doc.get(["after", "x"]).value().text, "2")
    _refused("a = 1\n[[a]]\n", "defined already")
    _refused("[[a]\n", "expected ']]'")


def test_errors_name_the_line() raises:
    try:
        _ = parse_toml("a = 1\n\n# c\nb = ?\n")
    except e:
        assert_true(String(e).startswith("4: "))
        return
    raise Error("accepted a bad value")


def test_dump_reads_back() raises:
    var text = String(
        "[defaults]\nplacement = 'ec2'\n[pipelines.\"odd name\"]\n",
        'env = { A = "x\\ny", "B.C" = "\\u0007" }\nworkers = 4\n',
        "a = [1, 'two', true]\n[empty]\n",
    )
    var doc = parse_toml(text)
    var again = parse_toml(doc.dump())
    assert_equal(len(again.entries), len(doc.entries))
    for e in doc.entries:
        var v = again.get(e.path)
        assert_true(Bool(v), String("lost ", e.path[len(e.path) - 1]))
        assert_equal(v.value().kind, e.value.kind)
        assert_equal(v.value().text, e.value.text)
        assert_equal(v.value().items, e.value.items)
    assert_equal(toml_key("plain-key_1"), "plain-key_1")
    assert_equal(toml_key("a.b"), '"a.b"')
    assert_equal(toml_key(""), '""')
    assert_equal(toml_string('a"b\\c\x7f'), '"a\\"b\\\\c\\u007F"')


def test_an_overlay_merges() raises:
    # A stack's overlay (TomlDoc.merge): a value replaces the one at its
    # path, a table is merged key by key, an array is replaced whole.
    var doc = parse_toml(
        """[defaults]
max_runs = 1
[pipelines.a]
fills = "all"
env = { X = "1", Y = "2" }
machines = [{ type = "cax31" }, { type = "cax41" }]
[pipelines.b]
limit = 3
"""
    )
    var over = parse_toml(
        """[pipelines.a]
fills = "since_registration"
env = { Y = "3", Z = "4" }
machines = [{ type = "cx22" }]
[pipelines.b]
limit = { soft = 1 }
[pipelines.c]
new = true
"""
    )
    var set = doc.merge(over)
    assert_equal(_text(doc, ["defaults", "max_runs"]), "1")
    assert_equal(_text(doc, ["pipelines", "a", "fills"]), "since_registration")
    assert_equal(_text(doc, ["pipelines", "a", "env", "X"]), "1")
    assert_equal(_text(doc, ["pipelines", "a", "env", "Y"]), "3")
    assert_equal(_text(doc, ["pipelines", "a", "env", "Z"]), "4")
    assert_equal(len(doc.get(["pipelines", "a", "machines"]).value().items), 1)
    assert_equal(
        _text(doc, ["pipelines", "a", "machines", "0", "type"]), "cx22"
    )
    assert_false(doc.get(["pipelines", "a", "machines", "1"]))
    assert_false(doc.get(["pipelines", "a", "machines", "1", "type"]))
    # A value becomes a table, with what's under it.
    assert_true(doc.has_table(["pipelines", "b", "limit"]))
    assert_equal(_text(doc, ["pipelines", "b", "limit", "soft"]), "1")
    assert_true(doc.get(["pipelines", "c", "new"]))
    # The order the file had is kept; a key new here goes last.
    assert_equal(doc.keys(["pipelines", "a"]), ["fills", "env", "machines"])
    assert_equal(doc.tables(["pipelines"]), ["a", "b", "c"])
    # What it set: values and new tables, not the tables it merged into.
    var keys = List[String]()
    for p in set:
        keys.append(path_key(p))
    assert_true(path_key(["pipelines", "a", "fills"]) in keys)
    assert_true(path_key(["pipelines", "a", "env", "Z"]) in keys)
    assert_true(path_key(["pipelines", "c"]) in keys)
    assert_false(path_key(["pipelines", "a"]) in keys)
    assert_false(path_key(["pipelines", "a", "env", "X"]) in keys)
    # pretty reads back to the same values, with the notes as comments.
    var notes = Dict[String, String]()
    notes[path_key(["pipelines", "a", "env", "Z"])] = "overlay"
    var text = doc.pretty(notes)
    assert_true('env = { X = "1", Y = "3", Z = "4" }  # overlay\n' in text)
    assert_true("[pipelines.a]\n" in text)
    assert_true("[defaults]\nmax_runs = 1\n" in text)
    var back = parse_toml(text)
    assert_equal(_text(back, ["pipelines", "a", "env", "Z"]), "4")
    assert_equal(
        _text(back, ["pipelines", "a", "machines", "0", "type"]), "cx22"
    )
    assert_equal(_text(back, ["pipelines", "b", "limit", "soft"]), "1")
    assert_equal(len(back.entries), len(doc.entries))
    # An empty table keeps its header.
    var e = parse_toml("[pipelines.bare]\n")
    assert_equal(e.pretty(Dict[String, String]()), "[pipelines.bare]\n")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
