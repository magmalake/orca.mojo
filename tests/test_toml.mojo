"""The parser and writer of orca.toml (`orca.toml`): what it reads, what it
refuses, and that what it writes reads back."""

from std.testing import TestSuite, assert_equal, assert_false, assert_true

from orca.toml import (
    ARRAY,
    BOOLEAN,
    FLOAT,
    INTEGER,
    STRING,
    TABLE,
    TomlDoc,
    parse_toml,
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
        """# orca.toml
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
    _refused("[[a]]\n", "arrays of tables")
    _refused('a = """x"""\n', "multi-line strings")
    _refused("a = '''x'''\n", "multi-line strings")
    _refused("a = 1979-05-27\n", "dates and times")
    _refused("a = 07:32:00\n", "dates and times")
    _refused("a = 0xff\n", "hexadecimal")
    _refused("a = inf\n", "inf and nan")
    _refused("a = -nan\n", "inf and nan")
    _refused("a = [[1]]\n", "arrays of arrays")
    _refused("a = [{b = 1}]\n", "arrays of arrays")
    _refused('"a\\u0001" = 1\n', "control character")


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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
