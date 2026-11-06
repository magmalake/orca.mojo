"""What `orca register` reads from orca.toml (`orca.config`): the keys it
checks, the order values win in, and what a registration records."""

from std.testing import TestSuite, assert_equal, assert_false, assert_true

from json import loads

from orca.config import (
    FROM_FILE,
    FROM_FLAG,
    ProjectConfig,
    RegisterFlags,
    canonical_flag,
    env_name_problem,
    parse_workers,
    resolve,
)

comptime EXAMPLE = """[defaults]
placement = "ec2"
head_type = "t4g.medium"
env = { SHARED = "d", ONLY_DEFAULT = "x" }

[pipelines.gharchive]
worker_type = "c7g.xlarge"
workers = "0..4"
worker_procs = 1
env = { DISTA_WORKER_SLOTS = "4", SHARED = "p", N = 16 }
max_runs = 2

[pipelines.mojo_repos]
placement = "control"
secrets = { GITHUB_TOKEN = "orca/github-token" }

[pipelines.bare]

[stack]
hostname = "orca.mlake.dev"
"""


def _refused(text: String, why: String) raises:
    try:
        _ = ProjectConfig(text, "orca.toml")
    except e:
        if why not in String(e):
            raise Error("refused ", repr(text), " but said: ", e)
        return
    raise Error("accepted: ", repr(text))


def test_the_example_reads() raises:
    var f = ProjectConfig(EXAMPLE, "orca.toml", "abc123", True)
    assert_equal(f.pipelines(), ["gharchive", "mojo_repos", "bare"])
    assert_true(f.has("bare"))
    assert_false(f.has("census"))
    assert_equal(f.hash.byte_length(), 64)
    assert_equal(f.commit, "abc123")


def test_pipeline_beats_defaults() raises:
    var f = ProjectConfig(EXAMPLE, "orca.toml")
    var r = resolve("gharchive", RegisterFlags(), f)
    assert_true(r.in_file)
    assert_equal(r.text("placement"), "ec2")
    assert_equal(r.values["placement"].detail, "[defaults]")
    assert_equal(r.text("head_type"), "t4g.medium")
    assert_equal(r.text("worker_type"), "c7g.xlarge")
    assert_equal(r.values["worker_type"].source, FROM_FILE)
    assert_equal(r.values["worker_type"].detail, "[pipelines.gharchive]")
    assert_equal(r.text("workers"), "0..4")
    assert_equal(r.text("worker_procs"), "1")
    assert_equal(r.text("max_runs"), "2")
    # env merges name by name, the pipeline's winning; a number is text.
    var env = r.env_pairs()
    assert_equal(len(env), 4)
    for kv in env:
        if kv[0] == "SHARED":
            assert_equal(kv[1], "p")
        if kv[0] == "N":
            assert_equal(kv[1], "16")
    var m = resolve("mojo_repos", RegisterFlags(), f)
    assert_equal(m.text("placement"), "control")
    assert_equal(m.secret_specs(), ["GITHUB_TOKEN=orca/github-token"])
    # [defaults] applies to a pipeline the file names, even with nothing
    # of its own, and to no other.
    var b = resolve("bare", RegisterFlags(), f)
    assert_equal(b.text("head_type"), "t4g.medium")
    var other = resolve("census", RegisterFlags(), f)
    assert_false(other.in_file)
    assert_equal(other.text("head_type"), "")
    assert_equal(len(other.env), 0)


def test_flags_beat_the_file_and_are_recorded() raises:
    var f = ProjectConfig(EXAMPLE, "orca.toml", "abc123")
    var flags = RegisterFlags()
    flags.values["workers"] = canonical_flag("workers", "2..8")
    flags.secrets.append("OTHER=orca/other")
    var r = resolve("gharchive", flags, f)
    assert_equal(r.text("workers"), "2..8")
    assert_equal(r.values["workers"].source, FROM_FLAG)
    assert_equal(r.values["workers"].detail, "--workers")
    assert_equal(r.secret_specs(), ["OTHER=orca/other"])
    var c = loads(r.config_json(f))
    assert_equal(c["file"].string_value(), "orca.toml")
    assert_equal(c["commit"].string_value(), "abc123")
    assert_equal(c["hash"].string_value(), f.hash)
    assert_equal(c["overrides"]["workers"].string_value(), "2..8")
    assert_equal(c["overrides"]["secrets"].string_value(), "OTHER=orca/other")
    assert_equal(c["sources"]["workers"]["source"].string_value(), FROM_FLAG)
    assert_equal(
        c["sources"]["env.SHARED"]["detail"].string_value(),
        "[pipelines.gharchive]",
    )
    assert_equal(
        c["sources"]["env.ONLY_DEFAULT"]["detail"].string_value(),
        "[defaults]",
    )
    # A pipeline the file doesn't name: flags only, no overrides, no file.
    var o = loads(resolve("census", flags, f).config_json(f))
    assert_false(o.get("file"))
    assert_equal(o["sources"]["workers"]["source"].string_value(), FROM_FLAG)


def test_values_are_checked() raises:
    _refused('[pipelines.a]\nworkers = "4..2"\n', "MIN..MAX")
    _refused("[pipelines.a]\nworkers = 0\n", "MIN..MAX")
    _refused("[pipelines.a]\nworkers = true\n", "a count or a string")
    _refused("[pipelines.a]\nworker_procs = 0\n", "1 to 256 or auto")
    _refused('[pipelines.a]\nworker_procs = "some"\n', "1 to 256 or auto")
    _refused('[pipelines.a]\nmax_runs = "2"\n', "is a count")
    _refused("[pipelines.a]\nmax_runs = 0\n", "from 1")
    _refused('[pipelines.a]\nplacement = "moon"\n', "control, ec2 or default")
    _refused('[pipelines.a]\nhead_type = "big"\n', "not an instance type")
    _refused("[pipelines.a]\nhead_type = 3\n", "is a string")
    _refused('[pipelines.a]\nsecrets = "x"\n', "is a table")
    _refused('[pipelines.a]\nsecrets = { T = "prod/t" }\n', "under orca/")
    _refused('[pipelines.a]\nsecrets = { t = "orca/t" }\n', "A-Z")
    _refused('[pipelines.a]\nenv = { GITHUB_TOKEN = "v" }\n', "secret")
    _refused('[pipelines.a]\nenv = { DISTA_ROLE = "x" }\n', "by orca")
    _refused("[pipelines.a]\nenv = { X = [1] }\n", "a string, a number")
    _refused("[pipelines.a]\nenv = { X = { Y = 1 } }\n", "a string, a number")


def test_keys_and_tables_are_checked() raises:
    _refused("[pipelines.a]\nworkrs = 4\n", "has no key workrs")
    _refused('[defaults]\nfills = ["x"]\n', "has no key fills")
    _refused("[pipeline.a]\n", "isn't a table orca knows")
    _refused("x = 1\n", "is a key at the top")
    _refused("[pipelines]\na = 1\n", "isn't a table")
    _refused("[pipelines.a]\nworkers = 4\nworkers = 5\n", "defined twice")
    # A message names the file and the line.
    try:
        _ = ProjectConfig("\n[pipelines.a]\n\nworkers = 0\n", "p/orca.toml")
    except e:
        assert_true(
            String(e).startswith("p/orca.toml:4: [pipelines.a] workers:"),
            String(e),
        )


def test_what_flags_and_names_take() raises:
    assert_equal(parse_workers("3")[0], -1)
    assert_equal(parse_workers("0..4")[1], 4)
    assert_equal(canonical_flag("worker_procs", "auto"), "auto")
    assert_equal(canonical_flag("workers", "5"), "5")
    assert_equal(env_name_problem("DISTA_WORKER_SLOTS"), "")
    assert_equal(env_name_problem("lower_ok"), "")
    assert_true(env_name_problem("1X") != "")
    assert_true(env_name_problem("A-B") != "")
    assert_true(env_name_problem("") != "")
    assert_true(env_name_problem("db_password") != "")


def test_no_file() raises:
    var none = ProjectConfig()
    assert_false(none.is_file())
    assert_false(resolve("x", RegisterFlags(), none).in_file)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
