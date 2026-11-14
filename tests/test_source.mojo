"""Where a binary's code came from (`morca._internal.source`): the links to
it, and the defines, which pixi.toml's test-source builds this with (repo
and commit; no MORCA_DIRTY, and no MORCA_TEST_SOURCE_ENV, as a production
build). tests/versions.sh checks a binary without them, which reads its
environment."""

from std.os import setenv, unsetenv
from std.testing import TestSuite, assert_equal, assert_false

from morca._internal.source import (
    commit_url,
    source_commit,
    source_dirty,
    source_repo,
    web_url,
)

comptime SHA = "0123456789abcdef0123456789abcdef01234567"
# pixi.toml's -D values.
comptime DEFINED_REPO = "https://example.com/lake.git"
comptime DEFINED_COMMIT = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a"


def test_defines_win_over_the_environment() raises:
    assert_equal(source_repo(), DEFINED_REPO)
    assert_equal(source_commit(), DEFINED_COMMIT)
    _ = setenv("MORCA_SOURCE", "elsewhere", True)
    _ = setenv("MORCA_COMMIT", SHA, True)
    assert_equal(source_repo(), DEFINED_REPO)
    assert_equal(source_commit(), DEFINED_COMMIT)
    _ = unsetenv("MORCA_SOURCE")
    _ = unsetenv("MORCA_COMMIT")


def test_dirty_only_from_the_define() raises:
    # Only scripts/pipelines.sh's test binary reads MORCA_DIRTY from the
    # environment; any other build, this one, reads the define alone.
    _ = setenv("MORCA_DIRTY", "1", True)
    assert_false(source_dirty())
    _ = unsetenv("MORCA_DIRTY")


def test_web_url_forms() raises:
    comptime want = "https://github.com/morca-dev/morca"
    assert_equal(web_url("https://github.com/morca-dev/morca"), want)
    assert_equal(web_url("https://github.com/morca-dev/morca.git"), want)
    assert_equal(web_url("https://github.com/morca-dev/morca/"), want)
    assert_equal(web_url("git@github.com:morca-dev/morca.git"), want)
    assert_equal(web_url("ssh://git@github.com/morca-dev/morca"), want)
    assert_equal(web_url(" https://github.com/morca-dev/morca\n"), want)


def test_web_url_refuses_others() raises:
    assert_equal(web_url(""), "")
    assert_equal(web_url("/home/me/lake"), "")
    assert_equal(web_url("git@nowhere"), "")
    assert_equal(web_url("javascript:alert(1)"), "")


def test_commit_url() raises:
    assert_equal(
        commit_url("git@github.com:me/lake.git", SHA),
        String("https://github.com/me/lake/tree/", SHA),
    )
    assert_equal(commit_url("", SHA), "")
    assert_equal(commit_url("https://github.com/me/lake", ""), "")
    # Only a hex commit goes into a URL.
    assert_equal(commit_url("https://github.com/me/lake", "main"), "")
    assert_equal(commit_url("https://github.com/me/lake", "ab/../x"), "")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
