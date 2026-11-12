"""Where a binary's code came from (`morca.source`): the links to it. The
defines themselves are checked by tests/versions.sh, which builds with and
without them."""

from std.testing import TestSuite, assert_equal

from morca.source import commit_url, web_url

comptime SHA = "0123456789abcdef0123456789abcdef01234567"


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
