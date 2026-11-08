"""The step cards' data (`orca.step_cards`): type names as code writes them,
the recorded signature, the signature and docstring found in a file, and
the page's JSON, which nothing in it can end."""

from std.testing import TestSuite, assert_equal, assert_true, assert_false

from orca.step_cards import (
    AttemptCard,
    StepCard,
    cards_script,
    def_json,
    def_summary,
    duration_text,
    first_line,
    function_name,
    short_type,
    signature,
)


def test_short_type() raises:
    assert_equal(short_type("orca.month.Month"), "Month")
    assert_equal(short_type("dista.seq.Seq[toy.Measured]"), "Seq[Measured]")
    assert_equal(short_type("SIMD[DType.int, 1]"), "Int")
    assert_equal(short_type("SIMD[DType.uint8, 1]"), "UInt8")
    assert_equal(short_type("SIMD[DType.float64, 4]"), "SIMD[DType.float64, 4]")
    assert_equal(short_type("orca::ctx::Ctx"), "Ctx")
    assert_equal(short_type("Tuple[a.B, String]"), "Tuple[B, String]")
    assert_equal(short_type(""), "")


def test_signature() raises:
    assert_equal(
        function_name("toy::days(orca::ctx::Ctx,orca::month::Month)"), "days"
    )
    assert_equal(function_name("s"), "s")
    assert_equal(
        signature(
            "toy::total(orca::ctx::Ctx,dista::seq::Seq[toy::Measured])",
            "dista.seq.Seq[toy.Measured]",
            "SIMD[DType.int, 1]",
        ),
        "def total(mut ctx: Ctx, input: Seq[Measured]) raises -> Int",
    )


comptime _SRC = '''from orca import Ctx


@always_inline
def one(mut ctx: Ctx, m: Month) raises -> Int:
    """Counts the month's days.

    More about it, not shown."""
    return 1


def two(
    mut ctx: Ctx,
    loads: Seq[Loaded],  # each month's
) raises -> Compacted:
    """Rewrites each month
    as one file."""
    pass


def three(mut ctx: Ctx, m: Month) raises -> Int:
    return 3


def four(mut ctx: Ctx, m: Month) raises -> Int:
    """One line."""
    return 4
'''


def test_def_summary() raises:
    var d = def_summary(_SRC, 5)
    assert_equal(d.start, 5)
    assert_equal(d.end, 5)
    assert_equal(d.doc, "Counts the month's days.")
    d = def_summary(_SRC, 12)
    assert_equal(d.start, 12)
    assert_equal(d.end, 15)
    assert_equal(d.doc, "Rewrites each month as one file.")
    d = def_summary(_SRC, 21)
    assert_equal(d.end, 21)
    assert_equal(d.doc, "")
    assert_equal(def_summary(_SRC, 25).doc, "One line.")
    # Not a def, or past the end: nothing.
    assert_equal(def_summary(_SRC, 1).start, 0)
    assert_equal(def_summary(_SRC, 4).start, 0)
    assert_equal(def_summary(_SRC, 999).start, 0)
    assert_equal(def_summary(_SRC, 0).start, 0)


def test_def_json() raises:
    var j = def_json(_SRC, 12)
    assert_true(
        j.startswith(
            '{"line": 12, "sig": ["<span class=\\"hl hl--kw\\">def</span>'
        ),
        j,
    )
    assert_true('"doc": "Rewrites each month as one file."}' in j, j)
    assert_equal(def_json(_SRC, 1), '{"line": 1, "sig": [], "doc": ""}')


def test_text() raises:
    assert_equal(first_line("  boom\nat line 2\n"), "boom")
    assert_equal(first_line("one"), "one")
    assert_equal(duration_text(-1), "")
    assert_equal(duration_text(0.2), "<1s")
    assert_equal(duration_text(42), "42s")
    assert_equal(duration_text(187), "3m 07s")
    assert_equal(duration_text(7500), "2h 05m")


def test_cards_script() raises:
    var steps: List[StepCard] = [
        StepCard(
            "days",
            "mapped",
            "Runs once per partition.",
            "",
            "toy::days(orca::ctx::Ctx,orca::month::Month)",
            "orca.month.Month",
            "toy.Measured",
            ["toy.in"],
            ["toy.out</script>"],
            "https://github.com/o/r/blob/abcdef1234/toy.mojo#L5",
            "toy.mojo:5",
            "abcdef1234",
            3,
            4,
            1,
            "p50 2s · max 5s",
            "",
        )
    ]
    var attempts: List[AttemptCard] = [
        AttemptCard(
            7,
            "days",
            "2024-02",
            2,
            "failed",
            "",
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:00:03Z",
            "3s",
            "12 MB",
            "",
            "",
            "<b>boom</b>\nmore",
            "/runs/2/attempts/7",
        )
    ]
    var s = cards_script(steps, attempts)
    assert_true(
        s.startswith(
            '<script type="application/json" class="step-cards">{"steps":'
            ' {"days": {"kind": "mapped"'
        ),
        s,
    )
    assert_true(s.endswith("}}</script>"), s)
    # Nothing in it ends the script early.
    assert_equal(s.count("</script>"), 1)
    assert_true('"commit": "abcdef1"' in s, s)
    assert_true('"input": "Month", "output": "Measured"' in s, s)
    assert_true('"recent": {"runs": 3, "attempts": 4, "failed": 1' in s, s)
    assert_true(
        '"attempts": {"7": {"step": "days", "partition": "2024-02",'
        ' "attempt": 2'
        in s,
        s,
    )
    assert_true('"error": "\\u003cb>boom\\u003c/b>"' in s, s)
    assert_false("more" in s, s)
    var none = cards_script(List[StepCard](), List[AttemptCard]())
    assert_true('{"steps": {}, "attempts": {}}' in none, none)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
