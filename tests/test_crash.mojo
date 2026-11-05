"""The summary of a stack dump (orca.crash) of a stack dump, from dumps Mojo 1.2's runtime
printed on Linux (in the project image) and macOS."""

from std.testing import TestSuite, assert_equal, assert_false, assert_true

from orca.crash import crash_summary, crashed_by_signal

comptime SEGV = """resuming run 7 with 0 steps done and 0 interrupted
[measure 2024-02] attempt 1 started
#0 0x0000ef2a516df820 (/opt/orca/lib/libKGENCompilerRTShared.so+0x10f820)
#1 0x0000ef2a516dcf00 (/opt/orca/lib/libKGENCompilerRTShared.so+0x10cf00)
#2 0x0000ef2a516e025c (/opt/orca/lib/libKGENCompilerRTShared.so+0x11025c)
#3 0x0000ef2a517808f8 (linux-vdso.so.1+0x8f8)
#4 0x0000ad4423fd1648 boom /src/project/pipelines/crash.mojo:9:11
#5 0x0000ad4423fd1648 step /src/project/pipelines/crash.mojo:13:9
#6 0x0000ad4423fd1648 main /src/project/pipelines/crash.mojo:17:9
#7 0x0000ad4423fd1648 __wrap_and_execute_raising_main Mojo/stdlib/std/builtin/_startup.mojo:107:18
#8 0x0000ad4423fd1648 main Mojo/stdlib/std/builtin/_startup.mojo:133:5
#9 0x0000f7fa20c87744 (/lib/aarch64-linux-gnu/libc.so.6+0x27744)
#10 0x0000f7fa20c87818 __libc_start_main (/lib/aarch64-linux-gnu/libc.so.6+0x27818)
#11 0x0000ad4423fd0cb0 _start (/out/g1+0xcb0)
"""

comptime ABORT = """--- start 1
ABORT: /src/project/pipelines/c2.mojo:6:14: the input is bad
 #0 0x0000000102be67b8 (/x/lib/libKGENCompilerRTShared.dylib+0xba7b8)
 #1 0x0000000187771744 (/usr/lib/system/libsystem_platform.dylib+0x180509744)
 #2 0x000000010286863c _abort_base Mojo/stdlib/std/os/os.mojo:242:31
 #3 0x000000010286863c abort Mojo/stdlib/std/os/os.mojo:261:16
 #4 0x000000010286863c main /src/project/pipelines/c2.mojo:6:14
 #5 0x000000010286863c __wrap_and_execute_raising_main Mojo/stdlib/std/builtin/_startup.mojo:107:18
"""

comptime BARE = """At: /p/c2.mojo:9:12: Assert Error: index 5 is out of bounds, valid range is 0 to 0
Stack dump without symbol names (ensure you have llvm-symbolizer in your PATH or set the environment var `LLVM_SYMBOLIZER_PATH` to point to it):
0  libKGENCompilerRTShared.so 0x0000e648c2f2f820
1  libKGENCompilerRTShared.so 0x0000e648c2f2cf00
2  g0                         0x0000c622abf81648
"""

comptime NO_LINES = """#0 0x0000e648c2f2f820 (/opt/orca/lib/libKGENCompilerRTShared.so+0x10f820)
#1 0x0000c622abf81648 main (/out/g0+0x1648)
#2 0x0000e648c2c97818 __libc_start_main (/lib/aarch64-linux-gnu/libc.so.6+0x27818)
"""


def test_segfault() raises:
    assert_equal(
        crash_summary(SEGV),
        (
            "crashed; at boom /src/project/pipelines/crash.mojo:9:11, step"
            " /src/project/pipelines/crash.mojo:13:9, main"
            " /src/project/pipelines/crash.mojo:17:9"
        ),
    )


def test_abort_says_why() raises:
    assert_equal(
        crash_summary(ABORT),
        (
            "crashed: /src/project/pipelines/c2.mojo:6:14: the input is bad;"
            " at main /src/project/pipelines/c2.mojo:6:14"
        ),
    )


def test_without_symbolizer() raises:
    var got = crash_summary(BARE)
    assert_true(got.startswith("crashed: /p/c2.mojo:9:12: Assert Error"), got)
    assert_true("no symbol names" in got, got)


def test_without_line_tables() raises:
    assert_equal(
        crash_summary(NO_LINES),
        (
            "crashed; its stack dump has no line numbers (build with mojo build"
            " -g1)"
        ),
    )


def test_only_the_last_start() raises:
    """A dump before the last start's line is an earlier start's."""
    assert_equal(
        crash_summary(String(SEGV, "--- start 2\nrun succeeded\n")), ""
    )
    assert_true(
        crash_summary(String("--- start 1\n", SEGV)).startswith(
            "crashed; at boom"
        )
    )
    assert_equal(crash_summary("run succeeded\n"), "")
    assert_equal(crash_summary(""), "")


def test_linkage_names_cut() raises:
    """A function that wasn't inlined has its linkage name, parameters,
    arguments and all."""
    var dump = String(
        "#0 0x0000000100f3a7b8 (/x/libKGENCompilerRTShared.dylib+0xba7b8)\n",
        "#1 0x0000000100d4c5a4 measure /w/examples/toy.mojo:65:14\n",
        "#2 0x0000000100d4c5a4 orca::step::Step::run[::Bool,LITOrigin[$11]](",
        "orca::ctx::Ctx),name={ #interp.memref<{[(#interp.memory_handle<16,",
        " ~Qmeasure ~Q string>, const_global, [], [])], []}, 0, 0>, 7 }",
        " src/orca/step.mojo:399:23\n",
        "#3 0x0000000100d4c5a4 orca_main src/orca/cli.mojo:133:27\n",
    )
    assert_equal(
        crash_summary(dump),
        (
            "crashed; at measure /w/examples/toy.mojo:65:14,"
            " orca::step::Step::run src/orca/step.mojo:399:23, orca_main"
            " src/orca/cli.mojo:133:27"
        ),
    )


def test_signals() raises:
    assert_true(crashed_by_signal(139))  # SIGSEGV
    assert_true(crashed_by_signal(134))  # SIGABRT
    assert_true(crashed_by_signal(133))  # SIGTRAP, Mojo's abort on macOS
    assert_false(crashed_by_signal(137))  # SIGKILL
    assert_false(crashed_by_signal(143))  # SIGTERM
    assert_false(crashed_by_signal(1))
    assert_false(crashed_by_signal(3))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
