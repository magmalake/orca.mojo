"""A daily pipeline for tests/fill_version.sh, built three ways from this
file: `-D VDAILY_VERSION=N` sets its step's `version=`, and
`-D VDAILY_READS=1` declares a read, a change of code (and of hash) that
keeps the data version. The day VDAILY_FAIL names fails, and with no run
retries its fill run gives up on it."""

from std.os import getenv
from std.sys import get_defined_int

from orca import Ctx, Daily, Day, Mapped, Pipeline, Step, orca_main


def make(mut ctx: Ctx, d: Day) raises -> Int:
    if getenv("VDAILY_FAIL") == String(d):
        raise Error("VDAILY_FAIL: ", d)
    return d.month * 100 + d.day


comptime V = get_defined_int["VDAILY_VERSION", 0]()
comptime Plain = Step["make", Day, Int, make, Mapped, version=V]
comptime Reading = Step[
    "make", Day, Int, make, Mapped, reads=["lake.x"], version=V
]
comptime VDaily = Pipeline[
    "vdaily", Plain, partitions=Daily["2024-01-01"], run_retries=0
]
comptime VDailyReads = Pipeline[
    "vdaily", Reading, partitions=Daily["2024-01-01"], run_retries=0
]


def main() raises:
    comptime if get_defined_int["VDAILY_READS", 0]() == 1:
        orca_main[VDailyReads]()
    else:
        orca_main[VDaily]()
