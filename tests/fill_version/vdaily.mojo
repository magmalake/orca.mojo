"""A daily pipeline for tests/fill_version.sh, three ways: `run[version,
reads]()` sets its step's `version=` and, with `reads`, declares a read, a
change of code (and of hash) that keeps the data version.
scripts/pipelines.sh's binary runs each as vdaily-v0, vdaily-other and
vdaily-v1; built on its own, `-D VDAILY_VERSION=N` and `-D VDAILY_READS=1`
choose `main`'s. The day VDAILY_FAIL names fails, and with no run retries
its fill run gives up on it."""

from std.os import getenv
from std.sys import get_defined_int

from morca import Ctx, Daily, Day, Mapped, Pipeline, Step, morca_main


def make(mut ctx: Ctx, d: Day) raises -> Int:
    if getenv("VDAILY_FAIL") == String(d):
        raise Error("VDAILY_FAIL: ", d)
    return d.month * 100 + d.day


def run[version: Int, reads: Bool]() raises:
    comptime if reads:
        morca_main[
            Pipeline[
                "vdaily",
                Step[
                    "make",
                    Day,
                    Int,
                    make,
                    Mapped,
                    reads=["lake.x"],
                    version=version,
                ],
                partitions=Daily["2024-01-01"],
                run_retries=0,
            ]
        ]()
    else:
        morca_main[
            Pipeline[
                "vdaily",
                Step["make", Day, Int, make, Mapped, version=version],
                partitions=Daily["2024-01-01"],
                run_retries=0,
            ]
        ]()


def main() raises:
    run[
        get_defined_int["VDAILY_VERSION", 0](),
        get_defined_int["VDAILY_READS", 0]() == 1,
    ]()
