"""Loads one dataset through morca.tables and prints what happened, for
tests/lake.sh's checks of how the library is found and refused."""

from std.sys import argv

from morca.tables import load_dataset


def main() raises:
    try:
        var t = load_dataset(String(argv()[1]))
        print("loaded", t.metadata.location)
    except e:
        print("error:", e)
