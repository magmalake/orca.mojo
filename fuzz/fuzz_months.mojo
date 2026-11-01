"""Fuzz: `parse_months` on arbitrary text, as `run --partitions` gets it.

Whatever it accepts must be well formed: months 1 to 12, no month twice, and
written back out it must parse to the same list.

    pixi run -e fuzz fuzz-months
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca import Month, parse_months


def target(data: List[UInt8]) raises:
    save_last("months", data)
    var s = String(from_utf8_lossy=Span(data))
    var ms: List[Month]
    try:
        ms = parse_months(s)
    except e:
        only_findings(e)
        return
    check(len(ms) > 0, "accepted an empty list")
    var again = String()
    for i in range(len(ms)):
        check(ms[i].month >= 1 and ms[i].month <= 12, "month out of range")
        check(ms[i].year >= 0 and ms[i].year <= 9999, "year out of range")
        for j in range(i):
            check(not (ms[i] == ms[j]), "a month twice")
        check(Month.parse(String(ms[i])) == ms[i], "a month doesn't round-trip")
        again.write("," if i > 0 else "", ms[i])
    var back = parse_months(again)
    check(len(back) == len(ms), "the list doesn't round-trip")
    for i in range(len(ms)):
        check(back[i] == ms[i], "the list doesn't round-trip")


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        "2024-01",
        "2024-01..2024-12",
        "2023-11..2024-02,2025-06",
        "2011-01..2024-12",
        "0000-01..0000-02",
        "9999-12",
    ]:
        seeds.append(bytes_of(s))
    run_harness[target]("months", seeds, max_len=256)
