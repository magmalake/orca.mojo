"""Fuzz: `parse_partitions` over days, on arbitrary text, as `run --partitions`
gets it.

Whatever it accepts must be well formed: real dates, no day twice, and
written back out it must parse to the same list.

    pixi run -e fuzz fuzz-days
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca import Daily, Day, days_in, parse_partitions


def target(data: List[UInt8]) raises:
    save_last("days", data)
    var s = String(from_utf8_lossy=Span(data))
    var ms: List[Day]
    try:
        ms = parse_partitions[Daily[]](s)
    except e:
        only_findings(e)
        return
    check(len(ms) > 0, "accepted an empty list")
    var again = String()
    for i in range(len(ms)):
        check(ms[i].month >= 1 and ms[i].month <= 12, "month out of range")
        check(ms[i].year >= 0 and ms[i].year <= 9999, "year out of range")
        check(
            ms[i].day >= 1 and ms[i].day <= days_in(ms[i].year, ms[i].month),
            "day out of range",
        )
        check(Day.parse(String(ms[i])) == ms[i], "a day doesn't round-trip")
        again.write("," if i > 0 else "", ms[i])
    # parse_keys rejects a key given twice, so no day repeats.
    var back = parse_partitions[Daily[]](again)
    check(len(back) == len(ms), "the list doesn't round-trip")
    for i in range(len(ms)):
        check(back[i] == ms[i], "the list doesn't round-trip")


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        "2024-01-01",
        "2024-02-27..2024-03-02",
        "2023-12-30..2024-01-02,2025-06-30",
        "1900-02-28..1900-03-01",
        "0000-02-28..0000-03-01",
        "9999-12-31",
    ]:
        seeds.append(bytes_of(s))
    run_harness[target]("days", seeds, max_len=256)
