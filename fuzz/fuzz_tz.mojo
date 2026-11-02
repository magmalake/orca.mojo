"""Fuzz: `PosixTZ.parse` on arbitrary text, and `Zone.parse` on arbitrary
bytes, as a TZif file.

A rule that parses gives one of its two offsets at any time, and its last
change is never after the time asked. A TZif file either parses or is
refused; one that parses answers `offset_at` within a day.

    pixi run -e fuzz fuzz-tz
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca import PosixTZ, Zone, utc


def target(data: List[UInt8]) raises:
    save_last("tz", data)
    var times = [
        utc(1969, 12, 31),
        utc(2024, 3, 10, 7, 0),
        utc(2024, 11, 3, 6),
        utc(2040, 7, 1),
    ]
    try:
        var z = Zone.parse("fuzz", data)
        for t in times:
            var o = z.offset_at(t)
            check(o > -86400 and o < 86400, "an offset is a day or more")
            check(z.last_change(t) <= t, "a change after the time asked")
    except e:
        only_findings(e)
    var s = String(from_utf8_lossy=Span(data))
    var p: PosixTZ
    try:
        p = PosixTZ.parse(s)
    except e:
        only_findings(e)
        return
    for t in times:
        var o = p.offset_at(t)
        check(o == p.std or o == p.dst, "an offset that's neither")
        check(p.last_change(t) <= t, "a change after the time asked")


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        "EST5EDT,M3.2.0,M11.1.0",
        "GMT0BST,M3.5.0/1,M10.5.0",
        "AEST-10AEDT,M10.1.0,M4.1.0/3",
        "<+0530>-5:30",
        "AAA0BBB-2,J60/0,300/0",
        "<-03>3<-02>,M3.5.0/-2,M10.5.0/-1",
    ]:
        seeds.append(bytes_of(s))
    with open("/usr/share/zoneinfo/Europe/London", "r") as f:
        seeds.append(f.read_bytes())
    run_harness[target]("tz", seeds, max_len=4096)
