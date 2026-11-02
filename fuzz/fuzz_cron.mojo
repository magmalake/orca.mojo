"""Fuzz: `CronExpr.parse` on arbitrary text, and the next fires of whatever
it accepts.

A fire must come after the time it was asked for, and in UTC, which has no
gaps, it must match every field. In New York it must at least come after.

    pixi run -e fuzz fuzz-cron
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca import Civil, CronExpr, Zone, utc


def _has(bits: UInt64, v: Int) -> Bool:
    return (bits >> UInt64(v)) & 1 == 1


def target(data: List[UInt8]) raises:
    save_last("cron", data)
    var s = String(from_utf8_lossy=Span(data))
    var c: CronExpr
    try:
        c = CronExpr.parse(s)
    except e:
        only_findings(e)
        return
    var again = CronExpr.parse(String(c))
    check(
        again.minutes == c.minutes and again.weekdays == c.weekdays,
        "doesn't re-parse",
    )
    var utc_zone = Zone("UTC")
    var t = utc(2024, 2, 28, 23, 59)
    for _ in range(3):
        var f = c.next_fire(t, utc_zone)
        check(f > t, "a fire isn't after the time asked")
        check(f % 60 == 0, "a fire isn't on a minute")
        var w = Civil.of(f)
        check(
            _has(c.minutes, w.minute) and _has(c.hours, w.hour),
            "time doesn't match",
        )
        check(c.matches_day(w), "day doesn't match")
        t = f
    var ny = Zone.load("America/New_York")
    t = utc(2024, 3, 10, 6, 0)
    for _ in range(3):
        var f = c.next_fire(t, ny)
        check(f > t, "a New York fire isn't after the time asked")
        t = f


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        "0 3 2 * *",
        "*/15 9-17 * jan,Jul MON-fri",
        "30 2 * * *",
        "0 0 13 * 5",
        "0 0 29 2 *",
        "10/20 1,2,3 1-31/2 * 7",
        "@hourly",
    ]:
        seeds.append(bytes_of(s))
    run_harness[target]("cron", seeds, max_len=128)
