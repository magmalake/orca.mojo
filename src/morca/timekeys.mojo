"""Time keys by their partitions' kind (`monthly`, `daily`, `hourly`), as
text: for code that has a pipeline's kind as a string, not its type."""

from orca.day import Day
from orca.hour import Hour
from orca.key import parse_keys
from orca.month import Month


def is_time_kind(kind: StringSlice) -> Bool:
    """Whether `kind` is a calendar's: `monthly`, `daily` or `hourly`."""
    return kind == "monthly" or kind == "daily" or kind == "hourly"


def unit_of(kind: StringSlice) -> String:
    """`month`, `day` or `hour`; `key` for any other kind."""
    if kind == "monthly":
        return "month"
    if kind == "daily":
        return "day"
    if kind == "hourly":
        return "hour"
    return "key"


def example_of(kind: StringSlice) -> String:
    """A key of `kind`, for help texts."""
    if kind == "hourly":
        return "2024-01-01T00"
    if kind == "daily":
        return "2024-01-01"
    return "2024-01"


def parse_time_keys(
    kind: StringSlice, s: StringSlice, first: StringSlice = ""
) raises -> List[String]:
    """`parse_keys` for the key type of `kind` (monthly for any other), as
    text."""
    var out = List[String]()
    if kind == "hourly":
        for k in parse_keys[Hour](s, first):
            out.append(String(k))
    elif kind == "daily":
        for k in parse_keys[Day](s, first):
            out.append(String(k))
    else:
        for k in parse_keys[Month](s, first):
            out.append(String(k))
    return out^


def containing_key(
    kind: StringSlice, year: Int, month: Int, day: Int, hour: Int
) -> String:
    """The key of `kind` (monthly for any other) a date and hour fall in."""
    if kind == "hourly":
        return String(Hour(year, month, day, hour))
    if kind == "daily":
        return String(Day(year, month, day))
    return String(Month(year, month))


def prev_key(kind: StringSlice, key: StringSlice) raises -> String:
    """The key before `key`, of `kind` (monthly for any other)."""
    if kind == "hourly":
        return String(Hour.parse(key).prev())
    if kind == "daily":
        return String(Day.parse(key).prev())
    return String(Month.parse(key).prev())
