"""Partition sets: which keys a pipeline is partitioned by, from when.

```mojo
comptime Taxi = Pipeline[
    "taxi", Download, Load, Compact, partitions=Monthly["2011-01"]
]
comptime Weather = Pipeline["weather", Fetch, partitions=Daily["2020-01-01"]]
```

A pipeline's first step takes its key (`Month` for `Monthly`, `Day` for
`Daily`), or a `Seq` of them for a first gather step. `start` is the first
partition: keys before it are refused, and phase 2's scheduler and backfills
count from it. A pipeline without `partitions=` is `Monthly[]`, with no start.
"""

from orca.day import Day
from orca.key import Key, parse_keys
from orca.month import Month


trait Partitions:
    comptime K: Key
    """The partition key."""

    @staticmethod
    def kind() -> StaticString:
        """`monthly` or `daily`, as `describe` writes it."""
        ...

    @staticmethod
    def start() -> StaticString:
        """The first partition, as text; empty for none."""
        ...


struct Monthly[first: StaticString = ""](Partitions):
    """A partition per month, from `first` (`YYYY-MM`) if given."""

    comptime K = Month

    @staticmethod
    def kind() -> StaticString:
        return "monthly"

    @staticmethod
    def start() -> StaticString:
        return Self.first


struct Daily[first: StaticString = ""](Partitions):
    """A partition per day, from `first` (`YYYY-MM-DD`) if given."""

    comptime K = Day

    @staticmethod
    def kind() -> StaticString:
        return "daily"

    @staticmethod
    def start() -> StaticString:
        return Self.first


def parse_partitions[P: Partitions](s: StringSlice) raises -> List[P.K]:
    """The keys of `P` that `s` names, as `run --partitions` takes them: a
    key, `A..B`, or a comma-separated list of either."""
    return parse_keys[P.K](s, P.start())
