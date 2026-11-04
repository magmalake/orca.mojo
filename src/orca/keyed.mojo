"""Keyed partitions: keys that are names, read from a column of a dataset.

```mojo
from orca.keyed import Keyed

comptime Labels = Pipeline[
    "labels", Label, Score, partitions=Keyed["av2.logs", "log_id"], keys_per_run=100
]
```

The keys are the distinct values of `column` in the current snapshot of the
warehouse dataset `dataset` (`ORCA_WAREHOUSE`), each a `Name`. They live in
data rather than in the source because there can be thousands and they
grow. A value that isn't a name, or is null, isn't a key: it's left out,
and `keys` says how many there were on stderr. `--partitions` takes a list of names, `@all` for every current key,
or `@file:<path>` for a list one per line; a range is refused, and so is a
name the dataset doesn't have, so a typo doesn't make a run of nothing.
`<binary> keys` prints the snapshot and the keys, for the scheduler, which
links no iceberg.

A keyed pipeline has no calendar, so `schedule=` and `fills=` don't compile
with one (`Pipeline.check`). This module imports iceberg, so only binaries
with a keyed pipeline build it.
"""

from std.sys import stderr

from orca.name import Name, parse_names, sort_texts
from orca.partitions import Partitions
from orca.tables import load_dataset


def _read_file(path: String) raises -> String:
    """The names in `path`, one per line, as a comma-separated list."""
    var text: String
    with open(path, "r") as f:
        text = f.read()
    var joined = String()
    for line in text.splitlines():
        var n = line.strip()
        if n:
            joined.write("," if joined else "", n)
    if not joined:
        raise Error("no names in ", path)
    return joined^


struct Keyed[dataset_: StaticString, column_: StaticString](Partitions):
    """A partition per distinct value of `column_` in the dataset
    `dataset_` (`namespace.table`)."""

    comptime K = Name

    @staticmethod
    def kind() -> StaticString:
        return "keyed"

    @staticmethod
    def start() -> StaticString:
        return ""

    @staticmethod
    def dataset() -> StaticString:
        return Self.dataset_

    @staticmethod
    def column() -> StaticString:
        return Self.column_

    @staticmethod
    def parse(s: StringSlice, check: Bool) raises -> List[Name]:
        if s == "@all":
            var out = List[Name]()
            for k in Self.keys()[1]:
                out.append(Name(k))
            if len(out) == 0:
                raise Error(Self.dataset_, " has no keys")
            return out^
        var names: List[Name]
        if s.startswith("@file:"):
            names = parse_names(_read_file(String(s[byte=6:])))
        else:
            names = parse_names(s)
        if check:
            var have = Dict[String, Bool]()
            for k in Self.keys()[1]:
                have[k] = True
            for n in names:
                if n.text not in have:
                    raise Error(
                        n,
                        " isn't a key of ",
                        Self.dataset_,
                        " (",
                        Self.column_,
                        ")",
                    )
        return names^

    @staticmethod
    def fill(year: Int, month: Int, day: Int, previous: Bool) raises -> Name:
        raise Error("orca: keyed partitions have no calendar to fill")

    @staticmethod
    def keys() raises -> Tuple[Int64, List[String]]:
        var t = load_dataset(Self.dataset_)
        if not t.metadata.has_current_snapshot:
            return (Int64(-1), List[String]())
        var id = t.metadata.current_snapshot_id
        var res = (
            t.scan().use_snapshot(id).select([String(Self.column_)]).to_table()
        )
        if res.num_columns() != 1:
            raise Error(Self.dataset_, " has no column ", Self.column_)
        var seen = Dict[String, Bool]()
        var out = List[String]()
        var bad = List[String]()
        var n_bad = 0
        for row in range(res.num_rows()):
            var d = res.value(row, 0)
            if not d.valid or d.s in seen:
                continue
            seen[d.s] = True
            # Each must be a name, or it can't be a partition: one that
            # isn't is left out, and said, rather than stopping the rest.
            try:
                _ = Name.parse(d.s)
            except e:
                n_bad += 1
                if len(bad) < 5:
                    bad.append(String(e))
                continue
            out.append(d.s)
        if n_bad > 0:
            var why = String()
            for i in range(len(bad)):
                why.write("; " if i else "", bad[i])
            print(
                "orca: ",
                Self.dataset_,
                " (",
                Self.column_,
                ") has ",
                n_bad,
                " values that aren't names, left out of its keys: ",
                why,
                "; ..." if n_bad > len(bad) else "",
                sep="",
                file=stderr,
            )
        sort_texts(out)
        return (id, out^)
