"""Table maintenance for a project's datasets: snapshot expiry.

The `morca` binary doesn't link iceberg, so expiry runs in a binary of the
project's own that does: `bin/morca_maintain.mojo`,

```mojo
from morca.maintain import maintain_main

def main() raises:
    maintain_main()
```

which `morca expire` and the scheduler's periodic expiry run (it is found as
`$MORCA_MAINTAIN`, beside the `morca` binary, or on `PATH`). Run by hand it
takes the snapshots to keep besides the policy's with `--retain`; `morca
expire` works those out from Postgres (`morca._internal.expiry.retained`) and passes
them, which is how a run that can still resume keeps the snapshots it read
and the ones it committed.

    morca_maintain expire <dataset> [--older-than 30d|12h|90m|<ms>]
        [--keep N] [--retain ID,...] [--if-policy] [--dry-run] [--report]

Without `--older-than` and `--keep` the table's own
`history.expire.max-snapshot-age-ms` and `history.expire.min-snapshots-to-keep`
decide (iceberg's defaults where unset: five days, one snapshot);
`--if-policy` expires nothing when the table has no
`history.expire.max-snapshot-age-ms`, which is how the scheduler leaves
tables morca didn't create alone. An expiry gives a table morca's metadata
defaults (`write.metadata.delete-after-commit.enabled`,
`write.metadata.previous-versions-max`) when it lacks them, so its own
commit already deletes the old metadata files it drops from the log. `--report` lists `metadata/` before and
after, to count its files.
"""

from std.sys import argv
from std.time import perf_counter_ns

from iceberg.catalog.filesystem import Table
from iceberg.io import dirname
from iceberg.maintain import (
    MAX_SNAPSHOT_AGE_MS,
    ExpirePolicy,
    ExpireResult,
)

from morca._internal.dataread import data_main
from morca._lake.impl import load_dataset, table_defaults


comptime USAGE = """usage:
  morca_maintain expire <dataset> [--older-than 30d|12h|90m|<ms>] [--keep N]
      [--retain ID,...] [--if-policy] [--dry-run] [--report]
  morca_maintain data <dataset> [--snapshot ID] [--where COL:OP:VALUE]...
      [--columns A,B] [--sort [-]COL] [--offset N] [--limit N]
      [--chart-x COL --chart-y A,B [--chart-agg avg|sum]] [--csv PATH]
      [--max-rows N] [--seconds S] [--memory-mb M] [--no-partitions]
      [--no-rows]"""


def parse_duration_ms(text: String) raises -> Int64:
    """`30d`, `12h`, `90m`, `45s` or plain milliseconds."""
    if text.byte_length() == 0:
        raise Error("morca: an empty duration")
    var unit: Int64 = 1
    var digits = text
    if text.endswith("d"):
        unit = 86_400_000
    elif text.endswith("h"):
        unit = 3_600_000
    elif text.endswith("m"):
        unit = 60_000
    elif text.endswith("s"):
        unit = 1_000
    if unit != 1:
        digits = String(text[byte = : text.byte_length() - 1])
    try:
        var n = Int64(Int(digits))
        if n < 0:
            raise Error("negative")
        return n * unit
    except:
        raise Error("morca: not a duration: ", text, " (30d, 12h, 90m, ms)")


@fieldwise_init
struct Expiry(Copyable, Movable, Writable):
    """What one dataset's expiry did, for the line `morca_maintain` prints."""

    var dataset: String
    var skipped: String
    """Why nothing was decided (no policy), or empty."""
    var result: ExpireResult
    var snapshots_before: Int
    var snapshots_after: Int
    var metadata_files_before: Int
    """-1 when not counted (no `--report`)."""
    var metadata_files_after: Int
    var log_after: Int
    var seconds: Float64

    def write_to(self, mut w: Some[Writer]):
        if self.skipped:
            w.write("kept ", self.dataset, ": ", self.skipped)
            return
        ref r = self.result
        w.write(
            "dry run " if r.dry_run else "expired ",
            self.dataset,
            ": snapshots ",
            self.snapshots_before,
            " -> ",
            self.snapshots_after,
            " (",
            len(r.expired),
            " expired",
        )
        if len(r.removed_refs) > 0:
            w.write(", refs removed:")
            for n in r.removed_refs:
                w.write(" ", n)
        w.write(
            "); would delete " if r.dry_run else "); deleted ",
            len(r.deleted_files),
            " data/delete files, ",
            len(r.deleted_manifests),
            " manifests and lists, ",
            len(r.deleted_statistics),
            " statistics; read ",
            r.manifest_lists_read,
            " manifest lists and ",
            r.manifests_read,
            " manifests",
        )
        if r.reachable:
            w.write(" (branches or tags: every retained manifest read)")
        w.write("; metadata-log ", self.log_after)
        if self.metadata_files_before >= 0:
            w.write(
                ", metadata files ",
                self.metadata_files_before,
                " -> ",
                self.metadata_files_after,
            )
        w.write("; ", self.seconds, " s")


def _metadata_files(table: Table) raises -> Int:
    var names = table.io.list_names(dirname(table.metadata_location))
    var n = 0
    for k in range(len(names)):
        if names[k].endswith(".metadata.json"):
            n += 1
    return n


def expire_dataset(
    dataset: String,
    older_than_ms: Int64 = -1,
    keep: Int = -1,
    retain: List[Int64] = List[Int64](),
    if_policy: Bool = False,
    dry_run: Bool = False,
    report: Bool = False,
) raises -> Expiry:
    """Expires `dataset`'s snapshots: older than `older_than_ms` ago (or by
    the table's properties when that is -1), keeping at least `keep` (or the
    table's `history.expire.min-snapshots-to-keep`) and every snapshot in
    `retain`. The metadata is committed first, then the files nothing
    retained points at are deleted."""
    var t0 = perf_counter_ns()
    var table = load_dataset(dataset)
    var before = len(table.metadata.snapshots)
    var files_before = _metadata_files(table) if report else -1
    if (
        if_policy
        and older_than_ms < 0
        and table.metadata.property_(MAX_SNAPSHOT_AGE_MS, "") == ""
    ):
        return Expiry(
            dataset,
            String("no ", MAX_SNAPSHOT_AGE_MS, " property"),
            ExpireResult(),
            before,
            before,
            files_before,
            files_before,
            len(table.metadata.metadata_log),
            0.0,
        )
    # The expiry's commit trims the metadata log like any other; with the
    # files that fall off it deleted only under delete-after-commit, a table
    # without morca's metadata defaults would leave them unreferenced for
    # good. So it gets them in the same commit.
    var defaults = table_defaults()
    for key in [
        String("write.metadata.delete-after-commit.enabled"),
        String("write.metadata.previous-versions-max"),
    ]:
        if key not in table.metadata.properties:
            table.metadata.properties[key] = defaults[key]
    var policy = ExpirePolicy.from_properties(table.metadata)
    if older_than_ms >= 0:
        policy.older_than_ms = policy.now_ms - older_than_ms
    if keep >= 0:
        policy.keep_last = keep
    for id in retain:
        policy.retain.append(id)
    var result = table.expire(policy, dry_run)
    if not dry_run:
        table.refresh()
    return Expiry(
        dataset,
        String(),
        result^,
        before,
        len(table.metadata.snapshots) if not dry_run else before
        - len(result.expired),
        files_before,
        _metadata_files(table) if report else -1,
        len(table.metadata.metadata_log),
        Float64(perf_counter_ns() - t0) / 1e9,
    )


def _usage(msg: String = ""):
    if msg:
        print(msg)
    print(USAGE)


def maintain_main() raises:
    """The `morca_maintain` command (see the module doc)."""
    var args = List[String]()
    for a in argv():
        args.append(String(a))
    if len(args) >= 3 and args[1] == "data":
        # The UI's data view (morca._internal.dataread).
        var rest = List[String]()
        for k in range(2, len(args)):
            rest.append(args[k])
        data_main(rest)
        return
    if len(args) < 3 or args[1] != "expire":
        _usage()
        raise Error("morca_maintain: expected `expire <dataset>`")
    var dataset = args[2]
    var older: Int64 = -1
    var keep = -1
    var retain = List[Int64]()
    var if_policy = False
    var dry_run = False
    var report = False
    var i = 3
    while i < len(args):
        var a = args[i]
        if a == "--older-than" and i + 1 < len(args):
            older = parse_duration_ms(args[i + 1])
            i += 2
        elif a == "--keep" and i + 1 < len(args):
            keep = Int(args[i + 1])
            i += 2
        elif a == "--retain" and i + 1 < len(args):
            for part in args[i + 1].split(","):
                if part.byte_length() > 0:
                    retain.append(Int64(Int(String(part))))
            i += 2
        elif a == "--if-policy":
            if_policy = True
            i += 1
        elif a == "--dry-run":
            dry_run = True
            i += 1
        elif a == "--report":
            report = True
            i += 1
        else:
            _usage()
            raise Error("morca_maintain: unknown argument: ", a)
    print(
        expire_dataset(dataset, older, keep, retain, if_policy, dry_run, report)
    )
