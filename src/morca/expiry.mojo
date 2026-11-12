"""Snapshot expiry, the scheduler's half: what orca still needs of a
dataset's history, and the binary that does the expiring.

Expiring needs iceberg, which the `orca` binary doesn't link, so it runs in
the project's `orca_maintain` binary (`orca.maintain`). This side decides
what must survive whatever the table's policy says, from Postgres:

- **the snapshots a run that can still go on has read.** A step's reads are
  pinned: a retry or `resume` scans the snapshot the first attempt read,
  so a queued, running, deferred or failed run keeps every snapshot its
  attempts read. (A failed run can be resumed; a succeeded or cancelled one
  can't, and is let go.)
- **the snapshots such a run committed.** A retry finds the commits its
  step made before it was killed by looking through the table's snapshots
  (`orca.tables.find_commit`); one that was expired would be committed a
  second time.
- **every snapshot of the last run that committed to the dataset**, so an
  expiry never leaves fewer than one run's worth of history.

What expiry costs: `orca lineage` and the UI's dataset and snapshot pages
read Postgres only, so they still show an expired snapshot, what made it
and what it was made from; but the snapshot itself is gone from the table,
so it can no longer be read (time travel, `use_snapshot`), and a `rebuild`
reruns the pipelines from the current data, not from it.
"""

from std.os import getenv
from std.os.path import exists
from std.sys import argv

from postgres import Params

from orca.state import State


comptime MAINTAIN_BINARY = String("orca_maintain")


def retained(mut db: State, dataset: String) raises -> List[Int64]:
    """The snapshots of `dataset` orca still needs (see the module doc)."""
    var out = List[Int64]()
    for r in db.conn.query(
        (
            "WITH live AS (SELECT a.id AS attempt FROM orca.attempts a JOIN"
            " orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id = s.run"
            " WHERE r.status NOT IN ('succeeded', 'cancelled')), last_run AS"
            " (SELECT s.run FROM orca.commits c JOIN orca.attempts a ON a.id ="
            " c.attempt JOIN orca.steps s ON s.id = a.step WHERE c.dataset ="
            " $1 ORDER BY c.attempt DESC LIMIT 1) SELECT DISTINCT snapshot_id"
            " FROM (SELECT rd.snapshot_id FROM orca.reads rd JOIN live ON"
            " live.attempt = rd.attempt WHERE rd.dataset = $1 UNION SELECT"
            " c.snapshot_id FROM orca.commits c JOIN live ON live.attempt ="
            " c.attempt WHERE c.dataset = $1 UNION SELECT c.snapshot_id FROM"
            " orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN"
            " orca.steps s ON s.id = a.step WHERE c.dataset = $1 AND s.run IN"
            " (SELECT run FROM last_run)) x WHERE snapshot_id >= 0 ORDER BY"
            " snapshot_id"
        ),
        Params().text(dataset),
    ):
        out.append(r.int64("snapshot_id"))
    return out^


def expirable(mut db: State) raises -> List[String]:
    """Every dataset orca has committed to, by name."""
    var out = List[String]()
    for r in db.conn.query(
        "SELECT DISTINCT dataset FROM orca.commits WHERE dataset IS NOT"
        " NULL AND dataset <> '' ORDER BY dataset"
    ):
        out.append(r.text("dataset"))
    return out^


def maintain_binary() -> String:
    """`$ORCA_MAINTAIN`, else `orca_maintain` beside this binary, else on
    `PATH`; empty when there is none."""
    var set = getenv("ORCA_MAINTAIN")
    if set:
        return set
    var me = String(argv()[0])
    var slash = me.rfind("/")
    if slash >= 0:
        var beside = String(me[byte = : slash + 1], MAINTAIN_BINARY)
        if exists(beside):
            return beside
    for dir in getenv("PATH").split(":"):
        if dir.byte_length() == 0:
            continue
        var p = String(dir, "/", MAINTAIN_BINARY)
        if exists(p):
            return p
    return String()


def expire_argv(
    mut db: State,
    binary: String,
    dataset: String,
    older_than: String = "",
    keep: String = "",
    dry_run: Bool = False,
    report: Bool = False,
) raises -> List[String]:
    """The `orca_maintain expire` command line for `dataset`: the policy
    given, or the table's own (`--if-policy`, so a table without one is
    left alone), and `--retain` with what orca still needs."""
    var a: List[String] = [binary, String("expire"), dataset]
    if older_than:
        a.append(String("--older-than"))
        a.append(older_than)
    else:
        a.append(String("--if-policy"))
    if keep:
        a.append(String("--keep"))
        a.append(keep)
    var keep_ids = retained(db, dataset)
    if len(keep_ids) > 0:
        var ids = String()
        for k in range(len(keep_ids)):
            ids.write("," if k > 0 else "", keep_ids[k])
        a.append(String("--retain"))
        a.append(ids^)
    if dry_run:
        a.append(String("--dry-run"))
    if report:
        a.append(String("--report"))
    return a^
