"""What Start runs would queue (`StartPreview`), as a sentence and as
the command that queues it, for a pipeline's Operate tab and its JSON.
"""

from orca.scheduler import preview_backfill, registered
from orca.state import State
from orca.views.pipelines import PipelineRow


@fieldwise_init
struct StartPreview(Copyable, Movable):
    """What the pipeline page's Start runs form would do over a range: the
    backfill's runs and the keys it would leave, and the one run's size, or
    why the range is refused."""

    var refused: String
    var keys: List[String]
    """The keys the backfill would queue, sorted."""
    var runs: Int
    """The runs they'd make, `keys_per_run` to a run."""
    var keys_per_run: Int
    var done: List[String]
    var live: List[String]
    var live_runs: List[Int]


def start_preview(
    mut db: State,
    name: String,
    keys: String,
    rerun: Bool,
    allow_append: Bool = False,
) raises -> Optional[StartPreview]:
    """The Start runs preview over `keys`: `preview_backfill`, the same
    logic as the backfill, in a transaction it rolls back."""
    var ps = registered(db, name)
    if len(ps) == 0:
        return None
    try:
        if String(keys.strip()).startswith("@file:"):
            raise Error(
                "partitions from a file (@file:) are for the command line,"
                " which reads its own files; list the names, or use @all"
            )
        var q = preview_backfill(db, ps[0], keys, rerun, allow_append)
        return StartPreview(
            "",
            q.keys.copy(),
            q.batches,
            ps[0].keys_per_run,
            q.done.copy(),
            q.live.copy(),
            q.live_runs.copy(),
        )
    except e:
        return StartPreview(
            String(e),
            List[String](),
            0,
            ps[0].keys_per_run,
            List[String](),
            List[String](),
            List[Int](),
        )


def unit_word(kind: String, n: Int) -> String:
    """A partition of `kind`, as a word: day, month or key, plural unless
    `n` is 1."""
    var one = String("day") if kind == "daily" else (
        String("month") if kind == "monthly" else String("key")
    )
    return one if n == 1 else String(one, "s")


def preview_line(p: PipelineRow, v: StartPreview) -> String:
    """ "This queues 30 runs: 2025-01-02 … 2025-01-31. 1 already done is
    skipped." and the like."""
    var n = len(v.keys)
    var out = String()
    if v.runs == 0:
        out.write("This queues no runs.")
    else:
        out.write("This queues ", v.runs, " run" if v.runs == 1 else " runs")
        if v.keys_per_run > 1:
            out.write(" over ", n, " ", unit_word(p.kind, n))
        out.write(": ", v.keys[0])
        if n > 1:
            out.write(" … ", v.keys[n - 1])
        out.write(".")
    if len(v.done) > 0:
        out.write(
            " ",
            len(v.done),
            " already done ",
            "is" if len(v.done) == 1 else "are",
            " skipped.",
        )
    if len(v.live) > 0:
        out.write(
            " ",
            len(v.live),
            " in a queued or running run ",
            "is" if len(v.live) == 1 else "are",
            " left to it (",
            v.live[0],
            ": run ",
            v.live_runs[0],
            ").",
        )
    return out^


def one_run_line(p: PipelineRow, v: StartPreview) -> String:
    """What "Start one run" would do over the same range."""
    var n = len(v.keys) + len(v.done) + len(v.live)
    if len(v.live) > 0:
        return String(
            "One run over the whole range is refused: ",
            v.live[0],
            " is in run ",
            v.live_runs[0],
            ", which is queued or running.",
        )
    return String(
        "One run over the whole range runs all ",
        n,
        " ",
        unit_word(p.kind, n),
        ", done or not.",
    )
