"""How long runs of a pipeline would take, from how long its recent runs
took.

A pipeline's last `SAMPLE` succeeded runs give its seconds per partition:
their running time (`started` to `ended`, not the wait in the queue) over
the partitions they ran. A backfill of `runs` runs over `keys` partitions
takes waves of `max_runs` runs at once, each about as long as a run of
its size; one run over them all takes them one after another.
"""

from postgres import Params

from morca.fill import eta_text
from morca.state import State


comptime SAMPLE = 20
"""The most recent succeeded runs the estimate is taken from."""


comptime _PACE_SQL = (
    "SELECT count(*)::bigint AS n, coalesce(sum(extract(epoch FROM ended -"
    " started)), 0)::float8 AS secs, coalesce(sum(cardinality("
    "string_to_array(partitions, ','))), 0)::bigint AS keys FROM (SELECT"
    " started, ended, partitions FROM orca.runs WHERE pipeline = $1 AND"
    " status = 'succeeded' AND started IS NOT NULL AND ended IS NOT NULL AND"
    " ended >= started ORDER BY ended DESC LIMIT 20) r"
)
"""Over the last `SAMPLE` (20) of them."""


@fieldwise_init
struct Pace(Copyable, Movable):
    """A pipeline's seconds per partition, and the runs that says it."""

    var per_key: Float64
    """-1 when no run has succeeded yet."""
    var runs: Int

    def known(self) -> Bool:
        return self.per_key >= 0.0

    def backfill(self, keys: Int, runs: Int, max_runs: Int) -> Int:
        """Seconds for `runs` runs over `keys` partitions, `max_runs` at
        once; -1 if not known."""
        if not self.known() or runs <= 0:
            return -1
        var at_once = max(min(max_runs, runs), 1)
        var waves = (runs + at_once - 1) // at_once
        var per_run = (keys + runs - 1) // runs
        return Int(Float64(waves * per_run) * self.per_key + 0.5)

    def one_run(self, keys: Int) -> Int:
        """Seconds for one run over `keys` partitions; -1 if not known."""
        if not self.known() or keys <= 0:
            return -1
        return Int(Float64(keys) * self.per_key + 0.5)


def pace(mut db: State, pipeline: String) raises -> Pace:
    """`pipeline`'s pace over its last `SAMPLE` succeeded runs."""
    var r = db.conn.query(_PACE_SQL, Params().text(pipeline)).row(0)
    var keys = Int(r.int64("keys"))
    if r.int64("n") == 0 or keys == 0:
        return Pace(-1.0, 0)
    return Pace(r.float64("secs") / Float64(keys), Int(r.int64("n")))


def per_key_text(p: Pace, unit: String) -> String:
    """ "95 s a day over its last 20 runs"."""
    var s = p.per_key
    var amount = String(Int(s + 0.5), " s") if s < 120.0 else String(
        Float64(Int(s / 6.0 + 0.5)) / 10.0, " min"
    )
    return String(
        amount,
        " a ",
        unit,
        " over its last ",
        p.runs,
        " run" if p.runs == 1 else " runs",
    )


def estimate_text(p: Pace, seconds: Int, unit: String, at_once: Int) -> String:
    """ "About 2h 10m, 1 run at once (95 s a day over its last 20 runs)." or
    why there's no estimate."""
    if seconds < 0:
        return "No estimate yet: no run of it has succeeded."
    var out = String("About ", eta_text(seconds))
    if at_once > 0:
        out.write(
            ", ", at_once, " run" if at_once == 1 else " runs", " at once"
        )
    out.write(" (", per_key_text(p, unit), ").")
    return out^
