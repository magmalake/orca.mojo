"""The watchdog: pauses pipelines when something leaves a sane range.

On 2026-11-05 nothing noticed ~360 EC2 launches in 38 minutes, or a run
waiting three hours for workers that never came. The scheduler evaluates
these rules every `ORCA_WATCHDOG_SECONDS` (60; 0 turns the watchdog off),
each from Postgres only (never object storage), with the thresholds below
(each rule's environment variable overrides its default):

| Rule | Trips when | Scope |
|---|---|---|
| `launches` | more than `ORCA_WATCHDOG_LAUNCHES` (200) instances were launched in the last hour (pool handovers aren't launches) | all pipelines |
| `cost` | the estimated cost of the instances up (`orca.fleet`'s price table) stayed over `ORCA_WATCHDOG_COST_HOUR` ($3) an hour for `ORCA_WATCHDOG_COST_MINUTES` (15) minutes, or the last 24 hours cost over `ORCA_WATCHDOG_COST_DAY` ($25) | all pipelines |
| `idle` | at least `ORCA_WATCHDOG_IDLE_WORKERS` (4) workers, up for `ORCA_WATCHDOG_IDLE_MINUTES` (10, at most 30) minutes, averaged under `ORCA_WATCHDOG_IDLE_CPU` (15) percent CPU over them (pooled idle and draining workers aren't counted) | the pipelines owning those workers |
| `failing` | a pipeline had more than `ORCA_WATCHDOG_FAILED` (20) failed attempts in the last hour, or at least `ORCA_WATCHDOG_FAILED_MIN` (10) attempts ended and more than `ORCA_WATCHDOG_FAILED_PERCENT` (50) percent failed | each such pipeline |
| `stuck` | a run with a running attempt, on instances that are sampled, has had no attempt start or end, no instance launched or ended and no sampled host over `ORCA_WATCHDOG_STUCK_CPU` (5) percent CPU for `ORCA_WATCHDOG_STUCK_MINUTES` (30, at most 30, since samples are kept that long) minutes | that run's pipeline |
| `control` | the control instance's newest minute has memory over `ORCA_WATCHDOG_MEMORY` (90) percent or its fullest disk over `ORCA_WATCHDOG_DISK` (85) | all pipelines |

A tripped rule **pauses** its scope through `orca.pause.set_paused`
(what the UI's Pause button does; pipelines already paused are left
alone), writes an `orca.audit` row per pipeline (user `watchdog`, the
rule's numbers), and a row of `orca.watchdog_trips`, which the banner on
`/` and `/fleet`, `/watchdog` and `/metrics` show for as long as any of its
pipelines is paused. It never resumes anything. A rule that is still over
its limit when a pipeline is unpaused trips again on the next pass, so
snooze it (`orca watchdog snooze <rule> 2h`, or on `/watchdog`) first;
that is stored in `orca.watchdog_rules` and audited.

Evaluation is pure (`eval_*`, over plain rows) apart from the gathering
(`gather_*`).
"""

from std.os import getenv

from json import Value, loads
from postgres import Params

from orca.auth import audit
from orca.fleet import hourly_price
from orca.html import Html, url_encode
from orca.json import quote
from orca.pause import set_paused
from orca.state import State
from orca.system import system_now

comptime SAMPLE_MINUTES = 30
"""How long fleet samples are kept, which bounds `idle` and `stuck`."""


def rule_names() -> List[String]:
    return [
        String("launches"),
        String("cost"),
        String("idle"),
        String("failing"),
        String("stuck"),
        String("control"),
    ]


def is_rule(name: String) -> Bool:
    for r in rule_names():
        if r == name:
            return True
    return False


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------


@fieldwise_init
struct Config(Copyable, Movable):
    var seconds: Float64
    var launches: Int
    var cost_hour: Float64
    var cost_minutes: Int
    var cost_day: Float64
    var idle_cpu: Float64
    var idle_minutes: Int
    var idle_workers: Int
    var failed: Int
    var failed_percent: Float64
    var failed_min: Int
    var stuck_minutes: Int
    var stuck_cpu: Float64
    var memory: Float64
    var disk: Float64
    var control_price: Float64
    """Dollars an hour of the control instance (`ORCA_CONTROL_TYPE`), or 0."""


def default_config() -> Config:
    return Config(
        60.0,
        200,
        3.0,
        15,
        25.0,
        15.0,
        10,
        4,
        20,
        50.0,
        10,
        30,
        5.0,
        90.0,
        85.0,
        0.0,
    )


def _number(name: String, default: Float64) raises -> Float64:
    var v = getenv(name)
    if not v:
        return default
    try:
        return Float64(v)
    except:
        raise Error("orca: ", name, " is a number, not ", v)


def config() raises -> Config:
    """The thresholds, from the environment."""
    var c = default_config()
    c.seconds = _number("ORCA_WATCHDOG_SECONDS", c.seconds)
    c.launches = Int(_number("ORCA_WATCHDOG_LAUNCHES", Float64(c.launches)))
    c.cost_hour = _number("ORCA_WATCHDOG_COST_HOUR", c.cost_hour)
    c.cost_minutes = Int(
        _number("ORCA_WATCHDOG_COST_MINUTES", Float64(c.cost_minutes))
    )
    c.cost_day = _number("ORCA_WATCHDOG_COST_DAY", c.cost_day)
    c.idle_cpu = _number("ORCA_WATCHDOG_IDLE_CPU", c.idle_cpu)
    c.idle_minutes = min(
        Int(_number("ORCA_WATCHDOG_IDLE_MINUTES", Float64(c.idle_minutes))),
        SAMPLE_MINUTES,
    )
    c.idle_workers = Int(
        _number("ORCA_WATCHDOG_IDLE_WORKERS", Float64(c.idle_workers))
    )
    c.failed = Int(_number("ORCA_WATCHDOG_FAILED", Float64(c.failed)))
    c.failed_percent = _number("ORCA_WATCHDOG_FAILED_PERCENT", c.failed_percent)
    c.failed_min = Int(
        _number("ORCA_WATCHDOG_FAILED_MIN", Float64(c.failed_min))
    )
    c.stuck_minutes = min(
        Int(_number("ORCA_WATCHDOG_STUCK_MINUTES", Float64(c.stuck_minutes))),
        SAMPLE_MINUTES,
    )
    c.stuck_cpu = _number("ORCA_WATCHDOG_STUCK_CPU", c.stuck_cpu)
    c.memory = _number("ORCA_WATCHDOG_MEMORY", c.memory)
    c.disk = _number("ORCA_WATCHDOG_DISK", c.disk)
    var control = getenv("ORCA_CONTROL_TYPE")
    if control:
        c.control_price = max(hourly_price(control), 0.0)
    return c^


# ---------------------------------------------------------------------------
# Readings (pure)
# ---------------------------------------------------------------------------


@fieldwise_init
struct Reading(Copyable, Movable):
    """What a rule saw: its current value against its threshold, and if
    that is a trip, why and which pipelines."""

    var rule: String
    var value: Float64
    """-1 when there is nothing to judge (no samples)."""
    var threshold: Float64
    var tripped: Bool
    var detail: String
    var pipelines: List[String]


def _fx(x: Float64, digits: Int = 1) -> String:
    var scale = 1
    for _ in range(digits):
        scale *= 10
    var n = Int(max(x, 0.0) * Float64(scale) + 0.5)
    var s = String(n // scale)
    if digits > 0:
        var f = String(n % scale)
        while f.byte_length() < digits:
            f = String("0", f)
        s.write(".", f)
    return s^


def _ok(rule: String, value: Float64, threshold: Float64) -> Reading:
    return Reading(rule, value, threshold, False, String(), List[String]())


def eval_launches(
    c: Config, launched: Int, everything: List[String]
) -> Reading:
    """`launched` instances in the last hour against the limit."""
    if launched <= c.launches:
        return _ok("launches", Float64(launched), Float64(c.launches))
    return Reading(
        "launches",
        Float64(launched),
        Float64(c.launches),
        True,
        String(
            launched,
            " instances launched in the last hour (limit ",
            c.launches,
            ")",
        ),
        everything.copy(),
    )


@fieldwise_init
struct Lease(Copyable, Movable):
    """An instance's row: its type, how long ago it started and ended
    (-1: it hasn't)."""

    var instance_type: String
    var start_ago: Float64
    var end_ago: Float64


def cost_rate(leases: List[Lease], ago: Float64, control: Float64) -> Float64:
    """Dollars an hour being spent `ago` seconds ago (unpriced types cost
    nothing here, as in `orca.fleet`)."""
    var total = control
    for l in leases:
        if l.start_ago >= ago and (l.end_ago < 0 or l.end_ago <= ago):
            total += max(hourly_price(l.instance_type), 0.0)
    return total


def cost_spent(
    leases: List[Lease], window: Float64, control: Float64
) -> Float64:
    """Dollars spent over the last `window` seconds."""
    var total = control * window / 3600.0
    for l in leases:
        var from_ = min(l.start_ago, window)
        var to = max(l.end_ago, 0.0)
        if from_ > to:
            total += (
                max(hourly_price(l.instance_type), 0.0) * (from_ - to) / 3600.0
            )
    return total


def eval_cost(
    c: Config, leases: List[Lease], everything: List[String]
) -> Reading:
    """The rate now (the reading's value), the lowest it was at each minute
    of the sustain period, and the last 24 hours' spend."""
    var now_rate = cost_rate(leases, 0.0, c.control_price)
    var lowest = now_rate
    for m in range(1, c.cost_minutes + 1):
        lowest = min(
            lowest, cost_rate(leases, Float64(m * 60), c.control_price)
        )
    var day = cost_spent(leases, 86400.0, c.control_price)
    var detail = String()
    if lowest > c.cost_hour:
        detail = String(
            "estimated $",
            _fx(now_rate, 2),
            "/h, over $",
            _fx(c.cost_hour, 2),
            "/h for ",
            c.cost_minutes,
            " minutes",
        )
    if day > c.cost_day:
        detail.write(
            ", " if detail else "",
            "estimated $",
            _fx(day, 2),
            " in the last 24 hours (limit $",
            _fx(c.cost_day, 2),
            ")",
        )
    if not detail:
        return _ok("cost", now_rate, c.cost_hour)
    return Reading(
        "cost", now_rate, c.cost_hour, True, detail, everything.copy()
    )


def eval_idle(
    c: Config, workers: Int, cpu: Float64, pipelines: List[String]
) -> Reading:
    """`workers` up for the whole window, averaging `cpu` percent over it
    (-1: no samples)."""
    if workers < c.idle_workers or cpu < 0 or cpu >= c.idle_cpu:
        return _ok("idle", cpu, c.idle_cpu)
    return Reading(
        "idle",
        cpu,
        c.idle_cpu,
        True,
        String(
            workers,
            " workers averaged ",
            _fx(cpu),
            "% CPU for ",
            c.idle_minutes,
            " minutes (limit ",
            _fx(c.idle_cpu, 0),
            "%)",
        ),
        pipelines.copy(),
    )


@fieldwise_init
struct Counts(Copyable, Movable):
    """A pipeline's attempts that ended in the last hour."""

    var pipeline: String
    var failed: Int
    var total: Int


def eval_failing(c: Config, counts: List[Counts]) -> Reading:
    """Pipelines over either failure limit; the value is the most failed
    attempts any had."""
    var worst = 0
    var detail = String()
    var out = List[String]()
    for k in counts:
        worst = max(worst, k.failed)
        var many = k.failed > c.failed
        var half = k.total >= c.failed_min and Float64(
            k.failed
        ) * 100.0 > c.failed_percent * Float64(k.total)
        if many or half:
            out.append(k.pipeline)
            detail.write(
                ", " if detail else "",
                k.pipeline,
                ": ",
                k.failed,
                " of ",
                k.total,
                " attempts failed in the last hour",
            )
    if len(out) == 0:
        return _ok("failing", Float64(worst), Float64(c.failed))
    return Reading(
        "failing", Float64(worst), Float64(c.failed), True, detail, out^
    )


@fieldwise_init
struct Quiet(Copyable, Movable):
    """A run with a running attempt, and the minutes nothing has moved."""

    var pipeline: String
    var run: Int
    var minutes: Float64


def eval_stuck(c: Config, runs: List[Quiet]) -> Reading:
    var worst = 0.0
    var detail = String()
    var out = List[String]()
    for r in runs:
        worst = max(worst, r.minutes)
        if r.minutes >= Float64(c.stuck_minutes):
            var seen = False
            for p in out:
                if p == r.pipeline:
                    seen = True
            if not seen:
                out.append(r.pipeline)
            detail.write(
                ", " if detail else "",
                r.pipeline,
                " run ",
                r.run,
                ": no progress for ",
                _fx(r.minutes, 0),
                " minutes",
            )
    if len(out) == 0:
        return _ok("stuck", worst, Float64(c.stuck_minutes))
    return Reading("stuck", worst, Float64(c.stuck_minutes), True, detail, out^)


def eval_control(
    c: Config, have: Bool, mem: Float64, disk: Float64, everything: List[String]
) -> Reading:
    """The newest minute's memory and fullest disk (`have` false: no fresh
    samples). The value is whichever is nearer its limit, in percent."""
    if not have:
        return _ok("control", -1.0, c.memory)
    var by_disk = disk / c.disk > mem / c.memory
    var value = disk if by_disk else mem
    var limit = c.disk if by_disk else c.memory
    if mem <= c.memory and disk <= c.disk:
        return _ok("control", value, limit)
    var detail = String()
    if mem > c.memory:
        detail = String(
            "control instance memory ",
            _fx(mem),
            "% (limit ",
            _fx(c.memory, 0),
            "%)",
        )
    if disk > c.disk:
        detail.write(
            ", " if detail else "",
            "control instance disk ",
            _fx(disk),
            "% (limit ",
            _fx(c.disk, 0),
            "%)",
        )
    return Reading("control", value, limit, True, detail, everything.copy())


# ---------------------------------------------------------------------------
# Gathering (Postgres only)
# ---------------------------------------------------------------------------


def all_pipelines(mut db: State) raises -> List[String]:
    var out = List[String]()
    for r in db.conn.query(
        "SELECT name FROM orca.pipelines ORDER BY name", Params()
    ):
        out.append(r.text("name"))
    return out^


def gather_launches(mut db: State) raises -> Int:
    return Int(
        db.conn.query(
            (
                "SELECT count(*) AS n FROM orca.instances WHERE launched >"
                " now() - interval '1 hour' AND NOT pooled"
            ),
            Params(),
        )
        .row(0)
        .int64("n")
    )


def gather_leases(mut db: State) raises -> List[Lease]:
    var out = List[Lease]()
    for r in db.conn.query(
        (
            "SELECT instance_type, extract(epoch FROM now() -"
            " launched)::float8 AS s, CASE WHEN ended IS NULL THEN -1.0::float8"
            " ELSE extract(epoch FROM now() - ended)::float8 END AS e FROM"
            " orca.instances WHERE ended IS NULL OR ended > now() - interval"
            " '24 hours'"
        ),
        Params(),
    ):
        out.append(
            Lease(r.text("instance_type"), r.float64("s"), r.float64("e"))
        )
    return out^


comptime _IDLE_WORKERS = (
    "SELECT i.id, r.pipeline FROM orca.instances i JOIN orca.runs r ON r.id ="
    " i.run WHERE i.ended IS NULL AND i.role = 'worker' AND i.drained IS NULL"
    " AND i.idle IS NULL AND i.launched < now() - make_interval(mins =>"
    " $1::int)"
)


def gather_idle(
    mut db: State, c: Config
) raises -> Tuple[Int, Float64, List[String]]:
    """The workers up the whole window that were sampled, their average
    CPU (-1 if none), and the pipelines that own them."""
    var row = db.conn.query(
        String(
            "WITH w AS (",
            _IDLE_WORKERS,
            (
                ") SELECT count(DISTINCT s.host) AS n, avg(s.cpu)::float8 AS"
                " cpu FROM orca.fleet_samples s JOIN w ON w.id = s.host WHERE"
                " s.t > now() - make_interval(mins => $1::int)"
            ),
        ),
        Params().int32(Int32(c.idle_minutes)),
    ).row(0)
    var pipelines = List[String]()
    for r in db.conn.query(
        String(
            "WITH w AS (",
            _IDLE_WORKERS,
            (
                ") SELECT DISTINCT w.pipeline FROM w WHERE EXISTS (SELECT 1"
                " FROM orca.fleet_samples s WHERE s.host = w.id AND s.t > now()"
                " - make_interval(mins => $1::int)) ORDER BY 1"
            ),
        ),
        Params().int32(Int32(c.idle_minutes)),
    ):
        pipelines.append(r.text("pipeline"))
    var cpu = -1.0 if row.is_null("cpu") else row.float64("cpu")
    return (Int(row.int64("n")), cpu, pipelines^)


def gather_failing(mut db: State) raises -> List[Counts]:
    var out = List[Counts]()
    for r in db.conn.query(
        (
            "SELECT r.pipeline, count(*) FILTER (WHERE a.status = 'failed') AS"
            " failed, count(*) AS total FROM orca.attempts a JOIN orca.steps s"
            " ON s.id = a.step JOIN orca.runs r ON r.id = s.run WHERE a.ended"
            " > now() - interval '1 hour' AND a.status IN ('succeeded',"
            " 'failed') GROUP BY r.pipeline ORDER BY r.pipeline"
        ),
        Params(),
    ):
        out.append(
            Counts(
                r.text("pipeline"),
                Int(r.int64("failed")),
                Int(r.int64("total")),
            )
        )
    return out^


def gather_stuck(mut db: State, c: Config) raises -> List[Quiet]:
    var out = List[Quiet]()
    for r in db.conn.query(
        (
            "SELECT r.pipeline, r.id AS run, extract(epoch FROM now() -"
            " greatest(ev.last, b.last))::float8 / 60 AS quiet FROM orca.runs r"
            " JOIN LATERAL (SELECT greatest((SELECT max(greatest(a.started,"
            " a.ended)) FROM orca.attempts a JOIN orca.steps s ON s.id ="
            " a.step WHERE s.run = r.id), (SELECT max(greatest(i.launched,"
            " i.joined, i.ended)) FROM orca.instances i WHERE i.run = r.id),"
            " r.started, r.created) AS last) ev ON true LEFT JOIN LATERAL"
            " (SELECT max(f.t) AS last FROM orca.fleet_samples f JOIN"
            " orca.instances i ON i.id = f.host WHERE i.run = r.id AND f.cpu"
            " >= $1) b ON true WHERE r.status = 'running' AND EXISTS (SELECT 1"
            " FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE"
            " s.run = r.id AND a.status = 'running') AND EXISTS (SELECT 1 FROM"
            " orca.fleet_samples f JOIN orca.instances i ON i.id = f.host"
            " WHERE i.run = r.id AND i.ended IS NULL AND f.t > now() -"
            " interval '5 minutes') ORDER BY r.id"
        ),
        Params().float64(c.stuck_cpu),
    ):
        out.append(
            Quiet(r.text("pipeline"), Int(r.int64("run")), r.float64("quiet"))
        )
    return out^


def evaluate(mut db: State, c: Config) raises -> List[Reading]:
    """Every rule's reading now."""
    var everything = all_pipelines(db)
    var out = List[Reading]()
    out.append(eval_launches(c, gather_launches(db), everything))
    out.append(eval_cost(c, gather_leases(db), everything))
    var idle = gather_idle(db, c)
    out.append(eval_idle(c, idle[0], idle[1], idle[2]))
    out.append(eval_failing(c, gather_failing(db)))
    out.append(eval_stuck(c, gather_stuck(db, c)))
    var now = system_now(db)
    if now and not now.value().stale():
        ref n = now.value()
        out.append(eval_control(c, True, n.mem_pct, n.disk_pct, everything))
    else:
        out.append(eval_control(c, False, 0.0, 0.0, everything))
    return out^


# ---------------------------------------------------------------------------
# Tripping
# ---------------------------------------------------------------------------


def snoozed(mut db: State, rule: String) raises -> Bool:
    return (
        len(
            db.conn.query(
                (
                    "SELECT 1 FROM orca.watchdog_rules WHERE rule = $1 AND"
                    " snoozed_until > now()"
                ),
                Params().text(rule),
            )
        )
        > 0
    )


def _record(mut db: State, r: Reading) raises:
    _ = db.conn.execute(
        (
            "INSERT INTO orca.watchdog_rules (rule, value, threshold, detail,"
            " checked) VALUES ($1, $2, $3, $4, now()) ON CONFLICT (rule) DO"
            " UPDATE SET value = EXCLUDED.value, threshold ="
            " EXCLUDED.threshold, detail = EXCLUDED.detail, checked ="
            " EXCLUDED.checked"
        ),
        Params()
        .text(r.rule)
        .float64(r.value)
        .float64(r.threshold)
        .text(r.detail),
    )


def trip(mut db: State, r: Reading) raises -> List[String]:
    """Pauses the unpaused pipelines in `r`'s scope and records why. Returns
    the ones it paused; none, and no record, if all were paused already."""
    var unpaused = List[String]()
    for row in db.conn.query(
        "SELECT name FROM orca.pipelines WHERE NOT paused", Params()
    ):
        unpaused.append(row.text("name"))
    var paused = List[String]()
    var joined = String()
    for p in r.pipelines:
        var live = False
        for u in unpaused:
            if u == p:
                live = True
        if not live:
            continue
        if set_paused(db, p, True):
            continue
        paused.append(p)
        joined.write("," if joined else "", p)
        audit(
            db,
            "watchdog",
            String("/pipelines/", url_encode(p), "/pause"),
            0,
            String("watchdog rule ", r.rule, ": ", r.detail),
            "watchdog",
        )
    if len(paused) > 0:
        _ = db.conn.execute(
            (
                "INSERT INTO orca.watchdog_trips (rule, value, threshold,"
                " detail, pipelines) VALUES ($1, $2, $3, $4, $5)"
            ),
            Params()
            .text(r.rule)
            .float64(r.value)
            .float64(r.threshold)
            .text(r.detail)
            .text(joined),
        )
        print(
            "orca: watchdog rule",
            r.rule,
            "tripped:",
            r.detail,
            "- paused",
            joined,
        )
    return paused^


def check(mut db: State, c: Config) raises -> Int:
    """One pass: evaluates the rules, keeps their readings, and trips those
    over their limits that aren't snoozed. Returns the pipelines paused."""
    var paused = 0
    for r in evaluate(db, c):
        _record(db, r)
        if r.tripped and not snoozed(db, r.rule):
            paused += len(trip(db, r))
    _ = db.conn.execute(
        "DELETE FROM orca.watchdog_trips WHERE at < now() - interval '30 days'",
        Params(),
    )
    return paused


# ---------------------------------------------------------------------------
# Snoozing
# ---------------------------------------------------------------------------

comptime MAX_SNOOZE_SECONDS = 30 * 86400


def parse_duration(text: String) raises -> Int:
    """`90s`, `30m`, `2h` or `1d` in seconds; `off` is 0 (end a snooze)."""
    var t = String(text.strip())
    if t == "off":
        return 0
    if t.byte_length() < 2:
        raise Error("orca: not a duration (30m, 2h, 1d or off): ", text)
    var unit = String(t[byte = t.byte_length() - 1 :])
    var n: Int
    try:
        n = Int(t[byte = : t.byte_length() - 1])
    except:
        raise Error("orca: not a duration (30m, 2h, 1d or off): ", text)
    var scale: Int
    if unit == "s":
        scale = 1
    elif unit == "m":
        scale = 60
    elif unit == "h":
        scale = 3600
    elif unit == "d":
        scale = 86400
    else:
        raise Error("orca: not a duration (30m, 2h, 1d or off): ", text)
    if n <= 0 or n * scale > MAX_SNOOZE_SECONDS:
        raise Error("orca: a snooze is from 1s to 30d: ", text)
    return n * scale


def snooze(
    mut db: State, rule: String, duration: String, by: String
) raises -> Int:
    """Silences `rule` for `duration` (`off` ends it). Returns the seconds."""
    if not is_rule(rule):
        var names = String()
        for n in rule_names():
            names.write(", " if names else "", n)
        raise Error("orca: no rule ", rule, " (", names, ")")
    var secs = parse_duration(duration)
    if secs == 0:
        _ = db.conn.execute(
            (
                "UPDATE orca.watchdog_rules SET snoozed_until = NULL,"
                " snoozed_by = NULL WHERE rule = $1"
            ),
            Params().text(rule),
        )
        return 0
    _ = db.conn.execute(
        (
            "INSERT INTO orca.watchdog_rules (rule, snoozed_until, snoozed_by)"
            " VALUES ($1, now() + make_interval(secs => $2::float8), $3) ON"
            " CONFLICT (rule) DO UPDATE SET snoozed_until ="
            " EXCLUDED.snoozed_until, snoozed_by = EXCLUDED.snoozed_by"
        ),
        Params().text(rule).float64(Float64(secs)).text(by),
    )
    return secs


# ---------------------------------------------------------------------------
# What the pages show
# ---------------------------------------------------------------------------


@fieldwise_init
struct Trip(Copyable, Movable):
    var rule: String
    var ago: Float64
    var at: String
    var value: Float64
    var threshold: Float64
    var detail: String
    var pipelines: List[String]
    """The pipelines it paused that are still paused."""


def active_trips(mut db: State) raises -> List[Trip]:
    """The trips with a pipeline still paused, newest first."""
    var out = List[Trip]()
    for r in db.conn.query(
        (
            "SELECT * FROM (SELECT t.rule, extract(epoch FROM now() -"
            " t.at)::float8 AS ago, to_char(t.at AT TIME ZONE 'UTC',"
            " 'YYYY-MM-DD HH24:MI:SS') || ' UTC' AS at, t.value, t.threshold,"
            " t.detail, t.id, (SELECT string_agg(p.name, ',' ORDER BY p.name)"
            " FROM orca.pipelines p WHERE p.paused AND p.name = ANY"
            "(string_to_array(t.pipelines, ','))) AS paused FROM"
            " orca.watchdog_trips t) x WHERE paused IS NOT NULL ORDER BY id"
            " DESC"
        ),
        Params(),
    ):
        var ps = List[String]()
        for p in r.text("paused").split(","):
            ps.append(String(p))
        out.append(
            Trip(
                r.text("rule"),
                r.float64("ago"),
                r.text("at"),
                r.float64("value"),
                r.float64("threshold"),
                r.text("detail"),
                ps^,
            )
        )
    return out^


@fieldwise_init
struct RuleRow(Copyable, Movable):
    var rule: String
    var value: Float64
    var threshold: Float64
    var checked: Bool
    var snoozed_until: String
    """UTC, or empty."""
    var snoozed_by: String
    var tripped: Bool


def rule_rows(mut db: State, c: Config) raises -> List[RuleRow]:
    """Each rule's last reading and snooze; thresholds from `c` where the
    scheduler hasn't read it yet."""
    var trips = active_trips(db)
    var out = List[RuleRow]()
    for name in rule_names():
        var tripped = False
        for t in trips:
            if t.rule == name:
                tripped = True
        var rows = db.conn.query(
            (
                "SELECT value, threshold, checked IS NOT NULL AS seen,"
                " coalesce(to_char(snoozed_until AT TIME ZONE 'UTC',"
                " 'YYYY-MM-DD HH24:MI:SS') || ' UTC', '') AS until,"
                " snoozed_until > now() AS live, coalesce(snoozed_by, '') AS by"
                " FROM orca.watchdog_rules WHERE rule = $1"
            ),
            Params().text(name),
        )
        if len(rows) == 0 or not rows.row(0).bool("seen"):
            out.append(
                RuleRow(
                    name,
                    -1.0,
                    _limit(c, name),
                    False,
                    String(),
                    String(),
                    tripped,
                )
            )
            continue
        var r = rows.row(0)
        var live = (not r.is_null("live")) and r.bool("live")
        out.append(
            RuleRow(
                name,
                r.float64("value") if not r.is_null("value") else -1.0,
                r.float64("threshold"),
                True,
                r.text("until") if live else String(),
                r.text("by") if live else String(),
                tripped,
            )
        )
    return out^


def _limit(c: Config, rule: String) -> Float64:
    if rule == "launches":
        return Float64(c.launches)
    if rule == "cost":
        return c.cost_hour
    if rule == "idle":
        return c.idle_cpu
    if rule == "failing":
        return Float64(c.failed)
    if rule == "stuck":
        return Float64(c.stuck_minutes)
    return c.memory


def unit_of(rule: String) -> String:
    if rule == "launches":
        return " launches/h"
    if rule == "cost":
        return " $/h"
    if rule == "idle":
        return " % CPU"
    if rule == "failing":
        return " failed attempts/h"
    if rule == "stuck":
        return " minutes quiet"
    return " %"


def _ago(seconds: Float64) -> String:
    var s = Int(seconds)
    if s < 90:
        return String(s, "s ago")
    if s < 5400:
        return String(s // 60, "m ago")
    return String(s // 3600, "h ago")


def _rule_link(t: Trip) -> String:
    if t.rule == "launches" or t.rule == "cost" or t.rule == "idle":
        return "/fleet"
    if t.rule == "control":
        return "/system"
    var status = "failed" if t.rule == "failing" else "running"
    if len(t.pipelines) > 0:
        return String(
            "/runs?pipeline=", url_encode(t.pipelines[0]), "&status=", status
        )
    return "/runs"


def banner_html(mut h: Html, trips: List[Trip]):
    """A red box listing the trips with a pipeline still paused, or
    nothing."""
    if len(trips) == 0:
        return
    h.raw('<div class="alert alert--trip" role="alert" id="watchdog">')
    for t in trips:
        h.raw("<div><strong>Watchdog paused pipelines: ")
        h.text(t.rule)
        h.raw("</strong> ")
        h.text(t.detail, " (", _ago(t.ago), ", ", t.at, "). Paused: ")
        for i in range(len(t.pipelines)):
            h.raw(", " if i else "")
            h.link(
                String("/pipelines/", url_encode(t.pipelines[i])),
                t.pipelines[i],
            )
        h.raw(". ")
        h.link(_rule_link(t), "See why")
        h.raw("</div>")
    h.raw(
        '<div>Unpause a pipeline to clear its trip; <a href="/watchdog">'
        "Watchdog</a> snoozes a rule.</div></div>"
    )


def trips_json(trips: List[Trip]) -> String:
    var out = String("[")
    for i in range(len(trips)):
        ref t = trips[i]
        out.write(
            ", " if i else "",
            '{"rule": ',
            quote(t.rule),
            ', "at": ',
            quote(t.at),
            ', "seconds_ago": ',
            Int(t.ago),
            ', "value": ',
            _fx(t.value, 2),
            ', "threshold": ',
            _fx(t.threshold, 2),
            ', "detail": ',
            quote(t.detail),
            ', "pipelines": [',
        )
        for k in range(len(t.pipelines)):
            out.write(", " if k else "", quote(t.pipelines[k]))
        out.write("]}")
    out.write("]")
    return out^


def rules_json(rows: List[RuleRow]) -> String:
    var out = String("[")
    for i in range(len(rows)):
        ref r = rows[i]
        out.write(
            ", " if i else "",
            '{"rule": ',
            quote(r.rule),
            ', "value": ',
            _fx(r.value, 2) if r.value >= 0 else String("null"),
            ', "threshold": ',
            _fx(r.threshold, 2),
            ', "tripped": ',
            "true" if r.tripped else "false",
            ', "snoozed_until": ',
            quote(r.snoozed_until) if r.snoozed_until else String("null"),
            "}",
        )
    out.write("]")
    return out^


def watchdog_json(mut db: State, c: Config) raises -> String:
    return String(
        '{"rules": ',
        rules_json(rule_rows(db, c)),
        ', "trips": ',
        trips_json(active_trips(db)),
        "}",
    )


def watchdog_html(mut h: Html, rows: List[RuleRow], trips: List[Trip]):
    """The `/watchdog` page's body: the rules, with a snooze form each."""
    h.raw("<h1>Watchdog</h1>")
    banner_html(h, trips)
    h.raw(
        '<p class="muted">Pauses pipelines when a rule leaves its range;',
        " never resumes them. Checked by the scheduler every",
        " <code>ORCA_WATCHDOG_SECONDS</code>.</p>",
        '<section class="card"><div class="scroll"><table><thead><tr>',
        "<th>Rule</th><th>Now</th><th>Limit</th><th>State</th><th>Snooze</th>",
        "</tr></thead><tbody>",
    )
    for r in rows:
        h.raw("<tr><td><code>")
        h.text(r.rule)
        h.raw('</code></td><td class="num">')
        if r.value >= 0:
            h.text(_fx(r.value, 1), unit_of(r.rule))
        else:
            h.raw("-")
        h.raw('</td><td class="num">')
        h.text(_fx(r.threshold, 1), unit_of(r.rule))
        h.raw("</td><td>")
        if r.tripped:
            h.raw('<span class="error">tripped</span>')
        elif r.snoozed_until:
            h.text("snoozed until ", r.snoozed_until)
            if r.snoozed_by:
                h.text(" by ", r.snoozed_by)
        elif r.checked:
            h.raw("ok")
        else:
            h.raw("not checked yet")
        h.raw('</td><td><form method="post" action="/watchdog/')
        h.text(r.rule)
        h.raw(
            '/snooze"><input name="duration" value="2h" size="4"> <button'
            ' type="submit">Snooze</button></form></td></tr>'
        )
    h.raw("</tbody></table></div></section>")


def _label(s: String) -> String:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def prometheus(mut db: State, c: Config) raises -> String:
    """`orca_watchdog_tripped{rule}` (1 while a trip of the rule has a
    pipeline paused), and the rule's last value and threshold."""
    var rows = rule_rows(db, c)
    var out = String(
        "# HELP orca_watchdog_tripped 1 while a trip of the rule has a",
        " pipeline still paused.\n# TYPE orca_watchdog_tripped gauge\n",
    )
    for r in rows:
        out.write(
            'orca_watchdog_tripped{rule="',
            r.rule,
            '"} ',
            1 if r.tripped else 0,
            "\n",
        )
    out.write(
        "# HELP orca_watchdog_value The rule's value at its last check (see",
        " /watchdog for units).\n# TYPE orca_watchdog_value gauge\n",
    )
    for r in rows:
        if r.value >= 0:
            out.write(
                'orca_watchdog_value{rule="',
                r.rule,
                '"} ',
                _fx(r.value, 3),
                "\n",
            )
    out.write(
        "# HELP orca_watchdog_threshold The rule's limit.\n",
        "# TYPE orca_watchdog_threshold gauge\n",
    )
    for r in rows:
        out.write(
            'orca_watchdog_threshold{rule="',
            r.rule,
            '"} ',
            _fx(r.threshold, 3),
            "\n",
        )
    return out^


def status_lines(json: String) raises -> List[String]:
    """`orca watchdog status`'s lines from `/api/watchdog`'s JSON."""
    var v = loads(json)
    var out = List[String]()
    for r in v["rules"].array_items():
        var line = String(r["rule"].string_value())
        while line.byte_length() < 10:
            line.write(" ")
        var val = r["value"]
        line.write(
            "now ",
            _num_text(val),
            unit_of(r["rule"].string_value()),
            "  limit ",
            _num_text(r["threshold"]),
            "  ",
        )
        if r["tripped"].bool_value():
            line.write("TRIPPED")
        elif not r["snoozed_until"].is_null():
            line.write("snoozed until ", r["snoozed_until"].string_value())
        else:
            line.write("ok")
        out.append(line^)
    for t in v["trips"].array_items():
        var line = String(
            "trip ",
            t["rule"].string_value(),
            " at ",
            t["at"].string_value(),
            ": ",
            t["detail"].string_value(),
            "; paused",
        )
        for p in t["pipelines"].array_items():
            line.write(" ", p.string_value())
        out.append(line^)
    return out^


def _num_text(v: Value) -> String:
    if v.is_null():
        return "-"
    return _fx(v.float_value(), 2)
