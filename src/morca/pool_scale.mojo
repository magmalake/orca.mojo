"""The pool autoscaler (#267): machines for the pools morca.toml declares
(`morca.pool_config`), made and removed by the leading scheduler every
`MORCA_POOL_SCALE_SECONDS` (15).

**Demand.** A pool needs a machine while runs of its pipelines (placement
`worker:<pool>`, not paused) have been queued `MORCA_POOL_GROW_AFTER`
seconds (20) that a worker could start: no more of a pipeline's than
`max_runs` less its running ones, and its fill runs. Workers already in
the pool take what they have room for first; what's left waiting is the
demand.

**Growing.** While there's demand, no machine of the pool is still booting
(made, its worker not yet seen) and it has fewer than `max_machines`, the
autoscaler makes one, at most once every `MORCA_POOL_TRY_SECONDS` (60):

- it tries the type it's on (the first, to start with) in each location in
  turn; the first that the provider has is made;
- a type the worker image has no variant for (`morca.registry`: the
  machine's architecture, as the provider says it, isn't among the
  image's) is skipped, saying so;
- when no location has the type, it keeps trying it until the type's
  `wait` has passed since it started, then moves to the next; the last has
  no wait and is tried for as long as there's demand;
- once one is made, or the demand goes, it starts again from the first.

The machine is named `orca-pool-<pool>-<n>`, labelled with the pool and this
stack's id (`orca.stack_identity`, so a project several stacks share keeps
them apart), and booted as a worker (`morca.pool_boot`) with a token of its
own. `orca.pools.status` says what the last attempt came to.

**Shrinking.** A machine is removed (its token revoked, then the machine
deleted, then its row ended with why) when:

- it has run nothing for its pool's `idle_seconds` and the pool has no
  demand;
- its pool is no longer in morca.toml, once it's idle;
- its worker never showed up within `MORCA_POOL_BOOT_SECONDS` (900), or went
  silent for `MORCA_POOL_LOST_SECONDS` (300): its runs are queued again as
  any lost worker's are (`morca.pool_runs`).

**Strays.** A machine of the provider's with the pool's and this stack's
labels that `orca.pool_machines` doesn't have live (made, but the row
wasn't written) is deleted.
"""

from std.os import getenv
from std.time import perf_counter_ns

from json import loads
from postgres import Params

from morca.machines import MachineSpec, Provider
from morca.pool_boot import CREATED_BY, WORKER_IMAGE, boot_script
from morca.pool_config import NO_WAIT
from morca.registry import Registry
from morca.state import State
from morca.tokens import token_hash
from morca.workers import create_worker_token


def _seconds(name: String, default: Float64) -> Float64:
    var v = getenv(name)
    if not v:
        return default
    try:
        return max(Float64(v), 0.0)
    except:
        return default


def scale_seconds() -> Float64:
    return _seconds("MORCA_POOL_SCALE_SECONDS", 15.0)


def stack_id(mut db: State) raises -> String:
    """This stack's id, made once (`orca.stack_identity`): the label that
    marks its machines as its own."""
    _ = db.conn.execute(
        "INSERT INTO orca.stack_identity (id) SELECT orca.uuidv7()::text WHERE"
        " NOT EXISTS (SELECT 1 FROM orca.stack_identity)"
    )
    return (
        db.conn.query("SELECT min(id) AS id FROM orca.stack_identity")
        .row(0)
        .text("id")
    )


@fieldwise_init
struct _Live(Copyable, Movable):
    var id: Int
    var name: String
    var provider_id: String
    var worker: Int
    var age: Float64
    var seen: Bool
    var silent: Float64
    var busy: Bool
    var idle_for: Float64
    """Seconds since it was last seen idle, from when; -1 if it isn't."""


def demand(mut db: State, pool: String, after: Float64) raises -> Int:
    """The runs of `pool`'s pipelines queued at least `after` seconds that
    a worker could start: per pipeline at most `max_runs` less its running
    ones, and its fill runs; and the members its running runs that spread
    (#268) still want, once they've run `after` seconds."""
    var r = db.conn.query(
        (
            "SELECT (SELECT coalesce(sum(least(q.n,"
            " greatest(coalesce(p.max_runs, (p.describe->>'max_runs')::bigint,"
            " 1) - coalesce(r.n, 0), 0))), 0) + coalesce(sum(q.fills), 0) FROM"
            " orca.pipelines p JOIN (SELECT pipeline, count(*) FILTER (WHERE"
            " trigger <> 'fill') AS n, count(*) FILTER (WHERE trigger ="
            " 'fill') AS fills FROM orca.runs WHERE status = 'queued' AND"
            " created <= now() - make_interval(secs => $2) GROUP BY pipeline)"
            " q ON q.pipeline = p.name LEFT JOIN (SELECT pipeline, count(*) AS"
            " n FROM orca.runs WHERE status = 'running' AND trigger <> 'fill'"
            " GROUP BY pipeline) r ON r.pipeline = p.name WHERE p.placement ="
            " 'worker:' || $1 AND NOT p.paused AND p.binary_path IS NOT NULL)"
            # Members a running run of the pool still wants (#268).
            " + (SELECT coalesce(sum(greatest(coalesce(p2.ec2_workers, 0) -"
            " (SELECT count(*) FROM orca.run_members m WHERE m.run = r2.id AND"
            " m.start = r2.starts AND m.ended IS NULL), 0)), 0) FROM orca.runs"
            " r2 JOIN orca.pipelines p2 ON p2.name = r2.pipeline WHERE"
            " r2.status = 'running' AND r2.cluster_head IS NOT NULL AND"
            " p2.placement = 'worker:' || $1 AND r2.started <= now() -"
            " make_interval(secs => $2)) AS n"
        ),
        Params().text(pool).float64(after),
    )
    return Int(r.row(0).int64("n"))


def _live(mut db: State, pool: String) raises -> List[_Live]:
    var out = List[_Live]()
    for r in db.conn.query(
        (
            "SELECT m.id, m.name, coalesce(m.provider_id, '') AS provider_id,"
            " coalesce(m.worker, 0) AS worker, extract(epoch FROM now() -"
            " m.launched)::float8 AS age, w.last_seen IS NOT NULL AS seen,"
            " coalesce(extract(epoch FROM now() - w.last_seen)::float8, 0) AS"
            " silent, (coalesce(w.running, '') <> '' OR EXISTS (SELECT 1 FROM"
            " orca.runs r WHERE r.worker = m.worker AND r.status = 'running')"
            " OR EXISTS (SELECT 1 FROM orca.run_members rm WHERE rm.worker ="
            " m.worker AND rm.ended IS NULL)) AS busy, coalesce(extract(epoch"
            " FROM now() - m.idle_since)::float8, -1) AS idle_for FROM"
            " orca.pool_machines m LEFT JOIN orca.workers w ON w.id = m.worker"
            " WHERE m.pool = $1 AND m.ended IS NULL ORDER BY m.id"
        ),
        Params().text(pool),
    ):
        out.append(
            _Live(
                Int(r.int64("id")),
                r.text("name"),
                r.text("provider_id"),
                Int(r.int64("worker")),
                r.float64("age"),
                r.bool("seen"),
                r.float64("silent"),
                r.bool("busy"),
                r.float64("idle_for"),
            )
        )
    return out^


def _status(mut db: State, pool: String, text: String) raises:
    _ = db.conn.execute(
        "UPDATE orca.pools SET status = $2 WHERE name = $1",
        Params().text(pool).text(text),
    )


def _remove(
    mut db: State, provider: Provider, m: _Live, why: String
) raises -> Bool:
    """Revokes `m`'s token, deletes it at the provider, and ends its row
    with `why`. False (left for the next pass) if the provider refused."""
    if m.worker > 0:
        _ = db.conn.execute(
            (
                "UPDATE orca.workers SET revoked = coalesce(revoked, now())"
                " WHERE id = $1"
            ),
            Params().int64(Int64(m.worker)),
        )
    if m.provider_id:
        try:
            _ = provider.delete(m.provider_id)
        except e:
            print("morca: can't remove machine", m.name, ":", e)
            return False
    _ = db.conn.execute(
        (
            "UPDATE orca.pool_machines SET ended = now(), why = $2 WHERE id ="
            " $1 AND ended IS NULL"
        ),
        Params().int64(Int64(m.id)).text(why),
    )
    print("removed machine", m.name, String("(", why, ")"))
    return True


@fieldwise_init
struct _Pool(Copyable, Movable):
    var name: String
    var provider: String
    var machines: List[Tuple[String, Int]]
    var locations: List[String]
    var max_machines: Int
    var idle_seconds: Int
    var configured: Bool
    var trying: Int
    var trying_for: Float64
    """Seconds since it started trying `trying`; -1 if it isn't."""
    var since_try: Float64
    """Seconds since the last attempt; -1 for none."""


def _pools(mut db: State) raises -> List[_Pool]:
    """The pools morca.toml has, then those it no longer has that still have
    machines live."""
    var out = List[_Pool]()
    for r in db.conn.query(
        (
            "SELECT name, provider, machines::text AS machines, locations::text"
            " AS locations, max_machines, idle_seconds, true AS configured,"
            " trying, coalesce(extract(epoch FROM now() -"
            " trying_since)::float8, -1) AS trying_for, coalesce(extract(epoch"
            " FROM now() - last_try)::float8, -1) AS since_try FROM orca.pools"
            " UNION ALL SELECT DISTINCT pool, provider, '[]', '[]', 0, 0,"
            " false, 0, -1, -1 FROM orca.pool_machines WHERE ended IS NULL AND"
            " pool NOT IN (SELECT name FROM orca.pools) ORDER BY 1"
        ),
        Params(),
    ):
        var machines = List[Tuple[String, Int]]()
        for m in loads(r.text("machines")).array_items():
            var wait = NO_WAIT
            if "wait" in m and m["wait"].is_int():
                wait = Int(m["wait"].int_value())
            machines.append((m["type"].string_value(), wait))
        var locations = List[String]()
        for l in loads(r.text("locations")).array_items():
            locations.append(l.string_value())
        out.append(
            _Pool(
                r.text("name"),
                r.text("provider"),
                machines^,
                locations^,
                Int(r.int64("max_machines")),
                Int(r.int64("idle_seconds")),
                r.bool("configured"),
                Int(r.int64("trying")),
                r.float64("trying_for"),
                r.float64("since_try"),
            )
        )
    return out^


def _sweep(mut db: State, provider: Provider, pool: String) raises:
    """Deletes the provider's machines of `pool` and this stack that no live
    row has."""
    var known = Dict[String, Bool]()
    for r in db.conn.query(
        (
            "SELECT provider_id FROM orca.pool_machines WHERE pool = $1 AND"
            " ended IS NULL AND provider_id IS NOT NULL"
        ),
        Params().text(pool),
    ):
        known[r.text("provider_id")] = True
    for m in provider.machines(pool):
        if m.id not in known:
            _ = provider.delete(m.id)
            print("deleted stray machine", m.name, "of pool", pool)


struct PoolScaler(Movable):
    """The autoscaler's state between passes: when it last ran, this stack's
    id, and the worker image's architectures (read once per image)."""

    var last: Float64
    var owner: String
    var image: String
    var arches: List[String]
    var arches_why: String
    var arches_at: Float64
    """When they were read (`perf_counter_ns` seconds): a tag can be
    pushed again, so they're read again after `MORCA_POOL_IMAGE_SECONDS`
    (600)."""

    def __init__(out self):
        self.last = -1.0e18
        self.owner = String()
        self.image = String()
        self.arches = List[String]()
        self.arches_why = String()
        self.arches_at = 0.0

    def due(mut self, now: Float64) -> Bool:
        if now - self.last < scale_seconds():
            return False
        self.last = now
        return True

    def _image_arches(mut self, image: String) -> Bool:
        """Reads `image`'s architectures if it's new; False if they can't be
        read (`arches_why`), when every type is tried."""
        var now = Float64(perf_counter_ns()) / 1e9
        if image == self.image and now - self.arches_at < _seconds(
            "MORCA_POOL_IMAGE_SECONDS", 600.0
        ):
            return len(self.arches) > 0
        self.image = image
        self.arches_at = now
        self.arches = List[String]()
        self.arches_why = String()
        try:
            self.arches = Registry.from_env().arches(image)
        except e:
            self.arches_why = String(e)
            print(
                "morca: pools: can't tell the worker image's architectures:", e
            )
        return len(self.arches) > 0

    def scale(mut self, mut db: State) raises:
        """One pass over every pool."""
        if not self.owner:
            self.owner = stack_id(db)
        for p in _pools(db):
            try:
                self._pool(db, p)
            except e:
                print("morca: can't scale pool", p.name, ":", e)
                _status(db, p.name, String("error: ", e))

    def _pool(mut self, mut db: State, p: _Pool) raises:
        var provider = Provider.of(p.provider, self.owner)
        var want = demand(
            db, p.name, _seconds("MORCA_POOL_GROW_AFTER", 20.0)
        ) if p.configured else 0
        var live = _live(db, p.name)
        var boot_s = _seconds("MORCA_POOL_BOOT_SECONDS", 900.0)
        var lost_s = _seconds("MORCA_POOL_LOST_SECONDS", 300.0)
        var kept = 0
        var booting = 0
        for m in live:
            if not m.seen and m.age >= boot_s:
                _ = _remove(
                    db,
                    provider,
                    m,
                    String("never joined in ", Int(boot_s), " s"),
                )
                continue
            if m.seen and m.silent >= lost_s:
                _ = _remove(
                    db,
                    provider,
                    m,
                    String("its worker went silent for ", Int(lost_s), " s"),
                )
                continue
            if not m.seen:
                booting += 1
                kept += 1
                continue
            if m.busy:
                if m.idle_for >= 0.0:
                    _ = db.conn.execute(
                        (
                            "UPDATE orca.pool_machines SET idle_since = NULL"
                            " WHERE id = $1"
                        ),
                        Params().int64(Int64(m.id)),
                    )
                kept += 1
                continue
            if m.idle_for < 0.0:
                _ = db.conn.execute(
                    (
                        "UPDATE orca.pool_machines SET idle_since = now() WHERE"
                        " id = $1"
                    ),
                    Params().int64(Int64(m.id)),
                )
            if not p.configured:
                if _remove(db, provider, m, "its pool left morca.toml"):
                    continue
            elif (
                want == 0
                and m.idle_for >= Float64(p.idle_seconds)
                and _remove(
                    db, provider, m, String("idle ", p.idle_seconds, " s")
                )
            ):
                continue
            kept += 1
        _sweep(db, provider, p.name)
        if not p.configured:
            return
        if want == 0:
            if p.trying > 0 or p.trying_for >= 0.0:
                _ = db.conn.execute(
                    (
                        "UPDATE orca.pools SET trying = 0, trying_since = NULL,"
                        " status = NULL WHERE name = $1"
                    ),
                    Params().text(p.name),
                )
            return
        if booting > 0 or kept >= p.max_machines:
            return
        var every = _seconds("MORCA_POOL_TRY_SECONDS", 60.0)
        if p.since_try >= 0.0 and p.since_try < every:
            return
        self._grow(db, provider, p)

    def _grow(mut self, mut db: State, provider: Provider, p: _Pool) raises:
        """Tries to make one machine for `p`, by the wait rule over the
        types the worker image runs on."""
        _ = db.conn.execute(
            (
                "UPDATE orca.pools SET last_try = now(), trying_since ="
                " coalesce(trying_since, now()) WHERE name = $1"
            ),
            Params().text(p.name),
        )
        var image = getenv(WORKER_IMAGE)
        var remote = getenv("MORCA_POOL_REMOTE", getenv("MORCA_UI_URL"))
        if not image or not remote:
            _status(
                db,
                p.name,
                String(
                    "can't grow: the scheduler needs ",
                    WORKER_IMAGE,
                    " and MORCA_UI_URL",
                ),
            )
            return
        # The types the image runs on, in order; the others said.
        var checked = self._image_arches(image)
        var usable = List[Int]()
        var skipped = String()
        for k in range(len(p.machines)):
            var t = p.machines[k][0]
            if checked:
                var arch = provider.arch(t)
                if arch not in self.arches:
                    skipped.write(
                        t, " skipped: ", image, " has no linux/", arch, "; "
                    )
                    continue
            usable.append(k)
        if len(usable) == 0:
            _status(
                db,
                p.name,
                String(
                    skipped, "can't grow: the image runs on none of its types"
                ),
            )
            return
        # The type it's on: the one it was trying, else the next usable one
        # after it, else back to the first.
        var at = -1
        for u in range(len(usable)):
            if usable[u] >= p.trying:
                at = u
                break
        var trying_for = max(p.trying_for, 0.0)
        if at < 0 or usable[at] != p.trying:
            trying_for = 0.0
            if at < 0:
                at = 0
        var locations = p.locations.copy()
        if len(locations) == 0:
            locations.append("")
        # A type refused for the project's quota: the next usable type may
        # fit, this pass (waiting doesn't free quota); every location shares
        # the quota, so it isn't tried in the others.
        var i = usable[at]
        var t = p.machines[i][0]
        var wait = p.machines[i][1]
        while True:
            i = usable[at]
            t = p.machines[i][0]
            wait = p.machines[i][1]
            var quota_hit = False
            for loc in locations:
                var made = self._make(
                    db, provider, p.name, t, loc, image, remote
                )
                if made[0]:
                    _ = db.conn.execute(
                        (
                            "UPDATE orca.pools SET trying = 0, trying_since ="
                            " NULL, status = $2 WHERE name = $1"
                        ),
                        Params().text(p.name).text(String(skipped, made[1])),
                    )
                    return
                if made[3]:
                    skipped.write(t, " refused for quota: ", made[1], "; ")
                    quota_hit = True
                    break
                if not made[2]:
                    # A refusal that isn't stock: say it, try again later.
                    _ = db.conn.execute(
                        (
                            "UPDATE orca.pools SET trying = $2, status = $3"
                            " WHERE name = $1"
                        ),
                        Params()
                        .text(p.name)
                        .int64(Int64(i))
                        .text(String(skipped, made[1])),
                    )
                    return
            if not quota_hit:
                break
            if at + 1 >= len(usable):
                _status(
                    db,
                    p.name,
                    String(
                        skipped,
                        "can't grow: the project's quota fits none of its",
                        " types",
                    ),
                )
                return
            at += 1
            trying_for = 0.0
        var places = String(" in ", ", ".join(p.locations)) if len(
            p.locations
        ) else String()
        var last = at == len(usable) - 1
        if wait != NO_WAIT and trying_for >= Float64(wait) and not last:
            var next = usable[at + 1]
            _ = db.conn.execute(
                (
                    "UPDATE orca.pools SET trying = $2, trying_since = now(),"
                    " status = $3 WHERE name = $1"
                ),
                Params()
                .text(p.name)
                .int64(Int64(next))
                .text(
                    String(
                        skipped,
                        "no ",
                        t,
                        places,
                        " for ",
                        wait,
                        " s: trying ",
                        p.machines[next][0],
                        " next",
                    )
                ),
            )
            return
        var left = String(
            " (", Int(trying_for), " of ", wait, " s)"
        ) if wait != NO_WAIT and not last else String(
            " (the last: keeps trying)"
        )
        var since = "trying_since" if usable[at] == p.trying else "now()"
        _ = db.conn.execute(
            String(
                "UPDATE orca.pools SET trying = $2, trying_since = ",
                since,
                ", status = $3 WHERE name = $1",
            ),
            Params()
            .text(p.name)
            .int64(Int64(i))
            .text(String(skipped, "waiting: no ", t, places, " yet", left)),
        )

    def _make(
        mut self,
        mut db: State,
        provider: Provider,
        pool: String,
        machine_type: String,
        location: String,
        image: String,
        remote: String,
    ) raises -> Tuple[Bool, String, Bool, Bool]:
        """Makes one machine: (made, what it came to, out of stock, refused
        for quota)."""
        var id = Int(
            db.conn.query("SELECT nextval('orca.pool_machines_id_seq') AS id")
            .row(0)
            .int64("id")
        )
        # orca-pool-<pool>-<n>: plainly a pool's, beside the stack's own.
        var name = String("orca-pool-", pool.replace("_", "-"), "-", id)
        var issued = create_worker_token(
            db, pool, name, 30, String(CREATED_BY, pool)
        )
        var worker = Int(
            db.conn.query(
                "SELECT id FROM orca.workers WHERE token_hash = $1",
                Params().text(token_hash(issued.token)),
            )
            .row(0)
            .int64("id")
        )
        var script = boot_script(
            remote,
            pool,
            name,
            issued.token,
            getenv("MORCA_POOL_WORKER_COMMAND"),
        )
        var arch = String()
        try:
            arch = provider.arch(machine_type)
        except:
            pass
        var got = provider.create(
            MachineSpec(name, machine_type, location, script, pool)
        )
        if not got.ok:
            # Nothing was made: the token goes with it.
            _ = db.conn.execute(
                "DELETE FROM orca.workers WHERE id = $1",
                Params().int64(Int64(worker)),
            )
            return (
                False,
                String(
                    "can't make ", machine_type, " in ", location, ": ", got.why
                ),
                got.no_stock,
                got.quota(),
            )
        _ = db.conn.execute(
            (
                "INSERT INTO orca.pool_machines (id, pool, provider, name,"
                " server_type, arch, location, provider_id, address, worker)"
                " VALUES ($1, $2, $3, $4, $5, nullif($6, ''), $7, $8,"
                " nullif($9, ''), $10)"
            ),
            Params()
            .int64(Int64(id))
            .text(pool)
            .text(provider.kind)
            .text(name)
            .text(machine_type)
            .text(arch)
            .text(location)
            .text(got.id)
            .text(got.address)
            .int64(Int64(worker)),
        )
        var what = String("made ", name, ": ", machine_type)
        if location:
            what.write(" in ", location)
        print("pool", pool, what)
        return (True, what^, False, False)
