"""Worker pools morca grows and shrinks itself (#267): a `[pools.<name>]`
table in morca.toml, read and checked here, recorded by `morca register` in
`orca.pools`, and acted on by the scheduler (`morca._internal.pool_scale`).

    [pools.lab]
    provider = "hetzner"
    machines = [
      { type = "cax31", wait = 600 },   # prefer this; keep trying it 10 min
      { type = "cax21", wait = 600 },
      { type = "cpx32" },               # the last: no wait, keep trying it
    ]
    locations = ["hel1", "nbg1", "fsn1"]
    max_machines = 3
    idle_seconds = 900

A pipeline runs on the pool with `placement = "worker:lab"`, as on a pool of
workers started by hand. `machines` is the order of preference: morca tries
the first type in each location in turn, retries it until its `wait`
(seconds) has passed since it started trying, then moves to the next. The
last type has no `wait`: morca keeps trying it. A machine that's running is
never replaced by a type higher in the list.
"""

import morca._internal.db.pools as pools_db

from morca._internal.control import pool_problem
from morca._internal.json import quote
from morca._internal.state import State
from morca._internal.toml import (
    ARRAY,
    INTEGER,
    STRING,
    TABLE,
    TomlDoc,
    toml_key,
)

comptime PROVIDERS = ["hetzner", "ec2"]
comptime POOL_KEYS = [
    "provider",
    "machines",
    "locations",
    "max_machines",
    "idle_seconds",
]
comptime DEFAULT_MAX_MACHINES = 1
comptime DEFAULT_IDLE_SECONDS = 900
comptime MAX_MACHINES = 100
comptime NO_WAIT = -1
"""A machine type's `wait` when it has none: keep trying it."""


@fieldwise_init
struct MachineChoice(Copyable, Movable):
    """One entry of a pool's `machines`: a provider's machine type, and how
    long to keep trying it before the next (`NO_WAIT` for the last)."""

    var type: String
    var wait: Int


@fieldwise_init
struct PoolConfig(Copyable, Movable):
    """A `[pools.<name>]` table, checked."""

    var name: String
    var provider: String
    var machines: List[MachineChoice]
    var locations: List[String]
    var max_machines: Int
    var idle_seconds: Int

    def machines_json(self) -> String:
        """`machines` as orca.pools keeps it: `[{"type": .., "wait": ..}]`,
        `wait` left out for none."""
        var out = String("[")
        for i in range(len(self.machines)):
            ref m = self.machines[i]
            out.write(", " if i > 0 else "", '{"type": ', quote(m.type))
            if m.wait != NO_WAIT:
                out.write(', "wait": ', m.wait)
            out.write("}")
        out.write("]")
        return out^

    def locations_json(self) -> String:
        var out = String("[")
        for i in range(len(self.locations)):
            out.write(", " if i > 0 else "", quote(self.locations[i]))
        out.write("]")
        return out^


def _type_problem(t: String) -> String:
    """Why `t` can't be a machine type, or empty: 1 to 64 of `a-z`, `0-9`,
    `.`, `-` and `_` (Hetzner's `cax31`, EC2's `c7g.large`)."""
    var b = t.as_bytes()
    if len(b) == 0 or len(b) > 64:
        return "a machine type is 1 to 64 characters"
    for c in b:
        if not (
            (c >= UInt8(ord("a")) and c <= UInt8(ord("z")))
            or (c >= UInt8(ord("0")) and c <= UInt8(ord("9")))
            or c == UInt8(ord("."))
            or c == UInt8(ord("-"))
            or c == UInt8(ord("_"))
        ):
            return String("not a machine type: ", quote(t))
    return ""


struct _Reader:
    """Reads one `[pools.<name>]` table, failing with its line."""

    var table: String
    var line: Int

    def __init__(out self, table: String, line: Int):
        self.table = table
        self.line = line

    def fail(self, line: Int, msg: String) raises:
        raise Error(line if line > 0 else self.line, ": ", self.table, " ", msg)


def _int(
    doc: TomlDoc,
    path: List[String],
    r: _Reader,
    key: String,
    least: Int,
    most: Int,
    default: Int,
) raises -> Int:
    var v = doc.get(path)
    if not v:
        return default
    ref x = v.value()
    if x.kind != INTEGER:
        r.fail(x.line, String(key, " is a whole number, not ", x.kind))
    var n = x.as_int()
    if n < least or n > most:
        r.fail(x.line, String(key, " is from ", least, " to ", most))
    return n


def read_pool(doc: TomlDoc, name: String) raises -> PoolConfig:
    """`[pools.<name>]` of `doc`, checked; raises `<line>: [pools.<name>]
    <why>`."""
    var base: List[String] = ["pools", name]
    var top = doc.get(base)
    var r = _Reader(
        String("[pools.", toml_key(name), "]"),
        top.value().line if top else 0,
    )
    if not top or top.value().kind != TABLE:
        r.fail(0, "isn't a table: a pool's settings are [pools.<name>]")
    var why = pool_problem(name)
    if why:
        r.fail(0, why)
    for key in doc.keys(base):
        var known = False
        for k in materialize[POOL_KEYS]():
            known = known or k == key
        if not known:
            var p = base.copy()
            p.append(key)
            r.fail(
                doc.get(p).value().line,
                String(
                    "has no key ",
                    toml_key(key),
                    " (it has provider, machines, locations, max_machines",
                    " and idle_seconds)",
                ),
            )
    # provider
    var pp = base.copy()
    pp.append("provider")
    var pv = doc.get(pp)
    if not pv:
        r.fail(0, 'needs a provider: "hetzner" or "ec2"')
    if pv.value().kind != STRING:
        r.fail(pv.value().line, "provider is a string")
    var provider = pv.value().text
    var ok = False
    for p in materialize[PROVIDERS]():
        ok = ok or p == provider
    if not ok:
        r.fail(
            pv.value().line,
            String('provider is "hetzner" or "ec2", not ', quote(provider)),
        )
    # machines
    var mp = base.copy()
    mp.append("machines")
    var mv = doc.get(mp)
    if not mv:
        r.fail(0, 'needs machines: [{ type = "...", wait = 600 }, ...]')
    ref m = mv.value()
    if not doc.is_table_array(m):
        r.fail(
            m.line,
            'machines is a list of tables: [{ type = "...", wait = 600 }, ...]',
        )
    var machines = List[MachineChoice]()
    for i in range(len(m.items)):
        var ep = mp.copy()
        ep.append(m.items[i])
        var line = m.line
        for k in doc.keys(ep):
            var kp = ep.copy()
            kp.append(k)
            line = doc.get(kp).value().line
            if k != "type" and k != "wait":
                r.fail(
                    line,
                    String(
                        "machines: has no key ",
                        toml_key(k),
                        " (an entry has type and wait)",
                    ),
                )
        var tp = ep.copy()
        tp.append("type")
        var tv = doc.get(tp)
        if not tv or tv.value().kind != STRING:
            r.fail(line, "machines: each entry has a type, a string")
        var t = tv.value().text
        var tw = _type_problem(t)
        if tw:
            r.fail(tv.value().line, String("machines: ", tw))
        var wp = ep.copy()
        wp.append("wait")
        var last = i == len(m.items) - 1
        var wait = _int(doc, wp, r, "machines: wait", 0, 86400, NO_WAIT)
        if wait == NO_WAIT and not last:
            r.fail(
                tv.value().line,
                String(
                    "machines: ",
                    quote(t),
                    " needs a wait (seconds to keep trying it before the",
                    " next); only the last may leave it out",
                ),
            )
        for seen in machines:
            if seen.type == t:
                r.fail(
                    tv.value().line, String("machines: ", quote(t), " twice")
                )
        machines.append(MachineChoice(t, wait))
    # locations
    var lp = base.copy()
    lp.append("locations")
    var lv = doc.get(lp)
    var locations = List[String]()
    if lv:
        ref l = lv.value()
        if l.kind != ARRAY or doc.is_table_array(l):
            r.fail(l.line, 'locations is a list of strings: ["hel1", ...]')
        for i in range(len(l.items)):
            if l.item_kinds[i] != STRING or not l.items[i]:
                r.fail(l.line, "locations: each is a non-empty string")
            locations.append(l.items[i])
    if provider == "hetzner" and len(locations) == 0:
        r.fail(0, 'needs locations on hetzner: ["hel1", "nbg1", ...]')
    var xp = base.copy()
    xp.append("max_machines")
    var ip = base.copy()
    ip.append("idle_seconds")
    return PoolConfig(
        name,
        provider,
        machines^,
        locations^,
        _int(doc, xp, r, "max_machines", 1, MAX_MACHINES, DEFAULT_MAX_MACHINES),
        _int(doc, ip, r, "idle_seconds", 10, 86400, DEFAULT_IDLE_SECONDS),
    )


def read_pools(doc: TomlDoc) raises -> List[PoolConfig]:
    """Every `[pools.<name>]` of `doc`, checked, in the file's order."""
    var out = List[PoolConfig]()
    for name in doc.keys(["pools"]):
        out.append(read_pool(doc, name))
    return out^


def record_pools(mut db: State, pools: List[PoolConfig]) raises -> List[String]:
    """Makes `orca.pools` what morca.toml says, `pools`: each upserted, and
    a pool the file no longer has deleted, so the scheduler stops adding
    machines to it and removes the ones it has once idle. The pools
    deleted."""
    var tx = db.conn.begin()
    var keep = List[String]()
    for p in pools:
        keep.append(p.name)
        pools_db.upsert_pool(
            tx,
            p.name,
            p.provider,
            p.machines_json(),
            p.locations_json(),
            p.max_machines,
            p.idle_seconds,
        )
    var names = String("[")
    for i in range(len(keep)):
        names.write(", " if i > 0 else "", quote(keep[i]))
    names.write("]")
    var gone = pools_db.delete_pools_but(tx, names)
    tx.commit()
    return gone^
