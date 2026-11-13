"""The pools morca grows and shrinks itself (`morca.pool_scale`), on Ops ›
Pools: each `[pools.<name>]` with its provider and machine types, how many
of its machines are live, the type it's trying and what its last try came
to (`orca.pools.status`), and as JSON for `/api/pools`.
"""

from json import loads
import morca.db.machines_view as machines_db

from morca.html import Html
from morca.json import quote
from morca.state import State
from morca.ui.layout import card_open

comptime _HELP = (
    "The pools morca makes and removes machines for itself, from"
    " [pools.&lt;name&gt;] in morca.toml: their provider, the machine types in"
    " order of preference (with how long each is tried), the live machines"
    " against max_machines, and what the last try to add one came to."
)


@fieldwise_init
struct ScaledPool(Copyable, Movable):
    var name: String
    var provider: String
    var machines: List[Tuple[String, Int]]
    """Each type, and its wait in seconds (-1 for none)."""
    var locations: List[String]
    var max_machines: Int
    var idle_seconds: Int
    var live: Int
    var trying: Int
    var status: String


def scaled_pools(mut db: State) raises -> List[ScaledPool]:
    var out = List[ScaledPool]()
    for r in machines_db.scaled_pools(db.conn):
        var ms = List[Tuple[String, Int]]()
        for m in loads(r.machines).array_items():
            var wait = -1
            if "wait" in m and m["wait"].is_int():
                wait = Int(m["wait"].int_value())
            ms.append((m["type"].string_value(), wait))
        var ls = List[String]()
        for l in loads(r.locations).array_items():
            ls.append(l.string_value())
        out.append(
            ScaledPool(
                r.name,
                r.provider,
                ms^,
                ls^,
                r.max_machines,
                r.idle_seconds,
                r.live,
                r.trying,
                r.status,
            )
        )
    return out^


def _types(p: ScaledPool) -> String:
    var out = String()
    for i in range(len(p.machines)):
        out.write(", " if i else "", p.machines[i][0])
        if p.machines[i][1] >= 0:
            out.write(" (", p.machines[i][1], " s)")
    return out^


def scaled_pools_html(mut h: Html, pools: List[ScaledPool]):
    """The card; nothing when morca.toml declares no pool."""
    if len(pools) == 0:
        return
    card_open(
        h, "autoscaled", "Autoscaled pools", _HELP, "worker-pools", facts=False
    )
    h.raw(
        '<div class="scroll"><table><thead><tr><th>Pool</th><th>Provider</th>',
        "<th>Types</th><th>Locations</th><th>Machines</th><th>Idle</th>",
        "<th>Trying</th><th>Last try</th></tr></thead><tbody>",
    )
    for p in pools:
        h.raw('<tr><td><a href="/ops/pools?pool=worker%3A')
        h.text(p.name)
        h.raw('"><code>')
        h.text(p.name)
        h.raw("</code></a></td><td>")
        h.text(p.provider)
        h.raw("</td><td>")
        h.text(_types(p))
        h.raw("</td><td>")
        h.text(", ".join(p.locations) if len(p.locations) else String("-"))
        h.raw('</td><td class="num">')
        h.text(String(p.live, " of ", p.max_machines))
        h.raw('</td><td class="num">')
        h.text(String(p.idle_seconds, " s"))
        h.raw("</td><td>")
        var trying = String("-")
        if p.status and p.trying < len(p.machines):
            trying = p.machines[p.trying][0]
        h.text(trying)
        h.raw("</td><td>")
        h.text(p.status if p.status else String("-"))
        h.raw("</td></tr>")
    h.raw("</tbody></table></div></section>")


def scaled_pools_json(pools: List[ScaledPool]) -> String:
    var out = String("[")
    for i in range(len(pools)):
        ref p = pools[i]
        out.write(
            ", " if i else "",
            '{"name": ',
            quote(p.name),
            ', "provider": ',
            quote(p.provider),
            ', "machines": [',
        )
        for k in range(len(p.machines)):
            out.write(
                ", " if k else "",
                '{"type": ',
                quote(p.machines[k][0]),
                ', "wait": ',
                String(p.machines[k][1]) if p.machines[k][1]
                >= 0 else String("null"),
                "}",
            )
        out.write('], "locations": [')
        for k in range(len(p.locations)):
            out.write(", " if k else "", quote(p.locations[k]))
        out.write(
            '], "max_machines": ',
            p.max_machines,
            ', "idle_seconds": ',
            p.idle_seconds,
            ', "live": ',
            p.live,
            ', "trying": ',
            quote(p.machines[p.trying][0]) if p.status
            and p.trying < len(p.machines) else String("null"),
            ', "status": ',
            quote(p.status) if p.status else String("null"),
            "}",
        )
    out.write("]")
    return out^
