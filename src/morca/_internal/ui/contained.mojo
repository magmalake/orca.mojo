"""Ops > Control's Runs card: each local and control-placed run going on this
host, with its cgroup's memory against its limit (`morca._internal.cgroup`), which
the scheduler records every 5 seconds, and its grant (`morca.resources`).
"""

import morca._internal.db.machines_view as machines_db
from morca._internal.db.machines_view import HostRun

from morca.html import Html
from morca.json import quote
from morca.state import State
from morca._internal.ui.parts import mb_text
from morca.usage import bytes_text


def host_runs(mut db: State) raises -> List[HostRun]:
    """The running runs placed on the scheduler's host."""
    return machines_db.host_runs(db.conn)


def host_runs_html(mut h: Html, runs: List[HostRun]):
    """The Runs card; nothing when none is going."""
    if len(runs) == 0:
        return
    h.raw(
        '<section class="card" id="runs"><h2><a class="anchor"'
        ' href="#runs">Runs</a></h2>'
        '<p class="muted">Local and control-placed runs going on this'
        " machine, each in a cgroup of its own where morca can make one: its"
        " memory now against its limit (memory.high), and the line past"
        " which the kernel kills it (memory.max).</p>"
    )
    h.raw('<div class="scroll"><table><thead><tr><th>Run</th>')
    h.raw("<th>Pipeline</th><th>Placement")
    h.help(
        "ctl-placement",
        "Placement",
        (
            "Where the run's pipeline is placed: <code>default</code> is the"
            " scheduler's local runner, <code>control</code> this host under"
            " MORCA_CONTROL_MEMORY_MB."
        ),
        "placement",
    )
    h.raw('</th><th class="num">Memory now')
    h.help(
        "ctl-memory",
        "Memory now",
        (
            "What the run's processes hold now, all of them together (its"
            " cgroup's memory.current), sampled every 5 seconds."
        ),
    )
    h.raw('</th><th class="num">Limit')
    h.help(
        "ctl-limit",
        "Limit",
        (
            "Where the kernel starts to throttle the run (memory.high): its"
            " memory grant, else its request's maximum, else what this host can"
            " grant. Past it the run slows down rather than dies."
        ),
        "resources",
    )
    h.raw('</th><th class="num">Killed past')
    h.help(
        "ctl-max",
        "Killed past",
        (
            "The hard ceiling (memory.max): the limit plus"
            " MORCA_RUN_MEMORY_MARGIN (25%). Past it the kernel kills the whole"
            " run and nothing else; morca fails it as out of memory and its"
            " retry asks for 1.5\u00d7 as much."
        ),
        "resources",
    )
    h.raw('</th><th class="num">Granted')
    h.help(
        "ctl-grant",
        "Granted",
        (
            "The memory admission gave this run when it started"
            " (MORCA_GRANT_MEMORY_MB), out of its pipeline's request"
            " (memory_gb). A dash: the pipeline asks for nothing, so it got no"
            " grant."
        ),
        "resources",
    )
    h.raw("</th></tr></thead><tbody>")
    for r in runs:
        h.raw("<tr><td>")
        h.link(String("/runs/", r.id), String(r.id))
        h.raw("</td><td>")
        h.text(r.pipeline)
        h.raw("</td><td>")
        h.text(r.placement)
        h.raw("</td>")
        if r.limit_mb <= 0:
            h.raw('<td class="num muted" colspan="3">no cgroup</td>')
        else:
            var pct = String()
            if r.memory_bytes >= 0:
                pct = String(
                    " (",
                    (r.memory_bytes * 100) // (r.limit_mb * 1024 * 1024),
                    "%)",
                )
            h.num(
                bytes_text(r.memory_bytes) if r.memory_bytes
                >= 0 else String("-"),
                pct,
            )
            h.num(mb_text(r.limit_mb))
            h.num(mb_text(r.max_mb))
        h.num(mb_text(r.grant_mb) if r.grant_mb > 0 else String("-"))
        h.raw("</tr>")
    h.raw("</tbody></table></div></section>")


def host_runs_json(runs: List[HostRun]) -> String:
    """`[{"run": .., "memory_limit_mb": ..|null, ...}]`."""
    var j = String("[")
    for i in range(len(runs)):
        ref r = runs[i]
        j.write(
            ", " if i else "",
            '{"run": ',
            r.id,
            ', "pipeline": ',
            quote(r.pipeline),
            ', "placement": ',
            quote(r.placement),
            ', "grant_memory_mb": ',
            String(r.grant_mb) if r.grant_mb > 0 else String("null"),
            ', "memory_limit_mb": ',
            String(r.limit_mb) if r.limit_mb > 0 else String("null"),
            ', "memory_max_mb": ',
            String(r.max_mb) if r.max_mb > 0 else String("null"),
            ', "memory_bytes": ',
            String(r.memory_bytes) if r.memory_bytes >= 0 else String("null"),
            "}",
        )
    j.write("]")
    return j^
