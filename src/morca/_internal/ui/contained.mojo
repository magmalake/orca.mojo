"""Ops > Control's Runs card: each local and control-placed run going on this
host, with its cgroup's memory against its limit (`morca._internal.cgroup`), which
the scheduler records every 5 seconds, its grant (`morca._internal.resources`), and
its CPU priority (`morca._internal.run_cpu`): its cgroup's `cpu.weight`, else
its `nice`, as this process's settings say.
"""

from std.os import getenv

import morca._internal.db.machines_view as machines_db
from morca._internal.db.machines_view import HostRun

from morca._internal.html import Html
from morca._internal.json import quote
from morca._internal.run_cpu import CpuKnobs
from morca._internal.state import State
from morca._internal.ui.parts import mb_text
from morca._internal.usage import bytes_text


def host_runs(mut db: State) raises -> List[HostRun]:
    """The running runs placed on the scheduler's host."""
    return machines_db.host_runs(db.conn)


def _control_nice() -> Int:
    """`MORCA_CONTROL_NICE` (10), as `morca._internal.control` reads it."""
    try:
        var s = getenv("MORCA_CONTROL_NICE")
        return Int(s) if s else 10
    except:
        return 10


def cpu_of(r: HostRun, knobs: CpuKnobs) -> Tuple[Int, Int]:
    """A run's CPU priority: its cgroup's `cpu.weight` (0 for none) and its
    `nice`. A run with a cgroup has the weight, where the host delegates
    the cpu controller; one without has `MORCA_RUN_NICE`; a
    control-placed run `MORCA_CONTROL_NICE` either way."""
    var weight = knobs.weight if r.limit_mb > 0 else 0
    var nice = 0 if r.limit_mb > 0 else knobs.nice
    if r.placement == "control":
        nice = _control_nice()
    return (weight, nice)


def cpu_text(weight: Int, nice: Int) -> String:
    var t = String()
    if weight > 0:
        t.write("weight ", weight)
    if nice > 0:
        t.write(", " if t else "", "nice ", nice)
    return t^ if t else String("-")


def host_runs_html(mut h: Html, runs: List[HostRun]):
    """The Runs card; nothing when none is going."""
    if len(runs) == 0:
        return
    var knobs = CpuKnobs.settings()
    h.raw(
        '<section class="card" id="runs"><h2><a class="anchor"'
        ' href="#runs">Runs</a></h2>'
        '<p class="muted">Local and control-placed runs going on this'
        " machine, each in a cgroup of its own where morca can make one: its"
        " memory now against its limit (memory.high), and the line past"
        " which the kernel kills it (memory.max). On CPU they yield to the"
        " scheduler, the UI and Postgres when the cores are busy: cpu.weight "
    )
    h.text(String(knobs.weight))
    h.raw(" (MORCA_RUN_CPU_WEIGHT) in a cgroup, else nice ")
    h.text(String(knobs.nice))
    h.raw(" (MORCA_RUN_NICE).</p>")
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
    h.raw("</th><th>CPU")
    h.help(
        "ctl-cpu",
        "CPU",
        (
            "The run's CPU priority, lower than the scheduler's: its cgroup's"
            " cpu.weight (MORCA_RUN_CPU_WEIGHT, 20, against the kernel's 100),"
            " with cpu.max at its CPU grant if it has one; without a cgroup,"
            " or where the host doesn't delegate the cpu controller, the nice"
            " it started at (MORCA_RUN_NICE, 10; MORCA_CONTROL_NICE for"
            " control-placed runs). It still uses every idle core."
        ),
        "placement",
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
        var cpu = cpu_of(r, knobs)
        h.raw("<td>")
        h.text(cpu_text(cpu[0], cpu[1]))
        h.raw("</td></tr>")
    h.raw("</tbody></table></div></section>")


def host_runs_json(runs: List[HostRun]) -> String:
    """`[{"run": .., "memory_limit_mb": ..|null, ..., "cpu_weight": ..|null,
    "nice": ..}]`."""
    var knobs = CpuKnobs.settings()
    var j = String("[")
    for i in range(len(runs)):
        ref r = runs[i]
        var cpu = cpu_of(r, knobs)
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
            ', "cpu_weight": ',
            String(cpu[0]) if cpu[0] > 0 else String("null"),
            ', "nice": ',
            cpu[1],
            "}",
        )
    j.write("]")
    return j^
