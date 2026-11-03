"""Fuzz: `parse_names` on arbitrary text, as a keyed `run --partitions`
gets it.

Whatever it accepts must be names: 1 to 128 bytes of `[A-Za-z0-9._:-]`, no
`..`, none twice; and joined with commas, as `runs.partitions` stores them,
they must read back to the same list.

    pixi run -e fuzz fuzz-names
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca import Name, parse_names


def target(data: List[UInt8]) raises:
    save_last("names", data)
    var s = String(from_utf8_lossy=Span(data))
    var ns: List[Name]
    try:
        ns = parse_names(s)
    except e:
        only_findings(e)
        return
    check(len(ns) > 0, "accepted an empty list")
    var again = String()
    for i in range(len(ns)):
        var b = ns[i].text.as_bytes()
        check(len(b) >= 1 and len(b) <= 128, "a name's length")
        check(".." not in ns[i].text, "a name with '..'")
        check("," not in ns[i].text, "a name with a comma")
        for j in range(i):
            check(not (ns[i] == ns[j]), "a name twice")
        again.write("," if i > 0 else "", ns[i])
    var back = parse_names(again)
    check(len(back) == len(ns), "the list doesn't round-trip")
    for i in range(len(ns)):
        check(back[i] == ns[i], "the list doesn't round-trip")


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        "log-00",
        "log-00,log-01, log-02",
        "315969629_a1b2:c.d",
        "a..b",
        "a,,b",
    ]:
        seeds.append(bytes_of(s))
    run_harness[target]("names", seeds, max_len=512)
