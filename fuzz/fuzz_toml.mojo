"""Fuzz: `parse_toml` on arbitrary text, as `orca register` reads
`orca.toml` (`orca.toml`, `orca.config`).

Whatever it accepts must read back the same from what `TomlDoc.dump`
writes of it: the same entries, each of the same kind and text, and a dump
of the second reading the same as the first.

    pixi run -e fuzz fuzz-toml
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca.toml import parse_toml


def target(data: List[UInt8]) raises:
    save_last("toml", data)
    var s = String(from_utf8_lossy=Span(data))
    try:
        var doc = parse_toml(s)
        var text = doc.dump()
        var again: type_of(doc)
        try:
            again = parse_toml(text)
        except e:
            check(False, String("its dump doesn't parse: ", e))
            return
        check(len(again.entries) == len(doc.entries), "entries lost or made")
        for e in doc.entries:
            var v = again.get(e.path)
            check(Bool(v), "an entry lost")
            check(v.value().kind == e.value.kind, "a kind changed")
            check(v.value().text == e.value.text, "a value changed")
            check(v.value().items == e.value.items, "an array changed")
            check(
                v.value().item_kinds == e.value.item_kinds,
                "an array's kinds changed",
            )
        check(again.dump() == text, "a second dump differs")
    except e:
        only_findings(e)


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        '[defaults]\nplacement = "ec2"\nhead_type = "t4g.medium"\n',
        (
            '[pipelines.gharchive]\nworkers = "0..4"\nworker_procs = 1\nenv'
            ' = { DISTA_WORKER_SLOTS = "4", X = "16" }\nmax_runs = 2 # c\n'
        ),
        "a.b.c = [1, 2.5e3, 'x', true,]\n[t]\n'k k' = -0_1\n",
        'x = { a = { b = "\\u00e9\\n" } }\n[y.z]\n[y]\n',
        "a = [\n  1, # one\n  2\n]\n",
    ]:
        seeds.append(bytes_of(s))
    run_harness[target]("toml", seeds, max_len=512)
