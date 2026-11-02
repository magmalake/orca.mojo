"""Fuzz: `current_of`, which reads a table's metadata JSON for `sense`.

On any text it returns or raises an error; with every bounds check on, it
must never read outside the text. What it returns is consistent: no
snapshot means nothing known of one.

    pixi run -e fuzz fuzz-metadata
"""

from harness import bytes_of, check, only_findings, run_harness, save_last

from orca.warehouse import current_of


def target(data: List[UInt8]) raises:
    save_last("metadata", data)
    var s = String(from_utf8_lossy=Span(data))
    try:
        var c = current_of("ns.t", s)
        if c.snapshot < 0:
            check(c.snapshot == -1, "a snapshot below -1")
            check(
                not c.by_orca and not c.partitions, "a summary of no snapshot"
            )
    except e:
        only_findings(e)


def main() raises:
    var seeds = List[List[UInt8]]()
    for s in [
        (
            '{"current-snapshot-id": 5, "snapshots": [{"snapshot-id": 5,'
            ' "summary": {"orca.run": "1", "orca.partitions": "2024-01"}}]}'
        ),
        '{"current-snapshot-id": null, "snapshots": []}',
        (
            '{"a": "\\u00e9\\ud83d\\ude00", "b": [1, {"c": [true, false]}],'
            ' "current-snapshot-id": -1}'
        ),
        (
            '{"current-snapshot-id": 2, "snapshots": [{"snapshot-id": 1},'
            ' {"snapshot-id": 2, "summary": {}}]}'
        ),
    ]:
        seeds.append(bytes_of(s))
    run_harness[target]("metadata", seeds, max_len=512)
