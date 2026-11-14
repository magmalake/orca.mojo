"""The UI's data view of a dataset, read with iceberg (#435): what
`morca_maintain data <dataset>` prints, as JSON, for
`morca._internal.views.data`.

The `morca` binary doesn't link iceberg, so the UI runs this in the
project's `morca_maintain` (as `morca expire` does), a short-lived process
it kills past its time limit. What it reads:

- **the table**: its schema (columns, types, field ids and docs), its
  partition spec and its snapshots, newest first, from the metadata alone;
- **partitions**: one row per partition of the snapshot, with its rows,
  files, bytes and the snapshot that last added a file to it, from the
  manifests alone;
- **rows**: a page of rows at the snapshot, with `--where` filters (on any
  top-level column, partition source columns included, so iceberg prunes
  partitions and files), a column pick and a sort, from `--offset`;
- **a chart**: the filtered rows' `--chart-y` columns, averaged (or summed)
  by `--chart-x`;
- **a download**: the filtered rows as CSV, to `--csv <path>`.

Each read stops at its cap, its time limit or its memory limit, and says
which in `stopped`; what it read up to then is still shown. A sort holds
every row it reads, so the memory limit is what bounds it.

    morca_maintain data <dataset> [--snapshot ID] [--where COL:OP:VALUE]...
        [--columns A,B] [--sort COL|-COL] [--offset N] [--limit N]
        [--chart-x COL --chart-y A,B [--chart-agg avg|sum]]
        [--csv PATH] [--max-rows N] [--seconds S] [--memory-mb M]
        [--no-partitions] [--no-rows]

`OP` is `eq`, `ne`, `lt`, `le`, `gt`, `ge` or `in` (values split on `,`).
"""

from iceberg.catalog.filesystem import Table
from iceberg.json import json_quote
from iceberg.manifest import (
    CONTENT_DATA,
    MANIFEST_CONTENT_DATA,
    STATUS_DELETED,
    ManifestCache,
    read_manifest_io,
    read_manifest_list_io_cached,
)

from morca._internal.dataask import DataAsk, columns_of, iso_ms, parse_ask
from morca._internal.datarows import chart_json, csv_json, rows_json
from morca._lake.impl import load_dataset

comptime SNAPSHOTS_SHOWN = 100
comptime PARTITIONS_SHOWN = 1000


def table_json(t: Table, snapshot: Int64, mut out: String) raises:
    """The schema, the partition spec and the snapshots, as fields of the
    object `out` is writing (no braces)."""
    ref m = t.metadata
    var scan = t.scan()
    if snapshot >= 0:
        scan = scan.use_snapshot(snapshot)
    var schema = scan.current_schema()
    out.write(
        '"snapshot": ',
        String(scan.snapshot().snapshot_id) if scan.has_any_snapshot() else (
            String("null")
        ),
        ', "current": ',
        String(m.current_snapshot_id) if m.has_current_snapshot else String(
            "null"
        ),
        ', "schema": {"schema_id": ',
        schema.schema_id,
        ', "columns": [',
    )
    var cols = columns_of(schema)
    for i in range(len(cols)):
        ref c = cols[i]
        out.write(
            ", " if i else "",
            '{"id": ',
            c.id,
            ', "name": ',
            json_quote(c.name),
            ', "type": ',
            json_quote(c.type),
            ', "required": ',
            "true" if c.required else "false",
            ', "doc": ',
            json_quote(c.doc) if c.doc else String("null"),
            "}",
        )
    var spec = m.spec()
    out.write(
        ']}, "partition_spec": {"spec_id": ', spec.spec_id, ', "fields": ['
    )
    for i in range(len(spec.fields)):
        ref f = spec.fields[i]
        out.write(
            ", " if i else "",
            '{"field_id": ',
            f.field_id,
            ', "name": ',
            json_quote(f.name),
            ', "source_id": ',
            f.source_id,
            ', "source": ',
            json_quote(m.schema().name_of(f.source_id)),
            ', "transform": ',
            json_quote(String(f.transform)),
            "}",
        )
    out.write(']}, "snapshots": [')
    var n = 0
    var k = len(m.snapshots) - 1
    while k >= 0 and n < SNAPSHOTS_SHOWN:
        ref s = m.snapshots[k]
        out.write(
            ", " if n else "",
            '{"snapshot": ',
            s.snapshot_id,
            ', "at": ',
            json_quote(iso_ms(s.timestamp_ms)),
            ', "operation": ',
            json_quote(s.operation()),
            ', "added_records": ',
            s.summary_int("added-records", 0),
            ', "total_records": ',
            s.summary_int("total-records", -1),
            ', "run": ',
            json_quote(s.summary.get("orca.run", "")),
            ', "partitions": ',
            json_quote(s.summary.get("orca.partitions", "")),
            "}",
        )
        n += 1
        k -= 1
    out.write("], ", '"snapshots_total": ', len(m.snapshots))


# ── partitions, from the manifests ───────────────────────────────────────────


struct _Part(Copyable, Movable):
    var key: String
    """The partition's values as a JSON object."""
    var rows: Int64
    var files: Int
    var bytes: Int64
    var last: Int64
    var last_ms: Int64

    def __init__(out self, key: String):
        self.key = key
        self.rows = 0
        self.files = 0
        self.bytes = 0
        self.last = -1
        self.last_ms = -1


def partitions_json(t: Table, snapshot: Int64, mut out: String) raises:
    """`"partitions": [...]` of the snapshot: a row per partition, newest
    partition values first, from its data manifests."""
    ref m = t.metadata
    var scan = t.scan()
    if snapshot >= 0:
        scan = scan.use_snapshot(snapshot)
    out.write('"partitions": [')
    if not scan.has_any_snapshot():
        out.write('], "partitions_total": 0')
        return
    var snap = scan.snapshot()
    var cache = ManifestCache()
    var parts = List[_Part]()
    var index = Dict[String, Int]()
    for mf in read_manifest_list_io_cached(t.io, snap.manifest_list, cache):
        if mf.content != MANIFEST_CONTENT_DATA:
            continue
        var man = read_manifest_io(t.io, mf.manifest_path, mf)
        ref spec = man.partition_spec
        for e in man.entries:
            if e.status == STATUS_DELETED:
                continue
            ref df = e.data_file
            if df.content != CONTENT_DATA:
                continue
            var key = String("{")
            for k in range(min(len(spec.fields), len(df.partition))):
                key.write(
                    ", " if k else "",
                    json_quote(spec.fields[k].name),
                    ": ",
                    df.partition[k].to_json(),
                )
            key.write("}")
            var at = index.get(key, -1)
            if at < 0:
                at = len(parts)
                index[key] = at
                parts.append(_Part(key))
            ref p = parts[at]
            p.rows += df.record_count
            p.files += 1
            p.bytes += df.file_size_in_bytes
            var sid = (
                e.snapshot_id if e.has_snapshot_id else mf.added_snapshot_id
            )
            var i = m.snapshot_index(sid)
            var ms = m.snapshots[i].timestamp_ms if i >= 0 else Int64(-1)
            if ms > p.last_ms or p.last < 0:
                p.last = sid
                p.last_ms = ms
    # Newest values first: the keys compare as text, which orders dates,
    # and numbers of a width, as the values do.
    var order = List[Int]()
    for i in range(len(parts)):
        order.append(i)
    _sort_text_desc(order, parts)
    for n in range(min(len(order), PARTITIONS_SHOWN)):
        ref p = parts[order[n]]
        out.write(
            ", " if n else "",
            '{"partition": ',
            p.key,
            ', "rows": ',
            p.rows,
            ', "files": ',
            p.files,
            ', "bytes": ',
            p.bytes,
            ', "last_snapshot": ',
            p.last,
            ', "last_at": ',
            json_quote(iso_ms(p.last_ms)) if p.last_ms >= 0 else String("null"),
            "}",
        )
    out.write('], "partitions_total": ', len(parts))


def _sort_text_desc(mut order: List[Int], parts: List[_Part]):
    """Insertion sort of `order` by the parts' keys, descending; a table's
    partitions are few enough (`PARTITIONS_SHOWN` shown)."""
    for i in range(1, len(order)):
        var v = order[i]
        var j = i - 1
        while j >= 0 and _num_aware_less(parts[order[j]].key, parts[v].key):
            order[j + 1] = order[j]
            j -= 1
        order[j + 1] = v


def _num_aware_less(a: String, b: String) -> Bool:
    """`a` before `b` as text, but a shorter run of digits is the smaller
    number (`{"lead": 6}` before `{"lead": 24}`)."""
    var x = a.as_bytes()
    var y = b.as_bytes()
    var i = 0
    var j = 0
    while i < len(x) and j < len(y):
        var dx = _digits(x, i)
        var dy = _digits(y, j)
        if dx > 0 and dy > 0:
            if dx != dy:
                return dx < dy
            for k in range(dx):
                if x[i + k] != y[j + k]:
                    return x[i + k] < y[j + k]
            i += dx
            j += dy
            continue
        if x[i] != y[j]:
            return x[i] < y[j]
        i += 1
        j += 1
    return len(x) - i < len(y) - j


def _digits(b: Span[Byte, _], at: Int) -> Int:
    var n = 0
    while at + n < len(b) and b[at + n] >= 48 and b[at + n] <= 57:
        n += 1
    return n


# ── the command ──────────────────────────────────────────────────────────────


def data_json(a: DataAsk) raises -> String:
    """What `morca_maintain data` prints for `a`."""
    var t = load_dataset(a.dataset)
    var out = String('{"dataset": ', json_quote(a.dataset), ", ")
    if a.csv:
        csv_json(t, a, out)
        out.write("}")
        return out^
    table_json(t, a.snapshot, out)
    if a.partitions:
        out.write(", ")
        partitions_json(t, a.snapshot, out)
    if a.rows:
        out.write(", ")
        rows_json(t, a, out)
    if a.chart_x:
        out.write(", ")
        try:
            chart_json(t, a, out)
        except e:
            out.write('"chart": {"error": ', json_quote(String(e)), "}")
    out.write("}")
    return out^


def data_main(args: List[String]) raises:
    """`morca_maintain data ...`: the JSON on stdout, or on a mistake in
    what was asked for (a column the table hasn't, a value of the wrong
    type) `{"error": ...}` and exit 0, since the UI shows it."""
    var a = parse_ask(args)
    try:
        print(data_json(a))
    except e:
        var msg = String(e)
        print(String('{"error": ', json_quote(msg), "}"))
