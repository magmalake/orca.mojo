"""Iceberg tables from steps: pinned reads, and commits a retry recognises.

```mojo
from morca.tables import append, scan

def load(mut ctx: Ctx, raw: RawFile) raises -> Loaded:
    var src = Table.load_local(raw.table)
    var batches = scan(ctx, src).to_batches()
    var dst = Table.load_local(OUT)
    var id = append(ctx, dst, transform(batches))
    return Loaded(raw.month, id)
```

- **`scan`** reads a table at a pinned snapshot: the one this step's first
  attempt read, or its current snapshot if this is the first attempt, which
  is then recorded. A retry or a resume of the step sees the same data,
  whatever was committed in between.
- **`append`** and **`commit`** put `morca.*` properties in the summary of the
  snapshot they commit (`Ctx.summary`): pipeline, run, step, partition,
  attempt, commit number, code, and the snapshots read. Before writing
  anything, they look through the table's snapshots for one this step
  already made in this run (`Ctx.made`). If an attempt committed and was
  killed before Postgres heard of it, the retry finds that snapshot and
  returns it instead of committing the data a second time.

A table is known by its location, `metadata.location`, and, when it's in
the warehouse, by its dataset name. `MORCA_WAREHOUSE` is the root of a
filesystem catalog: dataset `nyc.trips` is the table at
`<warehouse>/nyc/trips`, where the warehouse is a directory or a location
in object storage (`s3://bucket/warehouse`, as iceberg's `FileIO` reads it,
from the `AWS_*` environment). **`table`** loads a dataset the step declared, and
**`ensure_table`** creates one it writes if it's missing, and adds the
columns the step's schema has and the table lacks. `scan` refuses a
dataset the step didn't declare in `reads=`, and `append` and `commit` one it
didn't declare in `writes=`. A table outside the warehouse (or with
`MORCA_WAREHOUSE` unset) is used by location as before, and belongs to no
dataset.

**Files written on a worker.** A dista task can write a table's data
files itself and send back only their entries, so the step commits
metadata rather than receiving the rows:

```mojo
def write_hour(day: String, hour: Int) raises -> DataFiles:
    # On a worker: no Ctx, nothing checked or recorded.
    return write_files(load_dataset(EVENTS), fetch(day, hour))

comptime Hour = Task2[String, Int, DataFiles, write_hour]

def load_day(mut ctx: Ctx, d: Day) raises -> Int64:
    var t = ensure_table(ctx, EVENTS, schema())
    if find_commit(ctx, t) >= 0:  # a retry: don't write the files again
        return append_files(ctx, t, DataFiles())
    ref rt = ctx.runtime[Tasks[Hour]]()
    var got = rt.get(rt.remote[Hour](String(d), 0))
    return append_files(ctx, t, got)
```

- **`write_files`** (or a **`FileWriter`**, a batch at a time) writes
  batches into a table's `data/` without committing them, and returns their
  entries as **`DataFiles`**, a dista `Codec` over iceberg's
  `DataFile.encode`. A struct holding a `DataFiles` and plain fields (row
  counts, timings) crosses processes by dista's reflection, with no codec
  of its own.
- **`append_files`** commits entries written elsewhere as one `append`
  snapshot, as `append` commits the files it writes: the same `writes=`
  check, summary and recognition. A retry that finds its commit returns it
  and commits nothing; a step checks `find_commit` first, as above, so a
  retry doesn't have the workers write the files again. Files written by an
  attempt that died before committing them are left unreferenced in
  `data/`, as with an interrupted `append`.

**Replacing partitions.** A new run of a key (`morca backfill --rerun`)
isn't a retry: `find_commit` doesn't recognise an earlier run's commits, so
`append` adds the key's rows a second time. A step that recomputes a key's
rows declares the dataset in `replaces=` rather than `writes=`, and its
`append` and `append_files` to it then replace:

```mojo
comptime LoadDay = Step[
    "load_day", Day, Int64, load_day, Mapped, replaces=[EVENTS],
]
```

- The run's first commit to a partition is an `overwrite` that removes the
  files this run didn't commit from the partitions its new files land in,
  and adds them; its later commits find only the run's files there and
  append. A retry recognises its commits as before, so nothing is
  overwritten twice or lost.
- Another writer's file in such a partition, added after the run's first
  commit to the table, means two runs are replacing it at once: the commit
  raises, committing nothing, and only one run keeps its rows.
- `replace_partitions` and `replace_partition_files` do the same for one
  call, to any table. The summary has `orca.replaced-files` and
  `orca.replaced-records`.

`commit` refuses to remove files that aren't all in the current snapshot,
on every try: a removal planned before another writer replaced them
would otherwise keep both writers' rows.

This module imports iceberg, so only pipelines that use tables build it;
`morca` itself doesn't.
"""

from std.os import getenv
from std.random import random_float64
from std.time import sleep

from dista import Codec
from iceberg.append import AppendResult, prepare_append
from iceberg.catalog.filesystem import FilesystemCatalog, Table
from iceberg.commit import FileChanges, prepare_commit
from iceberg.io import FileIO
from iceberg.expressions import FieldSummary
from iceberg.manifest import (
    CONTENT_DATA,
    DataFile,
    ManifestFile,
    decode_data_files,
    encode_data_files,
    read_manifest_io,
    read_manifest_list_io,
)
from iceberg.metadata import TableMetadata
from iceberg.scan import TableScan
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec
from iceberg.update_schema import (
    SchemaCommit,
    UpdateSchema,
    prepare_schema_update,
)
from iceberg.util import now_ms
from iceberg.values import Datum, compare, datum_from_bytes_prim
from iceberg.write import WriteOptions, _partition_key, write_data_files
from parquet import RecordBatch

from morca.ctx import Ctx
from morca._internal.proc import absolute

comptime _COMMIT_RETRIES = 8
"""How many times to reload and retry when another writer got there first."""


def _backoff(attempt: Int):
    """Waits before retry `attempt` (1, 2, ...) of a commit that clashed:
    10 ms doubling with each, to 640 ms, times a random half to one and a
    half. Two writers that both wrote the same metadata version both back
    out (iceberg's filesystem catalog deletes both files), so retrying at
    once can clash again and again; the random wait lets one get ahead.
    Each process seeds `std.random` itself (`morca._internal.cli.seed_random`).
    """
    var base = 0.01 * Float64(1 << min(attempt - 1, 6))
    sleep(base * random_float64(0.5, 1.5))


def _reload(mut table: Table) raises:
    """`table.refresh()`, tried again when the metadata file it chose is
    gone: writers that clashed on a version delete the files they wrote, so
    a reader that listed one a moment before finds it missing."""
    var attempt = 0
    while True:
        try:
            table.refresh()
            return
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise Error(
                    "morca: can't reload ", table.metadata.location, ": ", e
                )
            _backoff(attempt)


def _load(cat: FilesystemCatalog, ns: String, name: String) raises -> Table:
    """`cat.load_table`, tried again as `_reload` is."""
    var attempt = 0
    while True:
        try:
            return cat.load_table(ns, name)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise Error("morca: can't load ", ns, ".", name, ": ", e)
            _backoff(attempt)


def _landed(table: Table, snapshot_id: Int64) -> Bool:
    """Whether a commit that raised is in `table` after all. With
    iceberg-mojo 0.11.0 and earlier, another writer could load a clashing
    writer's metadata file before it was deleted and commit on top of it,
    so retrying appended the same rows twice (#164). That iceberg race
    can also drop a commit that won, which this can't see; the fix is
    iceberg.mojo#88."""
    for s in table.metadata.snapshots:
        if s.snapshot_id == snapshot_id:
            return True
    return False


def warehouse() -> String:
    """`MORCA_WAREHOUSE`: the root datasets are found under, or empty."""
    return getenv("MORCA_WAREHOUSE")


def _norm(path: String) raises -> String:
    """A location without its trailing slashes; a local one absolute."""
    var p = String(path.removeprefix("file://"))
    while p.endswith("/") and p.byte_length() > 1:
        var cut = String(p[byte = : p.byte_length() - 1])
        p = cut^
    if "://" in p:
        return p^
    return absolute(p)


def dataset_of(location: String) raises -> String:
    """The dataset a table at `location` is, `namespace.table`, or empty if
    it isn't directly under a namespace in the warehouse."""
    var root = warehouse()
    if not root:
        return ""
    var base = String(_norm(root), "/")
    var loc = _norm(location)
    if not loc.startswith(base):
        return ""
    var rel = loc[byte = base.byte_length() :]
    var parts = rel.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return ""
    return String(parts[0], ".", parts[1])


def _split(dataset: String) raises -> Tuple[String, String]:
    var dot = dataset.rfind(".")
    if dot <= 0 or dot == dataset.byte_length() - 1:
        raise Error("morca: a dataset is named namespace.table, not ", dataset)
    return (
        String(dataset[byte=:dot]),
        String(dataset[byte = dot + 1 :]),
    )


def _catalog() raises -> FilesystemCatalog:
    """The warehouse's filesystem catalog: local, or in object storage
    (`s3://bucket/prefix`, through objectstore, configured from the
    `AWS_*` environment)."""
    var root = warehouse()
    if not root:
        raise Error(
            "morca: MORCA_WAREHOUSE isn't set, so datasets can't be found"
        )
    if "://" in root and not root.startswith("file://"):
        return FilesystemCatalog(String(root.rstrip("/")), FileIO())
    return FilesystemCatalog.local(root)


def load_dataset(dataset: String) raises -> Table:
    """A dataset's table, outside any step: nothing is checked or recorded.
    For reading what isn't a step's input, such as a keyed pipeline's keys
    (`morca.keyed`)."""
    var name = _split(dataset)
    try:
        return _catalog().load_table(name[0], name[1])
    except e:
        raise Error("morca: can't load ", dataset, ": ", e)


def table(mut ctx: Ctx, dataset: String) raises -> Table:
    """The table of a dataset the step declared it reads or writes."""
    if dataset not in ctx.declared_writes:
        ctx.check_declared(dataset, write=False)
    var name = _split(dataset)
    return _load(_catalog(), name[0], name[1])


comptime DAY_MS: Int64 = 86_400_000
comptime DEFAULT_HISTORY_DAYS = 30
"""How long a morca table keeps its snapshots by default."""


def table_defaults() -> Dict[String, String]:
    """The properties `ensure_table` gives every table it creates, and adds
    to one that lacks them.

    - `write.metadata.delete-after-commit.enabled=true` and
      `write.metadata.previous-versions-max=100`: each commit deletes the
      metadata files older than the last hundred, so `metadata/` stops
      growing by one file per commit.
    - `history.expire.max-snapshot-age-ms` (30 days): the history the
      scheduler's expiry keeps (`morca expire`, `MORCA_EXPIRE_HOURS`). Its
      presence is also what makes the scheduler expire the table at all;
      a table morca didn't create keeps all its snapshots.
    """
    var d = Dict[String, String]()
    d["write.metadata.delete-after-commit.enabled"] = "true"
    d["write.metadata.previous-versions-max"] = "100"
    d["history.expire.max-snapshot-age-ms"] = String(
        DAY_MS * DEFAULT_HISTORY_DAYS
    )
    return d^


def _wanted_properties(
    have: Dict[String, String], given: Dict[String, String]
) -> Dict[String, String]:
    """What to set: the defaults a table lacks, and what `given` says
    differently from it."""
    var out = Dict[String, String]()
    for e in table_defaults().items():
        if e.key not in have and e.key not in given:
            out[e.key] = e.value
    for e in given.items():
        if have.get(e.key, "\x00") != e.value:
            out[e.key] = e.value
    return out^


def _settle_properties(
    mut ctx: Ctx,
    mut table: Table,
    given: Dict[String, String],
    dataset: String,
) raises:
    """Commits the properties `_wanted_properties` finds missing, reloading
    and trying again when another writer commits first."""
    var attempt = 0
    while True:
        var want = _wanted_properties(table.metadata.properties, given)
        if len(want) == 0:
            return
        var m = table.metadata.copy()
        var names = String()
        for e in want.items():
            m.properties[e.key] = e.value
            names.write(", " if names else "", e.key, "=", e.value)
        m.last_updated_ms = now_ms()
        try:
            table.commit_metadata(m^)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise Error("morca: can't set ", dataset, "'s properties: ", e)
            _backoff(attempt)
            _reload(table)
            continue
        ctx.log(String("set ", dataset, " properties: ", names))
        return


def ensure_table(
    mut ctx: Ctx,
    dataset: String,
    schema: Schema,
    spec: PartitionSpec = PartitionSpec.unpartitioned(),
    properties: Dict[String, String] = Dict[String, String](),
) raises -> Table:
    """The table of a dataset the step writes, created empty with `schema`
    and `spec` if the warehouse doesn't have it yet.

    A table that exists is brought up to `schema`: the columns `schema` has
    and the table lacks, matched by name at any depth, are added in one
    schema commit, and a column `schema` makes optional becomes optional.
    Rows already in the table read the new columns as null. Columns the
    table has and `schema` doesn't are kept, and written as null.

    It refuses, with the dataset in the message, what that can't do: a
    column whose type differs, a new column marked required, a required
    column `schema` leaves out, and a field id in `schema` that isn't the
    id the table gave that column, since batches built from `schema` carry
    its ids and a column is matched by id first. A new column's id should be
    the next one up: the table numbers new fields from its highest id, in
    the order `schema` lists them, a struct's fields after the struct.
    `spec` is only used to create the table.

    `properties` are Iceberg table properties: a table is created with
    them on top of `table_defaults()`, and an existing one gets those it
    lacks or has with another value (one metadata commit; none when all
    are as given). `history.expire.max-snapshot-age-ms` sets how much
    history the scheduler's expiry keeps.
    """
    ctx.check_declared(dataset, write=True)
    var name = _split(dataset)
    var cat = _catalog()
    if not cat.table_exists(name[0], name[1]):
        var props = table_defaults()
        for e in properties.items():
            props[e.key] = e.value
        try:
            _ = cat.create_table(name[0], name[1], schema, spec, props^)
        except e:
            # Another run, started at the same time, created it first.
            if not cat.table_exists(name[0], name[1]):
                raise Error("morca: can't create ", dataset, ": ", e)
    var t = _load(cat, name[0], name[1])
    _evolve(ctx, t, schema, dataset)
    _settle_properties(ctx, t, properties, dataset)
    return t^


def _named(s: Schema, name: String) -> Int:
    """Position in `s.flat` of the field named exactly `name`, or -1."""
    for k in range(len(s.flat)):
        if s.flat[k].name == name:
            return k
    return -1


def _required_left_out(table: Schema, given: Schema) -> String:
    """A required field of `table` that `given` leaves out, or "". A field
    under a struct `given` leaves out as a whole doesn't count: the struct
    is optional or it is reported itself."""
    for k in range(len(table.flat)):
        ref f = table.flat[k]
        if not f.required or f.child_pos < 0:
            continue
        if _named(given, f.name) >= 0:
            continue
        if f.parent >= 0 and _named(given, table.flat[f.parent].name) < 0:
            continue
        return f.name
    return ""


def _other_id(table: Schema, given: Schema) -> String:
    """Why a field of `given` has another id in `table`, or ""."""
    for k in range(len(given.flat)):
        ref g = given.flat[k]
        var at = _named(table, g.name)
        if at >= 0 and table.flat[at].id != g.id:
            return String(
                "'",
                g.name,
                "' is field ",
                table.flat[at].id,
                " in the table but ",
                g.id,
                " in the schema given; give it id ",
                table.flat[at].id,
                ", or batches built from this schema land in the wrong",
                " column",
            )
    return ""


def _evolve(
    mut ctx: Ctx, mut table: Table, schema: Schema, dataset: String
) raises:
    """Adds to `table` what `schema` has and it lacks (see `ensure_table`),
    reloading and trying again when another writer commits first."""
    var attempt = 0
    while True:
        var before = table.metadata.schema()
        var left_out = _required_left_out(before, schema)
        if left_out:
            raise Error(
                "morca: ",
                dataset,
                " requires column '",
                left_out,
                "', which the schema given leaves out; a pipeline can add",
                " columns or make them optional, not drop a required one",
            )
        var update = UpdateSchema()
        update.union_by_name(schema)
        var change: SchemaCommit
        try:
            change = prepare_schema_update(table.metadata, update)
        except e:
            raise Error(
                "morca: ", dataset, " can't take the schema given: ", String(e)
            )
        # Checked against the schema the commit would make, so a schema
        # with the wrong ids is refused before anything is committed.
        var mismatch = _other_id(change.schema, schema)
        if mismatch:
            raise Error("morca: ", dataset, ": ", mismatch)
        if change.changed:
            try:
                table.commit_metadata(change.metadata.copy())
            except e:
                attempt += 1
                if attempt > _COMMIT_RETRIES:
                    raise Error(
                        "morca: can't commit ",
                        dataset,
                        "'s new schema (",
                        attempt,
                        " tries): ",
                        e,
                    )
                _backoff(attempt)
                _reload(table)
                continue
            var added = String("")
            var n_added = 0
            for f in change.schema.flat:
                if _named(before, f.name) < 0:
                    added += String(", " if n_added > 0 else "", f.name)
                    n_added += 1
            ctx.log(
                String(
                    "evolved ",
                    dataset,
                    " to schema ",
                    change.schema.schema_id,
                    ", adding ",
                    n_added,
                    " field(s): ",
                    added,
                )
            )
        return


def scan(mut ctx: Ctx, table: Table) raises -> TableScan:
    """A scan of `table` at this step's pinned snapshot.

    A table that had no snapshot when it was first read stays empty for the
    step: the scan sees no files even if some were committed since.
    """
    var m = table.metadata.copy()
    var dataset = dataset_of(m.location)
    ctx.check_declared(dataset, write=False)
    var current = m.current_snapshot_id if m.has_current_snapshot else -1
    var id = ctx.pin(m.location, current, dataset)
    if id < 0:
        m.has_current_snapshot = False
        m.current_snapshot_id = -1
        return TableScan(m^, table.io.copy())
    return table.scan().use_snapshot(id)


def find_commit(ctx: Ctx, table: Table) -> Int64:
    """The snapshot this step already committed as its next commit to
    `table` in this run, or -1.

    `append` and `commit` check this themselves. A step that writes files
    before committing them, such as a compaction, checks it first, so a retry
    of an attempt that committed doesn't write the files again.
    """
    var loc = table.metadata.location
    for s in table.metadata.snapshots:
        if ctx.made(loc, s.summary):
            return s.snapshot_id
    return -1


struct DataFiles(Codec, Copyable, Movable, Sized):
    """Data file entries, written but not committed, as a dista task returns
    them: `encode`/`decode` are iceberg's `encode_data_files` /
    `decode_data_files`, which carry every field of each entry."""

    var files: List[DataFile]

    def __init__(out self):
        self.files = List[DataFile]()

    def __init__(out self, var files: List[DataFile]):
        self.files = files^

    def __len__(self) -> Int:
        return len(self.files)

    def add(mut self, other: DataFiles):
        """Appends `other`'s entries, e.g. from another task."""
        for f in other.files:
            self.files.append(f.copy())

    def encode(self, mut out: List[UInt8]):
        encode_data_files(self.files, out)

    @staticmethod
    def decode(buf: Span[UInt8, _], mut pos: Int) raises -> Self:
        return Self(decode_data_files(buf, pos))


struct FileWriter(Movable):
    """Writes batches into a table's data files as they come, without
    committing them, and keeps their entries. For a worker that produces a
    table's rows a batch at a time; `write_files` writes a list at once.

    It holds the table's schema, spec, sort order and write properties as
    they were when it was made: a schema change committed meanwhile isn't
    seen, and `append_files` commits the entries all the same.
    """

    var io: FileIO
    var location: String
    var schema: Schema
    var spec: PartitionSpec
    var sort_order_id: Int
    var options: WriteOptions
    var files: DataFiles
    var rows: Int
    """The rows written so far."""

    def __init__(out self, table: Table) raises:
        self.io = table.io.copy()
        self.location = table.metadata.location
        self.schema = table.metadata.schema()
        self.spec = table.metadata.spec()
        self.sort_order_id = table.metadata.default_sort_order_id
        self.options = WriteOptions.from_properties(table.metadata.properties)
        self.files = DataFiles()
        self.rows = 0

    def write(mut self, batches: List[RecordBatch]) raises:
        """Writes `batches`: a file per partition they hold."""
        for b in batches:
            self.rows += b.num_rows
        var written: List[DataFile]
        try:
            written = write_data_files(
                self.io,
                self.location,
                batches,
                self.schema,
                self.spec,
                self.sort_order_id,
                self.options,
            )
        except e:
            var name = dataset_of(self.location)
            raise Error(
                "morca: can't write data files for ",
                name if name else self.location,
                ": ",
                e,
            )
        for i in range(len(written)):
            self.files.files.append(written[i].copy())

    def put(mut self, var batch: RecordBatch) raises:
        """Writes one batch."""
        var one = List[RecordBatch]()
        one.append(batch^)
        self.write(one)

    def finish(deinit self) -> DataFiles:
        """The entries of every file written."""
        return self.files^


def write_files(table: Table, batches: List[RecordBatch]) raises -> DataFiles:
    """Writes `batches` into `table`'s data files without committing them,
    and returns their entries, for `append_files` to commit, here or in
    another process. Nothing is checked or recorded: it's for a dista task,
    which has no `Ctx`, with the table from `load_dataset`."""
    var w = FileWriter(table)
    w.write(batches)
    return w^.finish()


def _found(mut ctx: Ctx, mut table: Table) raises -> Int64:
    """What `append`, `append_files` and `commit` do first: reload `table`,
    refuse a dataset the step didn't declare in `writes=`, and return the
    snapshot an earlier attempt committed (recorded again), or -1."""
    _reload(table)
    var loc = table.metadata.location
    var dataset = dataset_of(loc)
    ctx.check_declared(dataset, write=True)
    var found = find_commit(ctx, table)
    if found >= 0:
        ctx.log(String("found its commit ", found, " to ", loc))
        ctx.committed(loc, found, dataset)
    return found


def append(
    mut ctx: Ctx, mut table: Table, batches: List[RecordBatch]
) raises -> Int64:
    """Writes `batches` to `table` as one `append` snapshot and returns its ID,
    or returns the snapshot an earlier attempt of this step committed.

    To a dataset the step declared in `replaces=`, it's `replace_partitions`:
    the partitions the rows land in lose what earlier runs left there."""
    return _append(ctx, table, batches, ctx.replaces(_dataset(table)))


def _dataset(table: Table) -> String:
    try:
        return dataset_of(table.metadata.location)
    except:
        return ""


def _append(
    mut ctx: Ctx, mut table: Table, batches: List[RecordBatch], replace: Bool
) raises -> Int64:
    var found = _found(ctx, table)
    if found >= 0:
        return found
    var files: List[DataFile]
    try:
        files = write_data_files(
            table.io,
            table.metadata.location,
            batches,
            table.metadata.schema(),
            table.metadata.spec(),
            table.metadata.default_sort_order_id,
            WriteOptions.from_properties(table.metadata.properties),
        )
    except e:
        raise Error(
            "morca: can't write data files for ",
            dataset_of(table.metadata.location),
            ": ",
            e,
        )
    if replace:
        return _commit_replace(ctx, table, files^)
    return _commit_append(ctx, table, files^)


def append_files(
    mut ctx: Ctx, mut table: Table, files: List[DataFile]
) raises -> Int64:
    """Commits data files already written, here or on a worker
    (`write_files`), to `table` as one `append` snapshot and returns its ID,
    or returns the snapshot an earlier attempt of this step committed, as
    `append` does; the files are then not committed again. No files commits
    an empty snapshot, as `append` of no batches does, so a step's commits
    keep their numbers.

    Refuses a dataset the step didn't declare in `writes=`, and an entry
    that isn't a data file (a delete file goes through `commit`).

    To a dataset the step declared in `replaces=`, it's
    `replace_partition_files`.
    """
    return _append_files(ctx, table, files, ctx.replaces(_dataset(table)))


def _append_files(
    mut ctx: Ctx, mut table: Table, files: List[DataFile], replace: Bool
) raises -> Int64:
    for f in files:
        if f.content != CONTENT_DATA:
            raise Error(
                "morca: append_files takes data files, and ",
                f.file_path,
                " is a delete file (content ",
                f.content,
                "); commit it with morca.tables.commit",
            )
    var found = _found(ctx, table)
    if found >= 0:
        return found
    if replace:
        return _commit_replace(ctx, table, files.copy())
    return _commit_append(ctx, table, files.copy())


def append_files(
    mut ctx: Ctx, mut table: Table, files: DataFiles
) raises -> Int64:
    """`append_files` of the entries a task returned."""
    return append_files(ctx, table, files.files)


def _commit_append(
    mut ctx: Ctx, mut table: Table, var files: List[DataFile]
) raises -> Int64:
    """Commits `files` to `table` as an `append` with this step's summary,
    reloading and trying again when another writer commits first."""
    var loc = table.metadata.location
    var dataset = dataset_of(loc)
    var attempt = 0
    while True:
        var result = prepare_append(
            table.io, table.metadata, files.copy(), ctx.summary(loc)
        )
        # Not if this process may have lost its run (`Ctx.fence`).
        ctx.fence()
        try:
            table.commit(result)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise Error(
                    "morca: can't commit to ",
                    dataset,
                    " (",
                    attempt,
                    " tries): ",
                    e,
                )
            _backoff(attempt)
            _reload(table)
            # A commit that landed after all is done, not tried again.
            if not _landed(table, result.snapshot_id):
                continue
        ctx.committed(loc, result.snapshot_id, dataset)
        _reload(table)
        return result.snapshot_id


def commit(
    mut ctx: Ctx,
    mut table: Table,
    operation: String,
    var files: FileChanges,
) raises -> Int64:
    """Commits `files` (data files added and removed, already written) to
    `table` as one snapshot of `operation`, e.g. `replace` for a compaction,
    and returns its ID, or returns the snapshot an earlier attempt of this
    step committed.

    Every file `files` removes must be in the table's current snapshot, on
    the first try and on each retry after another writer committed first:
    if one isn't, another writer removed or replaced it since the caller
    read it, and the commit raises, committing nothing, rather than remove
    the rest and keep both writers' rows. The step's retry plans the change
    again. (`replace_partitions` plans it again itself on each try.)"""
    var found = _found(ctx, table)
    if found >= 0:
        return found
    var loc = table.metadata.location
    var dataset = dataset_of(loc)
    var removing = _distinct(files.removed)
    var attempt = 0
    while True:
        var result = prepare_commit(
            table.io, table.metadata, operation, files.copy(), ctx.summary(loc)
        )
        _check_removed(result, removing, dataset if dataset else loc)
        # Not if this process may have lost its run (`Ctx.fence`).
        ctx.fence()
        try:
            table.commit(result)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise Error(
                    "morca: can't commit to ",
                    dataset,
                    " (",
                    attempt,
                    " tries): ",
                    e,
                )
            _backoff(attempt)
            _reload(table)
            # A commit that landed after all is done, not tried again.
            if not _landed(table, result.snapshot_id):
                continue
        ctx.committed(loc, result.snapshot_id, dataset)
        _reload(table)
        return result.snapshot_id


def _distinct(paths: List[String]) -> Int:
    var seen = Dict[String, Bool]()
    for p in paths:
        seen[p] = True
    return len(seen)


def _summary_int(s: Dict[String, String], key: String) -> Int:
    try:
        return Int(s.get(key, "0"))
    except:
        return 0


def _check_removed(result: AppendResult, removing: Int, name: String) raises:
    """Raises unless the prepared commit removes all `removing` files: the
    ones it doesn't find aren't in the snapshot it was planned on."""
    if removing == 0:
        return
    ref m = result.metadata
    var s = m.snapshots[len(m.snapshots) - 1].summary.copy()
    var found = _summary_int(s, "deleted-data-files") + _summary_int(
        s, "removed-delete-files"
    )
    if found < removing:
        raise Error(
            "morca: ",
            removing - found,
            " of the ",
            removing,
            " files this commit to ",
            name,
            " removes aren't in its current snapshot: another writer removed",
            " or replaced them since they were read. Nothing was committed;",
            " plan the change again (the step's retry does)",
        )


# ── replacing partitions ─────────────────────────────────────────────────────


def replace_partitions(
    mut ctx: Ctx, mut table: Table, batches: List[RecordBatch]
) raises -> Int64:
    """Writes `batches` to `table` as one snapshot that also removes the
    files earlier runs left in the partitions the rows land in, and returns
    its ID, or the snapshot an earlier attempt of this step committed.

    What `append` does to a dataset the step declared in `replaces=`; this
    is for a table outside the warehouse, or a step that replaces one table
    and appends to another it declares in `writes=`.

    - The run's first commit to a partition is an `overwrite` that removes
      every data and delete file in it that this run didn't commit, and
      adds the new files. Later commits of the run to the partition find
      only its own files there and append; files of this run, from any of
      its steps and attempts, are never removed.
    - The partitions are those of the new files under the table's current
      spec: no rows replace nothing. Files written under an older spec are
      left alone. An unpartitioned table is one partition: the run replaces
      the whole table.
    - What to remove is planned against the snapshot each try commits on
      top of, again on every retry after another writer committed first.
    - A file in a partition being replaced that another writer committed
      after this run's first commit to the table means two writers are
      replacing (or writing) the partition at once. The commit raises,
      committing nothing, so only one of them keeps its rows: the other's
      attempt fails, and a rerun of it once the first has finished replaces
      the partition again.

    The snapshot's summary has `orca.replaced-files` and
    `orca.replaced-records`: the data files and rows it removed.
    """
    return _append(ctx, table, batches, True)


def replace_partition_files(
    mut ctx: Ctx, mut table: Table, files: List[DataFile]
) raises -> Int64:
    """`replace_partitions` of data files already written, here or on a
    worker (`write_files`), as `append_files` commits them."""
    return _append_files(ctx, table, files, True)


def replace_partition_files(
    mut ctx: Ctx, mut table: Table, files: DataFiles
) raises -> Int64:
    """`replace_partition_files` of the entries a task returned."""
    return _append_files(ctx, table, files.files, True)


struct _Replaced(Movable):
    """What a replace removes: the paths, and the data files and rows."""

    var removed: List[String]
    var files: Int
    var records: Int64

    def __init__(out self):
        self.removed = List[String]()
        self.files = 0
        self.records = 0


def _within(
    sums: List[FieldSummary],
    spec: PartitionSpec,
    ptype: Schema,
    value: List[Datum],
) raises -> Bool:
    """Whether a manifest with partition summaries `sums` may hold files of
    the partition `value`."""
    if len(value) != len(spec.fields) or len(sums) != len(spec.fields):
        return True
    for k in range(len(spec.fields)):
        ref sum = sums[k]
        ref v = value[k]
        if not v.valid:
            if not sum.contains_null:
                return False
            continue
        if not sum.has_lower or not sum.has_upper:
            return False
        ref tn = ptype.store.nodes[
            ptype.find_field(spec.fields[k].field_id).type
        ]
        var lo = datum_from_bytes_prim(
            tn.prim, tn.precision, tn.scale, tn.length, sum.lower_bound
        )
        var hi = datum_from_bytes_prim(
            tn.prim, tn.precision, tn.scale, tn.length, sum.upper_bound
        )
        if compare(lo, v) > 0 or compare(hi, v) < 0:
            return False
    return True


def _may_hold(
    mf: ManifestFile,
    spec: PartitionSpec,
    ptype: Schema,
    values: List[List[Datum]],
) -> Bool:
    """Whether manifest `mf` may hold files of any of the partitions
    `values`, by its partition summaries: the others aren't read."""
    if len(spec.fields) == 0 or not mf.has_partitions:
        return True
    try:
        for v in values:
            if _within(mf.partitions, spec, ptype, v):
                return True
        return False
    except:
        return True


def _by_whom(m: TableMetadata, at: Int) -> String:
    """Who committed snapshot `at` (an index into `m.snapshots`)."""
    if at < 0:
        return "a snapshot no longer in the table's history"
    ref s = m.snapshots[at].summary
    var run = s.get("orca.run")
    if run:
        return String(
            "run ",
            run.value(),
            " of ",
            s.get("orca.pipeline", "?"),
            ", step ",
            s.get("orca.step", "?"),
        )
    return String("snapshot ", m.snapshots[at].snapshot_id, ", not morca's")


def _plan_replace(
    ctx: Ctx, table: Table, added: List[DataFile]
) raises -> _Replaced:
    """The files a replace adding `added` removes from `table`'s current
    snapshot (see `replace_partitions`), or raises if another writer is
    writing one of its partitions."""
    var out = _Replaced()
    ref m = table.metadata
    if not m.has_current_snapshot or len(added) == 0:
        return out^
    var current = m.current_snapshot()
    if current.manifest_list == "":
        return out^
    var spec = m.spec()
    var ptype = spec.partition_type(m.schema())
    var keys = Dict[String, Bool]()
    var values = List[List[Datum]]()
    for f in added:
        var k = _partition_key(f.partition)
        if k not in keys:
            keys[k] = True
            values.append(f.partition.copy())
    # This run's snapshots, and where its first one is in the history.
    var loc = m.location
    var mine = Dict[Int64, Bool]()
    for c in ctx.commits:
        if c.table == loc:
            mine[c.snapshot_id] = True
    var index = Dict[Int64, Int]()
    var first_mine = -1
    for i in range(len(m.snapshots)):
        ref s = m.snapshots[i]
        index[s.snapshot_id] = i
        if ctx.same_run(s.summary):
            mine[s.snapshot_id] = True
        if s.snapshot_id in mine and first_mine < 0:
            first_mine = i
    var name = dataset_of(loc)
    for mf in read_manifest_list_io(table.io, current.manifest_list):
        if mf.partition_spec_id != spec.spec_id:
            continue
        if not _may_hold(mf, spec, ptype, values):
            continue
        var manifest = read_manifest_io(table.io, mf.manifest_path, mf)
        for e in manifest.entries:
            if not e.is_live():
                continue
            if _partition_key(e.data_file.partition) not in keys:
                continue
            if e.snapshot_id in mine:
                continue
            var at = index.get(e.snapshot_id, -1)
            if first_mine >= 0 and at > first_mine:
                raise Error(
                    "morca: another writer is writing a partition of ",
                    name if name else loc,
                    " that this run replaces: ",
                    _by_whom(m, at),
                    " committed ",
                    e.data_file.file_path,
                    " after this run's first commit to it. Nothing was",
                    " committed; rerun once the other has finished",
                )
            out.removed.append(e.data_file.file_path)
            if e.data_file.content == CONTENT_DATA:
                out.files += 1
                out.records += e.data_file.record_count
    return out^


def _commit_replace(
    mut ctx: Ctx, mut table: Table, var files: List[DataFile]
) raises -> Int64:
    """Commits `files` to `table` replacing their partitions (see
    `replace_partitions`), planned again and tried again when another writer
    commits first."""
    var loc = table.metadata.location
    var dataset = dataset_of(loc)
    var attempt = 0
    while True:
        # Planned against the snapshot this try builds on, never one an
        # earlier try saw: a stale list of files would leave another
        # writer's rows beside these.
        var plan = _plan_replace(ctx, table, files)
        var summary = ctx.summary(loc)
        summary["orca.replaced-files"] = String(plan.files)
        summary["orca.replaced-records"] = String(plan.records)
        var result: AppendResult
        if len(plan.removed) == 0:
            result = prepare_append(
                table.io, table.metadata, files.copy(), summary^
            )
        else:
            var change = FileChanges()
            change.added_data = files.copy()
            change.removed = plan.removed.copy()
            result = prepare_commit(
                table.io, table.metadata, "overwrite", change^, summary^
            )
            _check_removed(
                result, len(plan.removed), dataset if dataset else loc
            )
        # Not if this process may have lost its run (`Ctx.fence`).
        ctx.fence()
        try:
            table.commit(result)
        except e:
            attempt += 1
            if attempt > _COMMIT_RETRIES:
                raise Error(
                    "morca: can't commit to ",
                    dataset,
                    " (",
                    attempt,
                    " tries): ",
                    e,
                )
            _backoff(attempt)
            _reload(table)
            # A commit that landed after all is done, not tried again.
            if not _landed(table, result.snapshot_id):
                continue
        ctx.committed(loc, result.snapshot_id, dataset)
        if len(plan.removed) > 0:
            ctx.log(
                String(
                    "replaced ",
                    plan.files,
                    " files (",
                    plan.records,
                    " rows) of ",
                    dataset if dataset else loc,
                    " that earlier runs committed, in snapshot ",
                    result.snapshot_id,
                )
            )
        _reload(table)
        return result.snapshot_id
