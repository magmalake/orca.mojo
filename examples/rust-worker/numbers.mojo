"""The proxy pipeline of examples/rust-worker: a Mojo pipeline whose step
runs the Rust worker in src/lib.rs (docs/rust-workers.md).

The pipeline declares what any pipeline does: partitions, the dataset the
step writes (partitioned by its `partition` column), and the table's schema, from schemas/numbers.json, the file
the worker embeds as well (`include_str!`). This side embeds it at build
time (scripts/embed-json.sh writes the `numbers_schema` module), so the
binary needs no file next to it:

    cargo build --release --manifest-path examples/rust-worker/Cargo.toml
    scripts/embed-json.sh examples/rust-worker/schemas/numbers.json NUMBERS_SCHEMA \\
        > build/rust-worker/numbers_schema.mojo
    mojo build -I src -I build/rust-worker ... examples/rust-worker/numbers.mojo
    MORCA_WAREHOUSE=/tmp/wh NUMBERS_LIB=$PWD/build/cargo/release/libnumbers_worker.so \\
        ./numbers run --partitions 2024-01

    numbers check [run]   the table's rows, the snapshots of `run`, all
                          snapshots, the sum of ids

What tests/rust_worker.sh sets: NUMBERS_LIB, the library (by default
`numbers_worker`, found as `libnumbers_worker.so` in `$CONDA_PREFIX/lib`,
next to the binary or in its `../lib`); NUMBERS_PARAMS, the worker's
parameters; NUMBERS_STOP_FILE, a file whose existence stops the worker as
`morca.outdated()` would; and NUMBERS_CRASH_ONCE, a file the step creates
before it aborts after its commit, once.
"""

from std.os import abort, getenv
from std.os.path import exists
from std.sys import argv

from morca import (
    Ctx,
    Mapped,
    Month,
    Monthly,
    Pipeline,
    Step,
    morca_main,
    outdated,
)
from morca.rust_step import rust_step
from morca.tables import (
    PartitionField,
    PartitionSpec,
    Schema,
    ensure_table,
    load_dataset,
    parse_transform,
)

from numbers_schema import NUMBERS_SCHEMA

comptime NUMBERS = "demo.numbers"


def _stop() -> Bool:
    """Stop when a newer version leads, the default, or when asked."""
    var f = getenv("NUMBERS_STOP_FILE")
    return outdated() or (Bool(f) and exists(f))


def by_partition() raises -> PartitionSpec:
    """The table's spec: the worker's files hold one partition each."""
    return PartitionSpec(
        0,
        [
            PartitionField.single(
                2, 1000, "partition", parse_transform("identity")
            )
        ],
    )


def numbers(mut ctx: Ctx, m: Month) raises -> Int:
    var t = ensure_table(
        ctx, NUMBERS, Schema.parse(NUMBERS_SCHEMA), by_partition()
    )
    var r = rust_step(
        ctx,
        getenv("NUMBERS_LIB", "numbers_worker"),
        t,
        params=getenv("NUMBERS_PARAMS", "{}"),
        stop_when=_stop,
    )
    ctx.log(
        String(
            "numbers: ",
            r.rows,
            " rows, ",
            r.worker_files,
            " files of its own, snapshot ",
            r.snapshot_id,
            ", stopped=",
            r.stopped,
            ", resumed=",
            r.resumed,
        )
    )
    var crash = getenv("NUMBERS_CRASH_ONCE")
    if crash and not exists(crash):
        with open(crash, "w") as f:
            f.write("crashed\n")
        abort("numbers: crashing after the commit, as asked")
    return r.rows


comptime Numbers = Step[
    "numbers", Month, Int, numbers, Mapped, writes=[NUMBERS]
]
comptime NumbersPipeline = Pipeline[
    "numbers", Numbers, partitions=Monthly["2024-01"], run_retries=0
]


def _check(run: String) raises:
    var t = load_dataset(NUMBERS)
    var rows = 0
    var ids = Int64(0)
    for b in t.scan().to_batches():
        rows += b.num_rows
        var col = b.column_i64(0)
        for v in col[0]:
            ids += v
    var snaps = 0
    for s in t.metadata.snapshots:
        if run and s.summary.get("orca.run", "") == run:
            snaps += 1
    print("rows", rows, "snapshots", snaps)
    print("all snapshots", len(t.metadata.snapshots))
    print("ids", ids)


def main() raises:
    var args = argv()
    if len(args) >= 2 and String(args[1]) == "check":
        _check(String(args[2]) if len(args) >= 3 else String())
        return
    morca_main[NumbersPipeline]()
