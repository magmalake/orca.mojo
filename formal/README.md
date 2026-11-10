# orca/formal

Lean 4 models of orca properties, and tests that hold the models to the
Mojo code. The layout and conventions follow
[flare's `formal/`](https://github.com/ehsanmok/flare/tree/main/formal):
core Lean only (no Mathlib), and what the environment is trusted to do is
written as `Prop` hypotheses, not `axiom`s.

## The properties

**A retried step never double-commits** (`Orca/Retry`). A step's attempt
looks through the table's snapshots for its own commit before committing
(`orca.tables.find_commit`, by `Ctx.made`), so an attempt that committed and
was killed before Postgres heard of it is recognised by its retry.

**A retry reads what the first attempt read** (`Orca/Pins`). The first time
an attempt reads a table, `Ctx.pin` takes the snapshot an earlier attempt
read, or else the current one, and records it before answering; a new
attempt starts from the store's `pinned_reads`.

| Theorem | Says |
|---|---|
| `no_double_commit` | In every reachable state, at most one snapshot in the table is the step's commit number `n`. |
| `retry_records_its_own_commit` | Every commit on record in Postgres names a snapshot in the table that is the step's commit of that number. |
| `Pins.retry_reads_same_snapshot` | Any two answers `Ctx.pin` ever gave for one table, in any attempts of the step, are the same snapshot. |

Each holds for every environment that meets its assumptions, whatever the
order of events: Postgres writes and commits that fail or succeed, crashes
from any state, and other writers' commits to the same tables.

## Assumptions

`Orca/Core/Assumptions.lean`; each is a hypothesis of the theorems that
need it.

| Hypothesis | Says | Taken by |
|---|---|---|
| `IcebergAllOrNothing` | A commit call leaves the table as it was, or adds exactly one snapshot with the summary it was given. | `Retry` |
| `PostgresAtomic` | Recording a row writes nothing, or exactly that row. | `Retry`, `Pins` |
| `CrashLosesOnlyMemory` | A crash, possible from every state, leaves the table and the ledger as they were; the next attempt starts over. | `Retry` |

`no_double_commit` doesn't need `PostgresAtomic` in its model (recognition
reads the table only); it takes it because one invariant carries both
`Retry` theorems. `Orca/Witness.lean` gives an environment that meets all
three. In `Pins` a crash is a step of the model itself: the next attempt
keeps nothing but the ledger.

## What the models leave out

- **One live attempt** (both models)**.** Attempts follow each other; two processes running
  the same step of the same run at once are not modelled. Orca rules that
  out with the run's owner and `Ctx.fence`, which nothing here proves.
- **Other writers never carry this step's identity.** Another snapshot with
  the same pipeline, run, step, partition and commit number would be taken
  for the step's own. Run IDs come from Postgres; that they're unique is
  assumed.
- **One table, recorded runs.** A step's commits to several tables are
  independent in the code (`Ctx.commits_to` counts per table). An in-memory
  run (`run = 0`) is never recognised and isn't modelled.
- **Pins: a snapshot ID is the data.** That the same snapshot holds the same
  rows whenever it is read is Iceberg's, and not in the model. Nor is what a
  step does between reads: `Pipeline.execute` also carries an attempt's
  reads to the next attempt of the same process, which the model covers by
  reading them from the ledger.
- **The attempt's own code is deterministic**: its `n`-th commit is the same
  commit in every attempt. The model says nothing of what a commit holds,
  nor of `replaces=`.

## Layout

| Path | Contents |
|---|---|
| `Orca/Core/Types.lean` | step identity, summaries, snapshots, the ledger, a worker's state |
| `Orca/Core/Assumptions.lean` | the three environment assumptions |
| `Orca/Retry/Model.lean` | `made`, `findCommit`, the step relation, reachable states |
| `Orca/Retry/NoDoubleCommit.lean` | the invariant and the two theorems |
| `Orca/Pins/Model.lean` | `lookup`, `pinsOf`, the step relation for reads |
| `Orca/Pins/SameSnapshot.lean` | the invariant and the theorem |
| `Orca/Witness.lean` | an environment that meets the assumptions |
| `Orca/Audit.lean` | `#print axioms` for the theorems |
| `Oracle/` | `orca-oracle <model>`: each model's own functions run over event sequences |
| `difftest/retry.cases` | `orca-oracle retry`: 363 sequences of up to five events, and the table each leaves |
| `difftest/pins.cases` | `orca-oracle pins`: 3905 sequences of up to five events, and the answers `Ctx.pin` gives |
| `scripts/check.sh` | the gate CI runs |

## Build and check

Lean comes from [elan](https://github.com/leanprover/elan), at the version
in `lean-toolchain`.

```sh
cd formal
lake build                                    # the model, the proofs, the oracle
scripts/check.sh                              # no sorry or axiom; retry.cases is current
lake exe orca-oracle retry > difftest/retry.cases   # after changing an oracle
pixi run -e data test-formal-retry            # from the root: retry.cases through orca.tables.append
pixi run test-formal-pins                     # pins.cases through Ctx.pin and the stores
```

## The differential tests

A proof says nothing if the model isn't the code. Each test replays its
model's cases through the Mojo code and compares what comes out with the
oracle's line.

- `tests/test_formal_retry.mojo`: `orca.tables.append` on a local Iceberg
  table per sequence, with a new `Ctx` for each attempt; compared are the
  table's snapshots (attempt and commit number, in order). Events are `c`
  (the attempt's next commit, start to finish), `x` (a crash; the next
  attempt starts) and `o` (the same step commits for another partition).
- `tests/test_formal_pins.mojo`: `Ctx.pin` over `MemoryStore` (every
  sequence) and `SqliteStore` (up to four events), a run per sequence and a
  new `Ctx` from the store's `pinned_reads` for each attempt; compared are
  the answers. Events are `a`, `b` (the attempt reads the table), `A`, `B`
  (another writer commits to it) and `x`. `PgStore`'s `pinned_reads`, a
  different query, isn't replayed.

The oracles share their models' functions (`findCommit` and `made`;
`lookup`, `pinsOf` and `afterRecord`), but take whole calls as one event.
That each of their events is a run of the model's steps is by reading, not
by proof, and a crash inside a call isn't replayed as such: to the table or
the ledger it is a crash just before or just after.
