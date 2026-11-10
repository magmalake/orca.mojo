# orca/formal

A Lean 4 model of one orca property, and a test that holds the model to the
Mojo code. The layout and conventions follow
[flare's `formal/`](https://github.com/ehsanmok/flare/tree/main/formal):
core Lean only (no Mathlib), and what the environment is trusted to do is
written as `Prop` hypotheses, not `axiom`s.

## The property

**A retried step never double-commits.** A step's attempt looks through the
table's snapshots for its own commit before committing
(`orca.tables.find_commit`, by `Ctx.made`), so an attempt that committed and
was killed before Postgres heard of it is recognised by its retry.

| Theorem (`Orca/Retry/NoDoubleCommit.lean`) | Says |
|---|---|
| `no_double_commit` | In every reachable state, at most one snapshot in the table is the step's commit number `n`. |
| `retry_records_its_own_commit` | Every commit on record in Postgres names a snapshot in the table that is the step's commit of that number. |

Both hold for every environment that meets the assumptions below, whatever
the order of events: looks, commits that fail or succeed, Postgres writes
that fail or succeed, crashes from any state, and other writers' commits to
the same table.

## Assumptions

`Orca/Core/Assumptions.lean`; each is a hypothesis of both theorems.

| Hypothesis | Says |
|---|---|
| `IcebergAllOrNothing` | A commit call leaves the table as it was, or adds exactly one snapshot with the summary it was given. |
| `PostgresAtomic` | Recording a commit writes nothing, or exactly that record. |
| `CrashLosesOnlyMemory` | A crash, possible from every state, leaves the table and the ledger as they were; the next attempt starts over. |

`no_double_commit` doesn't need `PostgresAtomic` in this model (recognition
reads the table only); it takes it because one invariant carries both
theorems. `Orca/Witness.lean` gives an environment that meets all three.

## What the model leaves out

- **One live attempt.** Attempts follow each other; two processes running
  the same step of the same run at once are not modelled. Orca rules that
  out with the run's owner and `Ctx.fence`, which nothing here proves.
- **Other writers never carry this step's identity.** Another snapshot with
  the same pipeline, run, step, partition and commit number would be taken
  for the step's own. Run IDs come from Postgres; that they're unique is
  assumed.
- **One table, recorded runs.** A step's commits to several tables are
  independent in the code (`Ctx.commits_to` counts per table). An in-memory
  run (`run = 0`) is never recognised and isn't modelled.
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
| `Orca/Witness.lean` | an environment that meets the assumptions |
| `Orca/Audit.lean` | `#print axioms` for the theorems |
| `Oracle/Main.lean` | `orca-oracle`: the model's `findCommit` run over event sequences |
| `difftest/retry.cases` | the oracle's output: 363 sequences of up to five events, and the table each leaves |
| `scripts/check.sh` | the gate CI runs |

## Build and check

Lean comes from [elan](https://github.com/leanprover/elan), at the version
in `lean-toolchain`.

```sh
cd formal
lake build                                    # the model, the proofs, the oracle
scripts/check.sh                              # no sorry or axiom; retry.cases is current
lake exe orca-oracle > difftest/retry.cases   # after changing the oracle
pixi run -e data test-formal-retry            # from the root: the cases through orca.tables.append
```

## The differential test

A proof says nothing if the model isn't the code. `tests/test_formal_retry.mojo`
replays each sequence in `difftest/retry.cases` through `orca.tables.append`
on a local Iceberg table of its own, with a new `Ctx` for each attempt, and
compares the table's snapshots (attempt and commit number, in order) with
the oracle's. Events are `c` (the attempt's next commit, start to finish),
`x` (a crash; the next attempt starts) and `o` (the same step commits for
another partition).

The oracle shares `findCommit` and `made` with the proofs, but takes whole
commits as one event. That each of its events is a run of the model's steps
is by reading, not by proof, and a crash inside `append` isn't replayed as
such: to the table it is `x` or `c` then `x`.
