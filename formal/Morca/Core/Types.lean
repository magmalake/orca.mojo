/-!
# What the model is about

A step's identity, the snapshots of one Iceberg table, the commits Postgres
has on record, and a worker's memory while it runs one attempt of the step.
-/
namespace Orca

/-- The part of a snapshot's `orca.*` summary that `Ctx.made` compares, minus
the commit number: mirrors src/orca/ctx.mojo#Ctx.made. An in-memory run
(`run = 0`), which is never recognised, is outside the model. -/
structure StepId where
  pipeline : String
  run : Nat
  step : String
  partition : String
  deriving DecidableEq, Repr

/-- A snapshot's summary, as `Ctx.summary` writes it: who committed, in which
attempt, and which of the step's commits to this table it is (`orca.commit`,
counted from 0 within an attempt). -/
structure Summary where
  owner : StepId
  attempt : Nat
  commit : Nat
  deriving DecidableEq, Repr

structure Snapshot where
  id : Nat
  summary : Summary
  deriving DecidableEq, Repr

/-- One table's snapshots, oldest first (`table.metadata.snapshots`). -/
abbrev Table := List Snapshot

/-- A commit on record in Postgres: what `Ctx.committed` has the store write. -/
structure Record where
  attempt : Nat
  commit : Nat
  snapshot : Nat
  deriving DecidableEq, Repr

abbrev Ledger := List Record

/-- Where an attempt is with its next commit. -/
inductive Phase where
  /-- About to look through the table for its commit (`find_commit`). -/
  | looking
  /-- Found none, and is about to commit. -/
  | willCommit
  /-- Committed or found snapshot `id`, and has yet to record it. -/
  | toRecord (id : Nat)
  deriving DecidableEq, Repr

/-- The table, the ledger and the one live attempt's memory: its number and
how many commits it has made or found so far (`Ctx.commits_to`). -/
structure State where
  table : Table
  ledger : Ledger
  attempt : Nat
  done : Nat
  phase : Phase

end Orca
