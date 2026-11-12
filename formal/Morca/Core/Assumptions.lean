import Morca.Core.Types

/-!
# Trust base: environment assumptions

What the model takes on trust about Iceberg, Postgres and a crash. They are
**not** `axiom`s: each is a `Prop`-valued definition that the theorems take
as an explicit hypothesis, so `#print axioms` stays clean and every
dependency shows in a theorem's signature.
-/
namespace Morca.Assumptions

/-- An Iceberg commit is all-or-nothing: a call with summary `s` either
leaves the table as it was and reports failure, or adds exactly one snapshot
carrying `s` and reports its id. `commit t s t' out` reads "a call on table
`t` can leave `t'` and return `out`". -/
def IcebergAllOrNothing
    (commit : Table → Summary → Table → Option Nat → Prop) : Prop :=
  ∀ t s t' out, commit t s t' out →
    (t' = t ∧ out = none) ∨ ∃ id, t' = t ++ [Snapshot.mk id s] ∧ out = some id

/-- A Postgres transaction is atomic: recording row `r` either writes nothing
and fails, or writes exactly `r` and succeeds. For rows of any kind: the
commits of `Morca/Retry`, the reads of `Morca/Pins`. -/
def PostgresAtomic {α : Type}
    (record : List α → α → List α → Bool → Prop) : Prop :=
  ∀ l r l' ok, record l r l' ok →
    (l' = l ∧ ok = false) ∨ (l' = l ++ [r] ∧ ok = true)

/-- A worker may crash at any point (the model allows a crash from every
state), and a crash loses the worker's memory and nothing else: the table
and the ledger are as they were, and the next attempt starts over, looking
for its first commit. The next attempt's number is left open. -/
def CrashLosesOnlyMemory (crash : State → State → Prop) : Prop :=
  ∀ s s', crash s s' →
    s'.table = s.table ∧ s'.ledger = s.ledger ∧ s'.done = 0 ∧
      s'.phase = Phase.looking

/-- The database's clock doesn't run backwards: `advance a b` reads "the
clock can go from `a` to `b`". Heartbeats are written and compared by the
database's `now()` alone, so no two hosts' clocks are compared. -/
def ClockMonotone (advance : Nat → Nat → Prop) : Prop :=
  ∀ a b, advance a b → a ≤ b

/-- A commit lands at most `half` after the fence check before it, where
`half` is half the stale limit: `lands f t` reads "a commit fenced at `f`
can land at `t`". This is the condition `morca.ctx` states for fencing: a run
can't be claimed during a commit "unless the commit takes that long". -/
def CommitLandsWithin (half : Nat) (lands : Nat → Nat → Prop) : Prop :=
  ∀ f t, lands f t → t ≤ f + half

end Morca.Assumptions
