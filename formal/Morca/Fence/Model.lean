/-!
# Who may commit for a run

A run has one owner, which keeps its heartbeat fresh. Another process takes
the run over (`claim`) only once the heartbeat is older than the stale limit,
and a process commits to Iceberg only right after checking that it still
owns the run and that its heartbeat is younger than half the limit (`fence`).
All of it is by the database's clock.

Mirrors src/morca/state.mojo#State.claim+beat_run+PgStore.fence and
src/morca/ctx.mojo#Ctx.fence.
-/
namespace Morca.Fence

/-- The run's row and the processes about to commit: the database's clock,
the owner, the heartbeat, and for each process the time its fence check
passed, until its commit lands or it gives up. Processes are numbers. -/
structure State where
  now : Nat
  owner : Option Nat
  heartbeat : Option Nat
  fenced : Nat → Option Nat

/-- Whether a run with this heartbeat can be claimed at `now`: it has none,
or it is older than the stale limit (`heartbeat < now() - stale`). -/
def claimable (stale now : Nat) : Option Nat → Bool
  | none => true
  | some h => decide (h + stale < now)

/-- Whether a heartbeat is younger than `half` at `now`
(`heartbeat > now() - stale / 2`). -/
def freshAt (half now : Nat) : Option Nat → Bool
  | none => false
  | some h => decide (now < h + half)

/-- `f` with `p` mapped to `v`. -/
def setF (f : Nat → Option Nat) (p : Nat) (v : Option Nat) : Nat → Option Nat :=
  fun q => if q = p then v else f q

/-- One thing happening. In order: time passes; the owner's heartbeat beats;
a process claims the run; a process's fence check passes; a process's commit
lands, or it gives up or dies. -/
def Step (stale half : Nat) (advance : Nat → Nat → Prop) (s s' : State) : Prop :=
  (∃ t, advance s.now t ∧ s' = { s with now := t })
  ∨ (∃ p, s.owner = some p ∧ s' = { s with heartbeat := some s.now })
  ∨ (∃ q, claimable stale s.now s.heartbeat = true ∧
      s' = { s with owner := some q, heartbeat := some s.now })
  ∨ (∃ p, s.owner = some p ∧ freshAt half s.now s.heartbeat = true ∧
      s' = { s with fenced := setF s.fenced p (some s.now) })
  ∨ (∃ p, s' = { s with fenced := setF s.fenced p none })

/-- Where a run starts: nobody is about to commit. -/
def Initial (s : State) : Prop := ∀ p, s.fenced p = none

inductive Reachable (stale half : Nat) (advance : Nat → Nat → Prop) :
    State → Prop where
  | init {s : State} : Initial s → Reachable stale half advance s
  | step {s s' : State} :
      Reachable stale half advance s → Step stale half advance s s' →
      Reachable stale half advance s'

end Morca.Fence
