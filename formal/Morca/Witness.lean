import Orca.Core.Assumptions
import Orca.Retry.Model

/-!
# The assumptions can be met

An environment in which every commit and every Postgres write succeeds and a
crash starts the next attempt. It meets all three assumptions, so the
theorems don't hold only because nothing could satisfy their hypotheses, and
it can crash from every state.
-/
namespace Orca

open Orca.Assumptions

/-- The next attempt after a crash. -/
def restart (s : State) : State :=
  { s with attempt := s.attempt + 1, done := 0, phase := Phase.looking }

/-- Commits and writes always succeed; a new snapshot's id is its position. -/
def idealEnv : Env where
  commit t s t' out := t' = t ++ [Snapshot.mk t.length s] ∧ out = some t.length
  record l r l' ok := l' = l ++ [r] ∧ ok = true
  crash s s' := s' = restart s

theorem idealEnv_iceberg : IcebergAllOrNothing idealEnv.commit := by
  intro t s t' out h
  have h' : t' = t ++ [Snapshot.mk t.length s] ∧ out = some t.length := h
  exact Or.inr ⟨t.length, h'.1, h'.2⟩

theorem idealEnv_postgres : PostgresAtomic idealEnv.record := by
  intro l r l' ok h
  have h' : l' = l ++ [r] ∧ ok = true := h
  exact Or.inr h'

theorem idealEnv_crash : CrashLosesOnlyMemory idealEnv.crash := by
  intro s s' h
  have h' : s' = restart s := h
  subst h'
  exact ⟨rfl, rfl, rfl, rfl⟩

/-- A worker may crash at any point: every state has a crash step. -/
theorem idealEnv_crash_anywhere (me : StepId) (s : State) :
    Step idealEnv me s (restart s) := by
  unfold Step
  have h : idealEnv.crash s (restart s) := rfl
  exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inl h))))

end Orca
