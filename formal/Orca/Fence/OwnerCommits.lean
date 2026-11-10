import Orca.Core.Assumptions
import Orca.Fence.Model

/-!
# Only the run's owner commits

`commit_only_by_owner`: a commit that lands within half the stale limit of
its fence check lands while its process still owns the run. So a process
that lost its run to another (a paused VM, an unreachable database) commits
nothing after the other took over, and the attempts of a step follow each
other: what `Orca/Retry` and `Orca/Pins` take for granted.
-/
namespace Orca.Fence

open Orca.Assumptions

theorem setF_same (f : Nat → Option Nat) (p : Nat) (v : Option Nat) :
    setF f p v p = v := by
  simp [setF]

theorem setF_other (f : Nat → Option Nat) (p : Nat) (v : Option Nat) (q : Nat)
    (h : q ≠ p) : setF f p v q = f q := by
  simp [setF, h]

/-- What holds in every reachable state: a process whose fence check passed
at `f` either still owns the run, with a heartbeat younger than `half` at
`f`, or lost it more than `half` after `f`. -/
structure Inv (half : Nat) (s : State) : Prop where
  fenced : ∀ p f, s.fenced p = some f →
    f ≤ s.now ∧
    (s.owner = some p → ∃ h, s.heartbeat = some h ∧ f < h + half) ∧
    (s.owner ≠ some p → f + half < s.now)

theorem Inv.init {half : Nat} {s : State} (h : Initial s) : Inv half s := by
  refine ⟨?_⟩
  intro p f hf
  rw [h p] at hf
  cases hf

theorem Inv.step {stale half : Nat} {advance : Nat → Nat → Prop}
    (hH : 0 < half) (hS : 2 * half ≤ stale) (hC : ClockMonotone advance)
    {s s' : State} (inv : Inv half s) (h : Step stale half advance s s') :
    Inv half s' := by
  unfold Step at h
  rcases h with ⟨t, ha, rfl⟩ | ⟨p0, ho, rfl⟩ | ⟨q, hc, rfl⟩ |
    ⟨p0, ho, hfr, rfl⟩ | ⟨p0, rfl⟩
  · -- time passes
    have hle : s.now ≤ t := hC s.now t ha
    refine ⟨?_⟩
    intro p f hf
    obtain ⟨h1, h2, h3⟩ := inv.fenced p f hf
    exact ⟨Nat.le_trans h1 hle, h2,
      fun hne => Nat.lt_of_lt_of_le (h3 hne) hle⟩
  · -- a beat
    refine ⟨?_⟩
    intro p f hf
    obtain ⟨h1, h2, h3⟩ := inv.fenced p f hf
    exact ⟨h1, fun _ => ⟨s.now, rfl, by show f < s.now + half; omega⟩, h3⟩
  · -- a claim
    refine ⟨?_⟩
    intro p f hf
    obtain ⟨h1, h2, h3⟩ := inv.fenced p f hf
    refine ⟨h1, fun _ => ⟨s.now, rfl, by show f < s.now + half; omega⟩, ?_⟩
    intro _
    show f + half < s.now
    by_cases hown : s.owner = some p
    · obtain ⟨h, hh, hlt⟩ := h2 hown
      rw [hh] at hc
      have hc' : h + stale < s.now := by simpa [claimable] using hc
      omega
    · exact h3 hown
  · -- a fence check passes
    refine ⟨?_⟩
    intro p f hf
    have hf' : setF s.fenced p0 (some s.now) p = some f := hf
    have hfresh : ∃ h, s.heartbeat = some h ∧ s.now < h + half := by
      cases hhb : s.heartbeat with
      | none => simp [freshAt, hhb] at hfr
      | some h =>
        have hlt : s.now < h + half := by simpa [freshAt, hhb] using hfr
        exact ⟨h, rfl, hlt⟩
    by_cases e : p = p0
    · rw [e, setF_same] at hf'
      have hfn : s.now = f := Option.some.inj hf'
      obtain ⟨h, hhb, hlt⟩ := hfresh
      rw [e, ← hfn]
      exact ⟨Nat.le_refl _, fun _ => ⟨h, hhb, hlt⟩, fun hne => absurd ho hne⟩
    · rw [setF_other _ _ _ _ e] at hf'
      exact inv.fenced p f hf'
  · -- a commit lands, or its process gives up
    refine ⟨?_⟩
    intro p f hf
    have hf' : setF s.fenced p0 none p = some f := hf
    by_cases e : p = p0
    · rw [e, setF_same] at hf'
      cases hf'
    · rw [setF_other _ _ _ _ e] at hf'
      exact inv.fenced p f hf'

theorem Reachable.inv {stale half : Nat} {advance : Nat → Nat → Prop}
    (hH : 0 < half) (hS : 2 * half ≤ stale) (hC : ClockMonotone advance)
    {s : State} (h : Reachable stale half advance s) : Inv half s := by
  induction h with
  | init hs => exact Inv.init hs
  | step _ hstep ih => exact Inv.step hH hS hC ih hstep

/-- **Only the run's owner commits.** In every reachable state, a process
whose fence check passed at `f`, and whose commit lands now, owns the run. -/
theorem commit_only_by_owner (stale half : Nat)
    (advance lands : Nat → Nat → Prop)
    (hH : 0 < half) (hS : 2 * half ≤ stale)
    (hC : ClockMonotone advance) (hL : CommitLandsWithin half lands)
    (s : State) (hr : Reachable stale half advance s)
    (p f : Nat) (hf : s.fenced p = some f) (hl : lands f s.now) :
    s.owner = some p := by
  have i := (Reachable.inv hH hS hC hr).fenced p f hf
  have hle : s.now ≤ f + half := hL f s.now hl
  by_cases ho : s.owner = some p
  · exact ho
  · have hlate := i.2.2 ho
    exact absurd hle (by omega)

end Orca.Fence
