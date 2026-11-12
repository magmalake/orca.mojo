import Morca.Core.Assumptions
import Morca.Pins.Model

/-!
# Every attempt reads the snapshot the first one read

`retry_reads_same_snapshot`: whatever happens (crashes at any point, failed
Postgres writes, other writers committing to the tables), any two answers
`Ctx.pin` ever gave for one table, in any attempts, are the same snapshot.
-/
namespace Morca.Pins

open Morca.Assumptions

/-- Ledger `l` has a read of table `t` at snapshot `v`. -/
abbrev Has (l : Reads) (t : String) (v : Nat) : Prop :=
  ∃ r, r ∈ l ∧ r.table = t ∧ r.snapshot = v

theorem mem_snoc {α : Type} (l : List α) (x : α) : x ∈ l ++ [x] :=
  List.mem_append.mpr (Or.inr (List.mem_singleton.mpr rfl))

theorem Has.snoc {l : Reads} {t : String} {v : Nat} (h : Has l t v)
    (x : Read) : Has (l ++ [x]) t v := by
  obtain ⟨r, hr, ht, hv⟩ := h
  exact ⟨r, List.mem_append.mpr (Or.inl hr), ht, hv⟩

/-! ## `earliest` and `pinsOf` -/

theorem better_cases (r : Read) (o : Option Read) :
    better r o = r ∨ o = some (better r o) := by
  cases o with
  | none => exact Or.inl rfl
  | some q =>
    by_cases h : q.attempt < r.attempt
    · exact Or.inr (by simp [better, h])
    · exact Or.inl (by simp [better, h])

theorem earliest_some (t : String) (l : Reads) :
    ∀ q, earliest t l = some q → q ∈ l ∧ q.table = t := by
  induction l with
  | nil => intro q h; simp [earliest] at h
  | cons r rest ih =>
    intro q h
    by_cases hr : r.table = t
    · have hq : better r (earliest t rest) = q := by
        simpa [earliest, hr] using h
      rcases better_cases r (earliest t rest) with hb | hb
      · have e : q = r := hq.symm.trans hb
        rw [e]
        exact ⟨by simp, hr⟩
      · rw [hq] at hb
        have h' := ih q hb
        exact ⟨by simp [h'.1], h'.2⟩
    · have h' : earliest t rest = some q := by
        simpa [earliest, hr] using h
      have h'' := ih q h'
      exact ⟨by simp [h''.1], h''.2⟩

theorem earliest_none (t : String) (l : Reads) :
    earliest t l = none → ∀ r, r ∈ l → r.table ≠ t := by
  induction l with
  | nil => intro _ r hr; cases hr
  | cons x rest ih =>
    intro h r hr
    by_cases hx : x.table = t
    · simp [earliest, hx] at h
    · have h' : earliest t rest = none := by
        simpa [earliest, hx] using h
      rcases List.mem_cons.mp hr with e | e
      · rw [e]
        exact hx
      · exact ih h' r e

/-- A pin is a read on record. -/
theorem pinsOf_some (l : Reads) (t : String) (v : Nat)
    (h : pinsOf l t = some v) : Has l t v := by
  cases he : earliest t l with
  | none => simp [pinsOf, he] at h
  | some q =>
    have hv : q.snapshot = v := by simpa [pinsOf, he] using h
    have hq := earliest_some t l q he
    exact ⟨q, hq.1, hq.2, hv⟩

/-- A table with a read on record has a pin. -/
theorem pinsOf_none (l : Reads) (t : String) (h : pinsOf l t = none) :
    ∀ r, r ∈ l → r.table ≠ t := by
  cases he : earliest t l with
  | none => exact earliest_none t l he
  | some q => simp [pinsOf, he] at h

/-! ## The invariant -/

/-- Reads that agree stay in agreement when one more is added that agrees
with those of its table. -/
theorem agree_snoc (l : Reads) (n : Read)
    (ha : ∀ r1 r2, r1 ∈ l → r2 ∈ l → r1.table = r2.table →
      r1.snapshot = r2.snapshot)
    (hk : ∀ r, r ∈ l → r.table = n.table → r.snapshot = n.snapshot) :
    ∀ r1 r2, r1 ∈ l ++ [n] → r2 ∈ l ++ [n] → r1.table = r2.table →
      r1.snapshot = r2.snapshot := by
  intro r1 r2 h1 h2 ht
  rcases List.mem_append.mp h1 with a1 | a1 <;>
    rcases List.mem_append.mp h2 with a2 | a2
  · exact ha r1 r2 a1 a2 ht
  · have e2 : r2 = n := List.mem_singleton.mp a2
    rw [e2] at ht
    rw [e2]
    exact hk r1 a1 ht
  · have e1 : r1 = n := List.mem_singleton.mp a1
    rw [e1] at ht
    rw [e1]
    exact (hk r2 a2 ht.symm).symm
  · have e1 : r1 = n := List.mem_singleton.mp a1
    have e2 : r2 = n := List.mem_singleton.mp a2
    rw [e1, e2]

/-- What holds in every reachable state. -/
structure Inv (s : State) : Prop where
  /-- The reads on record of one table are of one snapshot. -/
  agree : ∀ r1 r2, r1 ∈ s.ledger → r2 ∈ s.ledger → r1.table = r2.table →
    r1.snapshot = r2.snapshot
  /-- Every answer given is a read on record. -/
  served : ∀ t v, (t, v) ∈ s.served → Has s.ledger t v
  /-- So is every read the attempt remembers. -/
  reads : ∀ t v, lookup t s.reads = some v → Has s.ledger t v
  /-- So is every pin it was given. -/
  pinned : ∀ t v, s.pinned t = some v → Has s.ledger t v
  /-- A table with a read on record is pinned, or the attempt has read it. -/
  known : ∀ r, r ∈ s.ledger → s.pinned r.table = none →
    lookup r.table s.reads = none → False

theorem Inv.init {s : State} (h : Initial s) : Inv s where
  agree := by
    intro r1 r2 h1
    rw [h.ledger] at h1
    cases h1
  served := by
    intro t v hm
    rw [h.served] at hm
    cases hm
  reads := by
    intro t v hl
    rw [h.reads] at hl
    simp [lookup] at hl
  pinned := by
    intro t v hp
    rw [h.pinned t] at hp
    cases hp
  known := by
    intro r hr
    rw [h.ledger] at hr
    cases hr

/-- The reads on record of table `t` are of the snapshot a first read of
`t` takes, when the attempt hasn't read `t` yet. -/
theorem Inv.first_read {s : State} (inv : Inv s) (t : String)
    (hl : lookup t s.reads = none) :
    ∀ r, r ∈ s.ledger → r.table = t → r.snapshot = pinValue s t := by
  intro r hr ht
  cases hp : s.pinned t with
  | none =>
    exact (inv.known r hr (by rw [ht]; exact hp) (by rw [ht]; exact hl)).elim
  | some v =>
    obtain ⟨q, hq, hqt, hqv⟩ := inv.pinned t v hp
    have e := inv.agree r q hr hq (ht.trans hqt.symm)
    have hv : pinValue s t = v := by simp [pinValue, hp]
    rw [hv]
    exact e.trans hqv

theorem Inv.step {record : Reads → Read → Reads → Bool → Prop}
    (hP : PostgresAtomic record)
    {s s' : State} (inv : Inv s) (h : Step record s s') : Inv s' := by
  unfold Step at h
  rcases h with ⟨t, v, hl, rfl⟩ | ⟨t, l', ok, hl, hr, hs⟩ | ⟨a, rfl⟩ | ⟨c, rfl⟩
  · -- a read the attempt remembers
    refine ⟨inv.agree, ?_, inv.reads, inv.pinned, inv.known⟩
    intro t' v' hm
    have hm' : (t', v') ∈ s.served ++ [(t, v)] := hm
    rcases List.mem_append.mp hm' with h1 | h1
    · exact inv.served t' v' h1
    · have e := Prod.mk.inj (List.mem_singleton.mp h1)
      rw [e.1, e.2]
      exact inv.reads t v hl
  · -- a first read, and its Postgres write
    rcases hP s.ledger _ l' ok hr with ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩
    · -- nothing was written
      subst hs
      exact ⟨inv.agree, inv.served, inv.reads, inv.pinned, inv.known⟩
    · -- the read was, and the attempt answers
      subst hs
      have hnew : Has (s.ledger ++ [Read.mk s.attempt t (pinValue s t)]) t
          (pinValue s t) :=
        ⟨Read.mk s.attempt t (pinValue s t), mem_snoc _ _, rfl, rfl⟩
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · exact agree_snoc s.ledger (Read.mk s.attempt t (pinValue s t))
          inv.agree (inv.first_read t hl)
      · intro t' v' hm
        have hm' : (t', v') ∈ s.served ++ [(t, pinValue s t)] := hm
        rcases List.mem_append.mp hm' with h1 | h1
        · exact Has.snoc (inv.served t' v' h1) _
        · have e := Prod.mk.inj (List.mem_singleton.mp h1)
          rw [e.1, e.2]
          exact hnew
      · intro t' v' h
        have h' : lookup t'
            (Read.mk s.attempt t (pinValue s t) :: s.reads) = some v' := h
        by_cases ht : t = t'
        · have hv : pinValue s t = v' := by simpa [lookup, ht] using h'
          rw [← ht, ← hv]
          exact hnew
        · have h'' : lookup t' s.reads = some v' := by
            simpa [lookup, ht] using h'
          exact Has.snoc (inv.reads t' v' h'') _
      · intro t' v' hp
        exact Has.snoc (inv.pinned t' v' hp) _
      · intro r hr' hp hl'
        have hr'' : r ∈ s.ledger ++ [Read.mk s.attempt t (pinValue s t)] := hr'
        have hl'' : lookup r.table
            (Read.mk s.attempt t (pinValue s t) :: s.reads) = none := hl'
        by_cases ht : t = r.table
        · simp [lookup, ht] at hl''
        · have hl3 : lookup r.table s.reads = none := by
            simpa [lookup, ht] using hl''
          rcases List.mem_append.mp hr'' with a | a
          · exact inv.known r a hp hl3
          · have e : r = Read.mk s.attempt t (pinValue s t) :=
              List.mem_singleton.mp a
            exact ht (congrArg Read.table e).symm
  · -- a crash, and the next attempt
    refine ⟨inv.agree, inv.served, ?_, ?_, ?_⟩
    · intro t v h
      have h' : lookup t ([] : Reads) = some v := h
      simp [lookup] at h'
    · intro t v hp
      exact pinsOf_some s.ledger t v hp
    · intro r hr hp _
      exact pinsOf_none s.ledger r.table hp r hr rfl
  · -- other writers
    exact ⟨inv.agree, inv.served, inv.reads, inv.pinned, inv.known⟩

theorem Reachable.inv {record : Reads → Read → Reads → Bool → Prop}
    (hP : PostgresAtomic record)
    {s : State} (h : Reachable record s) : Inv s := by
  induction h with
  | init hs => exact Inv.init hs
  | step _ hstep ih => exact Inv.step hP ih hstep

/-! ## The theorem -/

/-- **A retry reads what the first attempt read.** In every reachable state,
any two answers ever given for table `t` are the same snapshot. -/
theorem retry_reads_same_snapshot
    (record : Reads → Read → Reads → Bool → Prop)
    (hP : PostgresAtomic record)
    (s : State) (h : Reachable record s) (t : String) (v w : Nat)
    (hv : (t, v) ∈ s.served) (hw : (t, w) ∈ s.served) : v = w := by
  have inv := Reachable.inv hP h
  obtain ⟨r1, h1, t1, v1⟩ := inv.served t v hv
  obtain ⟨r2, h2, t2, v2⟩ := inv.served t w hw
  have e := inv.agree r1 r2 h1 h2 (t1.trans t2.symm)
  exact v1.symm.trans (e.trans v2)

end Morca.Pins
