import Morca.Core.Assumptions
import Morca.Retry.Model

/-!
# A retried step never double-commits

`no_double_commit`: whatever happens (crashes at any point, failed commits
and failed Postgres writes, other writers), the table never holds two
snapshots that are the step's commit number `n`.
`retry_records_its_own_commit`: and every commit on record in Postgres names
a snapshot in the table that is the step's commit of that number, so an
attempt that finds an earlier attempt's commit records that one.
-/
namespace Morca

open Morca.Assumptions

/-! ## `made`, `findCommit` and `countMade` -/

theorem made_iff (me : StepId) (n : Nat) (s : Summary) :
    made me n s = true ↔ s.owner = me ∧ s.commit = n := by
  simp [made]

theorem made_eq_false (me : StepId) (n : Nat) (s : Summary)
    (h : ¬(s.owner = me ∧ s.commit = n)) : made me n s = false := by
  cases hm : made me n s with
  | false => rfl
  | true => exact absurd ((made_iff me n s).mp hm) h

theorem ite_le_one (b : Bool) : (if b then 1 else 0 : Nat) ≤ 1 := by
  cases b <;> decide

theorem countMade_append (me : StepId) (n : Nat) (a b : Table) :
    countMade me n (a ++ b) = countMade me n a + countMade me n b := by
  induction a with
  | nil => simp [countMade]
  | cons x t ih =>
    show (if made me n x.summary then 1 else 0) + countMade me n (t ++ b)
        = (if made me n x.summary then 1 else 0) + countMade me n t
          + countMade me n b
    rw [ih]
    exact (Nat.add_assoc _ _ _).symm

theorem countMade_snoc (me : StepId) (n : Nat) (t : Table) (x : Snapshot) :
    countMade me n (t ++ [x])
      = countMade me n t + (if made me n x.summary then 1 else 0) := by
  exact countMade_append me n t [x]

theorem countMade_snoc_of_not (me : StepId) (n : Nat) (t : Table)
    (x : Snapshot) (h : made me n x.summary = false) :
    countMade me n (t ++ [x]) = countMade me n t := by
  have e := countMade_snoc me n t x
  rw [h] at e
  simpa using e

theorem countMade_snoc_le (me : StepId) (n : Nat) (t : Table) (x : Snapshot) :
    countMade me n (t ++ [x]) ≤ countMade me n t + 1 := by
  rw [countMade_snoc]
  exact Nat.add_le_add_left (ite_le_one _) _

theorem mem_snoc_self (t : Table) (x : Snapshot) : x ∈ t ++ [x] :=
  List.mem_append.mpr (Or.inr (List.mem_singleton.mpr rfl))

theorem mem_snoc_of_mem (t : Table) (x y : Snapshot) (h : y ∈ t) :
    y ∈ t ++ [x] :=
  List.mem_append.mpr (Or.inl h)

/-- A look that finds nothing means there is nothing to find. -/
theorem countMade_eq_zero_of_findCommit_none (me : StepId) (n : Nat)
    (t : Table) : findCommit me n t = none → countMade me n t = 0 := by
  induction t with
  | nil => intro _; rfl
  | cons x t ih =>
    intro h
    cases hm : made me n x.summary with
    | true => simp [findCommit, hm] at h
    | false =>
      have h' : findCommit me n t = none := by
        simpa [findCommit, hm] using h
      simp [countMade, hm, ih h']

/-- What a look finds is in the table and is the step's commit `n`. -/
theorem findCommit_some (me : StepId) (n : Nat) (t : Table) (id : Nat) :
    findCommit me n t = some id →
      ∃ x, x ∈ t ∧ x.id = id ∧ made me n x.summary = true := by
  induction t with
  | nil => intro h; simp [findCommit] at h
  | cons x t ih =>
    intro h
    cases hm : made me n x.summary with
    | true =>
      have hid : x.id = id := by simpa [findCommit, hm] using h
      exact ⟨x, by simp, hid, hm⟩
    | false =>
      have h' : findCommit me n t = some id := by
        simpa [findCommit, hm] using h
      obtain ⟨y, hy, hyid, hym⟩ := ih h'
      exact ⟨y, by simp [hy], hyid, hym⟩

theorem countMade_zero_of_fresh (me : StepId) (n : Nat) (t : Table)
    (h : ∀ x, x ∈ t → x.summary.owner ≠ me) : countMade me n t = 0 := by
  induction t with
  | nil => rfl
  | cons x t ih =>
    have hx : made me n x.summary = false :=
      made_eq_false _ _ _ (fun e => h x (by simp) e.1)
    have ht : ∀ y, y ∈ t → y.summary.owner ≠ me :=
      fun y hy => h y (by simp [hy])
    show (if made me n x.summary then 1 else 0) + countMade me n t = 0
    simp [hx, ih ht]

/-! ## The invariant -/

/-- What holds in every reachable state. -/
structure Inv (me : StepId) (s : State) : Prop where
  /-- At most one snapshot is the step's commit `n`. -/
  once : ∀ n, countMade me n s.table ≤ 1
  /-- An attempt about to commit has no such commit in the table yet. -/
  clear : s.phase = Phase.willCommit → countMade me s.done s.table = 0
  /-- The snapshot an attempt is about to record is its commit, in the
  table. -/
  pending : ∀ id, s.phase = Phase.toRecord id →
    ∃ x, x ∈ s.table ∧ x.id = id ∧ made me s.done x.summary = true
  /-- So is every snapshot on record. -/
  recorded : ∀ r, r ∈ s.ledger →
    ∃ x, x ∈ s.table ∧ x.id = r.snapshot ∧ made me r.commit x.summary = true

theorem Inv.init {me : StepId} {s : State} (h : Initial me s) : Inv me s where
  once := by
    intro n
    have e := countMade_zero_of_fresh me n s.table h.fresh
    omega
  clear := fun _ => countMade_zero_of_fresh me s.done s.table h.fresh
  pending := by
    intro id hp
    rw [h.phase] at hp
    cases hp
  recorded := by
    intro r hr
    rw [h.ledger] at hr
    cases hr

theorem Inv.step {env : Env} {me : StepId}
    (hI : IcebergAllOrNothing env.commit) (hP : PostgresAtomic env.record)
    (hC : CrashLosesOnlyMemory env.crash)
    {s s' : State} (inv : Inv me s) (h : Step env me s s') : Inv me s' := by
  unfold Step at h
  rcases h with ⟨id, hph, hf, rfl⟩ | ⟨hph, hf, rfl⟩ | ⟨t', out, hph, hc, hs⟩ |
    ⟨id, l', ok, hph, hr, hs⟩ | hc | ⟨x, hx, rfl⟩
  · -- looked and found `id`
    refine ⟨inv.once, ?_, ?_, inv.recorded⟩
    · intro h
      have h' : Phase.toRecord id = Phase.willCommit := h
      cases h'
    · intro id' h
      have h' : Phase.toRecord id = Phase.toRecord id' := h
      injection h' with e
      obtain ⟨x, hx, hxid, hxm⟩ := findCommit_some me s.done s.table id hf
      exact ⟨x, hx, hxid.trans e, hxm⟩
  · -- looked and found none
    refine ⟨inv.once, ?_, ?_, inv.recorded⟩
    · intro _
      exact countMade_eq_zero_of_findCommit_none me s.done s.table hf
    · intro id h
      have h' : Phase.willCommit = Phase.toRecord id := h
      cases h'
  · -- the commit call
    rcases hI s.table _ t' out hc with ⟨rfl, rfl⟩ | ⟨id, rfl, rfl⟩
    · -- nothing was committed
      subst hs
      refine ⟨inv.once, fun _ => inv.clear hph, ?_, inv.recorded⟩
      intro id h
      have h' : s.phase = Phase.toRecord id := h
      rw [hph] at h'
      cases h'
    · -- one snapshot was, with this attempt's summary
      subst hs
      refine ⟨?_, ?_, ?_, ?_⟩
      · intro n
        show countMade me n
          (s.table ++ [Snapshot.mk id (Summary.mk me s.attempt s.done)]) ≤ 1
        by_cases hn : n = s.done
        · subst hn
          have hle := countMade_snoc_le me s.done s.table
            (Snapshot.mk id (Summary.mk me s.attempt s.done))
          rw [inv.clear hph] at hle
          exact hle
        · have e := countMade_snoc_of_not me n s.table
            (Snapshot.mk id (Summary.mk me s.attempt s.done))
            (made_eq_false _ _ _ (fun h => hn h.2.symm))
          rw [e]
          exact inv.once n
      · intro h
        have h' : Phase.toRecord id = Phase.willCommit := h
        cases h'
      · intro id' h
        have h' : Phase.toRecord id = Phase.toRecord id' := h
        injection h' with e
        have hmem := mem_snoc_self s.table
          (Snapshot.mk id (Summary.mk me s.attempt s.done))
        exact ⟨_, hmem, e,
          (made_iff me s.done (Summary.mk me s.attempt s.done)).mpr ⟨rfl, rfl⟩⟩
      · intro r hr
        obtain ⟨y, hy, hyid, hym⟩ := inv.recorded r hr
        exact ⟨y, mem_snoc_of_mem _ _ _ hy, hyid, hym⟩
  · -- the Postgres write
    rcases hP s.ledger _ l' ok hr with ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩
    · -- nothing was written
      subst hs
      exact ⟨inv.once, inv.clear, inv.pending, inv.recorded⟩
    · -- the record was
      subst hs
      refine ⟨inv.once, ?_, ?_, ?_⟩
      · intro h
        have h' : Phase.looking = Phase.willCommit := h
        cases h'
      · intro id' h
        have h' : Phase.looking = Phase.toRecord id' := h
        cases h'
      · intro r hr
        have hr' : r ∈ s.ledger ++ [Record.mk s.attempt s.done id] := hr
        rcases List.mem_append.mp hr' with h1 | h1
        · exact inv.recorded r h1
        · have e : r = Record.mk s.attempt s.done id :=
            List.mem_singleton.mp h1
          obtain ⟨x, hx, hxid, hxm⟩ := inv.pending id hph
          rw [e]
          exact ⟨x, hx, hxid, hxm⟩
  · -- a crash
    obtain ⟨ht, hl, _, hp⟩ := hC s s' hc
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro n
      rw [ht]
      exact inv.once n
    · intro h
      rw [hp] at h
      cases h
    · intro id h
      rw [hp] at h
      cases h
    · intro r hr
      rw [hl] at hr
      rw [ht]
      exact inv.recorded r hr
  · -- another writer's commit
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro n
      show countMade me n (s.table ++ [x]) ≤ 1
      rw [countMade_snoc_of_not me n s.table x
        (made_eq_false _ _ _ (fun h => hx h.1))]
      exact inv.once n
    · intro h
      show countMade me s.done (s.table ++ [x]) = 0
      rw [countMade_snoc_of_not me s.done s.table x
        (made_eq_false _ _ _ (fun h => hx h.1))]
      exact inv.clear h
    · intro id h
      obtain ⟨y, hy, hyid, hym⟩ := inv.pending id h
      exact ⟨y, mem_snoc_of_mem _ _ _ hy, hyid, hym⟩
    · intro r hr
      obtain ⟨y, hy, hyid, hym⟩ := inv.recorded r hr
      exact ⟨y, mem_snoc_of_mem _ _ _ hy, hyid, hym⟩

theorem Reachable.inv {env : Env} {me : StepId}
    (hI : IcebergAllOrNothing env.commit) (hP : PostgresAtomic env.record)
    (hC : CrashLosesOnlyMemory env.crash)
    {s : State} (h : Reachable env me s) : Inv me s := by
  induction h with
  | init hs => exact Inv.init hs
  | step _ hstep ih => exact Inv.step hI hP hC ih hstep

/-! ## The theorems -/

/-- **A retried step never double-commits.** In every reachable state, at
most one snapshot in the table is the step's commit number `n`. -/
theorem no_double_commit (env : Env) (me : StepId)
    (hI : IcebergAllOrNothing env.commit) (hP : PostgresAtomic env.record)
    (hC : CrashLosesOnlyMemory env.crash)
    (s : State) (h : Reachable env me s) (n : Nat) :
    countMade me n s.table ≤ 1 :=
  (Reachable.inv hI hP hC h).once n

/-- **A retry recognises its own commit.** Every commit on record names a
snapshot in the table that is the step's commit of that number: with
`no_double_commit`, the only one. -/
theorem retry_records_its_own_commit (env : Env) (me : StepId)
    (hI : IcebergAllOrNothing env.commit) (hP : PostgresAtomic env.record)
    (hC : CrashLosesOnlyMemory env.crash)
    (s : State) (h : Reachable env me s) (r : Record) (hr : r ∈ s.ledger) :
    ∃ x, x ∈ s.table ∧ x.id = r.snapshot ∧
      x.summary.owner = me ∧ x.summary.commit = r.commit := by
  obtain ⟨x, hx, hid, hm⟩ := (Reachable.inv hI hP hC h).recorded r hr
  exact ⟨x, hx, hid, (made_iff me r.commit x.summary).mp hm⟩

end Morca
