import Morca.Migrate.Model

/-!
# A binary runs only on a schema it can run on

`runs_only_knowing_the_floor`: a binary that `migrate` lets go on, by
migrating the database or by running on one that is ahead, knows every
floor-raising migration the database has had.
`runs_when_only_additive_since`: and it is let go on whenever it does and
the database says so: a rollback past additive migrations is never refused.

Both are about the bookkeeping: that the floors binaries of different ages
write a row at a time, and fill in later, add up to the right answer. That a
migration left out of the floor-raisers really is additive is a reviewer's
call (AGENTS.md, "Migrations"), which nothing here checks.
-/
namespace Morca.Migrate

/-! ## `larger`, `floorOf`, `maxVersion`, `dbFloor` -/

theorem le_larger_left (a b : Nat) : a ≤ larger a b := by
  unfold larger
  split <;> omega

theorem le_larger_right (a b : Nat) : b ≤ larger a b := by
  unfold larger
  split <;> omega

theorem larger_le {a b k : Nat} (ha : a ≤ k) (hb : b ≤ k) : larger a b ≤ k := by
  unfold larger
  split <;> omega

theorem larger_cases (a b : Nat) : larger a b = a ∨ larger a b = b := by
  unfold larger
  split
  · exact Or.inr rfl
  · exact Or.inl rfl

theorem floorOf_cons (v r : Nat) (rs : List Nat) :
    floorOf v (r :: rs)
      = if r ≤ v ∧ floorOf v rs < r then r else floorOf v rs :=
  rfl

/-- The floor is at least every floor-raiser up to the version. -/
theorem le_floorOf (v : Nat) (R : List Nat) :
    ∀ r, r ∈ R → r ≤ v → r ≤ floorOf v R := by
  induction R with
  | nil => intro r h; cases h
  | cons x xs ih =>
    intro r hm hv
    rw [floorOf_cons]
    rcases List.mem_cons.mp hm with e | e
    · by_cases c : x ≤ v ∧ floorOf v xs < x
      · rw [if_pos c]
        omega
      · rw [if_neg c]
        omega
    · have h := ih r e hv
      by_cases c : x ≤ v ∧ floorOf v xs < x
      · rw [if_pos c]
        omega
      · rw [if_neg c]
        exact h

/-- The floor is 0 or a floor-raiser up to the version. -/
theorem floorOf_cases (v : Nat) (R : List Nat) :
    floorOf v R = 0 ∨ (floorOf v R ∈ R ∧ floorOf v R ≤ v) := by
  induction R with
  | nil => exact Or.inl rfl
  | cons x xs ih =>
    rw [floorOf_cons]
    by_cases c : x ≤ v ∧ floorOf v xs < x
    · rw [if_pos c]
      exact Or.inr ⟨by simp, c.1⟩
    · rw [if_neg c]
      rcases ih with h | h
      · exact Or.inl h
      · exact Or.inr ⟨by simp [h.1], h.2⟩

theorem maxVersion_cons (row : Row) (rest : Db) :
    maxVersion (row :: rest) = larger (maxVersion rest) row.version :=
  rfl

theorem le_maxVersion (db : Db) :
    ∀ row, row ∈ db → row.version ≤ maxVersion db := by
  induction db with
  | nil => intro row h; cases h
  | cons x xs ih =>
    intro row h
    rw [maxVersion_cons]
    rcases List.mem_cons.mp h with e | e
    · rw [e]
      exact le_larger_right _ _
    · exact Nat.le_trans (ih row e) (le_larger_left _ _)

theorem maxVersion_le (db : Db) (k : Nat) :
    (∀ row, row ∈ db → row.version ≤ k) → maxVersion db ≤ k := by
  induction db with
  | nil => intro _; exact Nat.zero_le k
  | cons x xs ih =>
    intro h
    rw [maxVersion_cons]
    exact larger_le (ih (fun row hr => h row (by simp [hr]))) (h x (by simp))

theorem maxVersion_attained (db : Db) :
    0 < maxVersion db → ∃ row, row ∈ db ∧ row.version = maxVersion db := by
  induction db with
  | nil => intro h; exact absurd h (Nat.lt_irrefl 0)
  | cons x xs ih =>
    intro h
    rw [maxVersion_cons] at h
    rw [maxVersion_cons]
    rcases larger_cases (maxVersion xs) x.version with c | c
    · rw [c] at h
      rw [c]
      obtain ⟨q, hq, e⟩ := ih h
      exact ⟨q, by simp [hq], e⟩
    · rw [c]
      exact ⟨x, by simp, rfl⟩

/-- A stated floor means every row states one, at most it. -/
theorem dbFloor_bounds (db : Db) :
    ∀ f, dbFloor db = some f →
      ∀ row, row ∈ db → ∃ m, row.minBinary = some m ∧ m ≤ f := by
  induction db with
  | nil => intro f _ row h; cases h
  | cons x xs ih =>
    intro f h row hr
    cases hm : x.minBinary with
    | none => simp [dbFloor, hm] at h
    | some m =>
      cases hf : dbFloor xs with
      | none => simp [dbFloor, hm, hf] at h
      | some g =>
        have e : larger g m = f := by simpa [dbFloor, hm, hf] using h
        rcases List.mem_cons.mp hr with e1 | e1
        · rw [e1]
          exact ⟨m, hm, by rw [← e]; exact le_larger_right g m⟩
        · obtain ⟨m', hm', hle⟩ := ih g hf row e1
          exact ⟨m', hm',
            by rw [← e]; exact Nat.le_trans hle (le_larger_left g m)⟩

/-- Rows that all state a floor of at most `k` make a stated floor of at
most `k`. -/
theorem dbFloor_of_bounds (db : Db) (k : Nat) :
    (∀ row, row ∈ db → ∃ m, row.minBinary = some m ∧ m ≤ k) →
      ∃ f, dbFloor db = some f ∧ f ≤ k := by
  induction db with
  | nil => intro _; exact ⟨0, rfl, Nat.zero_le k⟩
  | cons x xs ih =>
    intro h
    obtain ⟨m, hm, hmk⟩ := h x (by simp)
    obtain ⟨g, hg, hgk⟩ := ih (fun row hr => h row (by simp [hr]))
    exact ⟨larger g m, by simp [dbFloor, hm, hg], larger_le hgk hmk⟩

/-! ## `schemaAhead` -/

theorem schemaAhead_ahead {v : Nat} {fl : Option Nat} {k : Nat}
    (h : schemaAhead v fl k = Verdict.ahead) :
    k < v ∧ ∃ f, fl = some f ∧ f ≤ k := by
  unfold schemaAhead at h
  by_cases c : v ≤ k
  · rw [if_pos c] at h
    cases h
  · rw [if_neg c] at h
    cases fl with
    | none => simp at h
    | some f =>
      by_cases c2 : k < f
      · simp [c2] at h
      · exact ⟨by omega, f, rfl, by omega⟩

theorem schemaAhead_migrates {v : Nat} {fl : Option Nat} {k : Nat}
    (h : schemaAhead v fl k = Verdict.migrates) : v ≤ k := by
  unfold schemaAhead at h
  by_cases c : v ≤ k
  · exact c
  · rw [if_neg c] at h
    cases fl with
    | none => simp at h
    | some f =>
      by_cases c2 : k < f
      · simp [c2] at h
      · simp [c2] at h

theorem schemaAhead_not_refuses {v f k : Nat} (hf : f ≤ k) :
    schemaAhead v (some f) k ≠ Verdict.refuses := by
  intro h
  unfold schemaAhead at h
  by_cases c : v ≤ k
  · rw [if_pos c] at h
    cases h
  · rw [if_neg c] at h
    have c2 : ¬k < f := by omega
    simp [c2] at h

/-! ## `fill`, `newRows`, `forget` -/

theorem fillRow_version (R : List Nat) (k : Nat) (row : Row) :
    (fillRow R k row).version = row.version := by
  unfold fillRow
  split <;> rfl

theorem fillRow_pos (R : List Nat) (k : Nat) (row : Row)
    (c : row.minBinary = none ∧ 1 ≤ row.version ∧ row.version ≤ k) :
    fillRow R k row
      = { row with minBinary := some (floorOf row.version R) } := by
  unfold fillRow
  exact if_pos c

theorem fillRow_neg (R : List Nat) (k : Nat) (row : Row)
    (c : ¬(row.minBinary = none ∧ 1 ≤ row.version ∧ row.version ≤ k)) :
    fillRow R k row = row := by
  unfold fillRow
  exact if_neg c

/-- A filled row's floor is right if the row's was. -/
theorem fillRow_floor (R : List Nat) (k : Nat) (row : Row)
    (h : ∀ m, row.minBinary = some m → m = floorOf row.version R) :
    ∀ m, (fillRow R k row).minBinary = some m →
      m = floorOf (fillRow R k row).version R := by
  intro m hm
  rw [fillRow_version]
  by_cases c : row.minBinary = none ∧ 1 ≤ row.version ∧ row.version ≤ k
  · rw [fillRow_pos R k row c] at hm
    have hm' : some (floorOf row.version R) = some m := hm
    exact (Option.some.inj hm').symm
  · rw [fillRow_neg R k row c] at hm
    exact h m hm

/-- A row still without a floor had none, and isn't one the binary knows. -/
theorem fillRow_none (R : List Nat) (k : Nat) (row : Row)
    (h : (fillRow R k row).minBinary = none) :
    row.minBinary = none ∧ (row.version = 0 ∨ k < row.version) := by
  by_cases c : row.minBinary = none ∧ 1 ≤ row.version ∧ row.version ≤ k
  · rw [fillRow_pos R k row c] at h
    have h' : some (floorOf row.version R) = (none : Option Nat) := h
    cases h'
  · rw [fillRow_neg R k row c] at h
    refine ⟨h, ?_⟩
    by_cases c1 : 1 ≤ row.version ∧ row.version ≤ k
    · exact absurd ⟨h, c1⟩ c
    · omega

theorem mem_fill (R : List Nat) (k : Nat) (db : Db) :
    ∀ q, q ∈ fill R k db → ∃ row, row ∈ db ∧ fillRow R k row = q := by
  induction db with
  | nil =>
    intro q h
    have h' : q ∈ ([] : Db) := h
    cases h'
  | cons x xs ih =>
    intro q h
    have h' : q ∈ fillRow R k x :: fill R k xs := h
    rcases List.mem_cons.mp h' with e | e
    · exact ⟨x, by simp, e.symm⟩
    · obtain ⟨row, hr, er⟩ := ih q e
      exact ⟨row, by simp [hr], er⟩

theorem maxVersion_fill (R : List Nat) (k : Nat) (db : Db) :
    maxVersion (fill R k db) = maxVersion db := by
  induction db with
  | nil => rfl
  | cons x xs ih =>
    show larger (maxVersion (fill R k xs)) (fillRow R k x).version
      = larger (maxVersion xs) x.version
    rw [ih, fillRow_version]

theorem mem_newRows (R : List Nat) (v : Nat) :
    ∀ n q, q ∈ newRows R v n →
      q.version ≤ v + n ∧ 1 ≤ q.version ∧
        q.minBinary = some (floorOf q.version R) := by
  intro n
  induction n with
  | zero =>
    intro q h
    have h' : q ∈ ([] : Db) := h
    cases h'
  | succ n ih =>
    intro q h
    have h' : q ∈ newRows R v n
        ++ [Row.mk (v + n + 1) (some (floorOf (v + n + 1) R))] := h
    rcases List.mem_append.mp h' with a | a
    · have i1 := (ih q a).1
      exact ⟨by omega, (ih q a).2.1, (ih q a).2.2⟩
    · have e : q = Row.mk (v + n + 1) (some (floorOf (v + n + 1) R)) :=
        List.mem_singleton.mp a
      rw [e]
      exact ⟨by show v + n + 1 ≤ v + (n + 1); omega,
        by show 1 ≤ v + n + 1; omega, rfl⟩

theorem mem_forget (db : Db) :
    ∀ q, q ∈ forget db →
      q.minBinary = none ∧ ∃ row, row ∈ db ∧ row.version = q.version := by
  induction db with
  | nil =>
    intro q h
    have h' : q ∈ ([] : Db) := h
    cases h'
  | cons x xs ih =>
    intro q h
    have h' : q ∈ Row.mk x.version none :: forget xs := h
    rcases List.mem_cons.mp h' with e | e
    · rw [e]
      exact ⟨rfl, x, by simp, rfl⟩
    · obtain ⟨hn, row, hr, hv⟩ := ih q e
      exact ⟨hn, row, by simp [hr], hv⟩

/-! ## The invariant -/

/-- What holds of every reachable database. -/
structure Inv (R : List Nat) (db : Db) : Prop where
  /-- A floor a row states is its version's. -/
  floors : ∀ row, row ∈ db →
    ∀ m, row.minBinary = some m → m = floorOf row.version R
  /-- Versions count from 1. -/
  versions : ∀ row, row ∈ db → 1 ≤ row.version

theorem inv_fill {R : List Nat} {db : Db} (inv : Inv R db) (k : Nat) :
    Inv R (fill R k db) := by
  refine ⟨?_, ?_⟩
  · intro q hq
    obtain ⟨row, hr, e⟩ := mem_fill R k db q hq
    rw [← e]
    exact fillRow_floor R k row (inv.floors row hr)
  · intro q hq
    obtain ⟨row, hr, e⟩ := mem_fill R k db q hq
    rw [← e, fillRow_version]
    exact inv.versions row hr

theorem inv_migrate {R : List Nat} {db db' : Db} (inv : Inv R db) (k : Nat)
    (h : migrate R k db = some db') : Inv R db' := by
  have invf := inv_fill inv k
  cases hv : schemaAhead (maxVersion (fill R k db))
      (dbFloor (fill R k db)) k with
  | refuses => simp [migrate, hv] at h
  | ahead =>
    have e : fill R k db = db' := by simpa [migrate, hv] using h
    rw [← e]
    exact invf
  | migrates =>
    have e : fill R k db
        ++ newRows R (maxVersion (fill R k db))
          (k - maxVersion (fill R k db)) = db' := by
      simpa [migrate, hv] using h
    rw [← e]
    refine ⟨?_, ?_⟩
    · intro q hq m hm
      rcases List.mem_append.mp hq with a | a
      · exact invf.floors q a m hm
      · rw [(mem_newRows R _ _ q a).2.2] at hm
        exact (Option.some.inj hm).symm
    · intro q hq
      rcases List.mem_append.mp hq with a | a
      · exact invf.versions q a
      · exact (mem_newRows R _ _ q a).2.1

theorem inv_forget {R : List Nat} {db : Db} (inv : Inv R db) :
    Inv R (forget db) := by
  refine ⟨?_, ?_⟩
  · intro q hq m hm
    rw [(mem_forget db q hq).1] at hm
    cases hm
  · intro q hq
    obtain ⟨_, row, hr, hv⟩ := mem_forget db q hq
    rw [← hv]
    exact inv.versions row hr

theorem Reachable.inv {R : List Nat} {db : Db} (h : Reachable R db) :
    Inv R db := by
  induction h with
  | empty => exact ⟨fun row hr => (by cases hr), fun row hr => (by cases hr)⟩
  | byMigrate known _ hm ih => exact inv_migrate ih known hm
  | byForget _ ih => exact inv_forget ih

/-! ## The theorems -/

/-- **A binary that goes on knows the floor.** If `migrate` by a binary that
knows `k` migrations doesn't refuse, the binary knows every floor-raising
migration up to the version of the database it leaves. -/
theorem runs_only_knowing_the_floor (R : List Nat) (db db' : Db) (k : Nat)
    (hr : Reachable R db) (h : migrate R k db = some db') :
    ∀ r, r ∈ R → r ≤ maxVersion db' → r ≤ k := by
  have invf := inv_fill (Reachable.inv hr) k
  intro r hrR hle
  cases hv : schemaAhead (maxVersion (fill R k db))
      (dbFloor (fill R k db)) k with
  | refuses => simp [migrate, hv] at h
  | ahead =>
    have e : fill R k db = db' := by simpa [migrate, hv] using h
    rw [← e] at hle
    obtain ⟨hk, f, hf, hfk⟩ := schemaAhead_ahead hv
    obtain ⟨q, hq, hqv⟩ := maxVersion_attained (fill R k db) (by omega)
    obtain ⟨m, hm, hmf⟩ := dbFloor_bounds (fill R k db) f hf q hq
    have hmfl := invf.floors q hq m hm
    have hrf := le_floorOf q.version R r hrR (by omega)
    omega
  | migrates =>
    have e : fill R k db
        ++ newRows R (maxVersion (fill R k db))
          (k - maxVersion (fill R k db)) = db' := by
      simpa [migrate, hv] using h
    have hvk := schemaAhead_migrates hv
    have hmax : maxVersion db' ≤ k := by
      rw [← e]
      apply maxVersion_le
      intro q hq
      rcases List.mem_append.mp hq with a | a
      · have h1 := le_maxVersion (fill R k db) q a
        omega
      · have h1 := (mem_newRows R _ _ q a).1
        omega
    omega

/-- **A rollback past additive migrations isn't refused.** A binary that
knows every floor-raising migration up to the database's version is let go
on, provided every row whose version it doesn't know states its floor. -/
theorem runs_when_only_additive_since (R : List Nat) (db : Db) (k : Nat)
    (hr : Reachable R db)
    (hsay : ∀ row, row ∈ db → row.minBinary = none → row.version ≤ k)
    (hknows : ∀ r, r ∈ R → r ≤ maxVersion db → r ≤ k) :
    ∃ db', migrate R k db = some db' := by
  have inv := Reachable.inv hr
  have invf := inv_fill inv k
  have hb : ∀ q, q ∈ fill R k db → ∃ m, q.minBinary = some m ∧ m ≤ k := by
    intro q hq
    obtain ⟨row, hrow, e⟩ := mem_fill R k db q hq
    have hqv : q.version ≤ maxVersion db := by
      rw [← e, fillRow_version]
      exact le_maxVersion db row hrow
    cases hm : q.minBinary with
    | none =>
      rw [← e] at hm
      have h1 := inv.versions row hrow
      have h2 := hsay row hrow (fillRow_none R k row hm).1
      have h3 := (fillRow_none R k row hm).2
      exact absurd h2 (by omega)
    | some m =>
      refine ⟨m, rfl, ?_⟩
      have hfl := invf.floors q hq m hm
      rcases floorOf_cases q.version R with z | z
      · omega
      · have z2 := z.2
        have h4 := hknows (floorOf q.version R) z.1 (by omega)
        omega
  obtain ⟨f, hf, hfk⟩ := dbFloor_of_bounds (fill R k db) k hb
  cases hv : schemaAhead (maxVersion (fill R k db))
      (dbFloor (fill R k db)) k with
  | refuses =>
    rw [hf] at hv
    exact absurd hv (schemaAhead_not_refuses hfk)
  | ahead => exact ⟨fill R k db, by simp [migrate, hv]⟩
  | migrates =>
    exact ⟨fill R k db
      ++ newRows R (maxVersion (fill R k db))
        (k - maxVersion (fill R k db)), by simp [migrate, hv]⟩

end Morca.Migrate
