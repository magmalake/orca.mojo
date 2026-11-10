import Orca.Retry.Model

/-!
# The model as a program

Prints, for every sequence of up to five events, the snapshots the model
says the table holds afterwards. tests/test_formal_retry.mojo replays the
same sequences through `orca.tables.append` and compares
(formal/difftest/retry.cases is this program's output).

Events, one letter each:
- `c`: the attempt makes its next commit, start to finish: it looks
  (`findCommit`), commits unless it found its commit, and records;
- `x`: the worker crashes, and the next attempt starts;
- `o`: the same step commits for another partition.

A snapshot prints as `m<attempt>.<commit>` (the step's own) or `o<commit>`
(the other partition's); an empty table as `-`.
-/
namespace Orca.Oracle

def me : StepId := StepId.mk "formal_retry" 1 "load" "2024-01"

def other : StepId := StepId.mk "formal_retry" 1 "load" "2024-02"

structure Sim where
  table : Table
  attempt : Nat
  done : Nat
  otherDone : Nat

def start : Sim := Sim.mk [] 1 0 0

def step (s : Sim) (e : Char) : Sim :=
  if e = 'c' then
    match findCommit me s.done s.table with
    | some _ => { s with done := s.done + 1 }
    | none =>
      { s with
        table := s.table ++ [Snapshot.mk s.table.length (Summary.mk me s.attempt s.done)],
        done := s.done + 1 }
  else if e = 'x' then
    { s with attempt := s.attempt + 1, done := 0 }
  else if e = 'o' then
    { s with
      table := s.table ++ [Snapshot.mk s.table.length (Summary.mk other 1 s.otherDone)],
      otherDone := s.otherDone + 1 }
  else
    s

def token (x : Snapshot) : String :=
  if x.summary.owner = me then
    "m" ++ toString x.summary.attempt ++ "." ++ toString x.summary.commit
  else
    "o" ++ toString x.summary.commit

def render (t : Table) : String :=
  if t.isEmpty then "-" else ",".intercalate (t.map token)

def run (events : String) : Table :=
  (events.toList.foldl step start).table

/-- Every word one letter longer, in the order c, x, o. -/
def extend : List String → List String
  | [] => []
  | w :: ws => (w ++ "c") :: (w ++ "x") :: (w ++ "o") :: extend ws

def words : Nat → List String
  | 0 => [""]
  | n + 1 => extend (words n)

end Orca.Oracle

def main : IO Unit := do
  for n in [1, 2, 3, 4, 5] do
    for w in Orca.Oracle.words n do
      IO.println (w ++ " " ++ Orca.Oracle.render (Orca.Oracle.run w))
