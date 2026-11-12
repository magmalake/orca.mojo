import Morca.Retry.Model
import Oracle.Words

/-!
# `morca-oracle retry`

For every sequence of up to five events, the snapshots the retry model says
the table holds afterwards. tests/test_formal_retry.mojo replays the same
sequences through `morca.tables.append` and compares
(formal/difftest/retry.cases is this output).

Events, one letter each:
- `c`: the attempt makes its next commit, start to finish: it looks
  (`findCommit`), commits unless it found its commit, and records;
- `x`: the worker crashes, and the next attempt starts;
- `o`: the same step commits for another partition.

A snapshot prints as `m<attempt>.<commit>` (the step's own) or `o<commit>`
(the other partition's); an empty table as `-`.
-/
namespace Morca.Oracle.Retry

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

def lines : List String :=
  (upTo ["c", "x", "o"] 5).map fun w => w ++ " " ++ render (run w)

end Morca.Oracle.Retry
