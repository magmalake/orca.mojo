import Morca.Pins.Model
import Oracle.Words

/-!
# `morca-oracle pins`

For every sequence of up to five events, the answers the pins model says
`Ctx.pin` gives. tests/test_formal_pins.mojo replays the same sequences
through `Ctx.pin` over a store and compares (formal/difftest/pins.cases is
this output).

Events, one letter each, over two tables `a` and `b`, each at snapshot 1 to
begin with:
- `a`, `b`: the attempt reads the table;
- `A`, `B`: another writer commits to the table: its current snapshot is the
  next number;
- `x`: the worker crashes, and the next attempt starts.

An answer prints as the table and the snapshot, `a1`; no answers as `-`.
-/
namespace Morca.Oracle.Pins

open Morca.Pins

def start : State :=
  { current := fun _ => 1,
    ledger := [],
    attempt := 1,
    pinned := fun _ => none,
    reads := [],
    served := [] }

/-- A read, with a Postgres write that succeeds. -/
def read (s : State) (t : String) : State :=
  match lookup t s.reads with
  | some v => { s with served := s.served ++ [(t, v)] }
  | none =>
    afterRecord s t (s.ledger ++ [Read.mk s.attempt t (pinValue s t)]) true

def bump (s : State) (t : String) : State :=
  { s with current := fun u => if u = t then s.current u + 1 else s.current u }

def restart (s : State) : State :=
  { s with attempt := s.attempt + 1, pinned := pinsOf s.ledger, reads := [] }

def step (s : State) (e : Char) : State :=
  if e = 'a' then read s "a"
  else if e = 'b' then read s "b"
  else if e = 'A' then bump s "a"
  else if e = 'B' then bump s "b"
  else if e = 'x' then restart s
  else s

def render (served : List (String × Nat)) : String :=
  if served.isEmpty then "-"
  else ",".intercalate (served.map fun p => p.1 ++ toString p.2)

def run (events : String) : List (String × Nat) :=
  (events.toList.foldl step start).served

def lines : List String :=
  (upTo ["a", "b", "A", "B", "x"] 5).map fun w => w ++ " " ++ render (run w)

end Morca.Oracle.Pins
