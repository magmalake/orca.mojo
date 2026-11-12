import Morca.Fence.Model
import Oracle.Words

/-!
# `morca-oracle fence`

For every sequence of up to four events, which claims and fence checks the
fence model lets through. tests/test_formal_fence.mojo replays the same
sequences through `PgStore` and compares (formal/difftest/fence.cases is
this output).

Two processes, `p` and `q`, and a run that `p` has just created; the stale
limit is 1000 s. Events, one letter each:
- `P`, `Q`: the process claims the run;
- `p`, `q`: the process checks its fence;
- `b`, `c`: `p`'s, `q`'s heartbeat beats;
- `t`, `T`: 300 s, 600 s pass.

An outcome is `o` (the claim or the check passes), `x` (it is refused) or
`-` (a beat, or time).
-/
namespace Morca.Oracle.Fence

open Morca.Fence

def stale : Nat := 1000

def half : Nat := 500

structure Sim where
  st : State
  outcomes : String

def start : Sim :=
  Sim.mk (State.mk 0 (some 0) (some 0) (fun _ => none)) ""

def claim (s : Sim) (q : Nat) : Sim :=
  if claimable stale s.st.now s.st.heartbeat then
    Sim.mk { s.st with owner := some q, heartbeat := some s.st.now }
      (s.outcomes ++ "o")
  else Sim.mk s.st (s.outcomes ++ "x")

def fence (s : Sim) (p : Nat) : Sim :=
  if s.st.owner = some p ∧ freshAt half s.st.now s.st.heartbeat = true then
    Sim.mk { s.st with fenced := setF s.st.fenced p (some s.st.now) }
      (s.outcomes ++ "o")
  else Sim.mk s.st (s.outcomes ++ "x")

def beat (s : Sim) (p : Nat) : Sim :=
  if s.st.owner = some p then
    Sim.mk { s.st with heartbeat := some s.st.now } (s.outcomes ++ "-")
  else Sim.mk s.st (s.outcomes ++ "-")

def pass (s : Sim) (seconds : Nat) : Sim :=
  Sim.mk { s.st with now := s.st.now + seconds } (s.outcomes ++ "-")

def step (s : Sim) (e : Char) : Sim :=
  if e = 'P' then claim s 0
  else if e = 'Q' then claim s 1
  else if e = 'p' then fence s 0
  else if e = 'q' then fence s 1
  else if e = 'b' then beat s 0
  else if e = 'c' then beat s 1
  else if e = 't' then pass s 300
  else if e = 'T' then pass s 600
  else s

def lines : List String :=
  (upTo ["P", "Q", "p", "q", "b", "c", "t", "T"] 4).map fun w =>
    w ++ " " ++ (w.toList.foldl step start).outcomes

end Morca.Oracle.Fence
