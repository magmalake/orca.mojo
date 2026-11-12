import Oracle.Fence
import Oracle.Migrate
import Oracle.Pins
import Oracle.Retry

/-!
# The models as a program

`morca-oracle <model>` prints what a model says each sequence of events
leaves behind, a line a sequence: the file formal/difftest/<model>.cases,
which tests/test_formal_<model>.mojo replays through the Mojo code.
-/

def printAll (lines : List String) : IO UInt32 := do
  for l in lines do
    IO.println l
  return 0

def main (args : List String) : IO UInt32 :=
  match args with
  | ["retry"] => printAll Morca.Oracle.Retry.lines
  | ["pins"] => printAll Morca.Oracle.Pins.lines
  | ["migrate"] => printAll Morca.Oracle.Migrate.lines
  | ["fence"] => printAll Morca.Oracle.Fence.lines
  | _ => do
    IO.eprintln "usage: morca-oracle retry|pins|migrate|fence"
    return 1
