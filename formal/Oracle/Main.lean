import Oracle.Fence
import Oracle.Migrate
import Oracle.Pins
import Oracle.Retry

/-!
# The models as a program

`orca-oracle <model>` prints what a model says each sequence of events
leaves behind, a line a sequence: the file formal/difftest/<model>.cases,
which tests/test_formal_<model>.mojo replays through the Mojo code.
-/

def printAll (lines : List String) : IO UInt32 := do
  for l in lines do
    IO.println l
  return 0

def main (args : List String) : IO UInt32 :=
  match args with
  | ["retry"] => printAll Orca.Oracle.Retry.lines
  | ["pins"] => printAll Orca.Oracle.Pins.lines
  | ["migrate"] => printAll Orca.Oracle.Migrate.lines
  | ["fence"] => printAll Orca.Oracle.Fence.lines
  | _ => do
    IO.eprintln "usage: orca-oracle retry|pins|migrate|fence"
    return 1
