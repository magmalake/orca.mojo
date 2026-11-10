import Orca.Migrate.Model
import Oracle.Words

/-!
# `orca-oracle migrate`

What the migration model says, in three kinds of line, which
tests/test_formal_migrate.mojo checks against `orca.state`
(formal/difftest/migrate.cases is this output):

- `F <version> <floor>`: `migration_floor`, for versions 0 to 50;
- `S <version> <floor> <known> <verdict>`: `schema_ahead` for a database at
  `version` stating `floor` (-1 for none) and a binary that knows `known`
  migrations: `M` it migrates, `A` it runs on a schema ahead of it, `X` it
  refuses;
- `E <events> <outcomes> <version> <floor>`: binaries migrating an empty
  database in turn, for every sequence of up to three events: `a`, `b`, `c`,
  `d` are binaries that know 23, 24, 40 and 46 migrations, and `n` leaves
  every row without a floor, as binaries from before `min_binary` did. An
  outcome is `o` (the binary goes on), `x` (it refuses) or `-` (for `n`);
  then the database's version and stated floor.

`raisers` must be `_floor_raisers` of src/orca/state.mojo.
-/
namespace Orca.Oracle.Migrate

open Orca.Migrate

def raisers : List Nat := [1, 24]

def floorText : Option Nat → String
  | none => "-1"
  | some f => toString f

def verdictText : Verdict → String
  | Verdict.migrates => "M"
  | Verdict.ahead => "A"
  | Verdict.refuses => "X"

def floorLines : List String :=
  (List.range 51).map fun v =>
    "F " ++ toString v ++ " " ++ toString (floorOf v raisers)

def points : List Nat := [0, 1, 2, 23, 24, 25, 45, 46, 47]

def floors : List (Option Nat) := none :: points.map some

def schemaLines : List String := Id.run do
  let mut out : Array String := #[]
  for v in points do
    for f in floors do
      for k in points do
        out := out.push ("S " ++ toString v ++ " " ++ floorText f ++ " "
          ++ toString k ++ " " ++ verdictText (schemaAhead v f k))
  return out.toList

structure Sim where
  db : Db
  outcomes : String

def known (e : Char) : Nat :=
  if e = 'a' then 23 else if e = 'b' then 24 else if e = 'c' then 40 else 46

def step (s : Sim) (e : Char) : Sim :=
  if e = 'n' then
    Sim.mk (forget s.db) (s.outcomes ++ "-")
  else
    match migrate raisers (known e) s.db with
    | some db' => Sim.mk db' (s.outcomes ++ "o")
    | none => Sim.mk s.db (s.outcomes ++ "x")

def eventLine (w : String) : String :=
  let s := w.toList.foldl step (Sim.mk [] "")
  "E " ++ w ++ " " ++ s.outcomes ++ " " ++ toString (maxVersion s.db) ++ " "
    ++ floorText (dbFloor s.db)

def lines : List String :=
  floorLines ++ schemaLines ++ (upTo ["a", "b", "c", "d", "n"] 3).map eventLine

end Orca.Oracle.Migrate
