/-!
# Which binaries run on which schema

Each migration applied leaves a row in `orca.schema_version` with the fewest
migrations a binary must know to run on the schema it left (`min_binary`):
the newest floor-raising migration up to it. A binary older than the
database runs on it only if it knows that many. Rows written before the
column existed have none, until a binary that knows their version fills
them in.

Mirrors src/orca/migrations.mojo#migration_floor+schema_ahead and src/orca/state.mojo#State.migrate.
-/
namespace Orca.Migrate

/-- The larger of two numbers. -/
def larger (a b : Nat) : Nat := if a < b then b else a

/-- The newest of the floor-raising migrations `raisers` up to version `v`,
or 0: `migration_floor`. -/
def floorOf (v : Nat) : List Nat → Nat
  | [] => 0
  | r :: rs => if r ≤ v ∧ floorOf v rs < r then r else floorOf v rs

/-- A row of `orca.schema_version`. -/
structure Row where
  version : Nat
  minBinary : Option Nat
  deriving DecidableEq, Repr

abbrev Db := List Row

/-- The database's schema version: `coalesce(max(version), 0)`. -/
def maxVersion : Db → Nat
  | [] => 0
  | row :: rest => larger (maxVersion rest) row.version

/-- The floor the database states: none if a row doesn't say (the query's
-1), else the largest `min_binary`, 0 for no rows. -/
def dbFloor : Db → Option Nat
  | [] => some 0
  | row :: rest =>
    match row.minBinary with
    | none => none
    | some m =>
      match dbFloor rest with
      | none => none
      | some f => some (larger f m)

/-- What a binary does with a database. -/
inductive Verdict where
  /-- The database isn't ahead: the binary applies what it lacks. -/
  | migrates
  /-- It is ahead, by additive migrations only: the binary runs on it. -/
  | ahead
  /-- It needs a newer binary, or doesn't say. -/
  | refuses
  deriving DecidableEq, Repr

/-- `schema_ahead`, for a database at version `v` stating `floor` and a
binary that knows `known` migrations. -/
def schemaAhead (v : Nat) (floor : Option Nat) (known : Nat) : Verdict :=
  if v ≤ known then Verdict.migrates
  else
    match floor with
    | none => Verdict.refuses
    | some f => if known < f then Verdict.refuses else Verdict.ahead

/-- A binary fills in the floor of a row that lacks one, if it knows the
row's version. -/
def fillRow (raisers : List Nat) (known : Nat) (row : Row) : Row :=
  if row.minBinary = none ∧ 1 ≤ row.version ∧ row.version ≤ known then
    { row with minBinary := some (floorOf row.version raisers) }
  else row

def fill (raisers : List Nat) (known : Nat) : Db → Db
  | [] => []
  | row :: rest => fillRow raisers known row :: fill raisers known rest

/-- The rows of the `n` migrations after version `v`, each with its floor. -/
def newRows (raisers : List Nat) (v : Nat) : Nat → Db
  | 0 => []
  | n + 1 =>
    newRows raisers v n
      ++ [Row.mk (v + n + 1) (some (floorOf (v + n + 1) raisers))]

/-- `State.migrate` by a binary that knows `known` migrations: the database
it leaves, or none if it refuses (and its transaction changes nothing). -/
def migrate (raisers : List Nat) (known : Nat) (db : Db) : Option Db :=
  match schemaAhead (maxVersion (fill raisers known db))
      (dbFloor (fill raisers known db)) known with
  | Verdict.refuses => none
  | Verdict.ahead => some (fill raisers known db)
  | Verdict.migrates =>
    some (fill raisers known db
      ++ newRows raisers (maxVersion (fill raisers known db))
        (known - maxVersion (fill raisers known db)))

/-- The rows as binaries from before `min_binary` left them. -/
def forget : Db → Db
  | [] => []
  | row :: rest => { row with minBinary := none } :: forget rest

/-- The databases there can be: an empty one, then binaries of any age
migrating it in any order, and at any point the floors forgotten, as on a
database that binaries from before `min_binary` migrated. -/
inductive Reachable (raisers : List Nat) : Db → Prop where
  | empty : Reachable raisers []
  | byMigrate {db db' : Db} (known : Nat) :
      Reachable raisers db → migrate raisers known db = some db' →
      Reachable raisers db'
  | byForget {db : Db} : Reachable raisers db → Reachable raisers (forget db)

end Orca.Migrate
