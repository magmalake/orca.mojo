/-!
# A step's reads, pinned across attempts

A step reads tables through `Ctx.pin`: the first time an attempt reads a
table it takes the snapshot an earlier attempt read, or else the table's
current one, records the read in Postgres, and from then on answers from its
own memory. A new attempt starts from what Postgres has on record
(`pinned_reads`). Other writers move the tables' current snapshots in
between, and the worker can crash between any two of these.

Mirrors src/orca/ctx.mojo#Ctx.pin, src/orca/store.mojo#MemoryStore.pinned_reads
and src/orca/state.mojo#PgStore._pinned_reads.
-/
namespace Orca.Pins

/-- A read on record: an attempt read a table at a snapshot. -/
structure Read where
  attempt : Nat
  table : String
  snapshot : Nat
  deriving DecidableEq, Repr

abbrev Reads := List Read

/-- The snapshot of the first read of table `t`: how `Ctx.pin` answers from
the attempt's own reads. -/
def lookup (t : String) : Reads → Option Nat
  | [] => none
  | r :: rest => if r.table = t then some r.snapshot else lookup t rest

/-- The read of the earlier attempt, `r` if there is nothing to compare. -/
def better (r : Read) : Option Read → Read
  | some q => if q.attempt < r.attempt then q else r
  | none => r

/-- The read of table `t` by the earliest attempt that read it. -/
def earliest (t : String) : Reads → Option Read
  | [] => none
  | r :: rest =>
    if r.table = t then some (better r (earliest t rest)) else earliest t rest

/-- What a new attempt is given: each table at the snapshot its earliest
reader read (`pinned_reads`). -/
def pinsOf (l : Reads) (t : String) : Option Nat :=
  match earliest t l with
  | some q => some q.snapshot
  | none => none

/-- The tables' current snapshots, the reads on record, the live attempt's
number, the pins it was given and the reads it has made, and every answer
any attempt was given, oldest first: the last is there to state the theorem,
and nothing reads it. -/
structure State where
  current : String → Nat
  ledger : Reads
  attempt : Nat
  pinned : String → Option Nat
  reads : Reads
  served : List (String × Nat)

/-- The snapshot a first read of `t` takes: the pinned one, or the current. -/
def pinValue (s : State) (t : String) : Nat :=
  match s.pinned t with
  | some v => v
  | none => s.current t

/-- The state after the Postgres write of a first read of `t` left ledger
`l'`: the attempt remembers the read and answers only if the write
succeeded (`Ctx.pin` records before it returns). The model keeps an
attempt's newest read first; `Ctx.reads` keeps it last, which answers the
same, each table being there once. -/
def afterRecord (s : State) (t : String) (l' : Reads) : Bool → State
  | true =>
    { s with
      ledger := l',
      reads := Read.mk s.attempt t (pinValue s t) :: s.reads,
      served := s.served ++ [(t, pinValue s t)] }
  | false => { s with ledger := l' }

/-- One thing happening. In order: the attempt reads a table it has read;
reads one it hasn't, and calls Postgres to record it; the worker crashes and
a new attempt, of any number, starts from the pins on record; other writers
move the tables' current snapshots. -/
def Step (record : Reads → Read → Reads → Bool → Prop) (s s' : State) : Prop :=
  (∃ t v, lookup t s.reads = some v ∧
      s' = { s with served := s.served ++ [(t, v)] })
  ∨ (∃ t l' ok, lookup t s.reads = none ∧
      record s.ledger (Read.mk s.attempt t (pinValue s t)) l' ok ∧
      s' = afterRecord s t l' ok)
  ∨ (∃ a, s' = { s with attempt := a, pinned := pinsOf s.ledger, reads := [] })
  ∨ (∃ c, s' = { s with current := c })

/-- Where a step starts: nothing on record, nothing read, nothing pinned. -/
structure Initial (s : State) : Prop where
  ledger : s.ledger = []
  reads : s.reads = []
  served : s.served = []
  pinned : ∀ t, s.pinned t = none

inductive Reachable (record : Reads → Read → Reads → Bool → Prop) :
    State → Prop where
  | init {s : State} : Initial s → Reachable record s
  | step {s s' : State} :
      Reachable record s → Step record s s' → Reachable record s'

end Orca.Pins
