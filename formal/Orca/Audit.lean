import Orca

/-! The axioms each headline theorem depends on; scripts/check.sh fails on any
but `propext`, `Quot.sound` and `Classical.choice`. -/

#print axioms Orca.no_double_commit
#print axioms Orca.retry_records_its_own_commit
#print axioms Orca.Pins.retry_reads_same_snapshot
#print axioms Orca.Migrate.runs_only_knowing_the_floor
#print axioms Orca.Migrate.runs_when_only_additive_since
#print axioms Orca.idealEnv_iceberg
#print axioms Orca.idealEnv_postgres
#print axioms Orca.idealEnv_crash
#print axioms Orca.idealEnv_crash_anywhere
