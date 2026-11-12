import Morca

/-! The axioms each headline theorem depends on; scripts/check.sh fails on any
but `propext`, `Quot.sound` and `Classical.choice`. -/

#print axioms Morca.no_double_commit
#print axioms Morca.retry_records_its_own_commit
#print axioms Morca.Pins.retry_reads_same_snapshot
#print axioms Morca.Fence.commit_only_by_owner
#print axioms Morca.Migrate.runs_only_knowing_the_floor
#print axioms Morca.Migrate.runs_when_only_additive_since
#print axioms Morca.idealEnv_iceberg
#print axioms Morca.idealEnv_postgres
#print axioms Morca.idealEnv_crash
#print axioms Morca.idealEnv_crash_anywhere
