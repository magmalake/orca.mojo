- **`fill_lag` holds a partition back after it ends.** A pipeline's
  `fill_lag` in orca.toml (`"2h"`, `"90m"`; default `0`) fills a partition
  only once its end is at least that far in the past: a Daily day D from
  D+1 00:00 plus the lag. For sources that publish late, such as GH
  Archive, whose last hour of a day isn't there at 00:00 UTC, so the fill's
  attempts failed until it was and tripped the watchdog. Until then the
  partition is outside the fill's range and the coverage, and the Fill line
  and `/api/pipelines/<name>` (`waits_for_lag`) say when it fills. Shown and
  exported by `orca config`; kept in a new nullable column
  (`orca.pipelines.fill_lag`, an additive migration) (docs/orca-toml.md#the-lag).
