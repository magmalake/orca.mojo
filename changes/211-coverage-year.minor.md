- **A year of coverage.** A daily pipeline's Coverage card (Operate) has
  **Month** and **Year** tabs, Year the default and the choice kept in
  `localStorage` (`?cov=month|year` without scripts). Year is twelve small
  months of days in the same colours and tooltips, each named with its
  share done (`28/31 done, 1 failed`) and linking to its month, `‹ 2026 ›`
  to move within the range, a legend with the year's counts, and a strip
  of every month of the range (ten years at most) shaded by how much is
  done, each linking to its year; at phone width the months wrap two to a
  row. Both tabs come from one read of the year, the same three queries a
  month took (the strip's days join the year's in the one `DONE_SQL`).
  Monthly pipelines keep their year of months. `orca.ui_coverage`,
  `docs/ui.md#coverage`, `tests/calendar.sh`.
