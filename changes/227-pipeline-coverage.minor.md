- **Each pipeline shows one coverage number before anyone clicks.** The
  home page's table has a Coverage column (the percent over a thin bar,
  the counts on hover, with a "?" like every column), and a pipeline's
  Progress card a Coverage line (`1141 / 1407 days done (81%)`) linking to
  its year calendar, or to Keys for a keyed pipeline. It's a daily or
  monthly pipeline's partitions done of those in its range so far (its
  first through the last that has ended), or a keyed pipeline's keys done
  of the keys orca last read; done is what the calendar counts (every
  mapped step succeeded in a run of the current code), and a pipeline with
  nothing to count shows "—". The JSON has it as `coverage {done, total,
  pct, unit}` on each pipeline of `/` and on `/api/pipelines/<name>`
  (null for none). It's one query a pipeline, `orca.coverage`.
- **Which keys are done is fast for a pipeline without a mapped step.**
  The query the calendar, backfills and coverage share read every
  succeeded run once per key for such a pipeline (1.2 s for 1,500 days
  over 30,000 runs); it now reads their keys once (10 ms).
