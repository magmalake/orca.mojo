- **Runs lists filter by status, date, partition and run, and act on many
  runs at once.** `/runs` and a pipeline's Runs tab have a filter bar (a
  plain `GET` form; JavaScript only submits it on change): any of several
  statuses, each with its count under the other filters (`counts` in the
  JSON), the trigger, created `from`/`to` a day or time and quick picks
  (`since=1h`, `today`, `24h`, `7d`), a partition glob or part (`key=2023-08-*`,
  `key=modular/`) and a run id (`run=`, which goes to it). The filters
  combine with the keyset pager, are kept in every link, the live refresh
  and the `1–50 of N` count, and are the same parameters on the JSON
  (`filters` gains `statuses`, `since`, `from`, `to`, `key`, `run`; `status`
  still reads as before). Logged in, rows can be ticked (the page's at once,
  or all that match, their number on the box and checked again by the
  action) and cancelled, retried if failed, or backfilled again over their
  partitions: `POST /runs/bulk`, which says what it did ("Cancelled 12
  runs, 3 already finished") and audits each run as its own action. An index
  on `runs.created` (migration 38) keeps a date filter one index range on
  200,000 runs. `<pipeline> status --status failed,queued` filters a store's
  runs on the command line, on Postgres, SQLite and in memory.
