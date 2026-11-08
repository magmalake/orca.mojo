- **Ops › Pools: one page for every machine runs are placed on.** Fleet
  and Workers are one page, `/ops/pools`: a row per machine of each pool,
  the EC2 instances orca launches (`ec2`, live and for an hour after they
  end), a Kubernetes run's pods (`kubernetes`, on that runner) and the
  machines running `orca worker` (`worker:<pool>`), with its kind, state
  (`live`, `draining`, `gone`), image, CPUs, memory, runs, last heartbeat,
  CPU and the worker as columns of its row, each with its help. `?pool=`
  and `?kind=` filter it with plain links; with EC2 in view it keeps
  Fleet's cards and charts; it refreshes every 10 s from
  `/ops/pools/live`, and `/api/pools` is its JSON. A machine's page
  (`/ops/pools/<kind>/<id>`) has an instance's samples over its run, or
  the runs a worker claimed. The Ops menu is Control, Pools and Watchdog.
  `/ops/fleet`, `/ops/workers`, their fragments and `/fleet` redirect to
  Pools with the kind they showed (301), and still answer their JSON to a
  script that asks for JSON; `/api/fleet` and `/api/workers` are
  unchanged. `docs/ui.md#pools`, `tests/fleet.sh`.
