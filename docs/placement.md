# Placement: where a pipeline's runs run

A pipeline's placement says which machine starts its runs. Set it in
[orca.toml](orca-toml.md) or with `orca register --placement`:

```toml
[pipelines.mojo_repos]
placement = "control"        # small: on the scheduler's own host

[pipelines.gharchive]
placement = "ec2"            # big: instances of its own

[pipelines.mojo_history]
placement = "worker:bgent"   # on a remote worker of pool bgent
```

| Placement | Runs start | Use it for |
|---|---|---|
| `default` (or absent) | on the scheduler's runner, `ORCA_RUNNER`: `local`, `kubernetes` or `ec2` | what orca did before placement |
| `ec2` | on instances of their own, a head and workers per run (`orca.ec2`) | runs that need more than the control host has. On a scheduler whose runner isn't `ec2` they wait, saying why. |
| `control` | as child processes of the scheduler, on its host, with no instance to boot (`orca.control`) | small runs on an EC2 stack: they start in a second instead of minutes |
| `worker:<pool>` | on a remote worker of that pool, never by the scheduler | another machine with spare CPU: [workers.md](workers.md) |

The pipeline page's Config card and `orca config show <pipeline>` say a
pipeline's placement and where it came from. A run that waits for its
placement stays queued with the reason in its page's held line (and
`held` in the JSON).

## Control: limits

A control-placed run shares its host with Postgres, the scheduler and the
UI, so the scheduler bounds it:

| Variable | Default | |
|---|---|---|
| `ORCA_CONTROL_MAX_RUNS` | 2 | control-placed runs at once, of all pipelines |
| `ORCA_CONTROL_MEMORY_MB` | 1024 | a run whose processes' resident memory goes over is killed, its attempts failed saying why; where runs get cgroups (`orca.cgroup`), it's the most a run's limit (`memory.high`) can be, and the cgroup does the killing |
| `ORCA_CONTROL_NICE` | 10 | the `nice` its processes run at, so the host's own processes win |
| `ORCA_CONTROL_WORKERS` | 1 | local dista workers a run forks |

None starts while the control host's newest minute (Ops › Control) has
memory or its fullest disk over 85%. On Linux its processes are also the
first the kernel kills if memory runs out (`oom_score_adj` 1000).

Its secrets (`secrets` in orca.toml) are read by the scheduler when it
starts the run, with `ORCA_SECRET_FETCH` if set, else from Secrets Manager,
and go into that run's environment only. Its driver output is kept,
masked, in the run's `driver.log`.

```sh
orca register --placement control ./build/toy
pixi run test-placement      # that, within the limits, beside runs on the stand-in EC2
```

On the [Hetzner server](hetzner.md) the runner is `local`, so `default`
already runs on the server; `control` runs there too, within the limits
above.
