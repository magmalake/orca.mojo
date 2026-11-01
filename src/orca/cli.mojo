"""`orca_main`: the command line every pipeline binary gets.

```
taxi describe                              the DAG as JSON
taxi run --partitions 2024-01..2024-03     runs it, in this process
```

`run` prints one line per step and partition as it settles, then the run's
status, and exits 1 if any step didn't succeed.
"""

from std.sys import argv, exit

from orca.month import Month, parse_months
from orca.pipeline import Pipeline, RunResult, SUCCEEDED

comptime USAGE = """usage:
  <pipeline> describe
  <pipeline> run --partitions YYYY-MM[..YYYY-MM][,...]"""


def orca_main[P: type_of(Pipeline)]() raises:
    """Parses the command line and runs the command on `P`."""
    var args = argv()
    if len(args) < 2:
        print(USAGE)
        exit(2)
    var cmd = String(args[1])
    if cmd == "describe" and len(args) == 2:
        print(P.describe())
        return
    if cmd == "run":
        var partitions = String()
        var i = 2
        while i < len(args):
            var flag = String(args[i])
            if flag == "--partitions" and i + 1 < len(args):
                partitions = String(args[i + 1])
                i += 2
            else:
                print("unknown argument:", flag)
                print(USAGE)
                exit(2)
        if not partitions:
            print("run needs --partitions")
            exit(2)
        var months: List[Month]
        try:
            months = parse_months(partitions)
        except e:
            print(e)
            exit(2)
            return
        var res = P.run(months)
        report(res)
        if not res.succeeded():
            exit(1)
        return
    print(USAGE)
    exit(2)


def report(res: RunResult):
    """One line per step and partition, then the run's status."""
    for i in range(len(res.steps)):
        ref s = res.steps[i]
        var line = String(s.step)
        if s.partition:
            line.write(" ", s.partition)
        line.write(" ", s.status)
        if s.error:
            line.write(": ", s.error)
        print(line)
    print("run", SUCCEEDED if res.succeeded() else "failed")
