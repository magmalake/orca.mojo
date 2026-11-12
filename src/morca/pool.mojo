"""An EC2 instance's side of the warm pool (`orca.ec2`,
`ORCA_EC2_POOL_SECONDS`): the two commands its script runs between runs,
through the pipeline's binary, over `ORCA_DATABASE_URL`.

```
<binary> pool-wait <instance> <run> <start>             a worker: until its run is over
<binary> pool-next <instance> <run> <start> <seconds>   until it's handed another run
```

The scheduler hands an idle instance to a run by ending its row in
`orca.instances` and inserting one for the new run, `pooled`, in one
transaction. So the instance learns of its next run from its own rows, and
Postgres, which every instance reaches already, is the only channel: no
other service, port or permission. `pool-next` marks the row of the run the
instance served `idle` (its work has stopped: the head's driver and dista
head have exited, a worker's processes are killed), and beats `polled`
every second, which the scheduler checks before it hands the instance out.
When a new row appears, it records that it `joined`, prints `next <run>
<start> <head address> <0|1>` (the last, whether the run may use new code) and
exits 0; when its row ends with no other (the scheduler shut the idle
instance down), or `seconds` and a minute more pass, it exits 1 and the
script shuts the instance down.

`pool-wait`, beside a worker's processes, exits 0 once the run isn't running
at that start any more, or the worker's row has ended: dista workers don't
end with their head, so this is how a worker knows to stop. Both ride out
a database that can't be reached for a while, retrying every second.
"""

from std.os import getenv
from std.time import perf_counter_ns, sleep

from postgres import Params

from orca.state import State


def _over(mut db: State, iid: String, run: Int, start: Int) raises -> Bool:
    var rows = db.conn.query(
        (
            "SELECT r.status <> 'running' OR r.starts <> $3 OR i.id IS NULL OR"
            " i.ended IS NOT NULL AS over FROM orca.runs r LEFT JOIN"
            " orca.instances i ON i.id = $1 AND i.run = r.id AND i.start = $3"
            " WHERE r.id = $2"
        ),
        Params().text(iid).int64(Int64(run)).int64(Int64(start)),
    )
    return len(rows) == 0 or rows.row(0).bool("over")


def pool_wait_main(args: List[String]) raises -> Int:
    """`pool-wait <instance> <run> <start>`: returns once the run is over
    for the instance."""
    var iid = args[2]
    var run = Int(args[3])
    var start = Int(args[4])
    var url = getenv_url()
    var db: Optional[State] = None
    var said = False
    while True:
        try:
            if not db:
                db = State(url)
            if _over(db.value(), iid, run, start):
                return 0
        except e:
            if not said:
                print("orca: pool-wait can't read run", run, ":", e)
                said = True
            db = None
        sleep(1.0)


def pool_next_main(args: List[String]) raises -> Int:
    """`pool-next <instance> <run> <start> <seconds>`: waits, idle, for the
    run the scheduler hands the instance; see the module's doc."""
    var iid = args[2]
    var run = Int(args[3])
    var start = Int(args[4])
    var limit = Float64(args[5]) + 60
    var url = getenv_url()
    var t0 = perf_counter_ns()
    var db: Optional[State] = None
    var said = False
    while Float64(perf_counter_ns() - t0) / 1e9 < limit:
        try:
            if not db:
                db = State(url)
            ref c = db.value().conn
            var mine = c.execute(
                (
                    "UPDATE orca.instances SET idle = coalesce(idle, now()),"
                    " polled = now() WHERE id = $1 AND run = $2 AND start = $3"
                    " AND ended IS NULL"
                ),
                Params().text(iid).int64(Int64(run)).int64(Int64(start)),
            )
            if mine == 0:
                var got = c.query(
                    (
                        "UPDATE orca.instances i SET joined = now() FROM"
                        " orca.runs r WHERE r.id = i.run AND i.id = $1 AND"
                        " i.ended IS NULL AND i.pooled RETURNING i.run,"
                        " i.start, r.allow_new_code, coalesce((SELECT"
                        " h.address FROM orca.instances h WHERE h.run = i.run"
                        " AND h.start = i.start AND h.role = 'head' AND"
                        " h.ended IS NULL), '') AS head"
                    ),
                    Params().text(iid),
                )
                if len(got) == 0:
                    return 1  # ended: the scheduler shut it down
                var r = got.row(0)
                print(
                    "next",
                    r.int64("run"),
                    r.int64("start"),
                    r.text("head") if r.text("head") else String("-"),
                    1 if r.bool("allow_new_code") else 0,
                )
                return 0
        except e:
            if not said:
                print("orca: pool-next can't reach the database:", e)
                said = True
            db = None
        sleep(1.0)
    return 1


def getenv_url() raises -> String:
    var url = getenv("ORCA_DATABASE_URL")
    if not url:
        raise Error("orca: the pool's commands need ORCA_DATABASE_URL")
    return url^
