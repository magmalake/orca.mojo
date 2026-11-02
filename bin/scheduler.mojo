"""`orca`: the scheduler, a binary that links no pipeline.

    pixi run orca
    ./build/orca register ./build/taxi [...]
    ./build/orca backfill taxi 2011-01..2024-12
    ./build/orca tick
    ./build/orca scheduler

Needs `ORCA_DATABASE_URL`. See `orca.scheduler` for what a tick does.
Exit codes: 0, 2 for a usage error, 3 for any other error.
"""

from std.os import getenv
from std.sys import argv, exit
from std.time import sleep

from orca.civil import now
from orca.scheduler import Scheduler

comptime USAGE = """usage:
  orca register <pipeline binary> [...]
  orca backfill <pipeline> KEY[..KEY][,...]
  orca tick
  orca scheduler"""


def _usage(msg: String = ""):
    if msg:
        print(msg)
    print(USAGE)
    exit(2)


def _command(args: List[String]) raises -> Int:
    var url = getenv("ORCA_DATABASE_URL")
    if not url:
        _usage("orca needs ORCA_DATABASE_URL")
    var cmd = args[1]
    if cmd == "register" and len(args) >= 3:
        var s = Scheduler(url)
        for i in range(2, len(args)):
            print("registered", s.register(args[i], now()))
        return 0
    if cmd == "backfill" and len(args) == 4:
        var s = Scheduler(url)
        print("queued", s.backfill(args[2], args[3]), "runs")
        return 0
    if cmd == "tick" and len(args) == 2:
        var s = Scheduler(url)
        s.tick(now())
        return 0
    if cmd == "scheduler" and len(args) == 2:
        var s = Scheduler(url)
        while True:
            # A tick that fails (the database is down) is tried again; the
            # state it would have changed is all in Postgres.
            try:
                s.tick(now())
            except e:
                print("orca: tick failed:", e)
                try:
                    s = Scheduler(url)
                except:
                    pass
            sleep(1.0)
    _usage()
    return 2


def main() raises:
    var args = List[String]()
    for a in argv():
        args.append(String(a))
    if len(args) < 2:
        _usage()
        return
    var code: Int
    try:
        code = _command(args)
    except e:
        print(e)
        code = 3
    exit(code)
