"""`orca`: the scheduler, a binary that links no pipeline.

    pixi run orca
    ./build/orca register ./build/taxi [...]
    ./build/orca backfill taxi 2011-01..2024-12
    ./build/orca tick
    ./build/orca scheduler
    ./build/orca ui                 # http://127.0.0.1:8270, or ORCA_UI_ADDR
    ./build/orca lineage lake.report [<snapshot>]
    ./build/orca rebuild raw.events 2024-03

Needs `ORCA_DATABASE_URL`. See `orca.scheduler` for what a tick does, and
`orca.ui` for the pages, `orca.lineage` for what `lineage` prints, and
`orca.sensors` for what `rebuild` plans.
Exit codes: 0, 2 for a usage error, 3 for any other error.
"""

from std.os import getenv
from std.sys import argv, exit
from std.time import sleep

from orca.auth import add_user, remove_user, users
from orca.civil import now
from orca.lineage import Made, SnapshotRef, back, forward, latest
from orca.scheduler import Scheduler
from orca.sensors import rebuild, rebuilds
from orca.state import State
from orca.ui import DEFAULT_ADDR, serve_ui

comptime USAGE = """usage:
  orca register [--image <image>] <pipeline binary> [...]
  orca backfill <pipeline> KEY[..KEY][,...]   (keyed: NAME[,...]|@all|@file:<path>)
  orca tick
  orca scheduler
  orca ui
  orca lineage <dataset> [<snapshot>]
  orca rebuild <dataset> KEY[..KEY][,...]
  orca user add|remove <name>       (add reads the password from stdin)
  orca user list"""


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
        var image = String()
        var first = 2
        if args[2] == "--image":
            if len(args) < 5:
                _usage("register --image needs an image and a binary")
            image = args[3]
            first = 4
        var s = Scheduler(url)
        for i in range(first, len(args)):
            print("registered", s.register(args[i], now(), image))
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
    if cmd == "lineage" and (len(args) == 3 or len(args) == 4):
        var db = State(url)
        db.migrate()
        var id: Int64
        if len(args) == 4:
            try:
                id = Int64(Int(args[3]))
            except:
                _usage(String("not a snapshot: ", args[3]))
                return 2
        else:
            id = latest(db, args[2])
            if id < 0:
                raise Error("orca: no snapshot of ", args[2], " is recorded")
        var start = SnapshotRef(args[2], id)
        print(start)
        print("made from:")
        _print_made(back(db, start))
        print("made from it:")
        _print_made(forward(db, start))
        return 0
    if cmd == "rebuild" and len(args) == 4:
        var db = State(url)
        db.migrate()
        var id = rebuild(db, args[2], args[3])
        print("rebuild", id)
        for p in rebuilds(db, id)[0].plan:
            print("  level", p.level, p.pipeline, p.partitions)
        return 0
    if cmd == "user" and len(args) >= 3:
        var db = State(url)
        db.migrate()
        if args[2] == "list" and len(args) == 3:
            for u in users(db):
                print(u)
            return 0
        if args[2] == "add" and len(args) == 4:
            var password = String(input().rstrip("\r\n"))
            if add_user(db, args[3], password):
                print("added", args[3])
            else:
                print(
                    "changed the password of",
                    args[3],
                    "and ended their sessions",
                )
            return 0
        if args[2] == "remove" and len(args) == 4:
            if not remove_user(db, args[3]):
                raise Error("orca: no user ", args[3])
            print("removed", args[3])
            return 0
    if cmd == "ui" and len(args) == 2:
        var db = State(url)
        db.migrate()
        serve_ui(url, getenv("ORCA_UI_ADDR", DEFAULT_ADDR))
        return 0
    _usage()
    return 2


def _print_made(ms: List[Made]):
    """One line per snapshot: how far it is, what made it, and from what."""
    if len(ms) == 0:
        print("  nothing")
    for m in ms:
        var line = String("  ", m.depth, " ", m.at, " by ", m.pipeline, " run ")
        line.write(m.run, " ", m.step)
        if m.partition:
            line.write(" ", m.partition)
        line.write(" code ", m.code)
        if len(m.inputs) > 0:
            line.write(" from")
            for i in m.inputs:
                line.write(" ", i)
        print(line)


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
