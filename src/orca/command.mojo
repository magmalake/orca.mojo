"""The `orca` command: the scheduler, the UI and registration, in a binary
that links no pipeline. A project builds its own from two lines:

    from orca.command import orca_command

    def main() raises:
        orca_command()

Its commands:

    pixi run orca
    ./build/orca locate ./build/taxi --root .   # taxi.source.json, for the UI's links
    ./build/orca register ./build/taxi [...]
    ./build/orca backfill taxi 2011-01..2024-12
    ./build/orca backfill --rerun taxi 2024-03     # again, though it succeeded
    ./build/orca cancel taxi @all   # its queued, running and retrying runs
    ./build/orca pause taxi         # start none of its runs; orca unpause taxi
    ./build/orca tick
    ./build/orca scheduler
    ./build/orca ui                 # http://127.0.0.1:8270, or ORCA_UI_ADDR
    ./build/orca sample --out m.jsonl --pid 1234   # machine metrics
    ./build/orca lineage lake.report [<snapshot>]
    ./build/orca rebuild raw.events 2024-03
    ./build/orca status taxi        # its state and latest runs
    ./build/orca login https://orca.example.com   # then --remote, below
    ./build/orca --remote orca.example.com backfill taxi 2024-03

Needs `ORCA_DATABASE_URL`, but for the commands `--remote` (or
`ORCA_REMOTE`) sends to a deployed `orca ui` instead (`orca.remote`). See
`orca.scheduler` for what a tick does, and `orca.ui` for the pages,
`orca.lineage` for what `lineage` prints, and `orca.sensors` for what
`rebuild` plans.
Exit codes: 0, 2 for a usage error, 3 for any other error.
"""

from std.os import getenv
from std.sys import argv, exit, stderr
from std.time import sleep

from orca.actions import cancel_runs, pause_pipeline
from orca.auth import add_user, remove_user, users
from orca.aws import aws_command
from orca.civil import now
from orca.expiry import expirable, expire_argv, maintain_binary
from orca.lineage import Made, SnapshotRef, back, forward, latest
from orca.locate import locate
from orca.proc import binary_seconds, capture
from orca.remote import (
    Api,
    api_command,
    is_remote_command,
    login_command,
    logout_command,
    remote_api,
    remote_target,
)
from orca.sample import sample_main
from orca.scheduler import Scheduler
from orca.secrets import secret_problem
from orca.sensors import rebuild, rebuilds
from orca.state import State
from orca.tokens import revoke_user_tokens, user_tokens
from orca.ui import DEFAULT_ADDR, serve_ui

comptime USAGE = """usage:
  orca locate <pipeline binary> [...] [--root <dir>]
  orca aws up|push|register|app|status|down ...   (orca aws for more)
  orca register [--image <image>] [--head-type T] [--worker-type T]
                [--workers N|MIN..MAX] [--spot] [--secret NAME=ID ...]
                <pipeline binary> [...]
  orca backfill [--rerun] [--preview] <pipeline> KEY[..KEY][,...]
                (keyed: NAME[,...]|@all|@file:<path>; --rerun: done ones too;
                --preview: what it would queue, queueing nothing)
  orca start <pipeline> KEY[..KEY][,...]   (one run over them all)
  orca cancel <pipeline> KEY[..KEY][,...]|@all
                (its queued, running and retrying runs over those keys)
  orca cancel <run>
  orca resume [--allow-new-code] <run>
  orca rerun <run> <step>           (a new run, from that step on)
  orca pause|unpause <pipeline>     (the scheduler starts none of its runs)
  orca status [<pipeline>] [--json]
  orca tick
  orca scheduler
  orca ui
  orca sample --out <location>|- [--pid PID] [--match PATH] [--dirs D,...]
              [--seconds S] [--count N] [--keep N]
                                      (machine metrics, orca.sample)
  orca lineage <dataset> [<snapshot>]
  orca rebuild <dataset> KEY[..KEY][,...]
  orca expire <dataset>|--all [--older-than 30d|12h|90m] [--keep N]
              [--dry-run] [--report]   (snapshot expiry, via orca_maintain)
  orca user add|remove <name>       (add reads the password from stdin)
  orca user list
  orca user tokens <name>           (its API tokens, from orca login)
  orca user revoke <name> <id>|@all
  orca login [<url>] [--user NAME] [--days N] [--label L]
  orca logout [<url>]
  orca --remote <url> backfill|start|cancel|pause|unpause|resume|rerun|
                      rebuild|status|lineage ...
                (to a deployed orca ui, as the user orca login logged in;
                or ORCA_REMOTE=<url>)"""


def _usage(msg: String = ""):
    if msg:
        print(msg)
    print(USAGE)
    exit(2)


def _locate(args: List[String]) raises -> Int:
    """`orca locate`: each binary's `<binary>.source.json`."""
    var root = String(".")
    var binaries = List[String]()
    var i = 2
    while i < len(args):
        if args[i] == "--root" and i + 1 < len(args):
            root = args[i + 1]
            i += 2
        else:
            binaries.append(args[i])
            i += 1
    if len(binaries) == 0:
        _usage("locate needs a pipeline binary")
    for b in binaries:
        var got = capture([b, "describe"], binary_seconds())
        if got[0] != 0:
            raise Error("orca: ", b, " describe exited ", got[0], ": ", got[1])
        var map = locate(got[1], root)
        with open(String(b, ".source.json"), "w") as f:
            f.write(map, "\n")
        print("located", b, map)
    return 0


def _command(args: List[String], target: String) raises -> Int:
    if args[1] == "login":
        return login_command(args, target)
    if args[1] == "logout":
        return logout_command(args, target)
    if target and is_remote_command(args[1]):
        var api = remote_api(target)
        return api_command(api, args)
    if args[1] == "locate":
        return _locate(args)
    if args[1] == "aws":
        return aws_command(args)
    if args[1] == "sample":
        return sample_main(args)
    var url = getenv("ORCA_DATABASE_URL")
    if not url:
        _usage("orca needs ORCA_DATABASE_URL")
    var cmd = args[1]
    # Commands that print what a page of the UI shows, or do what its
    # buttons do, over the database here as `--remote` does over HTTPS.
    if (
        cmd == "status"
        or cmd == "start"
        or cmd == "resume"
        or cmd == "rerun"
        or (cmd == "cancel" and len(args) == 3)
        or (cmd == "backfill" and "--preview" in args)
    ):
        var api = Api(url)
        return api_command(api, args)
    if cmd == "register" and len(args) >= 3:
        var image = String()
        var head_type = String()
        var worker_type = String()
        var workers = 0
        var min_workers = -1
        var spot = False
        var secrets = List[String]()
        var i = 2
        while i < len(args) and args[i].startswith("--"):
            if args[i] == "--spot":
                spot = True
                i += 1
                continue
            if i + 1 >= len(args):
                _usage(String(args[i], " needs a value"))
            if args[i] == "--image":
                image = args[i + 1]
            elif args[i] == "--head-type":
                head_type = args[i + 1]
            elif args[i] == "--worker-type":
                worker_type = args[i + 1]
            elif args[i] == "--secret":
                # NAME=ID: the run's NAME is Secrets Manager's secret ID.
                var why = secret_problem(args[i + 1])
                if why:
                    _usage(why)
                secrets.append(args[i + 1])
            elif args[i] == "--workers":
                # N, or MIN..MAX for autoscaling on EC2.
                try:
                    var v = args[i + 1]
                    var dots = v.find("..")
                    if dots >= 0:
                        min_workers = Int(v[byte=:dots])
                        workers = Int(v[byte = dots + 2 :])
                        if min_workers < 0 or workers < max(min_workers, 1):
                            raise Error("bad range")
                    else:
                        workers = Int(v)
                except:
                    _usage(
                        String("not a count or a range MIN..MAX: ", args[i + 1])
                    )
            else:
                _usage(String("unknown option: ", args[i]))
            i += 2
        if i >= len(args):
            _usage("register needs a pipeline binary")
        var s = Scheduler(url)
        for k in range(i, len(args)):
            print(
                "registered",
                s.register(
                    args[k],
                    now(),
                    image,
                    head_type,
                    worker_type,
                    workers,
                    spot,
                    min_workers,
                    secrets,
                ),
            )
        return 0
    if cmd == "backfill" and (
        len(args) == 4 or (len(args) == 5 and args[2] == "--rerun")
    ):
        # --rerun queues keys that are done as well; a key in a queued or
        # running run is still left to it.
        var rerun = len(args) == 5
        var s = Scheduler(url)
        var got = s.backfill(args[len(args) - 2], args[len(args) - 1], rerun)
        # Why keys weren't queued goes to stderr, so the count stays the
        # one line of output.
        for line in got.skipped():
            print(line, file=stderr)
        print("queued", len(got.runs), "runs")
        return 0
    if cmd == "cancel" and len(args) == 4:
        var db = State(url)
        db.migrate()
        var o = cancel_runs(db, args[2], args[3])
        if o.refused:
            raise Error("orca: ", o.refused)
        print("cancelled", o.queued, "runs")
        return 0
    if (cmd == "pause" or cmd == "unpause") and len(args) == 3:
        var db = State(url)
        db.migrate()
        var o = pause_pipeline(db, args[2], cmd == "pause")
        if o.refused:
            raise Error("orca: ", o.refused)
        print(cmd + "d", args[2])
        return 0
    if cmd == "tick" and len(args) == 2:
        var s = Scheduler(url)
        s.wait_for_binaries = True
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
    if cmd == "expire" and len(args) >= 3:
        return _expire(url, args)
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
        if args[2] == "tokens" and len(args) == 4:
            var ts = user_tokens(db, args[3])
            if len(ts) == 0:
                print("no tokens")
            for t in ts:
                print(
                    t.id,
                    t.label,
                    "created",
                    t.created,
                    "used",
                    t.last_used if t.last_used else String("never"),
                    "expired" if t.expired else "expires",
                    t.expires,
                )
            return 0
        if args[2] == "revoke" and len(args) == 5:
            var id = 0
            if args[4] != "@all":
                try:
                    id = Int(args[4])
                except:
                    id = -1
                if id <= 0:
                    _usage(String("not a token's id, or @all: ", args[4]))
            var n = revoke_user_tokens(db, args[3], id)
            if n == 0 and id > 0:
                raise Error("orca: ", args[3], " has no token ", id)
            print("revoked", n, "tokens of" if n != 1 else "token of", args[3])
            return 0
    if cmd == "ui" and len(args) == 2:
        var db = State(url)
        db.migrate()
        serve_ui(url, getenv("ORCA_UI_ADDR", DEFAULT_ADDR))
        return 0
    _usage()
    return 2


def _expire(url: String, args: List[String]) raises -> Int:
    """`orca expire`: each dataset's `orca_maintain expire`, with the
    snapshots orca still needs retained (`orca.expiry`)."""
    var older = String()
    var keep = String()
    var dry_run = False
    var report = False
    var i = 3
    while i < len(args):
        if args[i] == "--older-than" and i + 1 < len(args):
            older = args[i + 1]
            i += 2
        elif args[i] == "--keep" and i + 1 < len(args):
            keep = args[i + 1]
            i += 2
        elif args[i] == "--dry-run":
            dry_run = True
            i += 1
        elif args[i] == "--report":
            report = True
            i += 1
        else:
            _usage(String("unknown argument: ", args[i]))
    var binary = maintain_binary()
    if not binary:
        raise Error(
            "orca: no orca_maintain binary (set ORCA_MAINTAIN, or build"
            " bin/orca_maintain.mojo calling orca.maintain.maintain_main"
            " beside orca)"
        )
    var db = State(url)
    db.migrate()
    var datasets = List[String]()
    if args[2] == "--all":
        datasets = expirable(db)
    else:
        datasets.append(args[2])
    var code = 0
    for ds in datasets:
        var a = expire_argv(db, binary, ds, older, keep, dry_run, report)
        var got = capture(a, 0)
        var out = String(got[1].strip())
        if got[0] != 0:
            print("orca: expiring", ds, "failed:", out, file=stderr)
            code = 1
        elif out:
            print(out)
    return code


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


def orca_command() raises:
    """The `orca` command: reads the command line, runs it, and exits with
    its code. A project's own `orca` binary is a `main` calling this."""
    var args = List[String]()
    for a in argv():
        args.append(String(a))
    var explicit = len(args) >= 2 and (
        args[1].startswith("--remote") or args[1].startswith("--url")
    )
    var target = String()
    try:
        target = remote_target(args)
    except e:
        print(e)
        exit(2)
    if len(args) < 2:
        _usage()
        return
    if explicit and not is_remote_command(args[1]):
        if args[1] != "login" and args[1] != "logout":
            _usage(
                String(
                    "orca ",
                    args[1],
                    " needs the database (ORCA_DATABASE_URL); --remote is for",
                    " the commands below that it lists",
                )
            )
    var code: Int
    try:
        code = _command(args, target)
    except e:
        var msg = String(e)
        if msg.startswith("usage: "):
            _usage(String(msg[byte=7:]))
        print(msg)
        code = 3
    exit(code)
