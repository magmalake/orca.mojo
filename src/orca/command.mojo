"""The `orca` command: the scheduler, the UI and registration, in a binary
that links no pipeline. A project builds its own from two lines:

    from orca.command import orca_command

    def main() raises:
        orca_command()

Its commands:

    pixi run orca
    ./build/orca locate ./build/taxi --root .   # taxi.source.json, for the UI's links
    ./build/orca register ./build/taxi [...]   # with ./orca.toml (orca.config)
    ./build/orca config show taxi   # its settings, and where each came from
    ./build/orca config export      # an orca.toml from the registrations
    ./build/orca backfill taxi 2011-01..2024-12
    ./build/orca backfill --rerun taxi 2024-03     # again, though it succeeded
    ./build/orca cancel taxi @all   # its queued, running and retrying runs
    ./build/orca pause taxi         # start none of its runs; orca unpause taxi
    ./build/orca tick
    ./build/orca scheduler
    ./build/orca ui                 # http://127.0.0.1:8270, or ORCA_UI_ADDR
                                    # (the worker API on ORCA_WORKER_ADDR too)
    ./build/orca sample --out m.jsonl --pid 1234   # machine metrics
    ./build/orca lineage lake.report [<snapshot>]
    ./build/orca rebuild raw.events 2024-03
    ./build/orca status taxi        # its state and latest runs
    ./build/orca login https://orca.example.com   # then --remote, below
    ./build/orca --remote orca.example.com backfill taxi 2024-03

Needs `ORCA_DATABASE_URL`, but for the commands `--remote` (or
`ORCA_REMOTE`) sends to a deployed `orca ui` instead (`orca.remote`). See
`orca.scheduler` for what a tick does, and `orca.web` for the pages,
`orca.lineage` for what `lineage` prints, and `orca.sensors` for what
`rebuild` plans.
Exit codes: 0, 2 for a usage error, 3 for any other error.
"""

from std.os import getenv
from std.os.path import exists
from std.sys import argv, exit, stderr
from std.time import sleep

from postgres import Params

from orca.actions import cancel_runs, pause_pipeline
from orca.auth import add_user, remove_user, users
from orca.aws import aws_command
from orca.aws_replace import replace_control
from orca.civil import now
from orca.expiry import expirable, expire_argv, maintain_binary
from orca.lineage import Made, SnapshotRef, back, forward, latest
from orca.locate import locate
from orca.notify import (
    alert,
    configured as notify_configured,
    flush as flush_notify,
    resolve as resolve_alert,
    send_now,
)
from orca.proc import absolute, binary_seconds, capture
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
from orca.config import (
    ProjectConfig,
    RegisterFlags,
    canonical_flag,
    export_toml,
    show_json,
    shown,
)
from orca.scheduler import Scheduler
from orca.control import fetch_secret
from orca.ec2 import vcpus
from orca.fleet import hourly_price
from orca.secrets import secret_problem
from orca.sensors import rebuild, rebuilds
from orca.state import State
from orca.watchdog import acknowledge, snooze as snooze_rule
from orca.auth import audit
from orca.tokens import revoke_user_tokens, user_tokens
from orca.web.server import DEFAULT_ADDR, serve_ui
from orca.worker import runner_command, worker_command
from orca.workers import (
    create_worker_token,
    revoke_worker_token,
    worker_rows,
    worker_token_days,
)

comptime USAGE = """usage:
  orca locate <pipeline binary> [...] [--root <dir>]
  orca aws up|push|register|app|status|replace-control|down ...
                                    (orca aws for more)
  orca register [--image <image>] [--head-type T] [--worker-type T]
                [--workers N|MIN..MAX] [--worker-procs N|auto] [--spot]
                [--secret NAME=ID ...]
                [--placement control|ec2|worker:<pool>|default]
                [--max-runs N] [--cpus N|MIN..MAX] [--memory-gb N|MIN..MAX]
                [--config <orca.toml>|--no-config]
                [<pipeline binary> ...]
                (no binaries: every [pipelines.<name>] of the file, in
                ORCA_PIPELINES_DIR or pipelines/ next to it; how its runs run comes from orca.toml, ./orca.toml or
                ORCA_CONFIG unless --config names another, flags over it;
                orca config show)
                (--worker-procs: dista worker processes per EC2 worker
                instance, auto = one per vCPU; default 1. --placement
                control: runs on the scheduler's own host, orca.control;
                default: the scheduler's runner)
                <pipeline binary> [...]
  orca backfill [--rerun [--allow-append]] [--preview] <pipeline>
                KEY[..KEY][,...]
                (keyed: NAME[,...]|@all|@file:<path>; --rerun: done ones too,
                refused for steps that append unless --allow-append;
                --preview: what it would queue, queueing nothing)
  orca start <pipeline> KEY[..KEY][,...]   (one run over them all)
  orca cancel <pipeline> KEY[..KEY][,...]|@all
                (its queued, running and retrying runs over those keys)
  orca cancel <run>
  orca resume [--allow-new-code] <run>
  orca rerun [--allow-append] <run> <step>
                                    (a new run, from that step on)
  orca pause|unpause <pipeline>     (the scheduler starts none of its runs)
  orca status [<pipeline>] [--json]
  orca config show [<pipeline>] [--json]
                (how each runs, and where each value came from: code
                default, orca.toml, flag, environment)
  orca config export [--out <path>|-] [--force]
                (an orca.toml from what's registered; default ./orca.toml)
  orca config check [<orca.toml>] [--secrets]
                (checks a file as register does, and its instance types,
                with no database; --secrets reads each secret too)
  orca watchdog status [--json]     (rules, each threshold and value, trips)
  orca watchdog snooze <rule> <duration>   (2h, 30m, 1d; off ends it)
  orca watchdog ack [--all|<trip id>]   (keep a trip's pipelines paused and
                                    clear its banner; status still lists it)
  orca alerts [--kind K] [--open|--resolved] [--limit N] [--json]
                                    (the alerts sent, newest first: Ops ›
                                    Alerts)
  orca notify test                  (sends a test message to ORCA_NOTIFY_URL,
                                    orca.notify)
  orca notify --kind K [--subject S] [--title T] [--priority high|default|
              low] [--tags a,b] [--click URL] [--resolved] [message ...]
                                    (an alert from a script, held back like
                                    the scheduler's: once per kind and subject
                                    in ORCA_NOTIFY_MIN_SECONDS)
  orca tick
  orca scheduler
  orca runner [--name N] [--max-runs N] [--poll-seconds S] [--dir D]
                                    (starts what the scheduler assigns it with
                                    ORCA_LOCAL_RUNNER=runner: docs/runner.md)
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
  orca worker-token create --pool <pool> [--days N] [--label L]
  orca worker-token list
  orca worker-token revoke <id>
  orca worker --pool <pool> [--remote <url>] [--token-file <path>]
              [--max-runs N] [--poll-seconds S] [--dir <dir>]
              [--image <image>] [--name <name>]
                (runs pool's runs here, over the UI's API: orca.worker)
  orca login [<url>] [--user NAME] [--days N] [--label L]
  orca logout [<url>]
  orca --remote <url> backfill|start|cancel|pause|unpause|resume|rerun|
                      rebuild|status|lineage|watchdog|alerts ...
                (to a deployed orca ui, as the user orca login logged in;
                or ORCA_REMOTE=<url>)"""


def _backfill_flags(args: List[String]) -> Bool:
    """Whether `orca backfill` has its two arguments after flags it knows
    here: `--rerun`, and `--allow-append` with it."""
    if len(args) < 4:
        return False
    var rerun = False
    var allow = False
    for i in range(2, len(args) - 2):
        if args[i] == "--rerun" and not rerun:
            rerun = True
        elif args[i] == "--allow-append" and not allow:
            allow = True
        else:
            return False
    return rerun or not allow


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


def _notify(args: List[String]) raises -> Int:
    """`orca notify`: a message to the person ORCA_NOTIFY_URL reaches
    (`orca.notify`). `test` sends one at once. Otherwise it's an alert of a
    kind and subject: recorded in Postgres, so it isn't sent twice in
    `ORCA_NOTIFY_MIN_SECONDS` and is tried again by the scheduler if ntfy is
    down; or sent at once when there is no database."""
    if not notify_configured():
        print(
            (
                "orca: alerts are off: set ORCA_NOTIFY_URL (and the topic's"
                " secret with ORCA_NOTIFY_SECRET)"
            ),
            file=stderr,
        )
        return 1
    if len(args) == 3 and args[2] == "test":
        try:
            send_now(
                "orca: test alert",
                "If you can read this, orca's alerts reach you.",
                "default",
                "white_check_mark",
            )
        except e:
            print("orca: the test alert wasn't sent:", e, file=stderr)
            return 1
        print("sent")
        return 0
    var kind = String()
    var subject = String("default")
    var title = String()
    var priority = String("default")
    var tags = String()
    var click = String()
    var resolved = False
    var words = List[String]()
    var i = 2
    while i < len(args):
        var a = args[i]
        if a == "--resolved":
            resolved = True
            i += 1
        elif (
            a == "--kind"
            or a == "--subject"
            or a == "--title"
            or a == "--priority"
            or a == "--tags"
            or a == "--click"
        ) and i + 1 < len(args):
            var v = args[i + 1]
            if a == "--kind":
                kind = v
            elif a == "--subject":
                subject = v
            elif a == "--title":
                title = v
            elif a == "--priority":
                priority = v
            elif a == "--tags":
                tags = v
            else:
                click = v
            i += 2
        else:
            words.append(a)
            i += 1
    if not kind:
        _usage("orca notify needs test, or --kind")
    if priority not in ["high", "default", "low", "urgent", "min"]:
        _usage("orca notify --priority is high, default or low")
    var body = String()
    for k in range(len(words)):
        body.write(" " if k else "", words[k])
    if not title:
        title = String("orca: ", kind)
    var url = getenv("ORCA_DATABASE_URL")
    if url:
        try:
            var db = State(url)
            db.migrate()
            if resolved:
                resolve_alert(db, kind, subject, title, body, click)
                _ = flush_notify(db)
                print("resolved")
                return 0
            var queued = alert(
                db, kind, subject, priority, tags, title, body, click
            )
            if not queued:
                print("held back: the same alert was sent lately")
                return 0
            var waiting = db.conn.query(
                (
                    "SELECT 1 FROM orca.notifications WHERE kind = $1 AND"
                    " subject = $2 AND pending"
                ),
                Params().text(kind).text(subject),
            )
            if len(waiting) > 0:
                print(
                    (
                        "not sent yet: ntfy didn't answer; the scheduler tries"
                        " again"
                    ),
                    file=stderr,
                )
                return 1
            print("sent")
            return 0
        except e:
            print(
                "orca: no database for the alert's record (",
                e,
                ")",
                file=stderr,
            )
    try:
        send_now(title, body, priority, tags, click)
    except e:
        print("orca: the alert wasn't sent:", e, file=stderr)
        return 1
    print("sent")
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
        if len(args) > 2 and args[2] == "replace-control":
            return replace_control(args)
        return aws_command(args)
    if args[1] == "sample":
        return sample_main(args)
    if args[1] == "notify":
        return _notify(args)
    if args[1] == "worker":
        # Talks only to the UI's API, with no database (orca.worker).
        return worker_command(args, target)
    if args[1] == "config" and len(args) >= 3 and args[2] == "check":
        # A file only, for a project's CI: no database.
        return _config_check(args)
    var url = getenv("ORCA_DATABASE_URL")
    if not url:
        _usage("orca needs ORCA_DATABASE_URL")
    var cmd = args[1]
    # Commands that print what a page of the UI shows, or do what its
    # buttons do, over the database here as `--remote` does over HTTPS.
    if (
        cmd == "watchdog"
        and (len(args) == 3 or len(args) == 4)
        and args[2] == "ack"
    ):
        var db = State(url)
        db.migrate()
        var which = String("all")
        if len(args) == 4 and args[3] != "--all":
            which = args[3]
        var trip = 0
        if which != "all":
            try:
                trip = Int(which)
            except:
                _usage("orca watchdog ack [--all|<trip id>]")
        var who = getenv("USER", "local")
        var n = acknowledge(db, trip, who)
        audit(db, who, String("/watchdog/", which, "/ack"), 0, "", "local")
        print("acknowledged", n, "trip" if n == 1 else "trips")
        return 0
    if cmd == "watchdog" and len(args) == 5 and args[2] == "snooze":
        # Audited as the local user, since there is no session or token.
        var db = State(url)
        db.migrate()
        var secs = snooze_rule(db, args[3], args[4], "cli")
        var who = getenv("USER", "local")
        audit(
            db,
            who,
            String("/watchdog/", args[3], "/snooze"),
            0,
            String("duration=", args[4]),
            "local",
        )
        if secs:
            print("snoozed", args[3], "for", args[4])
        else:
            print("unsnoozed", args[3])
        return 0
    if (
        cmd == "watchdog"
        or cmd == "alerts"
        or cmd == "status"
        or cmd == "start"
        or cmd == "resume"
        or cmd == "rerun"
        or (cmd == "cancel" and len(args) == 3)
        or (cmd == "backfill" and "--preview" in args)
    ):
        var api = Api(url)
        return api_command(api, args)
    if cmd == "register":
        return _register(url, args)
    if cmd == "config" and len(args) >= 3:
        return _config(url, args)
    if cmd == "backfill" and _backfill_flags(args):
        # --rerun queues keys that are done as well; a key in a queued or
        # running run is still left to it. --allow-append lets a rerun
        # append again to what its steps don't replace.
        var rerun = "--rerun" in args
        var s = Scheduler(url)
        var got = s.backfill(
            args[len(args) - 2],
            args[len(args) - 1],
            rerun,
            "--allow-append" in args,
        )
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
    if cmd == "runner":
        # Starts the runs the scheduler assigns it (docs/runner.md).
        return runner_command(args, url)
    if cmd == "scheduler" and len(args) == 2:
        var s = Scheduler(url)
        s.say_pressure()
        s.contain()
        while True:
            # A tick that fails (the database is down) is tried again; the
            # state it would have changed is all in Postgres.
            try:
                s.tick(now())
            except e:
                print("orca: tick failed:", e)
                try:
                    s = Scheduler(url)
                    s.contain()
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
    if cmd == "worker-token" and len(args) >= 3:
        return _worker_token(url, args)
    if cmd == "ui" and len(args) == 2:
        var db = State(url)
        db.migrate()
        serve_ui(
            url,
            getenv("ORCA_UI_ADDR", DEFAULT_ADDR),
            getenv("ORCA_WORKER_ADDR"),
        )
        return 0
    _usage()
    return 2


def _worker_token(url: String, args: List[String]) raises -> Int:
    """`orca worker-token create|list|revoke` (`orca.workers`)."""
    var db = State(url)
    db.migrate()
    var who = getenv("USER", "local")
    if args[2] == "create":
        var pool = String()
        var label = String()
        var days = 0
        var i = 3
        while i + 1 < len(args):
            if args[i] == "--pool":
                pool = args[i + 1]
            elif args[i] == "--label":
                label = args[i + 1]
            elif args[i] == "--days":
                try:
                    days = Int(args[i + 1])
                except:
                    _usage(String("not a count of days: ", args[i + 1]))
            else:
                _usage(String("unknown argument: ", args[i]))
            i += 2
        if i != len(args) or not pool:
            _usage("worker-token create needs --pool <pool>")
        var got = create_worker_token(
            db, pool, label, worker_token_days(days), who
        )
        var shown_label = label.copy() if label else pool.copy()
        audit(
            db,
            who,
            "worker-token create",
            0,
            String("pool=", pool, " label=", shown_label),
            "local",
        )
        # The token alone on stdout, for a script; the rest on stderr.
        print(got.token)
        print(
            "a worker token for pool", pool, "until", got.expires, file=stderr
        )
        return 0
    if args[2] == "list" and len(args) == 3:
        var rows = worker_rows(db)
        if len(rows) == 0:
            print("no worker tokens")
        for r in rows:
            print(
                r.id,
                r.pool,
                r.label,
                r.state,
                r.name if r.name else String("-"),
                "seen",
                r.last_seen if r.last_seen else String("never"),
                "expires",
                r.expires,
            )
        return 0
    if args[2] == "revoke" and len(args) == 4:
        var id: Int
        try:
            id = Int(args[3])
        except:
            _usage(String("not a worker token's id: ", args[3]))
            return 2
        if not revoke_worker_token(db, id):
            raise Error("orca: no live worker token ", id)
        audit(db, who, "worker-token revoke", 0, String("id=", id), "local")
        print("revoked worker token", id)
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


comptime _FLAG_KEYS = [
    ("--head-type", "head_type"),
    ("--worker-type", "worker_type"),
    ("--workers", "workers"),
    ("--worker-procs", "worker_procs"),
    ("--placement", "placement"),
    ("--max-runs", "max_runs"),
    ("--cpus", "cpus"),
    ("--memory-gb", "memory_gb"),
]
"""`orca register`'s flags for orca.toml's keys (`orca.config`)."""


def _config_binaries(file: ProjectConfig) raises -> List[String]:
    """The binary of each `[pipelines.<name>]` in `file`, in the directory
    ORCA_PIPELINES_DIR, else `pipelines/` next to the file. Raises, before
    anything is registered, if there is no such directory or a binary is
    missing."""
    var names = file.pipelines()
    if len(names) == 0:
        raise Error("orca: ", file.path, " names no [pipelines.<name>]")
    var dir = getenv("ORCA_PIPELINES_DIR")
    if not dir:
        var full = absolute(file.path)
        var slash = full.rfind("/")
        var beside = String(full[byte=:slash]) if slash > 0 else String("/")
        dir = String(beside, "/pipelines")
        if not exists(dir):
            raise Error(
                "orca: no pipeline binaries: set ORCA_PIPELINES_DIR, put them",
                " in ",
                dir,
                ", or name them: orca register <binary>...",
            )
    var out = List[String]()
    var missing = String()
    for n in names:
        var p = String(dir, "/", n)
        if not exists(p):
            missing.write(" " if missing else "", n)
        out.append(p)
    if missing:
        raise Error(
            "orca: ",
            file.path,
            " names pipelines with no binary in ",
            dir,
            ":",
            missing,
        )
    return out^


def _register(url: String, args: List[String]) raises -> Int:
    """`orca register`: each binary's pipeline, with how its runs run from
    the project's orca.toml and the flags (`orca.config`)."""
    var flags = RegisterFlags()
    var config_path = String()
    var no_config = False
    var b64 = String()
    var commit = String()
    var dirty = False
    var name = String("orca.toml")
    var i = 2
    while i < len(args) and args[i].startswith("--"):
        var a = args[i]
        if a == "--spot" or a == "--no-config" or a == "--config-dirty":
            if a == "--spot":
                flags.spot = True
            elif a == "--no-config":
                no_config = True
            else:
                dirty = True
            i += 1
            continue
        if i + 1 >= len(args):
            _usage(String(a, " needs a value"))
        var v = args[i + 1]
        var key = String()
        for fk in materialize[_FLAG_KEYS]():
            if a == fk[0]:
                key = String(fk[1])
        if key:
            try:
                flags.values[key] = canonical_flag(key, v)
            except e:
                if key == "workers":
                    _usage(String("not a count or a range MIN..MAX: ", v))
                if key == "cpus" or key == "memory_gb":
                    _usage(String(a, " ", e))
                _usage(String(e))
        elif a == "--image":
            flags.image = v
        elif a == "--secret":
            # NAME=ID: the run's NAME is Secrets Manager's secret ID.
            var why = secret_problem(v)
            if why:
                _usage(why)
            flags.secrets.append(v)
        elif a == "--config":
            config_path = v
        elif a == "--config-b64":
            # The file itself, from `orca aws register` on another machine.
            b64 = v
        elif a == "--config-commit":
            commit = v
        elif a == "--config-name":
            name = v
        else:
            _usage(String("unknown option: ", a))
        i += 2
    var explicit_paths = i < len(args)
    var file: ProjectConfig
    if b64:
        file = ProjectConfig.from_b64(b64, name, commit, dirty)
    elif no_config:
        file = ProjectConfig()
    elif config_path:
        file = ProjectConfig.load(config_path)
    elif getenv("ORCA_CONFIG"):
        file = ProjectConfig.load(getenv("ORCA_CONFIG"))
    elif exists("orca.toml"):
        file = ProjectConfig.load("orca.toml")
    else:
        file = ProjectConfig()
    if file.is_file():
        print(
            "orca: registering with",
            file.path,
            "(sha256 " + String(file.hash[byte=:12]) + ")",
            file=stderr,
        )
    var paths = List[String]()
    for k in range(i, len(args)):
        paths.append(args[k])
    if not explicit_paths:
        if not file.is_file():
            _usage("register needs a pipeline binary, or an orca.toml")
        paths = _config_binaries(file)
    var s = Scheduler(url)
    for k in range(len(paths)):
        var got = s.register(paths[k], now(), flags, file)
        if file.is_file() and not file.has(got):
            print(
                String(
                    "orca: warning: ",
                    got,
                    " isn't in ",
                    file.path,
                    "; flags left out keep what it was registered with",
                ),
                file=stderr,
            )
        print("registered", got)
    return 0


def _without_orca(e: String) -> String:
    return String(e[byte=6:]) if e.startswith("orca: ") else e


def _config_check(args: List[String]) raises -> Int:
    """`orca config check [<file>] [--secrets]`: `file` (`$ORCA_CONFIG`,
    else ./orca.toml) as registering checks it, and each instance type
    sized; with `--secrets`, each secret read. 1 if anything is wrong."""
    var path = String()
    var read_secrets = False
    for k in range(3, len(args)):
        if args[k] == "--secrets":
            read_secrets = True
        elif not path and not args[k].startswith("--"):
            path = args[k]
        else:
            _usage(String("unknown argument: ", args[k]))
    if not path:
        path = getenv("ORCA_CONFIG", "orca.toml")
    var file: ProjectConfig
    try:
        file = ProjectConfig.load(path)
    except e:
        print(e, file=stderr)
        return 1
    var problems = 0
    for key in ["head_type", "worker_type"]:
        for e in file.entries(String(key)):
            try:
                _ = vcpus(e[1])
            except err:
                print(
                    e[0], ": ", _without_orca(String(err)), sep="", file=stderr
                )
                problems += 1
                continue
            if hourly_price(e[1]) < 0:
                print(
                    "orca: warning: ",
                    e[0],
                    ": ",
                    e[1],
                    " isn't in orca's price table (orca.fleet), so Ops >",
                    " Fleet and the watchdog's cost rule count it as $0 an",
                    " hour",
                    sep="",
                    file=stderr,
                )
    var secrets = file.entries("secrets")
    if read_secrets:
        for e in secrets:
            try:
                _ = fetch_secret(e[1])
            except err:
                print(
                    e[0], ": ", _without_orca(String(err)), sep="", file=stderr
                )
                problems += 1
    if problems > 0:
        print("orca:", path, "has", problems, "problem(s)", file=stderr)
        return 1
    var n = len(file.pipelines())
    var line = String(path, ": ok, ", n, " pipeline", "" if n == 1 else "s")
    if read_secrets:
        line.write(", ", len(secrets), " secret(s) readable")
    print(line)
    return 0


def _config(url: String, args: List[String]) raises -> Int:
    """`orca config show [<pipeline>] [--json]` and `orca config export
    [--out <path>|-] [--force]`."""
    var db = State(url)
    db.migrate()
    if args[2] == "show":
        var json = False
        var only = String()
        for k in range(3, len(args)):
            if args[k] == "--json":
                json = True
            elif not only and not args[k].startswith("--"):
                only = args[k]
            else:
                _usage(String("unknown argument: ", args[k]))
        var items = shown(db, only)
        if only and len(items) == 0:
            raise Error("orca: no pipeline ", only, " is registered")
        if json:
            print(show_json(items, only != ""))
        else:
            for k in range(len(items)):
                print(("\n" if k > 0 else "") + items[k].text(), end="")
        return 0
    if args[2] == "export":
        var out = String("orca.toml")
        var force = False
        var k = 3
        while k < len(args):
            if args[k] == "--out" and k + 1 < len(args):
                out = args[k + 1]
                k += 2
            elif args[k] == "--force":
                force = True
                k += 1
            else:
                _usage(String("unknown argument: ", args[k]))
        var text = export_toml(db)
        if out == "-":
            print(text, end="")
            return 0
        if exists(out) and not force:
            raise Error("orca: ", out, " exists; --force replaces it")
        with open(out, "w") as f:
            f.write(text)
        print("wrote", out)
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
