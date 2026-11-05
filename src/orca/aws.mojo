"""`orca aws`: a project's orca on AWS, with the `aws` and `docker` commands
its user already has (`aws login`, or `AWS_PROFILE` for a named profile).

    orca aws up --email you@example.com [--stack orca] [--hostname H]
                [--console-hostname H] [--budget 100] [--zone us-east-2a]
                [--public-data true|false]
    orca aws push [--tag T]                     # the project's image, to ECR
    orca aws register [--tag T] [register flags] <pipeline> [...]
    orca aws app <name> [--host H] [--port 8080] [--tag T]
    orca aws status
    orca aws down --yes

`up` creates or updates the stack from the template orca carries
(deploy/aws/orca.yaml, in `orca.aws_assets`); the first time it needs
`--email`, for the Budgets alert, and makes the database's password. `push`
builds the project's image from a staged copy of `pixi.toml`, `pixi.lock`,
`bin/`, `pipelines/` and `src/`, with the project's `Dockerfile.orca` or
else the template (deploy/project.Dockerfile), for arm64, tagged with the
commit (or `--tag`) and `latest`, and pushes it to the stack's repository.
`register` and `app` run on the control instance through SSM: `register`
moves the scheduler and the UI to the image (`orca-refresh`) and registers
each pipeline, `/opt/orca/pipelines/<pipeline>`, with the flags given, as
`orca register` takes them; `app` runs the image's `/opt/orca/bin/<name>`
beside the UI, at `https://<host>` (for `console`, the stack's
`ConsoleHostname`). `down` deletes the stack, which fails while its buckets
hold objects.

Files go to `build/orca-aws/`. `--stack` (default `orca`) and `--region`
(default `AWS_REGION`, else the `aws` command's own) go anywhere.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.os.path import exists
from std.time import sleep

from json import loads

from orca.aws_assets import PROJECT_DOCKERFILE, STACK_TEMPLATE
from orca.json import quote
from orca.proc import capture

comptime _DIR = "build/orca-aws"
comptime AWS_USAGE = """usage:
  orca aws up --email ADDR [--hostname H] [--console-hostname H] [--budget N]
              [--zone AZ] [--public-data true|false]
  orca aws push [--tag T]
  orca aws register [--tag T] [register flags] <pipeline> [...]
  orca aws app <name> [--host H] [--port 8080] [--tag T]
  orca aws status
  orca aws down --yes
  (each takes --stack NAME, default orca, and --region R)"""

comptime _VALUED = [
    "--stack",
    "--region",
    "--tag",
    "--email",
    "--hostname",
    "--console-hostname",
    "--budget",
    "--zone",
    "--public-data",
    "--host",
    "--port",
]
"""`orca aws`'s own options with a value; any other is `register`'s."""
comptime _REGISTER_VALUED = [
    "--head-type",
    "--worker-type",
    "--workers",
    "--worker-procs",
    "--secret",
]


def _q(s: String) -> String:
    """`s` as one shell word."""
    return String("'", s.replace("'", "'\\''"), "'")


def _system(cmd: String) raises -> Int:
    """Runs `cmd` through the shell, its output to ours; its exit code."""
    var c = List[UInt8](cmd.as_bytes())
    c.append(0)
    var s = Int(external_call["system", c_int](c.unsafe_ptr()))
    if s < 0:
        raise Error("orca aws: can't run ", cmd)
    return (s >> 8) & 0xFF if s & 0x7F == 0 else 128 + (s & 0x7F)


def _run(cmd: String) raises:
    print("+", cmd)
    var code = _system(cmd)
    if code != 0:
        raise Error("orca aws: exit ", code, ": ", cmd)


def _out(cmd: String) raises -> String:
    """`cmd`'s standard output, stripped; raises if it fails."""
    var got = capture(["sh", "-c", cmd])
    if got[0] != 0:
        raise Error("orca aws: exit ", got[0], ": ", cmd)
    return String(got[1].strip())


struct _Opts(Movable):
    var stack: String
    var region: String
    var tag: String
    var email: String
    var hostname: String
    var console_hostname: String
    var budget: String
    var zone: String
    var public_data: String
    """`true` lets anyone read the lake's bucket unsigned; empty keeps the
    stack's setting (private for a new one)."""
    var host: String
    var port: String
    var yes: Bool
    var rest: List[String]
    """The arguments that aren't `orca aws`'s own options, in order."""

    def __init__(out self, args: List[String], start: Int) raises:
        self.stack = String("orca")
        self.region = getenv("AWS_REGION")
        self.tag = String()
        self.email = String()
        self.hostname = String()
        self.console_hostname = String()
        self.budget = String()
        self.zone = String()
        self.public_data = String()
        self.host = String()
        self.port = String("8080")
        self.yes = False
        self.rest = List[String]()
        var i = start
        while i < len(args):
            var a = args[i]
            if a == "--yes":
                self.yes = True
                i += 1
                continue
            var ours = False
            for v in materialize[_VALUED]():
                if a == v:
                    ours = True
            if not ours:
                self.rest.append(a)
                i += 1
                continue
            if i + 1 >= len(args):
                raise Error("orca aws: ", a, " needs a value")
            var v = args[i + 1]
            if a == "--stack":
                self.stack = v
            elif a == "--region":
                self.region = v
            elif a == "--tag":
                self.tag = v
            elif a == "--email":
                self.email = v
            elif a == "--hostname":
                self.hostname = v
            elif a == "--console-hostname":
                self.console_hostname = v
            elif a == "--budget":
                self.budget = v
            elif a == "--zone":
                self.zone = v
            elif a == "--public-data":
                if v != "true" and v != "false":
                    raise Error("orca aws: --public-data is true or false")
                self.public_data = v
            elif a == "--host":
                self.host = v
            else:
                self.port = v
            i += 2

    def aws(self, args: String) -> String:
        """An `aws` command line, in the region given."""
        var out = String("aws ")
        if self.region:
            out.write("--region ", _q(self.region), " ")
        out.write(args)
        return out^


def _outputs(o: _Opts) raises -> Dict[String, String]:
    """The stack's outputs, by key."""
    var text = _out(
        o.aws(
            String(
                "cloudformation describe-stacks --stack-name ",
                _q(o.stack),
                " --query 'Stacks[0].Outputs' --output json",
            )
        )
    )
    var out = Dict[String, String]()
    for item in loads(text).array_items():
        out[item["OutputKey"].string_value()] = item[
            "OutputValue"
        ].string_value()
    return out^


def _output(outs: Dict[String, String], key: String) raises -> String:
    var v = outs.get(key, "")
    if not v:
        raise Error("orca aws: the stack has no output ", key)
    return v^


def _stack_exists(o: _Opts) raises -> Bool:
    return (
        _system(
            String(
                o.aws(
                    String(
                        "cloudformation describe-stacks --stack-name ",
                        _q(o.stack),
                    )
                ),
                " > /dev/null 2>&1",
            )
        )
        == 0
    )


def _write(path: String, text: StaticString) raises:
    _run(String("mkdir -p ", _q(_DIR)))
    with open(path, "w") as f:
        f.write(text)


def _git(args: String) -> String:
    try:
        return _out(String("git ", args, " 2> /dev/null"))
    except:
        return ""


def _tag(o: _Opts) -> String:
    """The image tag: `--tag`, else the commit's first 12, else latest."""
    if o.tag:
        return o.tag
    var commit = _git("rev-parse HEAD")
    if commit.byte_length() >= 12:
        return String(commit[byte=:12])
    return "latest"


def _up(o: _Opts) raises -> Int:
    var template = String(_DIR, "/orca.yaml")
    _write(template, STACK_TEMPLATE)
    var params = List[String]()
    if not _stack_exists(o):
        if not o.email:
            print(
                "orca aws up: a new stack needs --email, for its budget alert"
            )
            return 2
        # The database's password, made once; updates keep it.
        params.append(
            String(
                "DbPassword=",
                _out("od -An -N18 -tx1 /dev/urandom | tr -d ' \\n'"),
            )
        )
    if o.email:
        params.append(String("BudgetEmail=", o.email))
    if o.hostname:
        params.append(String("Hostname=", o.hostname))
    if o.console_hostname:
        params.append(String("ConsoleHostname=", o.console_hostname))
    if o.budget:
        params.append(String("BudgetDollars=", o.budget))
    if o.zone:
        params.append(String("AvailabilityZone=", o.zone))
    if o.public_data:
        params.append(String("PublicData=", o.public_data))
    var cmd = o.aws(
        String(
            "cloudformation deploy --stack-name ",
            _q(o.stack),
            " --template-file ",
            _q(template),
            " --capabilities CAPABILITY_IAM --no-fail-on-empty-changeset",
        )
    )
    if len(params) > 0:
        cmd.write(" --parameter-overrides")
        for p in params:
            cmd.write(" ", _q(p))
    # The command line holds the password: run it without echoing it.
    print("+ aws cloudformation deploy --stack-name", o.stack, "...")
    var code = _system(cmd)
    if code != 0:
        raise Error("orca aws: cloudformation deploy exited ", code)
    return _status(o)


def _status(o: _Opts) raises -> Int:
    var outs = _outputs(o)
    for e in outs.items():
        print(e.key, e.value, sep="\t")
    return 0


def _push(o: _Opts) raises -> Int:
    if not exists("pixi.toml") or not exists("bin/scheduler.mojo"):
        print(
            "orca aws push: run it in the project, which needs pixi.toml and"
            " bin/scheduler.mojo"
        )
        return 2
    var manifest: String
    with open("pixi.toml", "r") as f:
        manifest = f.read()
    if '"linux-aarch64"' not in manifest:
        print(
            "orca aws push: the image is linux-aarch64, which pixi.toml's"
            " platforms don't list; add it with\n    pixi workspace platform"
            " add linux-aarch64"
        )
        return 2
    var repo = _output(_outputs(o), "Repository")
    var registry = String(repo[byte = : repo.find("/")])
    var tag = _tag(o)
    var ctx = String(_DIR, "/context")
    _run(
        String(
            "rm -rf ",
            _q(ctx),
            " && mkdir -p ",
            _q(ctx),
            " && cp pixi.toml pixi.lock ",
            _q(ctx),
            " && for d in bin pipelines src; do [ ! -d $d ] || cp -R $d ",
            _q(ctx),
            "/; done",
        )
    )
    var dockerfile = String("Dockerfile.orca")
    if not exists(dockerfile):
        dockerfile = String(_DIR, "/Dockerfile")
        _write(dockerfile, PROJECT_DOCKERFILE)
    var image = String(repo, ":", tag)
    _run(
        String(
            "docker build --platform linux/arm64 -f ",
            _q(dockerfile),
            " --build-arg ORCA_SOURCE=",
            _q(_git("remote get-url origin")),
            " --build-arg ORCA_COMMIT=",
            _q(_git("rev-parse HEAD")),
            " -t ",
            _q(image),
            " -t ",
            _q(String(repo, ":latest")),
            " ",
            _q(ctx),
        )
    )
    _run(
        String(
            o.aws("ecr get-login-password"),
            " | docker login --username AWS --password-stdin ",
            _q(registry),
        )
    )
    _run(String("docker push ", _q(image)))
    _run(String("docker push ", _q(String(repo, ":latest"))))
    print("pushed", image)
    return 0


def _ssm(o: _Opts, instance: String, commands: List[String]) raises -> Int:
    """Runs `commands` on `instance` through SSM, prints what they wrote,
    and returns 0 if they succeeded."""
    var params = String('{"commands": [')
    for i in range(len(commands)):
        params.write("" if i == 0 else ", ", quote(commands[i]))
    params.write("]}")
    for c in commands:
        print("+ on the control instance:", c)
    var id = _out(
        o.aws(
            String(
                "ssm send-command --instance-ids ",
                _q(instance),
                " --document-name AWS-RunShellScript --comment 'orca aws'",
                " --parameters ",
                _q(params),
                " --query Command.CommandId --output text",
            )
        )
    )
    var status = String()
    var ask = o.aws(
        String(
            "ssm get-command-invocation --command-id ",
            _q(id),
            " --instance-id ",
            _q(instance),
        )
    )
    for _ in range(900):
        # Until the instance has the command, there's no invocation yet.
        var got = capture(
            ["sh", "-c", String(ask, " --query Status --output text")]
        )
        status = String(got[1].strip())
        if got[0] == 0 and (
            status == "Success"
            or status == "Failed"
            or status == "Cancelled"
            or status == "TimedOut"
        ):
            break
        sleep(2.0)
    print(_out(String(ask, " --query StandardOutputContent --output text")))
    var err = _out(String(ask, " --query StandardErrorContent --output text"))
    if err and err != "None":
        print(err)
    if status != "Success":
        print("orca aws: the command on the control instance:", status)
        return 3
    return 0


def _register(o: _Opts) raises -> Int:
    var flags = String()
    var pipelines = List[String]()
    var i = 0
    while i < len(o.rest):
        var a = o.rest[i]
        if a == "--image":
            print("orca aws register: the image is the project's, from push")
            return 2
        var valued = False
        for v in materialize[_REGISTER_VALUED]():
            if a == v:
                valued = True
        if valued and i + 1 < len(o.rest):
            flags.write(" ", _q(a), " ", _q(o.rest[i + 1]))
            i += 2
            continue
        if a.startswith("--"):
            flags.write(" ", _q(a))
        else:
            pipelines.append(a)
        i += 1
    if len(pipelines) == 0:
        print("orca aws register needs a pipeline, as named in pipelines/")
        return 2
    var outs = _outputs(o)
    var repo = _output(outs, "Repository")
    var tag = _tag(o)
    var reg = String(
        "docker exec scheduler orca register --image ",
        _q(String(repo, ":", tag)),
        flags,
    )
    for p in pipelines:
        reg.write(" ", _q(String("/opt/orca/pipelines/", p)))
    var cmds: List[String] = [String("orca-refresh ", _q(tag)), reg^]
    return _ssm(o, _output(outs, "ControlInstance"), cmds)


def _app(o: _Opts) raises -> Int:
    if len(o.rest) != 1:
        print("orca aws app needs the app's name, a bin/<name>.mojo")
        return 2
    var name = o.rest[0]
    var outs = _outputs(o)
    var host = o.host
    if not host and name == "console":
        host = outs.get("ConsoleHostname", "")
    if not host:
        print("orca aws app needs --host")
        return 2
    var cmds = List[String]()
    if o.tag:
        cmds.append(String("orca-refresh ", _q(o.tag)))
    cmds.append(String("orca-app ", _q(name), " ", _q(host), " ", _q(o.port)))
    var code = _ssm(o, _output(outs, "ControlInstance"), cmds)
    if code == 0:
        print(name, " at https://", host, sep="")
    return code


def _down(o: _Opts) raises -> Int:
    if not o.yes:
        print(
            "orca aws down deletes the stack",
            o.stack,
            (
                "and everything in it: the control instance, its database,"
                " the repository and the buckets. Run it again with --yes."
            ),
        )
        return 2
    _run(
        o.aws(String("cloudformation delete-stack --stack-name ", _q(o.stack)))
    )
    var code = _system(
        o.aws(
            String(
                "cloudformation wait stack-delete-complete --stack-name ",
                _q(o.stack),
            )
        )
    )
    if code != 0:
        print(
            "orca aws: the stack didn't delete. A bucket or the repository"
            " that still holds something stays: empty it (aws s3 rm"
            " --recursive s3://<bucket>) and run down again."
        )
        return 3
    print("deleted the stack", o.stack)
    return 0


def aws_command(args: List[String]) raises -> Int:
    """`orca aws <cmd> ...`: `args` is the whole command line."""
    if len(args) < 3:
        print(AWS_USAGE)
        return 2
    var o = _Opts(args, 3)
    var cmd = args[2]
    if cmd == "up":
        return _up(o)
    if cmd == "push":
        return _push(o)
    if cmd == "register":
        return _register(o)
    if cmd == "app":
        return _app(o)
    if cmd == "status":
        return _status(o)
    if cmd == "down":
        return _down(o)
    print(AWS_USAGE)
    return 2
