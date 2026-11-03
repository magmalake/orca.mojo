"""Runs on EC2: a head instance and worker instances per run, started and
ended by the scheduler (DESIGN.md, phase 5, PR 5).

With `ORCA_RUNNER=ec2` the scheduler starts a run's start `s` as:

- **a head instance** (`--head-type` at registration, else
  `ORCA_EC2_HEAD_TYPE`, on-demand): the dista head, for a pipeline with
  tasks, and the driver, `<binary> resume <run>`, beside it on localhost;
- **`workers` worker instances** (`--worker-type`, else
  `ORCA_EC2_WORKER_TYPE`; Spot with `--spot`), for a pipeline with tasks,
  started straight after with `DISTA_HEAD` at the head's private address.

Each is tagged `orca.run`, `orca.start`, `orca.pipeline` and `orca.role`,
told to terminate when it shuts down, and started from user data that runs
the pipeline's image (`ORCA_EC2_MODE=docker`, the default: `docker run
--network host`, after `ORCA_EC2_PREAMBLE`, such as an ECR login) or the
binary itself (`direct`, for the tests' stand-in EC2), then shuts down
(`ORCA_EC2_SHUTDOWN`). The head keeps the driver's output in the run's
`driver.log` (`<binary> keep-log`) before it does.

Every tick, `sweep` terminates the instances of runs that aren't running at
that start, and those older than `ORCA_RUN_DEADLINE` (a day). Instances get
`ORCA_EC2_DATABASE_URL` as `ORCA_DATABASE_URL` (the control instance's
private address), the warehouse, the logs, the region and the scheduler's
variables named in `ORCA_RUN_ENV`; `ORCA_EC2_AMI`, `ORCA_EC2_SUBNET`,
`ORCA_EC2_SECURITY_GROUP` and `ORCA_EC2_PROFILE` say where and as what they
run. `cpu_millis` is a run's instances' vCPUs, which the scheduler checks
against `ORCA_VCPU_BUDGET` first.
"""

from std.base64 import b64encode
from std.os import getenv

from postgres import Params

from orca.awsec2 import Ec2, Instance
from orca.state import State

comptime HEAD_PORT = 6390

comptime _COPIED = [
    "ORCA_STALE_SECONDS",
    "ORCA_WAREHOUSE",
    "ORCA_LOGS",
    "AWS_REGION",
    "ORCA_HEAD_WAIT_SECONDS",
]
"""Variables every instance gets from the scheduler's environment."""


def vcpus(instance_type: String) raises -> Int:
    """An instance type's vCPUs: from `ORCA_EC2_VCPUS` (`type=n,...`), else
    by its size, as Graviton families are sized."""
    for pair in getenv("ORCA_EC2_VCPUS").split(","):
        var eq = pair.find("=")
        if eq > 0 and String(pair[byte=:eq].strip()) == instance_type:
            return Int(pair[byte = eq + 1 :].strip())
    var dot = instance_type.find(".")
    if dot < 0:
        raise Error("orca: not an instance type: ", instance_type)
    var family = String(instance_type[byte=:dot])
    var size = String(instance_type[byte = dot + 1 :])
    if family.startswith("t"):
        if size == "xlarge":
            return 4
        if size == "2xlarge":
            return 8
        return 2
    if size == "medium":
        return 1
    if size == "large":
        return 2
    if size == "xlarge":
        return 4
    if size.endswith("xlarge"):
        return Int(size[byte = : size.byte_length() - 6]) * 4
    raise Error(
        "orca: the vCPUs of ",
        instance_type,
        " aren't known; set ORCA_EC2_VCPUS",
    )


@fieldwise_init
struct Ec2Spec(Copyable, Movable):
    """One start of a run, as the instances it needs."""

    var run: Int
    var start: Int
    var pipeline: String
    var image: String
    var binary: String
    var tasks: Bool
    """Whether the pipeline calls dista tasks: a head and workers."""
    var head_type: String
    var worker_type: String
    var workers: Int
    var spot: Bool
    var allow_new_code: Bool


def _quote(s: String) -> String:
    """`s` as one shell word."""
    return String("'", s.replace("'", "'\\''"), "'")


struct Ec2Runner(Movable):
    var ec2: Ec2
    var mode: String
    var head_type: String
    var worker_type: String
    var deadline: Int
    var env: List[String]

    def __init__(out self) raises:
        self.ec2 = Ec2()
        self.mode = getenv("ORCA_EC2_MODE", "docker")
        if self.mode != "docker" and self.mode != "direct":
            raise Error(
                "orca: ORCA_EC2_MODE is docker or direct, not ", self.mode
            )
        self.head_type = getenv("ORCA_EC2_HEAD_TYPE", "c7g.large")
        self.worker_type = getenv("ORCA_EC2_WORKER_TYPE", "c7g.xlarge")
        self.deadline = Int(getenv("ORCA_RUN_DEADLINE", "86400"))
        self.env = List[String]()
        for n in getenv("ORCA_RUN_ENV").split(","):
            if n.strip():
                self.env.append(String(n.strip()))

    def head_of(self, s: String) -> String:
        return s if s else self.head_type

    def worker_of(self, s: String) -> String:
        return s if s else self.worker_type

    def cpu_of(
        self, tasks: Bool, head_type: String, worker_type: String, workers: Int
    ) raises -> Int:
        """A run's instances' vCPUs, in thousandths."""
        var n = vcpus(self.head_of(head_type))
        if tasks:
            n += workers * vcpus(self.worker_of(worker_type))
        return n * 1000

    def _exports(self, r: Ec2Spec, role: StaticString, head: String) -> String:
        """The script's environment: what every instance gets, and its
        role's own."""
        var out = String()
        var url = getenv("ORCA_EC2_DATABASE_URL", getenv("ORCA_DATABASE_URL"))
        out.write("export ORCA_DATABASE_URL=", _quote(url), "\n")
        # The run records its version with the image it ran from.
        out.write("export ORCA_IMAGE=", _quote(r.image), "\n")
        var names = List[String]()
        for n in materialize[_COPIED]():
            names.append(String(n))
        for n in self.env:
            names.append(n)
        for n in names:
            var v = getenv(n)
            if v:
                out.write("export ", n, "=", _quote(v), "\n")
        if role == "worker":
            out.write(
                "export DISTA_HEAD=",
                head,
                ":",
                HEAD_PORT,
                "\nexport DISTA_ADVERTISE=$(hostname -I 2>/dev/null | awk",
                " '{print $1}')\n",
            )
        return out^

    def _run(self, r: Ec2Spec, env: List[String], args: String) -> String:
        """A line running the image's binary with `args`, given the
        variables named in `env` as well as the exported ones."""
        if self.mode == "direct":
            var pre = String()
            for kv in env:
                pre.write(kv, " ")
            return String(pre, _quote(r.binary), " ", args)
        var flags = String("--rm --network host")
        var names = List[String]()
        names.append("ORCA_DATABASE_URL")
        names.append("ORCA_IMAGE")
        for n in materialize[_COPIED]():
            names.append(String(n))
        for n in self.env:
            names.append(n)
        for n in names:
            flags.write(" -e ", n)
        for kv in env:
            flags.write(" -e ", kv)
        return String(
            "docker run ",
            flags,
            " --entrypoint ",
            _quote(r.binary),
            " ",
            _quote(r.image),
            " ",
            args,
        )

    def _instance_id(self) -> String:
        """A line setting `iid` to this instance's ID: from the instance
        metadata (IMDSv2) on EC2, from `HOSTNAME` under the tests' stand-in,
        which sets it to the ID."""
        if self.mode == "direct":
            return "iid=$HOSTNAME"
        return String(
            "t=$(curl -s -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds:",
            " 60' http://169.254.169.254/latest/api/token)\niid=$(curl -s -H",
            ' "X-aws-ec2-metadata-token: $t"',
            " http://169.254.169.254/latest/meta-data/instance-id)",
        )

    def _mounted(self, line: String) -> String:
        """`line`, a container's run, with `/tmp` mounted, for the commands
        that read a log the script wrote there."""
        return line.replace("docker run --rm", "docker run --rm -v /tmp:/tmp")

    def _user_data(
        self, r: Ec2Spec, role: StaticString, head: String
    ) -> String:
        var s = String(
            "#!/bin/bash\n# orca run ",
            r.run,
            " start ",
            r.start,
            ": ",
            role,
            "\n",
        )
        s.write(self._exports(r, role, head))
        s.write(getenv("ORCA_EC2_PREAMBLE"), "\n")
        # The head's or worker's output, uploaded while it runs and once
        # when it ends: an instance can go without notice.
        var keeps = role == "worker" or r.tasks
        var log = String(
            "/tmp/orca-", r.run, "-", r.start, "-", role, "-$iid.log"
        )
        if keeps:
            s.write(self._instance_id(), "\n")
            s.write(
                'up() { [ -z "${ORCA_LOGS:-}" ] || ',
                self._mounted(
                    self._run(
                        r,
                        List[String](),
                        String(
                            "put-log ",
                            log,
                            ' "$ORCA_LOGS/',
                            r.pipeline,
                            "/",
                            r.run,
                            "/cluster/",
                            role,
                            '-$iid.log"',
                        ),
                    )
                ),
                " >/dev/null 2>&1; }\n",
                "( while sleep ",
                getenv("ORCA_CLUSTER_LOG_SECONDS", "30"),
                "; do up; done ) &\nuploader=$!\n",
            )
        if role == "worker":
            # The role goes to the worker alone: a binary given one serves
            # it, whatever its command line says.
            var env: List[String] = [String("DISTA_ROLE=worker")]
            if self.mode == "docker":
                env.append("DISTA_HEAD")
                env.append("DISTA_ADVERTISE")
            s.write(
                self._run(r, env, ""),
                " > ",
                log,
                " 2>&1\n",
            )
        else:
            if r.tasks:
                s.write(
                    self._run(
                        r,
                        [
                            String("DISTA_ROLE=head"),
                            String("DISTA_PORT=", HEAD_PORT),
                        ],
                        "",
                    ),
                    " > ",
                    log,
                    " 2>&1 &\nhead=$!\n",
                )
            var args = String("resume ", r.run)
            if r.allow_new_code:
                args.write(" --allow-new-code")
            var env = List[String]()
            if r.tasks:
                env.append(String("DISTA_HEAD=127.0.0.1:", HEAD_PORT))
            var out = String("/tmp/orca-", r.run, "-", r.start, "-driver.log")
            s.write(self._run(r, env, args), " > ", out, " 2>&1\n")
            if r.tasks:
                s.write("kill $head 2>/dev/null\nwait $head 2>/dev/null\n")
            s.write(
                'if [ -n "${ORCA_LOGS:-}" ]; then ',
                self._mounted(
                    self._run(
                        r,
                        List[String](),
                        String(
                            "keep-log ",
                            out,
                            ' "$ORCA_LOGS/',
                            r.pipeline,
                            "/",
                            r.run,
                            '/driver.log" ',
                            r.start,
                        ),
                    )
                ),
                "; fi\n",
            )
        if keeps:
            s.write("kill $uploader 2>/dev/null\nup\n")
        s.write(
            getenv(
                "ORCA_EC2_SHUTDOWN",
                "shutdown -h now" if self.mode == "docker" else "true",
            ),
            "\n",
        )
        return s^

    def _params(
        self,
        r: Ec2Spec,
        role: StaticString,
        itype: String,
        count: Int,
        spot: Bool,
        head: String,
    ) raises -> List[Tuple[String, String]]:
        var p: List[Tuple[String, String]] = [
            ("ImageId", getenv("ORCA_EC2_AMI")),
            ("InstanceType", itype),
            ("MinCount", String(count)),
            ("MaxCount", String(count)),
            ("InstanceInitiatedShutdownBehavior", "terminate"),
            ("UserData", b64encode(self._user_data(r, role, head))),
            ("MetadataOptions.HttpTokens", "required"),
            ("TagSpecification.1.ResourceType", "instance"),
            ("TagSpecification.1.Tag.1.Key", "orca.run"),
            ("TagSpecification.1.Tag.1.Value", String(r.run)),
            ("TagSpecification.1.Tag.2.Key", "orca.start"),
            ("TagSpecification.1.Tag.2.Value", String(r.start)),
            ("TagSpecification.1.Tag.3.Key", "orca.pipeline"),
            ("TagSpecification.1.Tag.3.Value", r.pipeline),
            ("TagSpecification.1.Tag.4.Key", "orca.role"),
            ("TagSpecification.1.Tag.4.Value", String(role)),
            ("TagSpecification.1.Tag.5.Key", "Name"),
            (
                "TagSpecification.1.Tag.5.Value",
                String("orca-", r.run, "-", r.start, "-", role),
            ),
        ]
        var subnet = getenv("ORCA_EC2_SUBNET")
        if subnet:
            p.append(("SubnetId", subnet))
        var sg = getenv("ORCA_EC2_SECURITY_GROUP")
        if sg:
            p.append(("SecurityGroupId.1", sg))
        var profile = getenv("ORCA_EC2_PROFILE")
        if profile:
            p.append(("IamInstanceProfile.Name", profile))
        if spot:
            p.append(("InstanceMarketOptions.MarketType", "spot"))
            p.append(
                (
                    "InstanceMarketOptions.SpotOptions.SpotInstanceType",
                    "one-time",
                )
            )
            p.append(
                (
                    "InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior",
                    "terminate",
                )
            )
        return p^

    def start(self, mut db: State, r: Ec2Spec) raises:
        """Starts the run's head instance, then its workers, and records
        them in `orca.instances`."""
        var head_type = self.head_of(r.head_type)
        var head = self.ec2.run_instances(
            self._params(r, "head", head_type, 1, False, "")
        )
        _record(db, r, "head", head, "on-demand")
        if len(head) != 1 or not head[0].private_ip:
            raise Error(
                "orca: RunInstances gave no head instance with an address"
            )
        if not r.tasks or r.workers == 0:
            return
        var workers = self.ec2.run_instances(
            self._params(
                r,
                "worker",
                self.worker_of(r.worker_type),
                r.workers,
                r.spot,
                head[0].private_ip,
            )
        )
        _record(db, r, "worker", workers, "spot" if r.spot else "on-demand")

    def sweep(self, mut db: State) raises -> Int:
        """Terminates the instances of runs that aren't running at their
        start, and those older than the deadline; returns how many."""
        var live = Dict[String, Bool]()
        for row in db.conn.query(
            (
                "SELECT id || '-' || starts AS k FROM orca.runs WHERE status ="
                " 'running'"
            ),
            Params(),
        ):
            live[row.text("k")] = True
        var old = (
            db.conn.query(
                (
                    "SELECT to_char((now() - make_interval(secs => $1)) AT TIME"
                    " ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') AS t"
                ),
                Params().float64(Float64(self.deadline)),
            )
            .row(0)
            .text("t")
        )
        var filters = List[Tuple[String, List[String]]]()
        var tag: List[String] = [String("orca.run")]
        var states: List[String] = [String("pending"), String("running")]
        filters.append((String("tag-key"), tag^))
        filters.append((String("instance-state-name"), states^))
        var doomed = List[String]()
        var up = List[String]()
        for i in self.ec2.describe(filters):
            up.append(i.id)
            var key = String(
                i.tags.get("orca.run", ""), "-", i.tags.get("orca.start", "")
            )
            # Launch times are ISO 8601 in UTC, so they order as text.
            if key not in live or (i.launch_time and i.launch_time < old):
                doomed.append(i.id)
        self.ec2.terminate(doomed)
        for id in doomed:
            _ended(db, id, "terminated by orca: its run had ended")
        self._ended_since(db, up)
        return len(doomed)

    def _ended_since(self, mut db: State, up: List[String]) raises:
        """Records the end of every instance `orca.instances` has as live
        that isn't among those `up`, with EC2's reason for it."""
        var gone = List[String]()
        for row in db.conn.query(
            "SELECT id FROM orca.instances WHERE ended IS NULL", Params()
        ):
            var id = row.text("id")
            if id not in up:
                gone.append(id)
        if len(gone) == 0:
            return
        var filters = List[Tuple[String, List[String]]]()
        filters.append((String("instance-id"), gone.copy()))
        var reasons = Dict[String, String]()
        for i in self.ec2.describe(filters):
            reasons[i.id] = i.state_reason
        for id in gone:
            _ended(db, id, _why(reasons.get(id, "")))


def _why(code: String) -> String:
    """EC2's state reason, said plainly."""
    if code == "Client.InstanceInitiatedShutdown":
        return "shut itself down"
    if code == "Client.UserInitiatedShutdown":
        return "terminated"
    if code == "Server.SpotInstanceTermination":
        return "Spot interruption"
    if code == "Server.InsufficientInstanceCapacity":
        return "no capacity"
    return code if code else String("gone")


def _record(
    mut db: State,
    r: Ec2Spec,
    role: StaticString,
    made: List[Instance],
    market: String,
) raises:
    for i in made:
        _ = db.conn.execute(
            (
                "INSERT INTO orca.instances (id, run, start, role,"
                " instance_type, market) VALUES ($1, $2, $3, $4, $5, $6) ON"
                " CONFLICT (id) DO NOTHING"
            ),
            Params()
            .text(i.id)
            .int64(Int64(r.run))
            .int64(Int64(r.start))
            .text(String(role))
            .text(i.instance_type)
            .text(market),
        )


def _ended(mut db: State, id: String, why: String) raises:
    """Records that instance `id` ended, unless it's recorded already."""
    _ = db.conn.execute(
        (
            "UPDATE orca.instances SET ended = now(), why = $2 WHERE id = $1"
            " AND ended IS NULL"
        ),
        Params().text(id).text(why),
    )
