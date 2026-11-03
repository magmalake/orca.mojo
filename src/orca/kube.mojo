"""Runs on Kubernetes: a dista cluster per run, made and removed by the
scheduler (K8S.md).

With `ORCA_RUNNER=kubernetes`, the scheduler starts a run by creating, in
`ORCA_RUNS_NAMESPACE` (default `orca-runs`), from the image the pipeline
was registered with (`orca register --image`):

| Object | |
|---|---|
| A Job, `orca-<run>-<start>` | the driver: `<binary> resume <run>` |
| A Pod and a Service, `orca-<run>-<start>-head` | the dista head, for a pipeline with tasks |
| `workers` Pods, `orca-<run>-<start>-worker-<i>` | dista's workers, for a pipeline with tasks |

All are labelled `orca.run`, `orca.start` (the run's start count),
`orca.pipeline` and `orca.role`, and the head, its Service and the workers
are owned by the Job, so deleting the Job deletes them. The Job has
`activeDeadlineSeconds` (`ORCA_RUN_DEADLINE`, default a day) and
`ttlSecondsAfterFinished` (10 minutes), and `backoffLimit: 0`, as orca
retries runs itself. The driver reaches Postgres through the Secret
`ORCA_RUN_SECRET` (default `orca-db`, key `url`) in that namespace, and the
head through `DISTA_HEAD`; the scheduler's variables named in
`ORCA_RUN_ENV` are copied to every pod.

Every tick, `sweep` deletes the objects of a run that isn't running at that
start any more, and those of a Job that has finished, first appending its
driver pod's log to the run's `driver.log` under `ORCA_LOGS`: what's left after a
run ends, a run restarted on a new cluster, and what a scheduler that was
killed left behind. Pods ask for `ORCA_DRIVER_CPU`, `ORCA_HEAD_CPU` and
`ORCA_WORKER_CPU` (default 500m, 250m and 1) and `ORCA_POD_MEMORY` (512Mi)
each; `cpu_millis` is a run's total, which the scheduler checks against
`ORCA_VCPU_BUDGET` before making a cluster.
"""

from std.os import getenv

from json import Value
from k8s import JOBS, PODS, SERVICES, Client, Config
from postgres import Params

from orca.json import quote
from orca.logs import location_exists, read_location, run_dir, write_location
from orca.state import State

comptime DISTA_PORT = 6390


def cpu_millis(s: String) raises -> Int:
    """A CPU quantity, `500m` or `2` or `1.5`, in thousandths."""
    if s.endswith("m"):
        return Int(s[byte = : s.byte_length() - 1])
    return Int(Float64(s) * 1000.0 + 0.5)


@fieldwise_init
struct RunSpec(Copyable, Movable):
    """What one start of a run needs made."""

    var run: Int
    var start: Int
    var pipeline: String
    var image: String
    var binary: String
    var workers: Int
    """0 for a pipeline without dista tasks: no head, no workers."""
    var allow_new_code: Bool


struct KubeRunner(Movable):
    var k: Client
    var namespace: String
    var secret: String
    var driver_cpu: Int
    var head_cpu: Int
    var worker_cpu: Int
    var memory: String
    var deadline: Int
    var env: List[String]
    """Names of the scheduler's variables copied to every pod."""

    def __init__(out self) raises:
        self.namespace = getenv("ORCA_RUNS_NAMESPACE", "orca-runs")
        var cfg = Config.load()
        cfg.namespace = self.namespace
        self.k = Client(cfg^)
        self.secret = getenv("ORCA_RUN_SECRET", "orca-db")
        self.driver_cpu = cpu_millis(getenv("ORCA_DRIVER_CPU", "500m"))
        self.head_cpu = cpu_millis(getenv("ORCA_HEAD_CPU", "250m"))
        self.worker_cpu = cpu_millis(getenv("ORCA_WORKER_CPU", "1"))
        self.memory = getenv("ORCA_POD_MEMORY", "512Mi")
        self.deadline = Int(getenv("ORCA_RUN_DEADLINE", "86400"))
        self.env = List[String]()
        for n in getenv("ORCA_RUN_ENV").split(","):
            if n.strip():
                self.env.append(String(n.strip()))

    def cpu_of(self, workers: Int) -> Int:
        """The CPU a run asks for, in thousandths: its driver, and its head
        and workers if it has any."""
        if workers == 0:
            return self.driver_cpu
        return self.driver_cpu + self.head_cpu + workers * self.worker_cpu

    def _name(self, r: RunSpec) -> String:
        return String("orca-", r.run, "-", r.start)

    def _labels(self, r: RunSpec, role: StaticString) -> String:
        return String(
            '{"orca.run": "',
            r.run,
            '", "orca.start": "',
            r.start,
            '", "orca.pipeline": ',
            quote(r.pipeline),
            ', "orca.role": "',
            role,
            '"}',
        )

    def _env(self, extra: String) -> String:
        """A container's `env`: the copied variables, then `extra`."""
        var out = String("[")
        var first = True
        for n in self.env:
            var v = getenv(n)
            if not v:
                continue
            out.write(
                "" if first else ", ",
                '{"name": ',
                quote(n),
                ', "value": ',
                quote(v),
                "}",
            )
            first = False
        if extra:
            out.write("" if first else ", ", extra)
        out.write("]")
        return out^

    def _resources(self, cpu: Int) -> String:
        return String(
            '{"requests": {"cpu": "',
            cpu,
            'm", "memory": ',
            quote(self.memory),
            "}}",
        )

    def _pod(
        self,
        r: RunSpec,
        name: String,
        role: StaticString,
        owner: String,
        command: String,
        env: String,
        cpu: Int,
    ) -> String:
        return String(
            '{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": ',
            quote(name),
            ', "labels": ',
            self._labels(r, role),
            ', "ownerReferences": [',
            owner,
            ']}, "spec": {"restartPolicy": "Never", "containers": [{"name":',
            ' "main", "image": ',
            quote(r.image),
            ', "imagePullPolicy": "IfNotPresent", "command": ',
            command,
            ', "env": ',
            self._env(env),
            ', "resources": ',
            self._resources(cpu),
            "}]}}",
        )

    def start(self, r: RunSpec) raises:
        """Makes the run's driver Job, and its head, Service and workers if
        it has tasks."""
        var name = self._name(r)
        var head = String(name, "-head")
        var argv = String("[", quote(r.binary), ', "resume", "', r.run, '"')
        if r.allow_new_code:
            argv.write(', "--allow-new-code"')
        argv.write("]")
        var driver_env = String(
            '{"name": "ORCA_DATABASE_URL", "valueFrom": {"secretKeyRef":',
            ' {"name": ',
            quote(self.secret),
            ', "key": "url"}}}, {"name": "ORCA_STALE_SECONDS", "value": ',
            quote(getenv("ORCA_STALE_SECONDS", "30")),
            '}, {"name": "ORCA_IMAGE", "value": ',
            quote(r.image),
            "}",
        )
        if r.workers > 0:
            driver_env.write(
                ', {"name": "DISTA_HEAD", "value": "',
                head,
                ":",
                DISTA_PORT,
                '"}',
            )
        var job = String(
            '{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": ',
            quote(name),
            ', "labels": ',
            self._labels(r, "driver"),
            '}, "spec": {"backoffLimit": 0, "activeDeadlineSeconds": ',
            self.deadline,
            ', "ttlSecondsAfterFinished": 600, "template": {"metadata":',
            ' {"labels": ',
            self._labels(r, "driver"),
            '}, "spec": {"restartPolicy": "Never", "containers": [{"name":',
            ' "driver", "image": ',
            quote(r.image),
            ', "imagePullPolicy": "IfNotPresent", "command": ',
            argv,
            ', "env": ',
            self._env(driver_env),
            ', "resources": ',
            self._resources(self.driver_cpu),
            "}]}}}}",
        )
        var made = self.k.create(JOBS, job)
        if r.workers == 0:
            return
        var owner = String(
            '{"apiVersion": "batch/v1", "kind": "Job", "name": ',
            quote(name),
            ', "uid": ',
            quote(made.at("/metadata/uid").string_value()),
            "}",
        )
        var bin = String("[", quote(r.binary), "]")
        _ = self.k.create(
            PODS,
            self._pod(
                r,
                head,
                "head",
                owner,
                bin,
                String(
                    '{"name": "DISTA_ROLE", "value": "head"}, {"name":',
                    ' "DISTA_PORT", "value": "',
                    DISTA_PORT,
                    '"}',
                ),
                self.head_cpu,
            ),
        )
        _ = self.k.create(
            SERVICES,
            String(
                '{"apiVersion": "v1", "kind": "Service", "metadata": {"name": ',
                quote(head),
                ', "labels": ',
                self._labels(r, "head"),
                ', "ownerReferences": [',
                owner,
                ']}, "spec": {"selector": ',
                self._labels(r, "head"),
                ', "ports": [{"port": ',
                DISTA_PORT,
                "}]}}",
            ),
        )
        for i in range(r.workers):
            _ = self.k.create(
                PODS,
                self._pod(
                    r,
                    String(name, "-worker-", i),
                    "worker",
                    owner,
                    bin,
                    String(
                        '{"name": "DISTA_ROLE", "value": "worker"}, {"name":',
                        ' "DISTA_HEAD", "value": "',
                        head,
                        ":",
                        DISTA_PORT,
                        '"}, {"name": "DISTA_ADVERTISE", "valueFrom":',
                        ' {"fieldRef": {"fieldPath": "status.podIP"}}}',
                    ),
                    self.worker_cpu,
                ),
            )

    def _keep_driver_log(self, job: Value):
        """Appends the log of the Job's driver pod to the run's `driver.log`
        under `ORCA_LOGS`, as the pod is about to go."""
        try:
            var run = Int(job.at("/metadata/labels/orca.run").string_value())
            var pipeline = job.at(
                "/metadata/labels/orca.pipeline"
            ).string_value()
            var start = job.at("/metadata/labels/orca.start").string_value()
            var at = run_dir(pipeline, run)
            if not at:
                return
            at.write("/driver.log")
            var text = String()
            for pod in self.k.list(
                PODS,
                String(
                    "orca.run=", run, ",orca.start=", start, ",orca.role=driver"
                ),
            ).items:
                text.write(self.k.logs(_name_of(pod)))
            if not text:
                return
            var before = String()
            if location_exists(at):
                before = String(from_utf8_lossy=Span(read_location(at)))
            var all = String(before, "--- start ", start, "\n", text)
            write_location(at, all.as_bytes())
        except e:
            print("orca: can't keep a driver log:", e)

    def remove(self, run: Int) raises:
        """Deletes everything labelled with the run, whatever its start."""
        var sel = String("orca.run=", run)
        _ = self.k.delete_all(JOBS, sel)
        _ = self.k.delete_all(SERVICES, sel)
        _ = self.k.delete_all(PODS, sel)

    def sweep(self, mut db: State) raises -> Int:
        """Deletes the objects of runs that aren't running at their start,
        and of Jobs that have finished; returns how many it deleted."""
        var live = Dict[String, Bool]()
        for r in db.conn.query(
            (
                "SELECT id || '-' || starts AS k FROM orca.runs WHERE status ="
                " 'running'"
            ),
            Params(),
        ):
            live[r.text("k")] = True
        var n = 0
        for o in self.k.list(JOBS, "orca.run").items:
            var finished = (
                _count(o, "/status/succeeded") > 0
                or _count(o, "/status/failed") > 0
            )
            if finished or _key(o) not in live:
                self._keep_driver_log(o)
                if self.k.delete(JOBS, _name_of(o)):
                    n += 1
        # What the Job owned goes with it, but not at once, and an orphan of
        # a Job deleted by hand has no owner left to go with.
        for o in self.k.list(SERVICES, "orca.run").items:
            if _key(o) not in live:
                if self.k.delete(SERVICES, _name_of(o)):
                    n += 1
        for o in self.k.list(PODS, "orca.run").items:
            if _key(o) not in live and not o.try_at(
                "/metadata/deletionTimestamp"
            ):
                if self.k.delete(PODS, _name_of(o)):
                    n += 1
        return n


def _name_of(o: Value) raises -> String:
    return o.at("/metadata/name").string_value()


def _key(o: Value) raises -> String:
    """`<run>-<start>` from an object's labels."""
    var run = o.try_at("/metadata/labels/orca.run")
    var start = o.try_at("/metadata/labels/orca.start")
    if not run or not start:
        return ""
    return String(run.value().string_value(), "-", start.value().string_value())


def _count(o: Value, ptr: String) raises -> Int:
    var v = o.try_at(ptr)
    return Int(v.value().int_value()) if v else 0
