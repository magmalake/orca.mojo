"""Placement: where a pipeline's runs run (`orca register --placement`), and
the control runner, which runs them on the scheduler's own host.

A pipeline's placement is `default` (stored as null: the scheduler's runner,
`ORCA_RUNNER`, as before), `ec2` (instances of their own, `orca.ec2`; on a
scheduler whose runner isn't `ec2` such runs wait, saying why) or `control`:
the scheduler starts the run as a child process of its own, the pipeline's
binary with `resume <run>` as the local runner does, which on AWS is in the
scheduler's container on the control instance (the binary is the project's
image's). Small runs then start in a second, without an instance to boot.

**Limits**, so a run can't starve Postgres, the scheduler or the UI:

- **Memory.** Each tick the scheduler adds up the resident memory of each
  control-placed run's process tree (`orca.sample.tree_rss`) and kills one
  over `ORCA_CONTROL_MEMORY_MB` (1024), failing its attempts with why. Its
  processes are also the first the kernel kills if memory runs out before
  that (`oom_score_adj` 1000, on Linux).
- **CPU.** Its processes run at `nice` `ORCA_CONTROL_NICE` (10), so the
  control instance's own processes win when the cores are busy.
- **How many.** At most `ORCA_CONTROL_MAX_RUNS` (2) control-placed runs, of
  all pipelines, run at once.
- **Pressure.** None starts while the control instance's newest minute
  (`orca.system`, from its sampler) has memory or its fullest disk over 85%.

A run held by either of the last two stays queued with the reason in
`runs.held`, which its page and JSON show. dista inside such a run is in
local mode, `ORCA_CONTROL_WORKERS` (1) forked workers.

**Secrets** (`--secret NAME=ID`) are read by the scheduler when it starts
the run, with `ORCA_SECRET_FETCH` as the instances read them, else from
Secrets Manager's API with the scheduler's credentials (the image has no
`aws` command), and put in the run's environment only, with `ORCA_SECRETS`
naming them so its logs mask them. Its driver's output is kept in a staging
file and appended to the run's `driver.log`, masked, by `<binary> keep-log`
once the process has ended, as on an EC2 head.
"""

from std.os import getenv

from json import loads
from objectstore.crypto import sha256_hex
from objectstore.http import Header, HttpClient, Response
from objectstore.s3 import S3Credentials, instance_credentials
from objectstore.sigv4 import AmzTime, QueryParam, sign_request

from orca.json import quote
from orca.proc import binary_seconds, capture
from orca.system import Now

comptime CONTROL = "control"
"""The placement, and the instance name of a control-placed run's samples."""

comptime HOLD_PERCENT: Float64 = 85.0
"""The control instance's memory or fullest disk, in percent, past which no
control-placed run starts."""


def parse_placement(v: String) raises -> String:
    """`--placement`'s value: `control`, `ec2` or `default`."""
    if v == "control" or v == "ec2" or v == "default":
        return v
    raise Error("--placement is control, ec2 or default, not ", v)


def _env_int(name: StaticString, default: Int, least: Int) raises -> Int:
    var s = getenv(name)
    if not s:
        return default
    try:
        var n = Int(s)
        if n >= least:
            return n
    except:
        pass
    raise Error("orca: ", name, " must be a number from ", least, ", not ", s)


@fieldwise_init
struct ControlLimits(Copyable, Movable):
    """What a control-placed run may use, from the scheduler's
    environment."""

    var max_runs: Int
    """`ORCA_CONTROL_MAX_RUNS` (2): control-placed runs at once."""
    var memory_mb: Int
    """`ORCA_CONTROL_MEMORY_MB` (1024): one run's resident memory."""
    var workers: Int
    """`ORCA_CONTROL_WORKERS` (1): its dista workers, forked locally."""
    var nice: Int
    """`ORCA_CONTROL_NICE` (10): its CPU priority, lowered by this."""

    @staticmethod
    def from_env() raises -> ControlLimits:
        return ControlLimits(
            _env_int("ORCA_CONTROL_MAX_RUNS", 2, 0),
            _env_int("ORCA_CONTROL_MEMORY_MB", 1024, 1),
            _env_int("ORCA_CONTROL_WORKERS", 1, 1),
            _env_int("ORCA_CONTROL_NICE", 10, 0),
        )


def _pct(x: Float64) -> String:
    return String(Int(x + 0.5))


def pressure_reason(now: Optional[Now]) -> String:
    """Why no control-placed run may start now, given the control
    instance's newest minute, or empty if one may. Samples that are old or
    missing hold nothing: a scheduler without a sampler isn't stopped."""
    if not now or now.value().stale():
        return ""
    ref n = now.value()
    if n.mem_pct > HOLD_PERCENT:
        return String(
            "held: the control instance's memory is ",
            _pct(n.mem_pct),
            "% used, over ",
            Int(HOLD_PERCENT),
            "%",
        )
    if n.disk_pct > HOLD_PERCENT:
        return String(
            "held: the control instance's disk ",
            n.disk_path,
            " is ",
            _pct(n.disk_pct),
            "% full, over ",
            Int(HOLD_PERCENT),
            "%",
        )
    return ""


def slots_reason(running: Int, max_runs: Int) -> String:
    """Why a control-placed run waits for one of the others to end."""
    return String(
        "held: ",
        running,
        " of ",
        max_runs,
        " control-placed runs are running (ORCA_CONTROL_MAX_RUNS)",
    )


def control_stage(pipeline: String, run: Int, start: Int) -> String:
    """Where a control-placed run's start writes its output before
    `keep-log` appends it to the run's `driver.log`: under `ORCA_LOG_STAGE`
    (else `$TMPDIR/orca-logs`), never in the logs themselves, since it isn't
    masked yet."""
    var root = getenv("ORCA_LOG_STAGE")
    if not root:
        root = String(getenv("TMPDIR", "/tmp").rstrip("/"), "/orca-logs")
    return String(
        root, "/control/", pipeline, "-", run, "-", start, "-driver.log"
    )


def run_env(
    limits: ControlLimits,
    image: String,
    secrets: List[Tuple[String, String]],
) -> List[String]:
    """What a control-placed run's process gets beside the scheduler's
    environment: its local workers, its image (which its version records)
    and its secrets' values, `secrets` being (NAME, value) pairs, named in
    `ORCA_SECRETS`."""
    var out: List[String] = [String("ORCA_WORKERS=", limits.workers)]
    if image:
        out.append(String("ORCA_IMAGE=", image))
    var names = String()
    for s in secrets:
        out.append(String(s[0], "=", s[1]))
        names.write("," if names else "", s[0])
    if names:
        out.append(String("ORCA_SECRETS=", names))
    return out^


def _sh_word(s: String) -> String:
    return String("'", s.replace("'", "'\\''"), "'")


def fetch_secret(id: String) raises -> String:
    """The value of the secret `id`: what `ORCA_SECRET_FETCH <id>` prints,
    as the instances' scripts read it (trailing newlines dropped, as `$(...)`
    drops them), else Secrets Manager's `GetSecretValue`."""
    var cmd = getenv("ORCA_SECRET_FETCH")
    if cmd:
        var got = capture(
            ["sh", "-c", String(cmd, " ", _sh_word(id))], binary_seconds()
        )
        if got[0] != 0:
            raise Error("can't read secret ", id, ": exit ", got[0])
        var n = got[1].byte_length()
        while n > 0 and got[1].as_bytes()[n - 1] == UInt8(ord("\n")):
            n -= 1
        return String(got[1][byte=:n])
    return get_secret_value(id)


def get_secret_value(id: String) raises -> String:
    """Secrets Manager's `GetSecretValue` of `id`, over objectstore's SigV4
    and HTTP, with the credentials of the `AWS_*` environment or else the
    instance role; `ORCA_SECRETS_ENDPOINT` points it elsewhere, as the tests
    do at their stand-in (`tests/fake_ec2.py`)."""
    var region = getenv("AWS_REGION", getenv("AWS_DEFAULT_REGION"))
    if not region:
        raise Error("can't read secret ", id, ": no AWS_REGION")
    var endpoint = String(
        getenv(
            "ORCA_SECRETS_ENDPOINT",
            String("https://secretsmanager.", region, ".amazonaws.com"),
        ).rstrip("/")
    )
    var host = String(endpoint[byte = endpoint.find("://") + 3 :])
    var cred = S3Credentials(
        getenv("AWS_ACCESS_KEY_ID"),
        getenv("AWS_SECRET_ACCESS_KEY"),
        getenv("AWS_SESSION_TOKEN"),
    )
    if not cred.is_set():
        var role = instance_credentials()
        if not role:
            raise Error("can't read secret ", id, ": no AWS credentials")
        cred = role.take()
    var body = String('{"SecretId": ', quote(id), "}")
    var when = AmzTime.now()
    var headers: List[Header] = [
        Header("content-type", "application/x-amz-json-1.1"),
        Header("host", host),
        Header("x-amz-date", when.amz_date),
        Header("x-amz-target", "secretsmanager.GetSecretValue"),
    ]
    if cred.session_token:
        headers.append(Header("x-amz-security-token", cred.session_token))
    var signed = sign_request(
        "POST",
        "/",
        List[QueryParam](),
        headers,
        sha256_hex(body),
        cred.access_key_id,
        cred.secret_access_key,
        region,
        "secretsmanager",
        when,
    )
    headers.append(Header("authorization", signed.authorization))
    var resp: Response
    try:
        resp = HttpClient().post(
            String(endpoint, "/"), body.as_bytes(), headers
        )
    except e:
        raise Error("can't read secret ", id, ": ", e)
    var text = String(from_utf8_lossy=Span(resp.body))
    if resp.status != 200:
        # Its error names a type and a message, never a value.
        raise Error("can't read secret ", id, ": ", resp.status, " ", text)
    return loads(text)["SecretString"].string_value()
