"""A stand-in for EC2's API, for tests/ec2.sh: RunInstances,
DescribeInstances and TerminateInstances, with each "instance" a local
process running its user data under sh.

    python3 tests/fake_ec2.py <port> <dir>

Every call is appended to <dir>/calls.jsonl as {"action", "params"}; each
instance's script and output are <dir>/<id>.sh and <dir>/<id>.log, its /tmp
is <dir>/<id>.tmp (the script's /tmp/morca-* files are moved there), and its
environment has HOSTNAME set to its ID, so a test can tell instances apart.
All instances share 127.0.0.1 as their private address. A process that
exits is an instance that terminated, as `InstanceInitiatedShutdownBehavior
=terminate` makes one; TerminateInstances kills its process group, and so
does the instance's end however it comes, as a machine's would: nothing
its script started, such as a dista head or worker, outlives it to reach
the next test's head (#403). A terminated instance's state reason says
which, as EC2's does. While the file
<dir>/no-workers exists, the instances tagged `orca.role=worker` that are
launched only sleep, so never join their run's head (tests/no_workers.sh).
Requests aren't checked for a signature.

It also stands in for Secrets Manager's GetSecretValue (a JSON POST with
`X-Amz-Target: secretsmanager.GetSecretValue`), as tests/fake_secret.sh
does: the value of ID is `s3cr3t-<ID with / as ->-value`, and orca/missing
isn't found.

For worker pools (morca.machines_ec2): DescribeInstanceTypes (a type with
`g.` in its name, `c7g.large`, is arm64, the rest x86_64), tag filters
(`tag:<key>`) in DescribeInstances, and SSM's GetParameter (`X-Amz-Target:
AmazonSSM.GetParameter`), which answers `ami-<arch>` for Canonical's
Ubuntu parameters. An instance tagged `orca-pool` gets MORCA_BOOT_DIR
(<dir>/<id>.boot) and SERVER_NAME (its Name tag); while <dir>/pool-stock
exists, one is made only for a `<type> <subnet>` line of it, and refused
`InsufficientInstanceCapacity` otherwise.
"""

import base64
import datetime
import http.server
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.parse

PORT = int(sys.argv[1])
DIR = sys.argv[2]
LOCK = threading.Lock()
INSTANCES = {}  # id -> {"proc", "tags", "type", "launch", "market"}
COUNTER = [0]


def _state(i):
    return "running" if i["proc"].poll() is None else "terminated"


def _reason(i):
    if _state(i) == "running":
        return ""
    code = "Client.UserInitiatedShutdown" if i.get("killed") else "Client.InstanceInitiatedShutdown"
    return f"<stateReason><code>{code}</code><message>{code}</message></stateReason>"


def _item(iid, i):
    tags = "".join(
        f"<item><key>{k}</key><value>{v}</value></item>" for k, v in i["tags"].items()
    )
    return (
        f"<item><instanceId>{iid}</instanceId><instanceType>{i['type']}</instanceType>"
        f"<instanceState><code>16</code><name>{_state(i)}</name></instanceState>{_reason(i)}"
        f"<privateIpAddress>127.0.0.1</privateIpAddress>"
        f"<launchTime>{i['launch']}</launchTime><tagSet>{tags}</tagSet></item>"
    )


class Ec2Error(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def _arch(t):
    return "arm64" if "g." in t else "x86_64"


def _run(p):
    count = int(p["MaxCount"])
    tags = {}
    n = 1
    while f"TagSpecification.1.Tag.{n}.Key" in p:
        tags[p[f"TagSpecification.1.Tag.{n}.Key"]] = p[f"TagSpecification.1.Tag.{n}.Value"]
        n += 1
    stock = os.path.join(DIR, "pool-stock")
    if "orca-pool" in tags and os.path.exists(stock):
        with open(stock) as f:
            have = {tuple(l.split()[:2]) for l in f if len(l.split()) >= 2}
        if (p["InstanceType"], p.get("SubnetId", "")) not in have:
            raise Ec2Error("InsufficientInstanceCapacity",
                           f"no {p['InstanceType']} capacity in {p.get('SubnetId', '')}", 500)
    script = base64.b64decode(p["UserData"]).decode()
    if tags.get("orca.role") == "worker" and os.path.exists(os.path.join(DIR, "no-workers")):
        script = "#!/bin/sh\nsleep 3600\n"
    launch = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    items = []
    for _ in range(count):
        COUNTER[0] += 1
        iid = f"i-{COUNTER[0]:017x}"
        path = os.path.join(DIR, f"{iid}.sh")
        # A /tmp of its own, as an instance has: the scripts name their
        # files there by run, start, role and ID, which every test's first
        # run and instances share, so in the one /tmp a test's file could
        # be another's, or what an earlier test's instance left.
        tmp = os.path.join(DIR, f"{iid}.tmp")
        os.makedirs(tmp, exist_ok=True)
        with open(path, "w") as f:
            f.write(re.sub(r"(?<=[\s>'\"])/tmp/morca-", tmp + "/morca-", script))
        env = dict(os.environ, HOSTNAME=iid)
        if "orca-pool" in tags:
            env.update(MORCA_BOOT_DIR=os.path.join(DIR, f"{iid}.boot"), SERVER_NAME=tags.get("Name", iid))
        out = open(os.path.join(DIR, f"{iid}.log"), "w")
        proc = subprocess.Popen(
            ["sh", path], env=env, stdout=out, stderr=subprocess.STDOUT, start_new_session=True
        )
        INSTANCES[iid] = {
            "proc": proc,
            "tags": tags,
            "type": p["InstanceType"],
            "launch": launch,
            "market": p.get("InstanceMarketOptions.MarketType", "on-demand"),
        }
        items.append(_item(iid, INSTANCES[iid]))
    return f"<RunInstancesResponse><instancesSet>{''.join(items)}</instancesSet></RunInstancesResponse>"


def _describe(p):
    filters = {}
    n = 1
    while f"Filter.{n}.Name" in p:
        values = []
        m = 1
        while f"Filter.{n}.Value.{m}" in p:
            values.append(p[f"Filter.{n}.Value.{m}"])
            m += 1
        filters[p[f"Filter.{n}.Name"]] = values
        n += 1
    items = []
    for iid, i in INSTANCES.items():
        if "instance-id" in filters and iid not in filters["instance-id"]:
            continue
        if "tag-key" in filters and not any(k in i["tags"] for k in filters["tag-key"]):
            continue
        if "instance-state-name" in filters and _state(i) not in filters["instance-state-name"]:
            continue
        if any(k.startswith("tag:") and i["tags"].get(k[4:]) not in v for k, v in filters.items()):
            continue
        items.append(_item(iid, i))
    return (
        "<DescribeInstancesResponse><reservationSet><item><instancesSet>"
        f"{''.join(items)}</instancesSet></item></reservationSet></DescribeInstancesResponse>"
    )


def _types(p):
    t = p.get("InstanceType.1", "")
    return (
        "<DescribeInstanceTypesResponse><instanceTypeSet><item>"
        f"<instanceType>{t}</instanceType><processorInfo><supportedArchitectures>"
        f"<item>{_arch(t)}</item></supportedArchitectures></processorInfo>"
        "</item></instanceTypeSet></DescribeInstanceTypesResponse>"
    )


def _kill_group(i):
    """Kills what's left of instance `i`'s processes, once: its script's
    process group (its own session)."""
    if i.get("gone"):
        return
    i["gone"] = True
    try:
        os.killpg(i["proc"].pid, signal.SIGKILL)
    except OSError:
        pass  # gone; macOS says EPERM for a group of only a zombie


def _reap():
    """An instance whose script exited has terminated: what it left running
    goes with it."""
    while True:
        with LOCK:
            for i in INSTANCES.values():
                if not i.get("gone") and i["proc"].poll() is not None:
                    _kill_group(i)
        time.sleep(0.2)


def _terminate(p):
    n = 1
    while f"InstanceId.{n}" in p:
        i = INSTANCES.get(p[f"InstanceId.{n}"])
        if i and i["proc"].poll() is None:
            i["killed"] = True
            _kill_group(i)
        n += 1
    return "<TerminateInstancesResponse><return>true</return></TerminateInstancesResponse>"


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _secret(self, body):
        sid = json.loads(body).get("SecretId", "")
        with LOCK:
            with open(os.path.join(DIR, "calls.jsonl"), "a") as f:
                f.write(json.dumps({"action": "GetSecretValue", "params": {"SecretId": sid}}) + "\n")
        if sid == "orca/missing":
            status, out = 400, {"__type": "ResourceNotFoundException", "message": "no such secret"}
        else:
            status, out = 200, {"Name": sid, "SecretString": "s3cr3t-" + sid.replace("/", "-") + "-value"}
        data = json.dumps(out).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/x-amz-json-1.1")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        if self.headers.get("X-Amz-Target") == "secretsmanager.GetSecretValue":
            self._secret(body)
            return
        if self.headers.get("X-Amz-Target") == "AmazonSSM.GetParameter":
            name = json.loads(body).get("Name", "")
            arch = "arm64" if "/arm64/" in name else "amd64"
            data = json.dumps({"Parameter": {"Name": name, "Value": f"ami-{arch}"}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        p = {k: v[0] for k, v in urllib.parse.parse_qs(body, keep_blank_values=True).items()}
        action = p.get("Action", "")
        with LOCK:
            with open(os.path.join(DIR, "calls.jsonl"), "a") as f:
                f.write(json.dumps({"action": action, "params": p}) + "\n")
            try:
                xml = {"RunInstances": _run, "DescribeInstances": _describe,
                       "TerminateInstances": _terminate, "DescribeInstanceTypes": _types}[action](p)
                status = 200
            except Ec2Error as e:
                xml = f"<Response><Errors><Error><Code>{e.code}</Code><Message>{e.message}</Message></Error></Errors></Response>"
                status = e.status
            except KeyError:
                xml = f"<Response><Errors><Error><Code>InvalidAction</Code><Message>{action}</Message></Error></Errors></Response>"
                status = 400
        data = xml.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _stop(*_):
    for i in list(INSTANCES.values()):
        _kill_group(i)
    sys.exit(0)


signal.signal(signal.SIGTERM, _stop)
threading.Thread(target=_reap, daemon=True).start()
http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
