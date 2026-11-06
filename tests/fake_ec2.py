"""A stand-in for EC2's API, for tests/ec2.sh: RunInstances,
DescribeInstances and TerminateInstances, with each "instance" a local
process running its user data under sh.

    python3 tests/fake_ec2.py <port> <dir>

Every call is appended to <dir>/calls.jsonl as {"action", "params"}; each
instance's script and output are <dir>/<id>.sh and <dir>/<id>.log, and its
environment has HOSTNAME set to its ID, so a test can tell instances apart.
All instances share 127.0.0.1 as their private address. A process that
exits is an instance that terminated, as `InstanceInitiatedShutdownBehavior
=terminate` makes one; TerminateInstances kills its process group. A
terminated instance's state reason says which, as EC2's does. While the file
<dir>/no-workers exists, the instances tagged `orca.role=worker` that are
launched only sleep, so never join their run's head (tests/no_workers.sh).
Requests aren't checked for a signature.
"""

import base64
import datetime
import http.server
import json
import os
import signal
import subprocess
import sys
import threading
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


def _run(p):
    count = int(p["MaxCount"])
    tags = {}
    n = 1
    while f"TagSpecification.1.Tag.{n}.Key" in p:
        tags[p[f"TagSpecification.1.Tag.{n}.Key"]] = p[f"TagSpecification.1.Tag.{n}.Value"]
        n += 1
    script = base64.b64decode(p["UserData"]).decode()
    if tags.get("orca.role") == "worker" and os.path.exists(os.path.join(DIR, "no-workers")):
        script = "#!/bin/sh\nsleep 3600\n"
    launch = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    items = []
    for _ in range(count):
        COUNTER[0] += 1
        iid = f"i-{COUNTER[0]:017x}"
        path = os.path.join(DIR, f"{iid}.sh")
        with open(path, "w") as f:
            f.write(script)
        env = dict(os.environ, HOSTNAME=iid)
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
        items.append(_item(iid, i))
    return (
        "<DescribeInstancesResponse><reservationSet><item><instancesSet>"
        f"{''.join(items)}</instancesSet></item></reservationSet></DescribeInstancesResponse>"
    )


def _terminate(p):
    n = 1
    while f"InstanceId.{n}" in p:
        i = INSTANCES.get(p[f"InstanceId.{n}"])
        if i and i["proc"].poll() is None:
            i["killed"] = True
            try:
                os.killpg(i["proc"].pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        n += 1
    return "<TerminateInstancesResponse><return>true</return></TerminateInstancesResponse>"


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        p = {k: v[0] for k, v in urllib.parse.parse_qs(body, keep_blank_values=True).items()}
        action = p.get("Action", "")
        with LOCK:
            with open(os.path.join(DIR, "calls.jsonl"), "a") as f:
                f.write(json.dumps({"action": action, "params": p}) + "\n")
            try:
                xml = {"RunInstances": _run, "DescribeInstances": _describe,
                       "TerminateInstances": _terminate}[action](p)
                status = 200
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
    for i in INSTANCES.values():
        if i["proc"].poll() is None:
            try:
                os.killpg(i["proc"].pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    sys.exit(0)


signal.signal(signal.SIGTERM, _stop)
http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
