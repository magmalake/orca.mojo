"""A worker pool's machines on EC2 (#267): `morca._internal.machines.Provider` for a
pool with `provider = "ec2"`.

- **Where:** a pool's `locations` are subnet IDs, tried in order (each is
  in one availability zone); with none, EC2's default (or
  `MORCA_EC2_SUBNET`).
- **What each instance gets:** the security group
  `MORCA_POOL_EC2_SECURITY_GROUP` (else `MORCA_EC2_SECURITY_GROUP`), and the
  instance profile `MORCA_POOL_EC2_PROFILE` (else `MORCA_EC2_PROFILE`), whose
  role is what its worker reaches the bucket with. IMDSv2 only;
  `InstanceInitiatedShutdownBehavior=terminate`.
- **Its OS image:** Ubuntu 24.04 for the instance type's architecture,
  from the public SSM parameter Canonical keeps
  (`/aws/service/canonical/ubuntu/server/24.04/stable/current/<arch>/hvm/ebs-gp3/ami-id`),
  or `MORCA_POOL_EC2_AMI_ARM64` / `MORCA_POOL_EC2_AMI_AMD64` to pin one.
  `MORCA_SSM_ENDPOINT` points the lookup elsewhere (tests).
- **Tags:** `morca=pool`, `orca-pool=<pool>`, `orca-owner=<stack>` and
  `Name`, which is how a pool's are listed.
- **No capacity:** `InsufficientInstanceCapacity`, `Unsupported` (the type
  isn't offered in that zone) and `InsufficientCapacity` are no stock;
  `VcpuLimitExceeded` and any other refusal are reported.
"""

from std.base64 import b64encode
from std.os import getenv

from json import loads

from morca._internal.awsec2 import Ec2
from morca._internal.awsjson import aws_json
from morca._internal.hcloud import Created
from morca._internal.json import quote

comptime NO_CAPACITY = [
    "InsufficientInstanceCapacity",
    "InsufficientCapacity",
    "Unsupported",
]
comptime _UBUNTU = "/aws/service/canonical/ubuntu/server/24.04/stable/current/"


def _first_env(a: String, b: String) -> String:
    var v = getenv(a)
    return v if v else getenv(b)


def ec2_arch(arch: String) -> String:
    """EC2's name for an architecture as Docker's (`x86_64` is `amd64`)."""
    if arch == "x86_64":
        return "amd64"
    return arch


def ubuntu_ami(docker_arch: String) raises -> String:
    """The Ubuntu 24.04 image for `docker_arch` (`arm64`, `amd64`): pinned
    by `MORCA_POOL_EC2_AMI_<ARCH>`, else Canonical's public SSM parameter."""
    var pinned = getenv(String("MORCA_POOL_EC2_AMI_", docker_arch.upper()))
    if pinned:
        return pinned
    var name = String(_UBUNTU, docker_arch, "/hvm/ebs-gp3/ami-id")
    var got = aws_json(
        "ssm",
        "AmazonSSM.GetParameter",
        "MORCA_SSM_ENDPOINT",
        String('{"Name": ', quote(name), "}"),
    )
    if got[0] != 200:
        raise Error(
            "morca: can't look up the Ubuntu image for ",
            docker_arch,
            ": ",
            got[0],
            " ",
            got[1],
        )
    return loads(got[1])["Parameter"]["Value"].string_value()


def is_no_capacity(error: String) -> Bool:
    for c in materialize[NO_CAPACITY]():
        if String(c, ":") in error:
            return True
    return False


struct Ec2Machines(Movable):
    var ec2: Ec2

    def __init__(out self) raises:
        self.ec2 = Ec2()

    def arch(self, instance_type: String) raises -> String:
        """`instance_type`'s architecture, Docker's name for it (the first
        it supports)."""
        return ec2_arch(self.ec2.instance_type_arches(instance_type)[0])

    def create(
        self,
        name: String,
        instance_type: String,
        subnet: String,
        user_data: String,
        tags: List[Tuple[String, String]],
    ) raises -> Created:
        var ami: String
        try:
            ami = ubuntu_ami(self.arch(instance_type))
        except e:
            return Created(False, False, "", "", String(e))
        var p: List[Tuple[String, String]] = [
            ("ImageId", ami),
            ("InstanceType", instance_type),
            ("MinCount", "1"),
            ("MaxCount", "1"),
            ("InstanceInitiatedShutdownBehavior", "terminate"),
            ("UserData", b64encode(user_data)),
            ("MetadataOptions.HttpTokens", "required"),
            ("TagSpecification.1.ResourceType", "instance"),
        ]
        var all = tags.copy()
        all.append(("Name", name))
        for i in range(len(all)):
            p.append(
                (String("TagSpecification.1.Tag.", i + 1, ".Key"), all[i][0])
            )
            p.append(
                (String("TagSpecification.1.Tag.", i + 1, ".Value"), all[i][1])
            )
        var sn = subnet if subnet else getenv("MORCA_EC2_SUBNET")
        if sn:
            p.append(("SubnetId", sn))
        var sg = _first_env(
            "MORCA_POOL_EC2_SECURITY_GROUP", "MORCA_EC2_SECURITY_GROUP"
        )
        if sg:
            p.append(("SecurityGroupId.1", sg))
        var profile = _first_env("MORCA_POOL_EC2_PROFILE", "MORCA_EC2_PROFILE")
        if profile:
            p.append(("IamInstanceProfile.Name", profile))
        try:
            var made = self.ec2.run_instances(p)
            if len(made) == 0:
                return Created(False, False, "", "", "EC2 made no instance")
            return Created(True, False, made[0].id, made[0].private_ip, "")
        except e:
            var why = String(e)
            return Created(False, is_no_capacity(why), "", "", why)

    def instances(
        self, tags: List[Tuple[String, String]]
    ) raises -> List[Tuple[String, String, String, String, String]]:
        """The live instances with every tag of `tags`: (id, name, state,
        type, address)."""
        var filters = List[Tuple[String, List[String]]]()
        for t in tags:
            var v = List[String]()
            v.append(t[1])
            filters.append((String("tag:", t[0]), v^))
        var states = List[String]()
        for st in ["pending", "running", "stopping", "stopped"]:
            states.append(String(st))
        filters.append((String("instance-state-name"), states^))
        var out = List[Tuple[String, String, String, String, String]]()
        for i in self.ec2.describe(filters):
            out.append(
                (
                    i.id,
                    i.tags.get("Name", ""),
                    i.state,
                    i.instance_type,
                    i.private_ip,
                )
            )
        return out^

    def delete(self, id: String) raises -> Bool:
        """Terminates `id`: True once it's gone, now or before."""
        try:
            self.ec2.terminate([id])
        except e:
            if "InvalidInstanceID.NotFound" in String(e):
                return True
            raise e^
        return True
