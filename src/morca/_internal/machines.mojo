"""Machines for a worker pool, from whichever provider the pool names (#267):
what the autoscaler (`morca._internal.pool_scale`) calls, so it never knows which.

    var p = Provider.of("hetzner", owner)   # raises without its settings
    p.arch("cax31")                  # "arm64": Docker's name, for the image
    var got = p.create(MachineSpec("morca-lab-7", "cax31", "hel1", data, "lab"))
    for m in p.machines("lab"): ...
    _ = p.delete(m.id)

**Hetzner** (`morca._internal.hcloud`): the token as there; a machine is a server of
`MORCA_HCLOUD_IMAGE` (`ubuntu-24.04`), with the project's SSH keys
`MORCA_HCLOUD_SSH_KEYS` (comma-separated names, none by default) and the
firewall `MORCA_HCLOUD_FIREWALL` (none by default), labelled `morca=pool`,
`orca-pool=<pool>` and `orca-owner=<owner>` (the stack's id, so stacks
sharing a project keep their machines apart), which is how `machines`
finds a pool's. Every machine joins the private network
`MORCA_HCLOUD_NETWORK` (`orca-<owner>`, made in `MORCA_HCLOUD_NETWORK_ZONE`,
`eu-central`, over `MORCA_HCLOUD_NETWORK_RANGE`, `10.77.0.0/16`, if it isn't
there; `none` for none): one run's dista cluster over several machines
talks over it (#268), its public side only ssh.

**EC2** (`morca._internal.machines_ec2`): subnets for locations, the security group
and instance profile from the scheduler's settings, Ubuntu for the type's
architecture, the same labels as tags.

Architectures are Docker's (`arm64`, `amd64`), the names an image's
manifest uses, so the autoscaler can tell whether the worker image runs on
a type before it makes one.
"""

from std.os import getenv

from morca._internal.hcloud import Created, HCloud, ServerSpec
from morca._internal.machines_ec2 import Ec2Machines

comptime POOL_LABEL = "orca-pool"
comptime OWNER_LABEL = "orca-owner"


@fieldwise_init
struct MachineSpec(Copyable, Movable):
    var name: String
    """A host name: 1 to 63 of a-z, 0-9 and `-`."""
    var machine_type: String
    var location: String
    var user_data: String
    var pool: String


@fieldwise_init
struct Machine(Copyable, Movable):
    var id: String
    var name: String
    var status: String
    var machine_type: String
    var location: String
    var address: String
    var pool: String


def docker_arch(provider_arch: String) -> String:
    """A provider's name for an architecture as Docker's: `arm`/`aarch64`
    is `arm64`, `x86`/`x86_64` is `amd64`; others as they are."""
    if provider_arch == "arm" or provider_arch == "aarch64":
        return "arm64"
    if provider_arch == "x86" or provider_arch == "x86_64":
        return "amd64"
    return provider_arch


def _list_env(name: String) -> List[String]:
    var out = List[String]()
    for part in getenv(name).split(","):
        var p = String(part.strip())
        if p:
            out.append(p)
    return out^


struct Provider(Movable):
    var kind: String
    var owner: String
    """The stack's id, a label on every machine; empty for none."""
    var _hcloud: Optional[HCloud]
    var _ec2: Optional[Ec2Machines]
    var image: String
    var ssh_keys: List[String]
    var firewall: String
    var network: String
    """The private network its machines join (`MORCA_HCLOUD_NETWORK`, else
    `orca-<owner>`; `none` for no network)."""

    def __init__(
        out self, kind: String, owner: String, var hcloud: Optional[HCloud]
    ):
        self.kind = kind
        self.owner = owner
        self._hcloud = hcloud^
        self._ec2 = None
        self.image = getenv("MORCA_HCLOUD_IMAGE", "ubuntu-24.04")
        self.ssh_keys = _list_env("MORCA_HCLOUD_SSH_KEYS")
        self.firewall = getenv("MORCA_HCLOUD_FIREWALL")
        self.network = getenv("MORCA_HCLOUD_NETWORK")
        if not self.network and owner:
            self.network = String("orca-", owner)
        if self.network == "none":
            self.network = String()

    @staticmethod
    def of(kind: String, owner: String = "") raises -> Provider:
        """The provider `kind` (`hetzner`; `ec2` isn't here yet), from this
        process's settings, its machines labelled `owner`; raises without
        them."""
        if kind == "hetzner":
            return Provider(kind, owner, HCloud.from_env())
        if kind == "ec2":
            var p = Provider(kind, owner, None)
            p._ec2 = Ec2Machines()
            return p^
        raise Error("morca: no provider ", kind)

    def arch(self, machine_type: String) raises -> String:
        """`machine_type`'s architecture, Docker's name for it."""
        if self._ec2:
            return self._ec2.value().arch(machine_type)
        return docker_arch(self._hcloud.value().arch(machine_type))

    def create(self, spec: MachineSpec) raises -> Created:
        """Makes the machine `spec` says: ok, or no stock of its type in its
        location (try again later, or elsewhere), or why not."""
        var labels: List[Tuple[String, String]] = [
            ("orca", "pool"),
            (POOL_LABEL, spec.pool),
        ]
        if self.owner:
            labels.append((OWNER_LABEL, self.owner))
        if self._ec2:
            return self._ec2.value().create(
                spec.name,
                spec.machine_type,
                spec.location,
                spec.user_data,
                labels,
            )
        # The pool's private network (dista's traffic, #268), made once.
        var network = String()
        if self.network:
            network = self._hcloud.value().network_id(
                self.network,
                getenv("MORCA_HCLOUD_NETWORK_ZONE", "eu-central"),
                getenv("MORCA_HCLOUD_NETWORK_RANGE", "10.77.0.0/16"),
            )
        return self._hcloud.value().create(
            ServerSpec(
                spec.name,
                spec.machine_type,
                spec.location,
                self.image,
                spec.user_data,
                labels^,
                self.ssh_keys.copy(),
                self.firewall,
                network,
            )
        )

    def machines(self, pool: String) raises -> List[Machine]:
        """The provider's machines of `pool`, whatever orca.pool_machines
        says: the autoscaler deletes those it doesn't know."""
        var out = List[Machine]()
        if self._ec2:
            var tags: List[Tuple[String, String]] = [(POOL_LABEL, pool)]
            if self.owner:
                tags.append((OWNER_LABEL, self.owner))
            for i in self._ec2.value().instances(tags):
                out.append(Machine(i[0], i[1], i[2], i[3], "", i[4], pool))
            return out^
        var selector = String(POOL_LABEL, "=", pool)
        if self.owner:
            selector.write(",", OWNER_LABEL, "=", self.owner)
        for s in self._hcloud.value().servers(selector):
            out.append(
                Machine(
                    s.id,
                    s.name,
                    s.status,
                    s.server_type,
                    s.location,
                    s.address,
                    s.pool,
                )
            )
        return out^

    def delete(self, id: String) raises -> Bool:
        """Deletes machine `id`: True once it's gone, now or before."""
        if self._ec2:
            return self._ec2.value().delete(id)
        return self._hcloud.value().delete(id)
