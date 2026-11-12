# Sourced by the tests that start a server (orca ui, the stand-ins for EC2
# and ntfy, a port-forward):
#
#   free_port port
#
# sets $port to a TCP port on 127.0.0.1 that nothing is bound to, picked at
# random from 10000-32767, and not one this shell was given before (so it's
# called in the test's shell, never in a $(...) subshell).
#
# The ports the tests used to pick, 18000-49999 offset by $$, were mostly in
# the kernel's ephemeral range (32768-60999 on Linux, 49152-65535 on macOS),
# which it hands to outgoing connections: a curl or a scheduler's request
# could hold the port, or a TIME_WAIT left by one, when the server bound it
# (AddressInUse). Below 32768 the kernel picks none itself; the bind check
# skips one a server left by an earlier test still holds, and the random
# pick keeps tests that run at once on one machine apart.
free_port() {
    _p=$(python3 -c '
import random, socket, sys
taken = set(sys.argv[1].split())
for _ in range(200):
    p = random.randint(10000, 32767)
    if str(p) in taken:
        continue
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", p))
    except OSError:
        continue
    finally:
        s.close()
    print(p)
    break
else:
    sys.exit("free_port: no free port in 10000-32767")
' "${_free_ports:-}") || return 1
    _free_ports="${_free_ports:-} $_p"
    eval "$1=\$_p"
}

# Sourced too by the tests whose runs reach a dista head on its fixed port
# (6390, orca.ec2's HEAD_PORT, on 127.0.0.1 with the stand-in clouds) or
# look for workers by name (pgrep): two of them at once on one machine, as
# on CI's self-hosted runners, find each other's head or workers.
#
#   one_cluster_test
#
# waits, up to 20 minutes, for the others on this machine to end, and holds
# the lock (/tmp/orca-cluster-tests.lock) until this shell and what it
# started have exited. Without flock (macOS) it does nothing.
one_cluster_test() {
    command -v flock > /dev/null 2>&1 || return 0
    exec 9>> /tmp/orca-cluster-tests.lock
    flock -w 1200 9 || { echo "one_cluster_test: another test held the lock for 20 minutes" >&2; return 1; }
}
