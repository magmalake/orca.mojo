# Sourced by the tests that start a server (morca ui, the stand-ins for EC2
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

# Sourced too by the tests whose runs reach a dista head on the stand-in
# clouds (on 127.0.0.1, which every stand-in instance shares) or look for
# workers by name (pgrep): two of them at once on one machine, as on CI's
# self-hosted runners, would find each other's head or workers.
#
#   one_cluster_test
#
# waits, up to 20 minutes, for the other cluster tests on this machine to
# end, and holds the lock until this shell and what it started have exited.
# The lock is $MORCA_CLUSTER_LOCK, by default under ~/.cache, not /tmp: a
# runner's service may have a /tmp of its own (systemd's PrivateTmp), and
# sai's runners don't share theirs (#403). Without flock (macOS) there's no
# lock. Then it gives the test a head port of its own (MORCA_EC2_HEAD_PORT,
# from free_port, instead of morca.ec2's 6390), so a run's driver and
# workers reach only its own head, whatever another test left running.
one_cluster_test() {
    if command -v flock > /dev/null 2>&1; then
        _lock=${MORCA_CLUSTER_LOCK:-${XDG_CACHE_HOME:-$HOME/.cache}/morca-tests/cluster.lock}
        mkdir -p "$(dirname "$_lock")"
        exec 9>> "$_lock"
        flock -w 1200 9 || { echo "one_cluster_test: another test held $_lock for 20 minutes" >&2; return 1; }
    fi
    free_port MORCA_EC2_HEAD_PORT || return 1
    export MORCA_EC2_HEAD_PORT
}
