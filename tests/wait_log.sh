# Sourced by the tests that read a log an instance uploads on its own time
# (a head uploads its driver log in its grace after the run, or, pooled, at
# the end of each run; a worker uploads when it exits), so a run being over
# doesn't mean its log is there yet.
#
#   wait_log FILE [PATTERN [TENTHS]]
#
# Waits, up to TENTHS tenths of a second (default 600), until FILE exists and,
# when given, a line of it matches the grep -e PATTERN. Returns 1 on timeout,
# for `|| fail "..."`.
wait_log() {
    _w=0
    until [ -f "$1" ] && { [ -z "${2:-}" ] || grep -q -e "$2" "$1"; }; do
        _w=$((_w + 1))
        [ "$_w" -lt "${3:-600}" ] || return 1
        sleep 0.1
    done
}
