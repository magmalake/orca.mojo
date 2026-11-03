#!/bin/sh
# Every pipeline in tests/compile_fail/ (or the directory given) breaks one
# rule and must not compile. tests/keyed.sh runs it on
# tests/compile_fail_keyed/, whose pipelines need iceberg.
# The first line of each file is `# expect: <text>`, and the compiler's
# output has to contain that text, so a file can't pass by failing for some
# other reason.
set -u
cd "$(dirname "$0")/.."
mkdir -p build
fails=0
dir=${1:-tests/compile_fail}
for f in "$dir"/*.mojo; do
    expect=$(head -1 "$f" | sed -n 's/^# expect: //p')
    if [ -z "$expect" ]; then
        echo "FAIL $f: no '# expect:' line"
        fails=$((fails + 1))
        continue
    fi
    # shellcheck disable=SC2086
    if out=$(mojo build -I src $DEP_INC "$f" -o build/compile-fail 2>&1); then
        echo "FAIL $f: compiled"
        fails=$((fails + 1))
    elif printf '%s' "$out" | grep -qF -- "$expect"; then
        echo "ok   $f"
    else
        echo "FAIL $f: rejected without '$expect':"
        printf '%s\n' "$out" | grep -E 'error|constraint' | head -5
        fails=$((fails + 1))
    fi
done
rm -f build/compile-fail
[ "$fails" -eq 0 ]
