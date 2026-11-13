#!/bin/sh
# A Mojo module holding a JSON file as a string, so a pipeline carries a
# schema file in its binary as a Rust worker does with include_str!
# (docs/rust-workers.md):
#
#     scripts/embed-json.sh schemas/numbers.json NUMBERS_SCHEMA > build/numbers_schema.mojo
#
# then `from numbers_schema import NUMBERS_SCHEMA`, with -I build.
set -eu
[ $# = 2 ] || { echo "usage: $0 <file.json> <NAME>" >&2; exit 2; }
if grep -q '"""' "$1"; then
    echo "$0: $1 has a triple quote" >&2
    exit 1
fi
printf '"""%s, embedded by scripts/embed-json.sh."""\n\ncomptime %s = """' "$1" "$2"
sed 's/\\/\\\\/g' "$1"
printf '"""\n'
