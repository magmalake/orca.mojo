#!/bin/sh
# Clones, next to morca, every repository CI's `data` job checks out, at the
# revision it pins there, so the data image builds what CI tests.
#
#     deploy/clone-data-deps.sh <workflow> <dir>
set -eu
workflow=$1
dir=$2
awk '/^  data:/,0' "$workflow" \
    | grep -o 'repository: magmalake/[^,]*, ref: [0-9a-f]*, path: [^}]*' \
    | sed 's/repository: magmalake\///; s/, ref: / /; s/, path: / /' \
    | while read -r repo ref path; do
        git clone -q "https://github.com/magmalake/$repo" "$dir/$path"
        git -C "$dir/$path" checkout -q "$ref"
    done
