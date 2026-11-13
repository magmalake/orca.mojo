#!/bin/sh
# Since the rename to morca (#384), nothing reads ORCA_* variables or
# orca.toml. What may still name them: the changelog's history, the
# one-time rename of a server's old keys (deploy/hetzner: morca-stack's
# migrate_orca, lib.sh's refusal of old keys, and their test), and the
# refusal of a leftover orca.toml (morca.config's refuse_orca_toml).
set -eu
cd "$(dirname "$0")/.."
found=$(git grep -nE '(^|[^M])ORCA_[A-Za-z]|(^|[^m])orca(\.<?[a-z<>]*)?\.toml' -- . \
    ':!CHANGELOG.md' ':!tests/no-orca.sh' ':!tests/hetzner.sh' \
    ':!deploy/hetzner/server/morca-stack' ':!deploy/hetzner/lib.sh' ':!src/morca/_internal/config.mojo' || true)
if [ -n "$found" ]; then
    echo "$found"
    echo "FAIL: ORCA_* or orca.toml outside the rename's own checks (#384)"
    exit 1
fi
echo "ok   no-orca"
