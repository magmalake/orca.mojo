#!/bin/sh
# Stands in for `aws secretsmanager get-secret-value ... --secret-id ID` in
# tests/secrets.sh: prints a value made from the ID, or fails for orca/missing.
[ "$1" = orca/missing ] && { echo "ResourceNotFoundException" >&2; exit 254; }
echo "s3cr3t-$(echo "$1" | tr / -)-value"
