#!/usr/bin/env bash
# Gate for the Lean development under formal/, after `lake build`:
#  1. no `sorry`, `admit`, `native_decide` or user `axiom` in the sources
#     (environment facts live in Morca/Core/Assumptions.lean as hypotheses);
#  2. `#print axioms` for every theorem in Morca/Audit.lean names nothing but
#     the three standard axioms;
#  3. each difftest/<model>.cases, which tests/test_formal_<model>.mojo
#     replays, is the oracle's output.
set -u
cd "$(dirname "$0")/.."
fail=0

if grep -rnE '\b(sorry|admit|native_decide)\b' --include='*.lean' Morca Morca.lean Oracle; then
  echo "FAIL: sorry, admit or native_decide found"; fail=1
fi
if grep -rnE '^\s*((private|protected|noncomputable|unsafe)\s+)*axiom\s' --include='*.lean' Morca Morca.lean Oracle; then
  echo "FAIL: user axiom declared"; fail=1
fi

lake build || { echo "FAIL: lake build"; exit 1; }

out="$(lake env lean Morca/Audit.lean 2>&1)" || { echo "$out"; echo "FAIL: Morca/Audit.lean did not elaborate"; fail=1; }
echo "$out"
if echo "$out" | grep -q 'sorryAx'; then echo "FAIL: sorryAx in an audited theorem"; fail=1; fi
stray=$(echo "$out" | tr '\n' ' ' | grep -o 'depends on axioms: \[[^]]*\]' \
  | sed 's/.*\[//; s/\]//' | tr ',' '\n' | tr -d ' ' | sort -u \
  | grep -v -x -e propext -e Quot.sound -e Classical.choice || true)
if [ -n "$stray" ]; then echo "FAIL: unexpected axioms: $stray"; fail=1; fi
n_thm=$(grep -c '^#print axioms' Morca/Audit.lean)
n_out=$(echo "$out" | grep -c -e 'depends on axioms' -e 'does not depend on any axioms')
if [ "$n_thm" != "$n_out" ]; then echo "FAIL: $n_thm theorems audited, $n_out reported"; fail=1; fi

for model in retry pins migrate fence; do
  if ! lake exe morca-oracle "$model" | diff -u "difftest/$model.cases" - > /dev/null; then
    lake exe morca-oracle "$model" | diff -u "difftest/$model.cases" - | head -40
    echo "FAIL: difftest/$model.cases isn't the oracle's output;"
    echo "      write it again: lake exe morca-oracle $model > difftest/$model.cases"
    fail=1
  fi
done

[ $fail -eq 0 ] && echo "formal-check: OK"
exit $fail
