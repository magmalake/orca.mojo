#!/bin/sh
# orca on a kind cluster (deploy/): builds the image, loads it, applies the
# manifests, and with the scheduler's clock fixed just after a fire,
# registers the toy pipeline from the image. The scheduler fires it for
# 2024-01 and runs it in its pod, and orca ui, through its Service, shows the
# run succeeded with its driver log. Then the scheduler's pod is deleted: its
# replacement finds the run in Postgres and fires nothing again.
#
# KEEP_CLUSTER=1 leaves a cluster this made; KIND_CLUSTER names one to use.
# Needs docker, kind, kubectl, curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
cluster=${KIND_CLUSTER:-orca}
export KUBECONFIG="$PWD/build/kind-$cluster.yaml"
fwd=
made=
cleanup() {
    [ -z "$fwd" ] || kill "$fwd" 2> /dev/null || true
    if [ -n "$made" ] && [ "${KEEP_CLUSTER:-0}" != 1 ]; then
        kind delete cluster --name "$cluster"
    fi
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    kubectl -n orca get pods -o wide || true
    kubectl -n orca logs deploy/orca-scheduler -c scheduler --tail=40 || true
    exit 1
}

docker build -f deploy/Dockerfile -t orca:dev .
if ! kind get clusters 2> /dev/null | grep -qx "$cluster"; then
    kind create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --wait 120s
    made=1
else
    kind get kubeconfig --name "$cluster" > "$KUBECONFIG"
fi
kind load docker-image orca:dev --name "$cluster"

kubectl delete namespace orca orca-runs --ignore-not-found --wait
kubectl apply -f deploy/k8s/orca.yaml -f deploy/k8s/runs.yaml
kubectl -n orca rollout status statefulset/postgres --timeout=180s || fail "postgres"
# The clock just after the toy's fire for January (03:00 UTC on 2 February).
kubectl -n orca set env deployment/orca-scheduler -c scheduler ORCA_NOW=2024-02-02T03:00Z
kubectl -n orca rollout status deployment/orca-scheduler --timeout=180s || fail "the scheduler"
kubectl -n orca exec deploy/orca-scheduler -c scheduler -- \
    env ORCA_NOW=2024-02-01T00:00Z orca register /opt/orca/pipelines/toy || fail "register"

port=$((26000 + $$ % 2000))
kubectl -n orca port-forward svc/orca-ui "$port:8270" > build/kind-forward.log 2>&1 &
fwd=$!
base="http://127.0.0.1:$port"
json() { curl -fsS -H 'Accept: application/json' "$base$1"; }
i=0
until [ "$(json '/runs?pipeline=toy' 2> /dev/null | jq -r '[.runs[] | select(.status == "succeeded")] | length')" = 1 ]; do
    i=$((i + 1)); [ "$i" -lt 180 ] || fail "no succeeded toy run on the UI"; sleep 1
done
run=$(json '/runs?pipeline=toy' | jq -r '.runs[0].id')
[ "$(json "/runs/$run" | jq -r '.run | "\(.trigger) \(.partitions)"')" = "schedule 2024-01" ] \
    || fail "run $run: $(json "/runs/$run")"
aid=$(json "/runs/$run" | jq -r '.steps[0].attempt_id')
json "/runs/$run/attempts/$aid" | jq -e '.text | contains("attempt 1 succeeded")' > /dev/null \
    || fail "the attempt's log: $(json "/runs/$run/attempts/$aid")"
echo "ok   the toy, fired by the scheduler in its pod, succeeded, and orca ui shows it"

# A new scheduler pod: everything it knows is in Postgres.
kill "$fwd"; fwd=
kubectl -n orca delete pod -l app=orca-scheduler --wait
kubectl -n orca rollout status deployment/orca-scheduler --timeout=180s || fail "the new scheduler"
sleep 5
n=$(kubectl -n orca exec statefulset/postgres -- psql -U orca -d orca -Atc "SELECT count(*) FROM orca.runs")
[ "$n" = 1 ] || fail "$n runs after the scheduler was replaced"
echo "ok   kind"
