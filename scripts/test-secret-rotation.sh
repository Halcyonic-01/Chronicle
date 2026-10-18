#!/usr/bin/env bash
# Rehearses a secret rotation on the kind cluster: External Secrets Operator (fake
# provider) fills chronicle-config, stakater/reloader rolls Chronicle when it
# changes, and the new value is read back from the RUNNING process. Needs ESO and
# Reloader installed (see deploy/secrets/README.md). Local kind only.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/kube-context.sh

NS=chronicle
kubectl get crd externalsecrets.external-secrets.io >/dev/null
kubectl -n reloader get deploy >/dev/null

cleanup() {
  kubectl delete externalsecret chronicle-config -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete clustersecretstore chronicle --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete secret chronicle-config --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The environment of the running Chronicle process: the image has no shell, so a
# throwaway container shares its process namespace and reads /proc/1/environ.
running_value() {
  local pod name
  # The newest ready pod: during a roll the older ones are on their way out.
  pod="$(kubectl -n "$NS" get pods -l app=chronicle --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | tail -1)"
  name="peek-$RANDOM"
  kubectl -n "$NS" debug "$pod" -q --image=busybox:1.36 --target=chronicle -c "$name" --profile=general -- \
    sh -c "tr '\0' '\n' < /proc/1/environ | grep '^SLACK_WEBHOOK_URL=' || true" >/dev/null 2>&1 || true
  for _ in 1 2 3 4 5 6; do
    out="$(kubectl -n "$NS" logs "$pod" -c "$name" 2>/dev/null | head -1)"
    [ -n "$out" ] && { echo "$out"; return 0; }
    sleep 2
  done
}
secret_value() { kubectl -n "$NS" get secret chronicle-config -o jsonpath='{.data.SLACK_WEBHOOK_URL}' 2>/dev/null | base64 -d; }
rolled() { kubectl -n "$NS" rollout status deployment/chronicle --timeout=300s >/dev/null; }

echo "== deliver: ESO fills chronicle-config from the fake store"
kubectl apply -k deploy/secrets/fake >/dev/null
kubectl -n "$NS" wait externalsecret/chronicle-config --for=condition=Ready --timeout=120s
echo "   secret holds: $(secret_value)"
[ "$(secret_value)" = "https://hooks.invalid/test-1" ]

echo "== the running process has to pick it up: restart once so it starts with the Secret"
kubectl -n "$NS" rollout restart deployment/chronicle >/dev/null; rolled
echo "   process sees: $(running_value)"

echo "== rotate at the source"
gen0="$(kubectl -n "$NS" get deploy chronicle -o jsonpath='{.metadata.generation}')"
t0=$(date +%s)
new="rot-$t0"   # a fresh value each time: Reloader skips a Secret whose hash it already reloaded from
kubectl get clustersecretstore chronicle -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for e in d["spec"]["provider"]["fake"]["data"]:
    e["value"]=e["value"].replace("test-1",sys.argv[1])
print(json.dumps(d))' "$new" | kubectl apply -f - >/dev/null
kubectl -n "$NS" annotate externalsecret chronicle-config "force-sync=$t0" --overwrite >/dev/null
until [ "$(secret_value)" = "https://hooks.invalid/$new" ]; do sleep 2; [ $(( $(date +%s) - t0 )) -gt 180 ] && { echo "the Secret never changed"; exit 1; }; done
t1=$(date +%s); echo "   secret updated after $(( t1 - t0 ))s"
# Reloader notices the Secret change and rolls the Deployment (a new generation).
until [ "$(kubectl -n "$NS" get deploy chronicle -o jsonpath='{.metadata.generation}')" -gt "$gen0" ]; do
  sleep 2
  [ $(( $(date +%s) - t1 )) -gt 120 ] && { echo "Reloader did not roll Chronicle within 120s"; exit 1; }
done
rolled; t2=$(date +%s)
echo "   Chronicle rolled and ready $(( t2 - t1 ))s after the Secret changed ($(( t2 - t0 ))s after the rotation)"
seen="$(running_value)"; echo "   process sees: $seen"
[ "$seen" = "SLACK_WEBHOOK_URL=https://hooks.invalid/$new" ]
echo "rotation reached the running process"
