#!/usr/bin/env bash
# Dependency drill, local kind only. Takes one dependency of Chronicle away for a
# while, keeps a steady stream of recordable changes flowing and the API being
# asked questions, then checks what survived. Usage:
#
#   scripts/drill-dependency.sh postgres|kafka|redis|prometheus|loki [seconds]
#
# Postgres and Prometheus are partitioned (a NetworkPolicy plus, for Postgres,
# dropping the open connections): the database runs without persistence on kind,
# so stopping it would erase the record. The others are scaled to zero and back.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/kube-context.sh

DEP="${1:?postgres|kafka|redis|prometheus|loki}"; OUT="${2:-60}"
NS=default NAME=drill-target
api() { kubectl get --raw "/api/v1/namespaces/chronicle/services/chronicle:8181/proxy/$1" --request-timeout=40s; }
sql() { kubectl -n chronicle exec postgres-postgresql-0 -- env PGPASSWORD=postgres psql -U postgres -At -F '|' -c "$1"; }
leader() { kubectl -n chronicle get lease chronicle-leader -o jsonpath='{.spec.holderIdentity}'; }
metric() { kubectl get --raw "/api/v1/namespaces/chronicle/pods/$(leader):9102/proxy/metrics" 2>/dev/null | grep -E "^$1" | awk '{s+=$2} END{print s+0}'; }
restarts() { kubectl -n chronicle get pods -l app=chronicle -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{" "}{end}'; }

# Policies only add, so the chart's own allow rule for a pod must be set aside
# for the length of the drill and put back afterwards.
SAVED=/tmp/drill-saved-netpol.json
set_aside() { # ns name
  kubectl -n "$1" get netpol "$2" -o json > "$SAVED" 2>/dev/null || { rm -f "$SAVED"; return 0; }
  kubectl -n "$1" delete netpol "$2" >/dev/null
  SAVED_NS="$1"
}
put_back() {
  [ -s "$SAVED" ] || return 0
  python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); m=d["metadata"]; [m.pop(k,None) for k in ("uid","resourceVersion","creationTimestamp","generation","managedFields")]; print(json.dumps(d))' "$SAVED" |
    kubectl -n "$SAVED_NS" apply -f - >/dev/null && rm -f "$SAVED"
}
partition() { # ns label-selector
  kubectl -n "$1" apply -f - >/dev/null <<YAML
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: drill-partition, namespace: $1}
spec:
  podSelector: {matchLabels: {$2}}
  policyTypes: [Ingress]
YAML
}
down() {
  case "$DEP" in
    postgres) set_aside chronicle postgres-postgresql
              partition chronicle 'app.kubernetes.io/name: postgresql'
              kubectl -n chronicle exec postgres-postgresql-0 -- env PGPASSWORD=postgres psql -U postgres -At -c \
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type='client backend'" >/dev/null ;;
    prometheus) partition monitoring 'app.kubernetes.io/name: prometheus' ;;
    kafka) kubectl -n chronicle scale sts/kafka-broker sts/kafka-controller --replicas=0 >/dev/null ;;
    redis) kubectl -n chronicle scale deploy/redis --replicas=0 >/dev/null ;;
    loki) kubectl -n monitoring scale sts/loki --replicas=0 >/dev/null ;;
    *) echo "unknown dependency $DEP" >&2; exit 2 ;;
  esac
}
up() {
  case "$DEP" in
    postgres) kubectl -n chronicle delete netpol drill-partition --ignore-not-found >/dev/null; put_back ;;
    prometheus) kubectl -n monitoring delete netpol drill-partition --ignore-not-found >/dev/null ;;
    kafka) kubectl -n chronicle scale sts/kafka-controller sts/kafka-broker --replicas=1 >/dev/null ;;
    redis) kubectl -n chronicle scale deploy/redis --replicas=1 >/dev/null ;;
    loki) kubectl -n monitoring scale sts/loki --replicas=1 >/dev/null ;;
  esac
}
trap up EXIT

kubectl -n "$NS" delete deployment "$NAME" --ignore-not-found >/dev/null
kubectl -n "$NS" create deployment "$NAME" --image=registry.k8s.io/pause:3.10 --replicas=0 >/dev/null
sleep 8
start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
r0="$(restarts)"; werr0="$(metric chronicle_event_write_errors_total)"

( i=0; end=$(( $(date +%s) + OUT + 150 ))
  while [ "$(date +%s)" -lt "$end" ]; do i=$((i+1)); kubectl -n "$NS" set env deployment/"$NAME" "DRILL=$i" >/dev/null 2>&1; sleep 3; done
  echo "$i" > /tmp/drill-changes ) &
gen=$!
sleep 15
echo "== $DEP down for ${OUT}s"
down
t0=$(date +%s)
probe() { # in this shell, so the context-pinned kubectl above is the one used
  local started; started=$(date +%s)
  if api "$1" >/dev/null 2>&1; then echo "ok $(( $(date +%s) - started ))s"; else echo "FAIL $(( $(date +%s) - started ))s"; fi
}
while [ $(( $(date +%s) - t0 )) -lt "$OUT" ]; do
  echo "  +$(( $(date +%s) - t0 ))s  rules: $(probe api/heal/rules)   events: $(probe 'api/events?limit=1')"
  sleep 10
done
echo "== $DEP restored"
up
t1=$(date +%s)
until api api/heal/actions >/dev/null 2>&1 && api 'api/events?limit=1' >/dev/null 2>&1; do sleep 2; [ $(( $(date +%s) - t1 )) -gt 300 ] && { echo "API did not recover in 5 min"; break; }; done
echo "  API answering again $(( $(date +%s) - t1 ))s after the dependency came back"
wait "$gen"; sleep 45
changes="$(cat /tmp/drill-changes)"
echo "== after: chronicle restarts $r0 -> $(restarts), event write errors +$(( $(metric chronicle_event_write_errors_total) - werr0 ))"
sql "SELECT occurred_at, coalesce(payload->>'from_template_hash',''), coalesce(payload->>'to_template_hash',''), coalesce(payload->>'missed','') FROM events WHERE entity_name='$NAME' AND type='config_change' AND occurred_at >= '$start' ORDER BY occurred_at" | python3 -c '
import sys
rows=[l.split("|") for l in sys.stdin.read().strip().splitlines()]
changes=int(sys.argv[1])
live=[r for r in rows if r[3]!="true"]; catch=[r for r in rows if r[3]=="true"]
edges={}
for r in rows:
    if r[1] and r[2]: edges.setdefault(r[1],set()).add(r[2])
seen,todo=set(),[live[0][1]] if live else []
while todo:
    h=todo.pop()
    if h not in seen: seen.add(h); todo+=list(edges.get(h,()))
print(f"changes made: {changes}  recorded individually: {len(live)}  catch-up summaries: {len(catch)}  not individually: {changes-len(live)}")
print("every state still reachable through recorded moves:", bool(live) and live[-1][2] in seen)' "$changes"
kubectl -n "$NS" delete deployment "$NAME" --ignore-not-found >/dev/null
