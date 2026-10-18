#!/usr/bin/env bash
# Runs a local Chronicle collector against a DISPOSABLE KWOK-filled cluster and a
# scratch PostgreSQL, and measures it: start-up, memory, graph sync, ingestion lag
# and how long an analysis takes on the resulting graph. See scale-kwok.sh.
#
#   KUBE_CONTEXT=kind-kyverno-test-p1 scripts/scale-measure.sh [ANALYSES]
#
# Needs a scratch PostgreSQL on localhost:55432 (see internal/heal/README.md).
set -euo pipefail
cd "$(dirname "$0")/.."
case "${KUBE_CONTEXT:-}" in
  kind-kyverno-test*) ;;
  *) echo "refusing KUBE_CONTEXT='${KUBE_CONTEXT:-}'" >&2; exit 2 ;;
esac
ANALYSES="${1:-100}"
PSQL="psql -h /tmp -p 55432 -U postgres -v ON_ERROR_STOP=1 -q"
DB=chronicle_scale API=18181 METRICS=19102
tmp="$(mktemp -d)"; collector_pid=""
cleanup() { [ -n "$collector_pid" ] && kill "$collector_pid" 2>/dev/null; true; }
trap cleanup EXIT

kubectl --context "$KUBE_CONTEXT" config view --minify --flatten --context "$KUBE_CONTEXT" > "$tmp/kubeconfig" 2>/dev/null || \
  kubectl config view --minify --flatten --context "$KUBE_CONTEXT" > "$tmp/kubeconfig"
kubectl --context "$KUBE_CONTEXT" create namespace chronicle --dry-run=client -o yaml | kubectl --context "$KUBE_CONTEXT" apply -f - >/dev/null
$PSQL -d postgres -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB"
for f in migrations/*.sql; do $PSQL -d $DB -f "$f" >/dev/null; done
go build -o "$tmp/collector" ./cmd/collector

pods="$(kubectl --context "$KUBE_CONTEXT" get pods -A --no-headers | grep -c sim- || true)"
nodes="$(kubectl --context "$KUBE_CONTEXT" get nodes -l type=kwok --no-headers | wc -l | tr -d ' ')"
echo "cluster: $nodes simulated nodes, $pods simulated pods"

started=$(date +%s)
KUBECONFIG="$tmp/kubeconfig" POSTGRES_URL="postgres://postgres@localhost:55432/$DB?sslmode=disable" \
  PROMETHEUS_URL=http://127.0.0.1:1 LOKI_URL=http://127.0.0.1:1 API_ADDR=":$API" METRICS_ADDR=":$METRICS" \
  CHRONICLE_NAMESPACE=chronicle LEADER_ELECTION_NAMESPACE=chronicle POD_NAME=scale-test \
  "$tmp/collector" > "$tmp/collector.log" 2>&1 &
collector_pid=$!
m() { { curl -s localhost:$METRICS/metrics | grep -E "^$1" || true; } | awk '{s+=$2} END{printf "%s", s+0}'; }
rss() { ps -o rss= -p "$collector_pid" | awk '{printf "%.0f", $1/1024}'; }

until [ "$(m chronicle_graph_sync_duration_seconds_count)" -ge 1 ] 2>/dev/null; do
  sleep 1; kill -0 "$collector_pid" 2>/dev/null || { echo "collector died"; tail -20 "$tmp/collector.log"; exit 1; }
done
echo "first graph sync after $(( $(date +%s) - started ))s: $(m chronicle_graph_sync_duration_seconds_sum)s, $(m chronicle_graph_edges) edges"

# Initial listing emits nothing (only changes are events), so roll every
# deployment: each change brings a config event and the pod churn behind it.
churn_started=$(date +%s)
for round in $(seq 1 "${CHURN_ROUNDS:-2}"); do
  for ns in $(kubectl --context "$KUBE_CONTEXT" get ns -l chronicle.io/scale=kwok -o name | sed 's#namespace/##'); do
    kubectl --context "$KUBE_CONTEXT" -n "$ns" set env deployment --all "CHURN=$(date +%s)-$round" >/dev/null
  done
  sleep 20
done
prev=-1
while [ "$(m chronicle_events_stored_total)" != "$prev" ]; do prev="$(m chronicle_events_stored_total)"; sleep 10; done
echo "churn: $(m chronicle_events_stored_total) events stored in $(( $(date +%s) - churn_started ))s; collector RSS $(rss) MiB"

# Symptoms on random simulated services, then analyse each over HTTP.
$PSQL -d $DB -c "
INSERT INTO events (id, occurred_at, ingested_at, source, namespace, entity_kind, entity_name, type, severity, title, payload, trace_id, correlation_key)
SELECT 'scale-symptom-'||g, now(), now(), 'prometheus', 'sim-'||(g % 10), 'Service', 'svc-'||(g % 50), 'error_spike', 'warning',
       'high_error_rate on svc-'||(g % 50)||' ('||g||')', '{}', '', 'scale-'||g
FROM generate_series(1, $ANALYSES) g"
lat="$tmp/latencies"; : > "$lat"
for g in $(seq 1 "$ANALYSES"); do
  curl -s -o /dev/null -w "%{time_total}\n" -X POST "localhost:$API/api/analyze?event_id=scale-symptom-$g" >> "$lat"
done
python3 - "$lat" <<'PY'
import sys
v = sorted(float(x) for x in open(sys.argv[1]).read().split())
q = lambda p: v[min(len(v)-1, int(p*len(v)))]
print(f"analyses: {len(v)}  p50 {q(.5):.3f}s  p95 {q(.95):.3f}s  p99 {q(.99):.3f}s  max {v[-1]:.3f}s")
PY
echo "collector RSS now $(rss) MiB; goroutines $(m go_goroutines); heap $(( $(m go_memstats_heap_inuse_bytes) / 1048576 )) MiB; write errors $(m chronicle_event_write_errors_total)"
echo "event lag (k8s), events stored within: $({ curl -s localhost:$METRICS/metrics | grep -E '^chronicle_event_lag_seconds_bucket\{source="k8s"' || true; } | sed -E 's/.*le="([^"]+)"\} ([0-9]+).*/\1s:\2/' | tr '\n' ' ')"
