#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8181}"
POSTGRES_URL="${POSTGRES_URL:-postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable}"
NAMESPACE="${VICTIM_NAMESPACE:-default}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"

command -v kubectl >/dev/null || { echo "kubectl is required" >&2; exit 1; }
command -v psql >/dev/null || { echo "psql is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }

echo "Creating a deployment event for redis..."
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl set image deployment/redis redis=redis:7-alpine -n "$NAMESPACE" >/dev/null
kubectl rollout status deployment/redis -n "$NAMESPACE" --timeout=90s >/dev/null
kubectl set image deployment/redis redis=redis:7.2-alpine -n "$NAMESPACE" >/dev/null
kubectl rollout status deployment/redis -n "$NAMESPACE" --timeout=90s >/dev/null

echo "Deleting one redis pod to create the symptom..."
kubectl delete pod -n "$NAMESPACE" -l app=redis --wait=false >/dev/null

event_id=""
for _ in $(seq 1 "$WAIT_SECONDS"); do
  event_id="$(psql "$POSTGRES_URL" -Atc "SELECT id FROM events WHERE namespace = '$NAMESPACE' AND type = 'became_unready' AND ingested_at >= '$started_at' ORDER BY ingested_at DESC LIMIT 1" 2>/dev/null || true)"
  if [[ -n "$event_id" ]]; then break; fi
  sleep 1
done

if [[ -z "$event_id" ]]; then
  echo "No became_unready event was observed within ${WAIT_SECONDS}s" >&2
  exit 1
fi

echo "Analyzing event: $event_id"
response="$(curl -fsS -X POST "$API_URL/api/analyze?event_id=$event_id")"
printf '%s\n' "$response"

RESPONSE="$response" python3 - <<'PY'
import json, os, sys
data = json.loads(os.environ["RESPONSE"])
required = ["symptom", "candidates", "confidence", "blast_radius", "evidence"]
missing = [key for key in required if key not in data]
if missing:
    print(f"Missing Phase 4 fields: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)
if data["symptom"].get("type") != "became_unready":
    print("The analyzed symptom was not became_unready", file=sys.stderr)
    sys.exit(1)
print(f"Validated: {len(data['candidates'])} candidate(s), "
      f"{data['blast_radius'].get('affected_services', 0)} affected service(s), "
      f"{len(data['evidence'])} evidence edge(s).")
PY
