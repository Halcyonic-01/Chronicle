#!/usr/bin/env bash
set -euo pipefail

if [[ "${CHRONICLE_RUN_KIND_E2E:-false}" != "true" ]]; then
  cat <<'MSG'
Phase 5 kind E2E is destructive: it creates and deletes one labeled test pod and
asks the running Chronicle API to execute a restart action. To run it explicitly:

  CHRONICLE_RUN_KIND_E2E=true make test-phase5-kind

The cluster must already be running, live healing must be enabled, the kill switch
must be off, the 30-day observation gate must have elapsed, and the approval token
must be exported as HEAL_APPROVAL_TOKEN.
MSG
  exit 0
fi

: "${HEAL_APPROVAL_TOKEN:?export HEAL_APPROVAL_TOKEN before running the E2E test}"
API_URL="${CHRONICLE_API_URL:-http://localhost:8181}"
NAMESPACE="${PHASE5_E2E_NAMESPACE:-default}"
POD="chronicle-phase5-e2e"

cleanup() { kubectl delete pod "$POD" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true; }
trap cleanup EXIT

kubectl get deployment chronicle -n chronicle >/dev/null
kubectl get serviceaccount chronicle -n chronicle >/dev/null
kubectl auth can-i delete pods --as=system:serviceaccount:chronicle:chronicle >/dev/null

kubectl run "$POD" -n "$NAMESPACE" --image=redis:7-alpine --restart=Never \
  --labels=chronicle.phase5=e2e >/dev/null
kubectl wait --for=jsonpath='{.status.phase}'=Running "pod/$POD" -n "$NAMESPACE" --timeout=90s

EVENT_ID="phase5-e2e-${POD}-$(date +%s)"
psql "${POSTGRES_URL:?export POSTGRES_URL pointing at Chronicle PostgreSQL}" -v ON_ERROR_STOP=1 \
  --set=event_id="$EVENT_ID" --set=namespace="$NAMESPACE" --set=target="$POD" <<'SQL'
INSERT INTO heal_actions
  (id, incident_id, rule, cause_type, action_type, namespace, target,
   confidence, reasoning, status, result, approval, payload, dry_run, created_at)
VALUES
  (:'event_id', :'event_id', 'restart-deadlocked-pod', 'became_unready', 'restart_pod',
   :'namespace', :'target', 1.0, '[]'::jsonb, 'would_run',
   'E2E approval test', 'pending', '{}'::jsonb, true, now());
SQL

curl --fail-with-body -sS -X POST \
  -H "Authorization: Bearer ${HEAL_APPROVAL_TOKEN}" \
  -H 'Content-Type: application/json' \
  "${API_URL}/api/heal/actions/${EVENT_ID}/approve" \
  -d '{"by":"phase5-kind-e2e","reason":"controlled pod restart test"}'

kubectl wait --for=delete "pod/$POD" -n "$NAMESPACE" --timeout=90s
echo "Phase 5 kind E2E passed: approved pod restart was executed and the pod was deleted."
