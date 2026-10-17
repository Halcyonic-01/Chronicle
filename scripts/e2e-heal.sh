#!/usr/bin/env bash
# Runs the RCA-to-remediation chaos campaign against the disposable cluster
# prepared by scripts/e2e-setup.sh. Needs the port-forwards it prints, and
# Prometheus on :9090 (the kind NodePort mapping). Extra flags pass through,
# e.g. -only redis-deadlocked.
set -euo pipefail
realm=deploy/e2e/keycloak/realm.json
cred() { python3 -c 'import json,sys; u={x["username"]:x["credentials"][0]["value"] for x in json.load(open(sys.argv[1]))["users"]}; print(u[sys.argv[2]])' "$realm" "$1"; }
export E2E_APPROVER_USER=ana E2E_APPROVER_PASSWORD="$(cred ana)"
export E2E_VIEWER_USER=vic E2E_VIEWER_PASSWORD="$(cred vic)"
exec go run ./cmd/chaos run -heal -context kind-chronicle "$@"
