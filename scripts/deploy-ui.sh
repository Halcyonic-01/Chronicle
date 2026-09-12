#!/usr/bin/env bash
set -euo pipefail

kubectl apply -f deploy/chronicle/namespace.yaml

ui_manifest="$(mktemp)"
trap 'rm -f "$ui_manifest"' EXIT

kubectl create configmap chronicle-ui \
  --namespace chronicle \
  --from-file=internal/web/static/ \
  --dry-run=client \
  --output yaml > "$ui_manifest"
kubectl apply -f "$ui_manifest"
kubectl apply -k deploy/chronicle
kubectl rollout restart deployment/chronicle --namespace chronicle
kubectl rollout status deployment/chronicle --namespace chronicle --timeout=180s

echo "UI deployed without rebuilding the Chronicle image."
