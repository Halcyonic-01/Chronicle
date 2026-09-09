#!/usr/bin/env bash
set -euo pipefail

kubectl apply -f deploy/chronicle/namespace.yaml

migrations_manifest="$(mktemp)"
trap 'rm -f "$migrations_manifest"' EXIT

kubectl create configmap chronicle-migrations \
  --namespace chronicle \
  --from-file=migrations/ \
  --dry-run=client \
  --output yaml > "$migrations_manifest"
kubectl apply -f "$migrations_manifest"
kubectl apply -k deploy/chronicle
kubectl rollout status deployment/chronicle --namespace chronicle --timeout=180s
