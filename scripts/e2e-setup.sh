#!/usr/bin/env bash
# Prepares the disposable kind cluster for the RCA-to-remediation campaign:
# Keycloak with the test realm, the victim app, and Chronicle with live,
# approval-gated healing. Run scripts/setup.sh first. See deploy/e2e/README.md.
set -euo pipefail

CONTEXT="${E2E_CONTEXT:-kind-chronicle}"
if [[ "$CONTEXT" != "kind-chronicle" ]]; then
  echo "refusing context $CONTEXT: this enables live healing, kind-chronicle only" >&2
  exit 1
fi
k() { kubectl --context "$CONTEXT" "$@"; }
k get nodes >/dev/null

echo "== Keycloak"
k create namespace keycloak --dry-run=client -o yaml | k apply -f -
if ! k -n keycloak get secret keycloak-admin >/dev/null 2>&1; then
  # Random, never printed: the realm is imported, so nobody needs to log in as admin.
  k -n keycloak create secret generic keycloak-admin \
    --from-literal=username=admin --from-literal=password="$(openssl rand -hex 16)"
fi
k apply -k deploy/e2e/keycloak
# A changed realm file is only read at start-up.
k -n keycloak rollout restart deployment/keycloak
k -n keycloak rollout status deployment/keycloak --timeout=300s

echo "== CoreDNS: keycloak.localhost -> the Keycloak Service"
corefile="$(k -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}')"
if ! grep -q "keycloak.localhost" <<<"$corefile"; then
  patched="$(awk '{print} /^\.:53 \{/{print "    rewrite name keycloak.localhost keycloak.keycloak.svc.cluster.local"}' <<<"$corefile")"
  k -n kube-system create configmap coredns --from-literal=Corefile="$patched" --dry-run=client -o yaml | k apply -f -
  k -n kube-system rollout restart deployment/coredns
  k -n kube-system rollout status deployment/coredns --timeout=120s
fi

echo "== Victim application"
docker build -q -t chronicle-victim:latest -f victim/Dockerfile .
kind load docker-image chronicle-victim:latest --name chronicle
k apply -f deploy/victim/

echo "== Chronicle (live healing, OIDC approval)"
docker build -q -t chronicle:latest -f Dockerfile .
kind load docker-image chronicle:latest --name chronicle
k apply -f deploy/chronicle/namespace.yaml
k -n chronicle create configmap chronicle-migrations --from-file=migrations/ --dry-run=client -o yaml | k apply -f -
k -n chronicle create configmap chronicle-ui --from-file=internal/web/static/ --dry-run=client -o yaml | k apply -f -
k apply -k deploy/e2e/chronicle
k -n chronicle rollout restart deployment/chronicle
k -n chronicle rollout status deployment/chronicle --timeout=300s
k -n default wait --for=condition=available deployment --all --timeout=300s

echo "ready. Port-forwards for the campaign:"
echo "  kubectl --context $CONTEXT -n chronicle port-forward svc/chronicle 8181:8181"
echo "  kubectl --context $CONTEXT -n keycloak port-forward svc/keycloak 8180:8180"
