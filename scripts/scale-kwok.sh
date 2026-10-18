#!/usr/bin/env bash
# Fills a DISPOSABLE kind cluster that runs KWOK with simulated nodes and
# workloads, to measure how Chronicle behaves at size. Nothing real runs: KWOK
# marks the pods ready. Usage:
#
#   KUBE_CONTEXT=kind-kyverno-test-p1 scripts/scale-kwok.sh NODES WORKLOADS REPLICAS [NAMESPACES]
#   KUBE_CONTEXT=kind-kyverno-test-p1 scripts/scale-kwok.sh clean
#
# WORKLOADS is per namespace. Pods = WORKLOADS x REPLICAS x NAMESPACES.
set -euo pipefail
cd "$(dirname "$0")/.."
case "${KUBE_CONTEXT:-}" in
  kind-kyverno-test*) ;;
  *) echo "refusing KUBE_CONTEXT='${KUBE_CONTEXT:-}': only a disposable kind-kyverno-test* cluster may be filled with simulated load" >&2; exit 2 ;;
esac
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
k -n kube-system get deploy kwok-controller >/dev/null

if [ "${1:-}" = clean ]; then
  k delete deploy,svc --all-namespaces -l chronicle.io/scale=kwok --wait=false >/dev/null || true
  k delete node -l type=kwok --wait=false >/dev/null || true
  for i in $(k get ns -l chronicle.io/scale=kwok -o name); do k delete "$i" --wait=false >/dev/null; done
  exit 0
fi

NODES="${1:?nodes}" WORKLOADS="${2:?workloads per namespace}" REPLICAS="${3:?replicas}" NAMESPACES="${4:-10}"
python3 - "$NODES" "$WORKLOADS" "$REPLICAS" "$NAMESPACES" <<'PY' | k apply --server-side --force-conflicts -f - >/dev/null
import sys
nodes, workloads, replicas, namespaces = map(int, sys.argv[1:5])
out = []
for n in range(nodes):
    out.append(f"""apiVersion: v1
kind: Node
metadata:
  name: kwok-node-{n}
  annotations: {{kwok.x-k8s.io/node: fake, node.alpha.kubernetes.io/ttl: "0"}}
  labels: {{type: kwok, kubernetes.io/hostname: kwok-node-{n}, kubernetes.io/os: linux, kubernetes.io/arch: amd64}}
spec:
  taints: [{{key: kwok.x-k8s.io/node, value: fake, effect: NoSchedule}}]
status:
  allocatable: {{cpu: "32", memory: 256Gi, pods: "110"}}
  capacity: {{cpu: "32", memory: 256Gi, pods: "110"}}
  nodeInfo: {{architecture: amd64, kubeletVersion: fake, operatingSystem: linux}}
  phase: Running""")
for ns in range(namespaces):
    out.append(f"apiVersion: v1\nkind: Namespace\nmetadata: {{name: sim-{ns}, labels: {{chronicle.io/scale: kwok}}}}")
    for w in range(workloads):
        name = f"svc-{w}"
        # each workload calls the next one: a chain, so the graph has real depth
        peer = f"svc-{(w + 1) % workloads}"
        out.append(f"""apiVersion: apps/v1
kind: Deployment
metadata: {{name: {name}, namespace: sim-{ns}, labels: {{chronicle.io/scale: kwok}}}}
spec:
  replicas: {replicas}
  selector: {{matchLabels: {{app: {name}}}}}
  template:
    metadata: {{labels: {{app: {name}}}}}
    spec:
      tolerations: [{{key: kwok.x-k8s.io/node, operator: Exists}}]
      nodeSelector: {{type: kwok}}
      containers:
        - name: app
          image: fake:1
          env: [{{name: UPSTREAM_URL, value: "http://{peer}:80"}}]
---
apiVersion: v1
kind: Service
metadata: {{name: {name}, namespace: sim-{ns}, labels: {{chronicle.io/scale: kwok}}}}
spec:
  selector: {{app: {name}}}
  ports: [{{port: 80}}]""")
print("\n---\n".join(out))
PY
echo "applied; waiting for pods"
until [ "$(k get pods -A -l 'app' --field-selector=status.phase=Running --no-headers 2>/dev/null | grep -c sim-)" -ge "$(( WORKLOADS * REPLICAS * NAMESPACES * 95 / 100 ))" ]; do sleep 5; done
echo "running pods: $(k get pods -A --no-headers | grep -c sim-)"
