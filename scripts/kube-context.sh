# Source this from any script that changes a cluster. It pins every kubectl
# call to one context and refuses anything that is not a local kind cluster,
# so a kubeconfig that also holds a real cluster is never touched by accident.
#
#   KUBE_CONTEXT=kind-other ./scripts/deploy-ui.sh   # another kind cluster
#
# Deliberately no override for non-kind contexts.
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-chronicle}"
case "$KUBE_CONTEXT" in
  kind-*) ;;
  *) echo "refusing context '$KUBE_CONTEXT': these scripts act only on a local kind cluster (kind-*)" >&2; exit 2 ;;
esac
command kubectl --context "$KUBE_CONTEXT" version --request-timeout=5s >/dev/null 2>&1 ||
  { echo "context '$KUBE_CONTEXT' is not reachable; is the kind cluster running?" >&2; exit 2; }
kubectl() { command kubectl --context "$KUBE_CONTEXT" "$@"; }
