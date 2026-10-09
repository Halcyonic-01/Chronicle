#!/usr/bin/env bash
# Runs the healing guardrail policies against a REAL Kyverno admission
# controller in a disposable kind cluster, acting as Chronicle's service
# account by impersonation. The CLI tests (make kyverno-test) cannot supply a
# different old object, so what a change ADDS is only verifiable here.
#
# Phase 1 (Audit, as shipped): violations are allowed but recorded.
# Phase 2 (Deny, a temporary copy): violations are rejected, legitimate work is
#   not, and other users are untouched.
#
# Safety: only a kind cluster whose name starts with "kyverno-test" is used,
# because this grants Chronicle's service account broad RBAC to prove the
# policy, not RBAC, is what stops it. Chronicle's real cluster is never touched.
#
#   KYVERNO_TEST_CLUSTER   cluster name (default kyverno-test)
#   KEEP=1                 keep the cluster afterwards
set -uo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${KYVERNO_TEST_CLUSTER:-kyverno-test}"
case "$CLUSTER" in kyverno-test*) ;; *) echo "refusing: cluster name must start with kyverno-test" >&2; exit 2 ;; esac
CTX="kind-$CLUSTER"
K="kubectl --context $CTX"
AS="--as=system:serviceaccount:chronicle:chronicle"
POLICIES=deploy/kyverno/chronicle-heal-guardrails.yaml
CHART_VERSION=3.9.1
FAILED=0
IMG=registry.k8s.io/pause:3.10

for tool in kind kubectl helm python3; do command -v "$tool" >/dev/null || { echo "$tool not found" >&2; exit 2; }; done

# `kind create cluster` makes the new cluster kubectl's current context, so a
# plain kubectl afterwards would act on the disposable cluster. Put it back.
PREVIOUS_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
restore_context() {
	if [ -n "$PREVIOUS_CONTEXT" ] && [ "$PREVIOUS_CONTEXT" != "$CTX" ]; then
		kubectl config use-context "$PREVIOUS_CONTEXT" >/dev/null 2>&1 || true
	fi
}
trap restore_context EXIT

pass() { echo "  ok    $1"; }
fail() { echo "  FAIL  $1" >&2; FAILED=1; }

ensure_cluster() {
	if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
		echo "creating kind cluster $CLUSTER"
		kind create cluster --name "$CLUSTER" --wait 180s >/dev/null 2>&1 || { echo "kind create failed" >&2; exit 1; }
	fi
	if ! $K get ns kyverno >/dev/null 2>&1; then
		echo "installing Kyverno chart $CHART_VERSION"
		helm repo add kyverno https://kyverno.github.io/kyverno/ >/dev/null 2>&1
		helm repo update kyverno >/dev/null 2>&1
		helm --kube-context "$CTX" install kyverno kyverno/kyverno --version "$CHART_VERSION" -n kyverno --create-namespace \
			--set backgroundController.enabled=false --set cleanupController.enabled=false \
			--set admissionController.replicas=1 --set reportsController.replicas=1 \
			--wait --timeout 8m >/dev/null 2>&1 || { echo "kyverno install failed" >&2; exit 1; }
	fi
}

deploy() { # name [namespace] [extra pod-spec json]
	local name="$1" ns="${2:-default}"
	cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: $name, namespace: $ns${3:-}}
spec:
  replicas: 1
  selector: {matchLabels: {app: $name}}
  template:
    metadata: {labels: {app: $name}}
    spec:
${4:-}
      containers: [{name: app, image: $IMG, imagePullPolicy: IfNotPresent}]
---
EOF
}
pod() { printf 'apiVersion: v1\nkind: Pod\nmetadata: {name: %s, namespace: default}\nspec:\n  containers: [{name: app, image: %s, imagePullPolicy: IfNotPresent}]\n---\n' "$1" "$IMG"; }

fixtures() {
	# Scenarios mutate their objects, so every run starts from clean ones. Safe:
	# the cluster is disposable and its name is checked above.
	$K -n default delete deploy,pod --all --wait=true >/dev/null 2>&1
	$K -n staging delete deploy,pod --all --wait=true >/dev/null 2>&1
	$K create namespace chronicle >/dev/null 2>&1; $K create namespace staging >/dev/null 2>&1
	$K -n chronicle create serviceaccount chronicle >/dev/null 2>&1
	$K -n default create serviceaccount other-sa >/dev/null 2>&1
	{
		cat <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: chronicle-broad-test}
rules:
- apiGroups: ["", "apps", "batch"]
  resources: [pods, deployments, replicasets, statefulsets, daemonsets, jobs, cronjobs]
  verbs: [get, list, watch, create, update, patch, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: chronicle-broad-test}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: chronicle-broad-test}
subjects: [{kind: ServiceAccount, name: chronicle, namespace: chronicle}]
---
EOF
		for d in ok host sa priv privinit hostpath replicas \
			dn-ok dn-host dn-sa dn-replicas admin-host; do deploy "d-$d"; done
		deploy d-prehost default "" "      hostNetwork: true"
		deploy d-argo default ", annotations: {argocd.argoproj.io/tracking-id: \"shop:apps/Deployment:default/d-argo\"}"
		deploy d-flux default ", labels: {kustomize.toolkit.fluxcd.io/name: apps}"
		deploy d-dn-argo default ", annotations: {argocd.argoproj.io/tracking-id: \"shop:apps/Deployment:default/d-dn-argo\"}"
		deploy d-admin-argo default ", annotations: {argocd.argoproj.io/tracking-id: \"shop:apps/Deployment:default/d-admin-argo\"}"
		deploy stg-dep staging
		for p in bare bare-admin bare-deny; do pod "$p"; done
	} | $K apply -f - >/dev/null || { echo "fixture apply failed" >&2; exit 1; }
}

patch() { local ns="$1" name="$2" body="$3"; shift 3; $K "$@" -n "$ns" patch deploy "$name" --type=merge -p "$body"; }

# Expectations: "Kind|namespace|name|policy" (policy "none" = no violation).
EXPECT=()
expect() { EXPECT+=("$1"); }

run() { # label, expectation..., -- command
	local label="$1"; shift
	while [ "$1" != "--" ]; do expect $1; shift; done; shift
	if "$@" >/dev/null 2>&1; then pass "allowed: $label"; else fail "audit mode blocked: $label"; fi
}

violations() { # prints "policy|Kind|ns|name" lines from PolicyViolation events
	$K get events -A -o json 2>/dev/null | python3 -c '
import json,sys,re
for e in json.load(sys.stdin)["items"]:
    if e.get("reason")!="PolicyViolation": continue
    o=e["involvedObject"]; m=re.match(r"policy (chronicle-heal-[a-z-]+)/ fail", e.get("message",""))
    if m and o["kind"] in ("Pod","Deployment"): print("|".join([m.group(1),o["kind"],o.get("namespace",""),o["name"]]))
' | sort -u
}

audit_phase() {
	echo "== Phase 1: Audit (as shipped) =="
	$K apply -f "$POLICIES" >/dev/null || { echo "policy apply failed" >&2; exit 1; }
	sleep 20
	$K delete events -A --all >/dev/null 2>&1 # results from an earlier run must not satisfy this one
	local ctl; ctl=$($K -n default get pod -l app=d-ok -o jsonpath='{.items[0].metadata.name}')
	run "delete bare pod"                 "Pod|default|bare|chronicle-heal-delete-controlled-pods" -- $K $AS -n default delete pod bare --wait=false
	run "delete controller-owned pod"     "Pod|default|$ctl|none" -- $K $AS -n default delete pod "$ctl" --wait=false
	run "create a pod"                    "Pod|default|created|chronicle-heal-never-create" -- $K $AS -n default run created --image=$IMG --image-pull-policy=IfNotPresent
	run "change a deployment in staging"  "Deployment|staging|stg-dep|chronicle-heal-namespaces" -- patch staging stg-dep '{"spec":{"minReadySeconds":5}}' $AS
	run "change an Argo CD workload"      "Deployment|default|d-argo|chronicle-heal-no-gitops-writes" -- patch default d-argo '{"spec":{"minReadySeconds":5}}' $AS
	run "change a Flux workload"          "Deployment|default|d-flux|chronicle-heal-no-gitops-writes" -- patch default d-flux '{"spec":{"minReadySeconds":5}}' $AS
	run "add hostNetwork"                 "Deployment|default|d-host|chronicle-heal-no-privilege-widening" -- patch default d-host '{"spec":{"template":{"spec":{"hostNetwork":true}}}}' $AS
	run "change the service account"      "Deployment|default|d-sa|chronicle-heal-no-privilege-widening" -- patch default d-sa '{"spec":{"template":{"spec":{"serviceAccountName":"other-sa"}}}}' $AS
	run "make a container privileged"     "Deployment|default|d-priv|chronicle-heal-no-privilege-widening" -- patch default d-priv "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"app\",\"image\":\"$IMG\",\"imagePullPolicy\":\"IfNotPresent\",\"securityContext\":{\"privileged\":true}}]}}}}" $AS
	run "make an init container privileged" "Deployment|default|d-privinit|chronicle-heal-no-privilege-widening" -- patch default d-privinit "{\"spec\":{\"template\":{\"spec\":{\"initContainers\":[{\"name\":\"setup\",\"image\":\"$IMG\",\"imagePullPolicy\":\"IfNotPresent\",\"securityContext\":{\"privileged\":true}}]}}}}" $AS
	run "mount a host path"               "Deployment|default|d-hostpath|chronicle-heal-no-privilege-widening" -- patch default d-hostpath '{"spec":{"template":{"spec":{"volumes":[{"name":"host","hostPath":{"path":"/tmp"}}]}}}}' $AS
	run "scale past the ceiling (21)"     "Deployment|default|d-replicas|chronicle-heal-replica-ceiling" -- patch default d-replicas '{"spec":{"replicas":21}}' $AS
	run "restart an already-hostNetwork workload (not widening)" "Deployment|default|d-prehost|none" -- patch default d-prehost '{"spec":{"minReadySeconds":5}}' $AS
	run "legitimate update + restore"     "Deployment|default|d-ok|none" -- patch default d-ok '{"spec":{"minReadySeconds":3,"replicas":2}}' $AS
	run "ADMIN adds hostNetwork"          "Deployment|default|d-admin-host|none" -- patch default d-admin-host '{"spec":{"template":{"spec":{"hostNetwork":true}}}}'
	run "ADMIN changes an Argo workload"  "Deployment|default|d-admin-argo|none" -- patch default d-admin-argo '{"spec":{"minReadySeconds":5}}'
	run "ADMIN deletes a bare pod"        "Pod|default|bare-admin|none" -- $K -n default delete pod bare-admin --wait=false

	echo "waiting for Kyverno to record the results"
	local want=0 deadline=$((SECONDS + 120)) got
	for e in "${EXPECT[@]}"; do [ "${e##*|}" != none ] && want=$((want + 1)); done
	while [ $SECONDS -lt $deadline ]; do
		got=$(violations); seen=0
		for e in "${EXPECT[@]}"; do IFS='|' read -r kind ns name pol <<<"$e"; [ "$pol" != none ] && echo "$got" | grep -qx "$pol|$kind|$ns|$name" && seen=$((seen + 1)); done
		[ "$seen" -ge "$want" ] && break
		sleep 5
	done
	sleep 10 # a late violation on a "none" object would show up now
	got=$(violations)
	for e in "${EXPECT[@]}"; do
		IFS='|' read -r kind ns name pol <<<"$e"
		if [ "$pol" = none ]; then
			if echo "$got" | grep -q "|$kind|$ns|$name\$"; then fail "unexpected violation recorded for $kind $ns/$name: $(echo "$got" | grep "|$kind|$ns|$name\$")"; else pass "no violation for $kind $ns/$name"; fi
		else
			if echo "$got" | grep -qx "$pol|$kind|$ns|$name"; then pass "recorded $pol on $kind $ns/$name"; else fail "missing $pol on $kind $ns/$name"; fi
		fi
	done
}

denied() { # label, output-pattern, -- command: must be rejected with the pattern in the message
	local label="$1" pattern="$2"; shift 3
	if out=$("$@" 2>&1); then fail "deny mode allowed: $label"; elif echo "$out" | grep -q "$pattern"; then pass "denied: $label"; else fail "denied for the wrong reason: $label :: $out"; fi
}
allowed() { local label="$1"; shift 2; if "$@" >/dev/null 2>&1; then pass "allowed: $label"; else fail "deny mode blocked legitimate work: $label"; fi; }

deny_phase() {
	echo "== Phase 2: Deny (temporary copy; the shipped file stays Audit) =="
	local tmp; tmp=$(mktemp)
	sed 's/validationActions: \[Audit\]/validationActions: [Deny]/' "$POLICIES" >"$tmp"
	$K apply -f "$tmp" >/dev/null || { echo "deny apply failed" >&2; exit 1; }
	rm -f "$tmp"; sleep 20
	local ctl; ctl=$($K -n default get pod -l app=d-dn-ok -o jsonpath='{.items[0].metadata.name}')
	denied  "delete a bare pod"            "may delete only a Pod a controller" -- $K $AS -n default delete pod bare-deny
	denied  "create a pod"                 "never creates" -- $K $AS -n default run created2 --image=$IMG
	denied  "change an Argo CD workload"   "reconciled from Git" -- patch default d-dn-argo '{"spec":{"minReadySeconds":9}}' $AS
	denied  "add hostNetwork"              "host's network" -- patch default d-dn-host '{"spec":{"template":{"spec":{"hostNetwork":true}}}}' $AS
	denied  "change the service account"   "service account" -- patch default d-dn-sa '{"spec":{"template":{"spec":{"serviceAccountName":"other-sa"}}}}' $AS
	denied  "scale past the ceiling"       "ceiling of 20" -- patch default d-dn-replicas '{"spec":{"replicas":21}}' $AS
	denied  "change a deployment in staging" "allowlist" -- patch staging stg-dep '{"spec":{"minReadySeconds":9}}' $AS
	allowed "delete a controller-owned pod" -- $K $AS -n default delete pod "$ctl" --wait=false
	allowed "legitimate update + restore"   -- patch default d-dn-ok '{"spec":{"minReadySeconds":4,"replicas":2}}' $AS
	allowed "ADMIN adds hostNetwork"        -- patch default d-dn-host '{"spec":{"template":{"spec":{"hostNetwork":true}}}}'
	$K apply -f "$POLICIES" >/dev/null
}

ensure_cluster
fixtures
audit_phase
deny_phase

echo
if [ "$FAILED" -eq 0 ]; then echo "kyverno live tests passed"; else echo "kyverno live tests FAILED" >&2; fi
[ "${KEEP:-}" = 1 ] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
exit "$FAILED"
