#!/usr/bin/env bash
# GitOps end-to-end test, on a throwaway one-node kind cluster:
#
#   scripts/e2e-gitops.sh up     # cluster, Gitea, Argo CD, Prometheus, demo app, Chronicle (~10 min)
#   scripts/e2e-gitops.sh run    # the whole remediation path (~15 min)
#   scripts/e2e-gitops.sh down   # delete the cluster
#
# The path under test: a bad release is committed to Git; Argo CD syncs it and the
# app breaks; Chronicle diagnoses it and, because the app is GitOps-managed,
# proposes a git_revert instead of touching the cluster; one person approves; the
# worker opens a pull request; a reviewer merges it; Argo CD syncs the revert;
# Chronicle sees the sync and checks the symptom cleared. Chronicle must never
# write to the Deployment itself. See deploy/e2e-gitops/README.md.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=chronicle-gitops
CTX="kind-$CLUSTER"
case "$CTX" in kind-*) ;; *) echo "refusing a non-kind context" >&2; exit 2 ;; esac
k() { kubectl --context "$CTX" "$@"; }
ARGO_VERSION=v2.14.15
STATE="${TMPDIR:-/tmp}/chronicle-e2e-gitops"   # generated passwords and tokens, never printed
mkdir -p "$STATE"; chmod 700 "$STATE"
GITEA_LOCAL=13000 CHRONICLE_LOCAL=18182 ARGO_LOCAL=18080
REPO_IN_CLUSTER="http://gitea-http.gitea.svc:3000/acme/gitops.git"
log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }
secret() { [ -s "$STATE/$1" ] || openssl rand -hex 16 > "$STATE/$1"; cat "$STATE/$1"; }

forward() { # name ns svc local remote
  pkill -f "port-forward -n $2 svc/$3 $4:" 2>/dev/null || true
  (k port-forward -n "$2" "svc/$3" "$4:$5" >/dev/null 2>&1 &)
  for _ in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:$4/" && return 0; sleep 1; done
  echo "port-forward to $3 did not come up" >&2; return 1
}
gitea() { # method path [json]
  curl -sf -u "e2e-admin:$(secret gitea-admin)" -X "$1" -H 'Content-Type: application/json' \
    "http://127.0.0.1:$GITEA_LOCAL/api/v1$2" ${3:+-d "$3"}
}
chronicle() { curl -sf "http://127.0.0.1:$CHRONICLE_LOCAL$1" "${@:2}"; }

up() {
  kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
  log "images"
  docker image inspect chronicle-victim:latest >/dev/null 2>&1 || docker build -q -t chronicle-victim:latest -f victim/Dockerfile .
  docker build -q -t chronicle:gitops -f Dockerfile . >/dev/null
  kind load docker-image chronicle-victim:latest chronicle:gitops --name "$CLUSTER" >/dev/null

  log "Gitea"
  k apply -f deploy/e2e-gitops/gitea.yaml >/dev/null
  k -n gitea rollout status deploy/gitea --timeout=300s >/dev/null
  k -n gitea exec deploy/gitea -- gitea admin user create --admin --username e2e-admin --password "$(secret gitea-admin)" \
    --email admin@e2e.test --must-change-password=false >/dev/null 2>&1 || true
  k -n gitea exec deploy/gitea -- gitea admin user create --username chronicle --password "$(secret gitea-chronicle)" \
    --email chronicle@e2e.test --must-change-password=false >/dev/null 2>&1 || true
  [ -s "$STATE/gitea-token" ] || k -n gitea exec deploy/gitea -- gitea admin user generate-access-token --username chronicle \
    --token-name "chronicle-$(date +%s)" --scopes write:repository,read:user --raw > "$STATE/gitea-token"
  forward gitea gitea gitea-http "$GITEA_LOCAL" 3000
  gitea POST /orgs '{"username":"acme"}' >/dev/null 2>&1 || true
  gitea POST /orgs/acme/repos '{"name":"gitops","private":false,"default_branch":"main"}' >/dev/null 2>&1 || true
  gitea PUT /repos/acme/gitops/collaborators/chronicle '{"permission":"write"}' >/dev/null
  if ! gitea GET /repos/acme/gitops/branches/main >/dev/null 2>&1; then
    work="$(mktemp -d)"; mkdir -p "$work/apps/victim"
    for f in api frontend loadgen postgres redis worker; do cp "deploy/victim/$f.yaml" "$work/apps/victim/"; done
    # A bad release must take the api down, as it would in production: Recreate.
    python3 - "$work/apps/victim/api.yaml" <<'PY'
import sys; p=sys.argv[1]; s=open(p).read()
s=s.replace("kind: Deployment\nmetadata:\n  name: api\nspec:\n", "kind: Deployment\nmetadata:\n  name: api\nspec:\n  strategy: {type: Recreate}\n", 1)
open(p,"w").write(s)
PY
    grep -q "Recreate" "$work/apps/victim/api.yaml" || { echo "could not set Recreate on api" >&2; exit 1; }
    git -C "$work" init -q -b main
    git -C "$work" -c user.name=dev -c user.email=dev@e2e.test add -A
    git -C "$work" -c user.name=dev -c user.email=dev@e2e.test commit -qm "victim app"
    git -C "$work" push -q "http://e2e-admin:$(secret gitea-admin)@127.0.0.1:$GITEA_LOCAL/acme/gitops.git" main
    rm -rf "$work"
  fi

  log "Argo CD $ARGO_VERSION"
  [ -s "$STATE/argocd-install.yaml" ] || curl -sfL -o "$STATE/argocd-install.yaml" \
    "https://raw.githubusercontent.com/argoproj/argo-cd/$ARGO_VERSION/manifests/install.yaml"
  k create namespace argocd --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n argocd apply --server-side --force-conflicts -f "$STATE/argocd-install.yaml" >/dev/null
  k -n argocd patch configmap argocd-cmd-params-cm --type merge -p '{"data":{"server.insecure":"true"}}' >/dev/null
  k -n argocd patch configmap argocd-cm --type merge \
    -p '{"data":{"timeout.reconciliation":"15s","application.resourceTrackingMethod":"annotation"}}' >/dev/null
  k -n argocd scale deploy argocd-dex-server argocd-notifications-controller argocd-applicationset-controller --replicas=0 >/dev/null
  k -n argocd rollout restart deploy/argocd-server deploy/argocd-repo-server sts/argocd-application-controller >/dev/null
  k -n argocd rollout status deploy/argocd-server --timeout=300s >/dev/null
  k -n argocd rollout status sts/argocd-application-controller --timeout=300s >/dev/null
  k -n argocd apply -f - >/dev/null <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: victim, namespace: argocd}
spec:
  project: default
  source: {repoURL: $REPO_IN_CLUSTER, path: apps/victim, targetRevision: main}
  destination: {server: https://kubernetes.default.svc, namespace: default}
  syncPolicy: {automated: {prune: true, selfHeal: true}}
YAML
  for _ in $(seq 1 60); do
    s="$(k -n argocd get application victim -o jsonpath='{.status.sync.status}/{.status.health.status}' 2>/dev/null || true)"
    [ "$s" = "Synced/Healthy" ] && break; sleep 5
  done
  log "  victim: $s"
  forward argocd argocd argocd-server "$ARGO_LOCAL" 80
  pw="$(k -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d)"
  curl -sf -H 'Content-Type: application/json' "http://127.0.0.1:$ARGO_LOCAL/api/v1/session" -d "{\"username\":\"admin\",\"password\":\"$pw\"}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' > "$STATE/argocd-token"

  log "Prometheus"
  k apply -f deploy/e2e-gitops/prometheus.yaml >/dev/null

  log "Chronicle"
  k apply -f deploy/chronicle/namespace.yaml >/dev/null
  k -n chronicle create secret generic postgres-postgresql --from-literal=postgres-password="$(secret pg)" --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n chronicle apply -f - >/dev/null <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: postgres-postgresql, namespace: chronicle}
spec:
  replicas: 1
  selector: {matchLabels: {app: postgres-e2e}}
  template:
    metadata: {labels: {app: postgres-e2e}}
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          env: [{name: POSTGRES_PASSWORD, valueFrom: {secretKeyRef: {name: postgres-postgresql, key: postgres-password}}}]
          readinessProbe: {exec: {command: [pg_isready, -U, postgres]}, periodSeconds: 3}
---
apiVersion: v1
kind: Service
metadata: {name: postgres-postgresql, namespace: chronicle}
spec:
  selector: {app: postgres-e2e}
  ports: [{port: 5432}]
YAML
  k -n chronicle create secret generic chronicle-config \
    --from-literal=HEAL_APPROVAL_TOKEN="$(secret approval)" \
    --from-literal=HEAL_GIT_TOKEN="$(cat "$STATE/gitea-token")" \
    --from-literal=ARGOCD_URL="http://argocd-server.argocd.svc" \
    --from-literal=ARGOCD_TOKEN="$(cat "$STATE/argocd-token")" --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n chronicle create configmap chronicle-migrations --from-file=migrations/ --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n chronicle create configmap chronicle-ui --from-file=internal/web/static/ --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n chronicle rollout status deploy/postgres-postgresql --timeout=300s >/dev/null
  k apply -k deploy/e2e-gitops/chronicle >/dev/null
  k -n chronicle rollout restart deploy/chronicle >/dev/null
  k -n chronicle rollout status deploy/chronicle --timeout=300s >/dev/null
  k -n default wait --for=condition=available deployment --all --timeout=300s >/dev/null
  log "up. Run: scripts/e2e-gitops.sh run"
}

pass=0 fail=0
check() { if [ "$1" = ok ]; then pass=$((pass+1)); log "  PASS  $2"; else fail=$((fail+1)); log "  FAIL  $2"; fi; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

run() {
  forward gitea gitea gitea-http "$GITEA_LOCAL" 3000
  forward chronicle chronicle chronicle "$CHRONICLE_LOCAL" 8181
  # An earlier, interrupted run can leave the bad release in Git: restore it as
  # the developer would, before anything is measured.
  file="$(gitea GET /repos/acme/gitops/contents/apps/victim/api.yaml?ref=main)"
  if echo "$file" | json 'd["content"]' | base64 -d | grep -q missing-release; then
    log "restoring the demo app an earlier run left broken"
    body="$(echo "$file" | json 'd["content"]' | base64 -d | sed 's#image: chronicle-victim:missing-release#image: chronicle-victim:latest#')"
    gitea PUT /repos/acme/gitops/contents/apps/victim/api.yaml "$(python3 -c 'import json,sys,base64; print(json.dumps({"content":base64.b64encode(sys.argv[1].encode()).decode(),"sha":sys.argv[2],"message":"restore api","branch":"main"}))' "$body" "$(echo "$file" | json 'd["sha"]')")" >/dev/null
    # The restore is itself a recent change: it must age out of the analyzer's
    # 10-minute lookback, or it competes with the bad release as a weak rival.
    log "  waiting 11 minutes so the restore is outside the analyzer's lookback"
    sleep 660
  fi
  log "steady state: every Deployment ready, Chronicle up for a while"
  k -n default wait --for=condition=available deployment --all --timeout=300s >/dev/null
  good="$(k -n argocd get application victim -o jsonpath='{.status.sync.revision}')"
  log "  victim runs $good"
  sleep 150   # the good revision must have run healthily for a while (HEAL_ROLLBACK_MIN_HEALTHY=60s)

  log "1. a bad release is committed to Git"
  t0="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  file="$(gitea GET /repos/acme/gitops/contents/apps/victim/api.yaml?ref=main)"
  sha="$(echo "$file" | json 'd["sha"]')"
  body="$(echo "$file" | json 'd["content"]' | base64 -d | sed 's#image: chronicle-victim:latest#image: chronicle-victim:missing-release#')"
  gitea PUT /repos/acme/gitops/contents/apps/victim/api.yaml "$(python3 -c 'import json,sys,base64; print(json.dumps({"content":base64.b64encode(sys.argv[1].encode()).decode(),"sha":sys.argv[2],"message":"release api v2","branch":"main"}))' "$body" "$sha")" >/dev/null
  for _ in $(seq 1 40); do bad="$(k -n argocd get application victim -o jsonpath='{.status.sync.revision}')"; [ "$bad" != "$good" ] && break; sleep 3; done
  check "$([ "$bad" != "$good" ] && echo ok)" "Argo CD synced the bad revision ${bad:0:8}"

  log "2. the app breaks and Chronicle raises an alert"
  alert=""
  for _ in $(seq 1 60); do
    alert="$(chronicle "/api/events?from=$t0&limit=500" | python3 -c '
import json,sys
ev=[e for e in json.load(sys.stdin)["events"] if e["type"] in ("error_spike","latency_spike")]
ev.sort(key=lambda e: e["ingested_at"]); print(ev[0]["id"] if ev else "")' 2>/dev/null || true)"
    [ -n "$alert" ] && break; sleep 5
  done
  check "$([ -n "$alert" ] && echo ok)" "an alert was raised (${alert:-none})"
  [ -n "$alert" ] || return 1
  sleep 65   # past the analyzer's allowance for late events

  log "3. Chronicle diagnoses it and proposes a Git revert"
  analysis="$(chronicle "/api/analyze?event_id=$alert" -X POST)"
  echo "$analysis" > "$STATE/analysis.json"
  echo "$analysis" | json '"  verdict %s, confidence %.2f, top %s:%s" % (d["verdict"], d["confidence"], d["candidates"][0]["event"]["type"], d["candidates"][0]["event"]["entity_name"])'
  id="$(echo "$analysis" | json '(d.get("action") or {}).get("id","")')"
  atype="$(echo "$analysis" | json '(d.get("action") or {}).get("action_type","")')"
  state="$(echo "$analysis" | json '(d.get("action") or {}).get("state","")')"
  echo "$analysis" | json '"  decision: %s %s -> %s" % ((d.get("action") or {}).get("action_type"), (d.get("action") or {}).get("state"), (d.get("action") or {}).get("result"))'
  check "$([ "$atype" = git_revert ] && echo ok)" "the remedy is a git_revert, not a write to the cluster"
  check "$([ "$state" = pending_approval ] && echo ok)" "it waits for approval"
  [ "$atype" = git_revert ] && [ "$state" = pending_approval ] || return 1

  log "4. nobody approved yet: nothing has changed anywhere"
  check "$([ "$(gitea GET '/repos/acme/gitops/pulls?state=all' | json 'len(d)')" = 0 ] && echo ok)" "no pull request exists before approval"
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$CHRONICLE_LOCAL/api/heal/actions/$id/approve")"
  check "$([ "$code" = 401 ] && echo ok)" "approval without the token is refused ($code)"

  log "5. one person approves"
  chronicle "/api/heal/actions/$id/approve" -X POST -H "X-Chronicle-Heal-Token: $(secret approval)" -H 'Content-Type: application/json' -d '{"by":"e2e-approver","reason":"bad api release"}' >/dev/null
  pr="" ; for _ in $(seq 1 40); do
    pr="$(chronicle /api/heal/actions | python3 -c "
import json,sys
for a in json.load(sys.stdin)['actions']:
    if a['id']=='$id' and a.get('git_change'): print(a['git_change']['number']); break" || true)"
    [ -n "$pr" ] && break; sleep 3
  done
  check "$([ -n "$pr" ] && echo ok)" "the worker opened pull request #${pr:-?}"
  [ -n "$pr" ] || return 1
  pull="$(gitea GET "/repos/acme/gitops/pulls/$pr")"
  echo "$pull" | json '"  %s  (%s -> %s)" % (d["title"], d["head"]["ref"], d["base"]["ref"])'
  files="$(gitea GET "/repos/acme/gitops/pulls/$pr/files" | json '",".join(f["filename"] for f in d)')"
  check "$([ "$files" = apps/victim/api.yaml ] && echo ok)" "it changes only the file the bad release changed ($files)"
  check "$(gitea GET '/repos/acme/gitops/raw/apps/victim/api.yaml?ref=main' 2>/dev/null | grep -q missing-release && echo ok)" "main is untouched until someone merges"
  check "$(k -n default get deploy api -o jsonpath='{.spec.template.spec.containers[0].image}' | grep -q missing-release && echo ok)" "the cluster is untouched: Chronicle did not write the Deployment"

  log "6. a reviewer merges the pull request"
  gitea POST "/repos/acme/gitops/pulls/$pr/merge" '{"Do":"merge"}' >/dev/null
  synced=""; for _ in $(seq 1 60); do
    synced="$(chronicle /api/heal/actions | python3 -c "
import json,sys
for a in json.load(sys.stdin)['actions']:
    if a['id']=='$id': print((a.get('git_change') or {}).get('state','')); break" || true)"
    [ "$synced" = synced ] && break; sleep 5
  done
  check "$([ "$synced" = synced ] && echo ok)" "Chronicle saw the merge and Argo CD's sync of it"
  k -n default rollout status deploy/api --timeout=180s >/dev/null && img="$(k -n default get deploy api -o jsonpath='{.spec.template.spec.containers[0].image}')"
  check "$([ "${img:-}" = chronicle-victim:latest ] && echo ok)" "the api runs the good release again ($img)"

  log "7. the symptom check: the alert must clear and stay clear"
  verdict=""; for _ in $(seq 1 110); do
    verdict="$(chronicle /api/heal/actions | python3 -c "
import json,sys
for a in json.load(sys.stdin)['actions']:
    if a['id']=='$id': print(a.get('symptom_check','')); break" || true)"
    case "$verdict" in succeeded|failed|unknown) break ;; esac; sleep 5
  done
  check "$([ "$verdict" = succeeded ] && echo ok)" "symptom check: ${verdict:-did not finish}"

  log "8. who wrote what"
  managers="$(k -n default get deploy api -o jsonpath='{range .metadata.managedFields[*]}{.manager}{" "}{end}')"
  check "$(echo "$managers" | grep -qiE 'chronicle|collector' || echo ok)" "only Argo CD and Kubernetes wrote the Deployment ($managers)"
  chronicle /api/heal/actions | python3 -c "
import json,sys
for a in json.load(sys.stdin)['actions']:
    if a['id']=='$id':
        g=a.get('git_change') or {}
        print('  audit:', a['action_type'], a['target'], '| approved by', a.get('decision_by'), '| PR #%s %s' % (g.get('number'), g.get('state')), '|', g.get('detail',''), '| symptom', a.get('symptom_check'))"
  log "result: $pass passed, $fail failed"
  [ "$fail" = 0 ]
}

case "${1:-}" in
  up) up ;;
  run) run ;;
  down) kind delete cluster --name "$CLUSTER"; rm -rf "$STATE" ;;
  *) echo "usage: $0 up|run|down" >&2; exit 2 ;;
esac
