# Disposable end-to-end environment

**For a local kind cluster only.** This overlay turns on approval-gated live
healing and waives the evidence gates that a fresh cluster cannot meet. Never
apply it to a cluster you care about. `scripts/e2e-setup.sh` refuses any kube
context other than `kind-chronicle`.

What it adds to `deploy/chronicle`:

| Piece | Purpose |
|---|---|
| `keycloak/` | Keycloak 26.6 in dev mode with the `chronicle` test realm: a public PKCE client, an approver (`ana`, group `chronicle-approvers`) and a non-approver (`vic`). Issuer `http://keycloak.localhost:8180/realms/chronicle`. |
| CoreDNS rewrite | `keycloak.localhost` resolves to the Keycloak Service inside the cluster, so the browser, the harness and Chronicle all see one issuer URL. |
| `chronicle/` | Chronicle with OIDC on, live healing on, kill switch off, the executor's namespaced RBAC, and the test-only gate values below. |

**Test-only values, and why:**

| Setting | Base | Here | Why |
|---|---|---|---|
| `HEAL_LIVE_ENABLED` / `HEAL_KILL_SWITCH` | `false` / `true` | `true` / `false` | the point of the environment |
| `HEAL_OBSERVATION_STARTED_AT` | first decision | `2026-01-01T00:00:00Z` | a new cluster has no 30-day history |
| `HEAL_MIN_DECISIVE_DECISIONS` / `HEAL_MIN_PRECISION` | `20` / `0.80` | `0` / `0` | nor any labelled outcomes |
| `HEAL_TARGET_COOLDOWN` | `30m` | `10m` | the campaign tests the cooldown once, then needs the workload again |
| `rollback-bad-deploy` / `restore-scaled-down-workload` hourly caps | `1` / `2` | `3` / `6` | the campaign runs two rollbacks and three restores within an hour; the caps count across targets |

Confidence floors, approval, allowlists, preconditions and every other check
are unchanged. The test users' passwords are in `keycloak/realm.json`; they
exist only in this throwaway realm.

## Reproducing the demo

Needs Docker (8 GB is enough), kind, kubectl, helm, Go 1.26 and the Linkerd
CLI on `PATH`. Everything runs in a local kind cluster named `chronicle`.

```bash
./scripts/setup.sh          # kind cluster, Prometheus, Loki, Kafka, PostgreSQL, Linkerd
./scripts/e2e-setup.sh      # Keycloak + realm, CoreDNS rewrite, victim app, Chronicle (live, OIDC)
```

In two other terminals (the Chronicle one in a loop: a port-forward pins one
pod, and dies when that pod is replaced):

```bash
while true; do kubectl --context kind-chronicle -n chronicle port-forward svc/chronicle 8181:8181; sleep 1; done
kubectl --context kind-chronicle -n keycloak port-forward svc/keycloak 8180:8180
```

Then:

```bash
./scripts/e2e-heal.sh                             # the whole suite, about an hour
./scripts/e2e-heal.sh -only redis-scaled-to-zero  # one step
go run ./cmd/chaos report chaos-results/run-*.jsonl
```

**By hand, in the console:** open `http://localhost:8181/#healing`, click the
badge at the top right to sign in (`ana` approves; `vic` is refused with 403;
passwords in `keycloak/realm.json`), break something
(`kubectl --context kind-chronicle scale deploy/redis --replicas=0`), open the
incident, analyse it, and approve the proposal. The worker executes within a
few seconds and the queue shows the verification.

**Tear down:** `kind delete cluster --name chronicle`.
