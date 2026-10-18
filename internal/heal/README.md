# Healing safety

Chronicle's self-healing is **dry-run by default** and stays that way unless
an operator deliberately enables it. This document describes how decisions
move from proposal to execution, what each safety layer does, how to
configure and deploy it, and what is still open.

## Decision life cycle

```
RCA result ──► engine ──► skipped / blocked              (terminal, never executable)
                    └──► would_run + approval pending ──► denied   (terminal)
                                     │                 └──► expired  (deadline passed)
                                     ▼ approve (HTTP: records the decision, nothing else)
                                  approved ──► expired  (not executed before the deadline)
                                     │
                                     ▼ execution worker (leader only, every 5s, only when live)
                        re-check everything ──► blocked  (any gate fails; terminal)
                                     │
                                     ▼ atomic claim (limits + cooldown + one per outage)
                                 executing ──► succeeded | failed
                                     │         └──► symptom check: pending ► succeeded | failed | unknown
                                     ├──► blocked  (the executor refused: the cluster moved on; nothing changed)
                                     └──► failed "interrupted" if the worker died (never retried)
```

`state` in the API is one of `pending_approval`, `approved`, `executing`,
`succeeded`, `failed`, `expired`, `blocked`, `denied`, `skipped`.

### Safety layers, in order

1. **Engine (plan time).** Each outage (cause event) gets at most one
   proposal. It must pass the action's preconditions, so nothing is planned
   that the executor would refuse. It must also clear the rule's confidence
   floor and its hourly limit, counted per outage. Only then does it become
   `would_run`, awaiting approval, with a deadline (`HEAL_APPROVAL_TTL`).
2. **Approval (HTTP).** Needs an approver identity (an OIDC sign-in, or the
   shared token if no provider is configured). It succeeds only for a
   pending, unexpired `would_run`. It records the decision and executes nothing.
3. **Worker (execution time).** Re-checks, against the present:
   - approval state and deadline;
   - the rule's *current* floor and action type;
   - the preconditions;
   - live mode, the kill switch, the observation period and evidence;
   - the action, namespace and workload allowlists.

   Then, in one transaction under an advisory lock, it checks:
   - the per-rule hourly limit;
   - the global hourly cap;
   - the per-workload cooldown;
   - that no other decision for the same outage was executed.

   Only then does it claim the row. Any refusal is final.
4. **Executor (live state).** Re-reads the object and refuses if it changed
   since the decision:
   - a restored workload must still be at zero, with no HPA owning replicas;
   - a pod must still exist, still be unready, and still be controller-owned
     (deleted with a UID precondition);
   - a rollback runs only while the bad image is still the one running;
   - a memory bump changes only the OOM-killed container, by name.

   Any GitOps-managed workload is refused, and Chronicle proposes a Git
   revert instead.

   A refusal (`Refusal` in `executor.go`) is recorded as `blocked`, not
   `failed`: nothing was changed, so the claim is undone and the attempt does
   not count toward the hourly caps or start a cooldown. A change that was
   applied and did not take is `failed`.
5. **Symptom check (after success).** Verifying the workload is not verifying
   the alert. After an action succeeds, the symptom that started the incident is
   measured again with the collector's own alert expression and threshold
   (`collect.AlertRules`): it must start clearing within
   `SYMPTOM_IMPROVE_WITHIN` (2 min) and then stay clear for `SYMPTOM_HOLD`
   (5 min). The verdict is three-valued:
   - **succeeded**: recovered and held;
   - **failed**: no improvement in time, or the alert came back during the hold;
   - **unknown**: no data (no series, 0/0 from no traffic, Prometheus
     unreachable) or a symptom that is not a metric alert. "No data" is never
     recovery.

   It is stored on the decision (`symptom_check`, `symptom_detail`) and survives
   a leader change. It never touches `outcome` or the evidence counts
   (finding 20 stands: Chronicle's own actions are not evidence). A failure
   notifies Slack once; only a **critical** symptom with
   `CHRONICLE_ENVIRONMENT=production` and a `PAGER_WEBHOOK_URL` pages. Nothing
   is reverted automatically.
6. **Kubernetes RBAC.** The default deployment can write only its own Lease
   and checkpoint ConfigMap. Live write access comes from
   `deploy/heal-executor`, one namespaced Role per allowlisted namespace.
7. **Kyverno (optional, Audit).** `deploy/kyverno/chronicle-heal-guardrails.yaml`
   repeats the refusals above at admission time, as a backstop.

## Tool decisions

### Temporal: not adopted; a Postgres-backed worker instead

Healing actions are single-step, take under a minute, and run a few times an
hour at most. What Temporal would add (durable history, timers, retries) is
already covered by rows Chronicle owns:

- the state machine lives in `heal_actions`;
- claims are compare-and-set inside a transaction with an advisory lock;
- deadlines are columns;
- interrupted executions are detected and closed, never blindly retried.

Temporal would add a server, its own datastore, an SDK with determinism
constraints and a second source of truth, for no safety property the worker
lacks.

**When to revisit:** multi-step remediations (drain, then wait, then verify,
then roll back), human-in-the-loop waits longer than a single deadline, or
many concurrent workflows. The `Executor` interface and the `ExecuteApproved`
contract are the seam: a Temporal activity could call `ExecuteApproved`
unchanged.

### Argo CD: optional, no dependency

Argo CD is not installed in the kind cluster, and Chronicle does not need it.

- **How GitOps is detected.** The collector stamps `gitops` into deploy,
  scale, resource and config events when the Deployment carries
  `argocd.argoproj.io/tracking-id`, `argocd.argoproj.io/instance`, or a Flux
  label. `app.kubernetes.io/instance` counts only when its value is an
  Application Argo CD itself reports (the Argo collector keeps that set
  current), because plain Helm sets the same label on every release. Before
  Argo CD has answered once, no instance label is trusted.
- **For GitOps-managed workloads**, the engine records a proposal ("revert
  in Git, or `argocd app rollback <app>` with auto-sync paused") and plans no
  execution. The executor re-checks the live object, and Kyverno audits the
  same rule.
- **Rollback for unmanaged workloads** restores the previous ReplicaSet's
  whole pod template, as `kubectl rollout undo` does, so images, env,
  resources and every container come back together. It is refused if a newer
  deploy has happened since.

### Kyverno: defence in depth, Audit first

Six CEL `ValidatingPolicy` objects (`policies.kyverno.io/v1`) restrict the
`chronicle` service account:

- Pods and Deployments only, in allowlisted namespaces only;
- no creates;
- only pods with a controller reference may be deleted;
- no GitOps-managed workloads;
- nothing that *adds* privilege: a changed service account, newly enabled
  host namespaces, a newly privileged container (init containers included), a
  newly mounted host path. A workload that was already privileged is not
  flagged when Chronicle merely restarts it;
- a replica ceiling of 20.

They need Kyverno ≥ 1.17 and run in Audit, with `failurePolicy: Ignore`, so a
policy fault can never block the cluster. They do **not** replace Chronicle's
own checks.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `HEAL_LIVE_ENABLED` | `false` | The worker executes approved decisions only when `true` |
| `HEAL_KILL_SWITCH` | on unless `false` | Blocks every execution |
| `HEAL_ALLOWED_ACTIONS` / `_NAMESPACES` / `_TARGETS` | `restart_pod` / `default` / `default/redis` | Allowlists; targets name the **workload** (a pod matches through its owner) |
| `HEAL_APPROVAL_TTL` | `30m` | Deadline for approval *and* execution |
| `HEAL_MAX_EXECUTIONS_PER_HOUR` | `3` | Global cap across rules (0 = none) |
| `HEAL_TARGET_COOLDOWN` | `30m` | Least time between two actions on one workload |
| `HEAL_ROLLBACK_MIN_HEALTHY` | `30m` | A revision must have run this long without a recorded failure before a rollback returns to it; otherwise an older verified revision is named, or none |
| `HEAL_RESTART_EXECUTION_ENABLED` | `false` | `restart_pod` is proposed and approved but never executed until this is `true` |
| `HEAL_RESTART_UNREADY_FOR` / `HEAL_RESTART_QUIET` | `5m` / `10m` | A pod is restarted only after this long unready, and not within the quiet period of a restart of its container |
| `SYMPTOM_IMPROVE_WITHIN` / `SYMPTOM_HOLD` | `2m` / `5m` | After a successful action: the alert must start clearing within the first, then stay clear for the second |
| `CHRONICLE_ENVIRONMENT`, `PAGER_WEBHOOK_URL` | unset | A failed check pages only when the symptom is critical, the environment is `production` and the webhook is set |
| `HEAL_ACTION_TIMEOUT` / `HEAL_VERIFY_TIMEOUT` | `30s` / `3m` | Separate budgets for the write and its verification |
| `HEAL_OBSERVATION_STARTED_AT` | first decision, stored once | Overrides the stored start |
| `HEAL_MIN_DECISIVE_DECISIONS` / `HEAL_MIN_PRECISION` | `20` / `0.80` | Evidence gate |
| `HEAL_RULES` | built-in | JSON rules. `require_approve: false` is accepted and has no effect: automatic execution is not implemented |
| `ARGOCD_URL`, `ARGOCD_TOKEN` | unset | Optional Argo CD collector; its list of Applications also decides which `app.kubernetes.io/instance` labels mean Argo CD ownership |

**Secrets** are referenced by name and never logged:

- `chronicle-config/HEAL_APPROVAL_TOKEN` (approval when no OIDC provider is configured; without it, and without OIDC, nothing can be approved). OIDC needs no secret;
- `ARGOCD_TOKEN`;
- `SLACK_WEBHOOK_URL`.

## Approver authentication (OIDC)

Without a provider, approval takes one shared token, and the approver's name is
whatever the caller types. With `OIDC_ISSUER_URL` set, an approver is a person
the identity provider vouches for, and that is what the audit trail records.

**How it works.** The console signs the approver in with the OpenID Connect
authorization-code flow and PKCE (a public client: no client secret exists to
leak). It sends the resulting ID token as `X-Chronicle-Heal-Token`. For every
approve or deny, the server:

1. verifies the token against the provider's published keys: signature,
   issuer, audience (`OIDC_CLIENT_ID`), expiry, and an age bound
   (`OIDC_MAX_TOKEN_AGE`, by `iat`);
2. requires the person to be in `OIDC_ALLOWED_GROUPS` (the `OIDC_GROUPS_CLAIM`
   claim, default `groups`) or `OIDC_ALLOWED_EMAILS` (counted only when the
   provider marks the email verified);
3. records the verified email (or subject), plus issuer and subject, as the
   decider. The `by` field in the request body is ignored.

Authentication runs before anything else, so a caller who may not approve
learns nothing about the actions or the store.

| Variable | Default | Meaning |
|---|---|---|
| `OIDC_ISSUER_URL` | empty | Empty keeps the shared token. Must be `https` (plain `http` only for localhost) |
| `OIDC_CLIENT_ID` | none | Required with an issuer: the audience tokens must carry |
| `OIDC_ALLOWED_GROUPS`, `OIDC_ALLOWED_EMAILS` | none | Comma-separated; at least one is required. Empty never means everyone |
| `OIDC_GROUPS_CLAIM` | `groups` | Claim holding the groups (a list or a comma-separated string) |
| `OIDC_MAX_TOKEN_AGE` | `1h` | Older tokens are refused even if not yet expired |
| `OIDC_SCOPES` | `openid email profile groups` | Requested at sign-in; drop `groups` if the provider rejects it |

**Provider setup.** Register a *public* client (no secret) with PKCE, with the
redirect URI `<console origin>/` (for example `https://chronicle.example.com/`),
and allow the console's origin for CORS on the token endpoint, since the
browser makes that call. Add the groups claim to the ID token.

**Failure modes.**

- *Invalid configuration* (issuer without a client id or allowlist, or a
  non-https issuer): the collector refuses to start.
- *Provider unreachable*: Chronicle starts and everything read-only works.
  Approvals fail closed with 503 and discovery is retried every 5 seconds.
- *Token refused*: 401 with no detail (the reason is in the log). *Valid token,
  not an approver*: 403.
- *With OIDC on, the shared token is ignored.* It is not a second door.

**Rollback.** Empty `OIDC_ISSUER_URL` and redeploy. Approvals go back to the
shared token, and decisions already recorded keep their identity.

**Limits, stated plainly.**

- Group membership is read from the token, so removing someone from a group
  takes effect when their token ages out (at most `OIDC_MAX_TOKEN_AGE`).
- The ID token lives in the tab's `sessionStorage`; a script injected into the
  console could read it. The console loads no third-party script.
- One approver may approve a proposal alone; there is no second-person rule.
- The server was tested against a fake provider that signs real tokens, and
  the console flow under Node with a fake window. **It has not been run against a
  real identity provider** (Keycloak, Dex, Google, Okta); do that before
  relying on it, especially the groups claim and CORS on the token endpoint.

## Deploying

**Dry-run (the default).** Run `make deploy-chronicle`. Migration
`011_heal_safety.sql` runs at start; it is idempotent and repairs older rows:

- blocked decisions stop awaiting approval;
- denials are recorded as `denied`;
- pending or approved decisions from before deadlines existed are expired;
- the observation start is stored.

**Enabling live healing.** Do this only after the observation period and
evidence gate are met.

1. Review the evidence:
   ```
   GET /api/heal/calibration
   ```
2. If you use Kyverno, install the guardrails policy and review its
   PolicyReports for a period:
   ```
   kubectl apply -f deploy/kyverno/chronicle-heal-guardrails.yaml
   ```
3. Grant the executor's write access. Edit the namespace first, once per
   allowlisted namespace:
   ```
   kubectl apply -k deploy/heal-executor
   ```
4. Set `HEAL_LIVE_ENABLED=true` and `HEAL_KILL_SWITCH=false`, together with
   narrow allowlists.

**Failure modes:**

| Failure | What happens |
|---|---|
| Worker crashes mid-execution | The row is failed as "interrupted" after `2 × (timeout + verify timeout)`. It is never retried. |
| Leadership changes | Only the leader runs the worker. The advisory lock and compare-and-set claim stop a double execution if two briefly overlap. |
| API server is slow | Conflicts and timeouts are retried up to 3 times. Each retry re-reads the object, so a stale write is refused, not repeated. |
| Rollout is slow | Verification has its own 3-minute budget. If it runs out, the action is failed with the rollout's last known state. |
| Slack is down | The decision is kept, and the failure is logged. |

**Rolling back the integration:**

| To | Do |
|---|---|
| Stop all execution immediately | Set `HEAL_KILL_SWITCH=true`. Approved decisions are then blocked or expire. |
| Remove write access | `kubectl delete -k deploy/heal-executor` |
| Remove the guardrails | `kubectl delete -f deploy/kyverno/chronicle-heal-guardrails.yaml` |
| Undo migration 011 | Not needed: it only adds columns, a table and indexes, and older code ignores them. Its row repairs are one-way and intended. |
| Undo migrations 012-014 | Not needed: they add a column, a table and indexes. The relabelling cleared live labels but copied each to `heal_outcome_history` first, so an earlier labeller's view can be restored from there. |

## Status of the review findings

Issues were numbered in the healing review. "Fixed" means implemented and
covered by a test that was checked to fail when the fix is removed (mutation
tested).

| # | Finding | Status | Evidence |
|---|---|---|---|
| 1 | Blocked decisions approvable | **Fixed** | `TestARefusedDecisionIsNeverAwaitingApproval`, `TestEngineBlocksLowConfidenceCause`, `TestPostgresApprovalOnlyForPendingUnexpiredProposals`, migration repair test |
| 2 | Stale approvals | **Fixed** | Deadline on approval and execution, plus live-state checks: `TestOnlyApprovedUnexpiredDecisionsExecute`, `TestRollbackNeverOverwritesALaterDeploy`, `TestRestartRevalidatesThePod` |
| 3 | One outage, many decisions | **Fixed** | `TestOneProposalPerOutage`, `TestPostgresProposalsAreCountedPerOutage` |
| 4 | No execution-time limits | **Fixed** | `TestPostgresClaimEnforcesLimitsAtomically`, `TestAClaimRefusalBlocksWithoutExecuting` |
| 5 | Restart is a raw delete | **Fixed** | Eviction API with a UID precondition, so PodDisruptionBudgets apply; recovery means the owner's replicas are all ready again: `TestRestartEvictsAndNeverDeletes`, `TestADisruptionBudgetRefusalIsReportedAndRetryable`, `TestVerificationRequiresTheWorkloadToRecover`, `TestRealClusterEvictionHonoursTheDisruptionBudget` (real API server) |
| 6 | Execution inside HTTP | **Fixed** | Worker plus separate verify budget: `TestTheWorkerExecutesNothingWhileLiveIsOff`. Post-action symptom check added (see above): `TestWorkerChecksTheSymptomAfterASuccessfulAction`, `TestWorkerRecordsAFailedSymptomCheckWithoutTouchingTheOutcome`, `TestASymptomCheckSurvivesALeaderChange`. Auto-revert stays deliberately absent. |
| 7 | Image-only rollback | **Fixed** | `TestRollbackRestoresThePreviousPodTemplate` plus GitOps refusal |
| 8 | Memory bump unworkable | **Partial** | No longer planned without evidence, and the container is chosen by name. Still deferred: the collector recording the original limit, and a cumulative cap. |
| 9 | Restore ignores intent | **Partial** | GitOps-managed workloads are refused. Still deferred: using `changed_by` to recognise deliberate scale-to-zero. |
| 10 | Allowlist never matches | **Fixed** | `TestAllowlistMatchesTheOwningWorkload` |
| 11 | "Automatic" rules never run | **Fixed** | Every action requires approval, and this is reported truthfully: `TestEveryProposalNeedsApproval` |
| 12 | "Queued" was not a queue | **Fixed** | Worker plus truthful API and console states |
| 13 | Observation start drifts | **Fixed** | `TestPostgresObservationStartSurvivesPruning` |
| 14 | Evidence gate is global | Deferred | Needs labelled data per action type |
| 15 | Floor below the inconclusive line | **Fixed** | An `inconclusive` verdict is never proposed, whatever the floor: `TestAnInconclusiveVerdictIsNeverProposedEvenUnderALowFloor` |
| 16 | Namespace-wide recovery | **Fixed** | `TestRecoveryElsewhereInTheNamespaceDoesNotConfirm`, `TestPostgresLabellingUsesTheSymptomAndOrder` |
| 17 | Event order ignored | **Fixed** | `TestRecoveryBeforeTheFixDoesNotConfirm` |
| 18 | Loose attribution | **Fixed** | `TestAttributionIsExact` |
| 19 | Unrelated changes contradict | **Fixed** | `TestAnUnrelatedChangeDoesNotContradict`. Relatedness comes from the entities the RCA weighed. |
| 20 | Self-confirming evidence | **Fixed** | `TestChroniclesOwnChangeDoesNotConfirm`, `TestPostgresEvidenceExcludesSelfExecutedOutages` |
| 21 | No rule for config/service/resource/node causes | **Partly fixed** | `revert-bad-config` rolls a Deployment back to an exact earlier pod template (P1 below). ConfigMap and Secret contents are never reverted, by design; service, node and autoscaler causes still have no rule. |
| 22 | 0.65 floor is uncalibrated | Deferred | Floors were deliberately not lowered. |
| 23 | Restart rule fires on any unready pod | **Fixed** (not live) | Eligibility is checked at execution: unready 5 min, quiet 10 min, not mid-rollout; until then the approved decision waits (P1 below): `TestARestartWaitsUntilThePodHasBeenUnreadyForFiveMinutes` and the tests beside it |
| 24 | Duplicate-insert race returns a phantom ID | **Fixed** | `RecordAction` returns the existing decision (`ErrAlreadyRecorded`) and a duplicate is not announced twice: `TestPostgresADuplicateIncidentReturnsTheWinnersDecision`, `TestPostgresConcurrentAnalysesOfOneIncidentAgree`, `TestALostInsertReturnsTheWinnersDecisionAndDoesNotNotify` |
| 25 | Re-analysis hides the existing decision | **Fixed** | Re-analysis returns the decision already made, with its state: `TestReAnalysisReturnsTheExistingDecision` |
| 26 | Approver identity is self-declared | **Fixed** (with limits) | OIDC: `TestTheApproverIsTheVerifiedIdentityNotTheBody`, `TestTheSharedTokenStopsWorkingOnceOIDCIsConfigured`, `TestTokensThatDoNotVerifyAreRefused`, `TestPostgresRecordsTheVerifiedApprover`. Not run against a real provider; see *Approver authentication* |
| 27 | Slack failures silent, no action link | **Partial** | Failures are now logged. A link is still missing. |
| 28 | Console claims dry-run regardless of mode | **Fixed** | The banner reads the live, kill-switch, TTL, cap and cooldown values from `/api/heal/rules`. |
| 29 | RBAC wider than needed | **Fixed** | `TestDefaultRBACGrantsNoWritesOutsideChronicle`, `TestExecutorRBACIsNamespaced` |
| 30 | Chaos records only the main alert's decision | Deferred | |

**Kyverno, verified two ways (CLI and chart 1.19.1):**

- **CLI, `make kyverno-test`:** 56 expectations over CREATE, UPDATE and DELETE:
  each policy catches its violation and passes a benign request from
  Chronicle, and every policy skips the same requests from another user.
- **Real admission controller, `make kyverno-live-test`:** a disposable kind
  cluster (`kyverno-test*`, never Chronicle's) with the Kyverno chart. The
  script acts as Chronicle's service account by impersonation, with
  deliberately broad RBAC so only the policy can stop it.
  - In Audit mode, 17 scenarios are allowed and every violation is recorded
    as a `PolicyViolation` event, while legitimate work and the same actions
    by other users record none.
  - With the policies flipped to Deny (a temporary copy), violations are
    rejected with the policy's message and legitimate work still goes through.
- **Bugs the tests caught, now fixed:**
  - `volumes[?hostPath]` errored on Deployments without volumes.
  - The namespace check read `request.namespace`, which a DELETE does not
    carry; the CLI sends an empty `object` and the API server a null one, so
    the policy now keys on the operation.
  - The privilege rules judged the *resulting state*, so every later update to
    a once-privileged workload was flagged; they now judge what the change adds.
- **Mutation-tested:** 20 distinct deliberate policy breakages, all caught by the CLI
  suite, the manifest test or the live suite. The live suite alone catches the
  service-account rule and the "changed, not merely present" semantics, because
  the CLI cannot supply a different old object.
- **Harness hazard:** `kyverno test` exits 0 when a policy fails to load,
  reporting every expectation as skipped. `scripts/test-kyverno.sh` treats
  skipped or invalid results as failures.

**Healing, verified end to end on the kind cluster (dry-run):** see
"End-to-end run" below.

## End-to-end run (dry-run, 2026-10-09)

The current tree was deployed to the local kind cluster with live healing off
(`HEAL_LIVE_ENABLED=false`, kill switch on), then two real faults were injected
with the chaos harness (`chaos-results/e2e-healing*.jsonl`). Nothing was
executed, and no Deployment's generation changed.

| Check | Result |
|---|---|
| Migration 011 on the cluster's Postgres | applied by the init container; older proposals awaiting approval became `expired`, blocked ones left the approval queue |
| Default RBAC | `auth can-i` as the service account: cannot delete pods or update Deployments; can still write its own ConfigMap and Lease |
| Worker and leader election | leader elected, worker running, expiry sweeps logged |
| One proposal per outage | each fault: **1** `pending_approval` proposal and **4** skipped ("same outage as decision …"). The same faults on the old build had spent the hourly limit on duplicates. |
| Approval authentication | no token and a wrong token: 401 |
| Refusals | approving a blocked, expired, skipped, already-decided or denied decision: 409; unknown id: 404 |
| Approve while live is off | state `approved`, result says it will not run; after 15s (three worker passes) still `approved`, `attempts 0`, no start time |
| Expiry | the approved, unexecuted decision became `expired` at its deadline ("approved but not executed"); approving it afterwards: 409 |
| Deny | state `denied` (distinct from `blocked`), terminal, reviewer and reason recorded |

Found while running it:

- **`--from-file` tokens carry a trailing newline.** A secret created from a
  file with `kubectl create secret --from-file` holds the newline, and the
  comparison then rejects the correct token. Create it with
  `--from-literal` or strip the newline.
- **The labels already stored were written by the old labeller.** They are
  now preserved and re-judged; see "Outcome labels and their history".

## Outcome labels and their history

How a decision turned out is judged by `classifyOutcome`. The logic has changed
twice, and a label is only evidence of what the labeller that wrote it saw, so
every label carries its labeller (`outcome_labeller`) and a change of logic
means a new version plus a migration, never an overwrite:

| Version | Judged | Superseded because |
|---|---|---|
| `v1-namespace-wide` | any recovery in the namespace, any fix-like change | issues 16-20: unrelated recoveries confirmed, unrelated changes contradicted |
| `v2-entity-ordered` | the symptom's own recovery, after the fix, by related changes only | the 45-minute window ran into the *next* fault's events, and fixes dated within the decision's own second were missed |
| `v3-incident-bounded` (current) | as v2, but the window ends at the incident's first recovery that held, and events up to 2s before the decision count | |

Migrations 012 (v1) and 013 (v2) copy the superseded labels to
`heal_outcome_history`, then clear the live label so the current labeller
re-judges the decision. Nothing is deleted. `GET /api/heal/calibration` reports
`labeller` and `superseded_labels_preserved`, and every decision in
`GET /api/heal/actions` carries `outcome`, `outcome_detail` and
`outcome_labeller`.

Found by checking the v2 labels against the events themselves (the chaos
harness runs faults minutes apart, so the next fault was inside the window):
the harness's restore, dated `12:36:29` against a decision at `12:36:29.4`, was
skipped, and a config change four minutes later was blamed. The first
re-judging also exposed a bug in the migration itself: both replicas ran 013 at
once and copied the same labels twice (69 rows instead of 46). Migrations now
take an advisory lock, migration 014 removed only the exact duplicate copies and
made "one label per decision per labeller" unique, and the init container runs
every migration in one locked session that stops at the first error.

**Verified on real faults (2026-10-09, after the 45-minute settle window).**
Four faults on the final build (redis, postgres and worker scaled to zero, a bad
api release) scored correct (0.81, 0.75, 1.00, 0.73). Their 20 decisions, all
judged by `v3-incident-bounded`, are `confirmed`, and two were checked against the
raw events: the fix (`scale 0 to 1`, and `deploy ... to the previous image`) is
dated to the same second as the decision, which v2 missed, and the symptom
recovered after it. Across all 53 labelled decisions: 50 confirmed, 3 unknown
(the redis-restart cases: "both the proposed action and another change preceded
the recovery"; "recovered but no change explains it"), 0 contradicted.

What this does *not* show: the chaos harness always restores exactly what
Chronicle proposed, so these faults can only reach the `confirmed` path. They
verify the labeller's mechanics on real events, not how often Chronicle's
proposals are right. The `contradicted` and `unknown` paths are covered by unit
and Postgres tests, and the evidence gate (20 decisive decisions, 80% precision)
is nowhere near met: 5 decisive so far.

## Worker and executors together (dry-run)

`internal/heal/worker_integration_test.go` runs the execution worker, the real
Postgres store and the real Kubernetes executor together. Kubernetes is a fake
client, so nothing can touch a cluster; "live" is switched on only inside the
test to see what the gates allow and refuse. It needs a database with the
migrations (`CHRONICLE_TEST_POSTGRES_URL=... go test ./internal/heal`).

| Scenario | Result asserted |
|---|---|
| approved decision, everything open | one write, `succeeded`, verification recorded; further passes do nothing |
| live off | nothing executes; the approved decision expires unexecuted |
| kill switch on | `blocked`, and still blocked after the switch is lifted |
| stale approval (deadline passed) | `expired`, executor never called |
| workload changed after approval | `failed` naming the current count; the newer count is untouched |
| two proposals for one outage | the second is `blocked` ("already acted on") |
| cooldown and global hourly cap | the second action on a workload and the third overall are `blocked` |
| interrupted run | left alone inside its time budget; afterwards `failed`, "not retried", and never run again |
| six workers racing for one decision | exactly one attempt, one write |
| denied and blocked decisions | never touched, whatever the clock |

It found a real race: a worker that lost the claim was told it had hit the
cooldown the winner had just started, and then wrote `blocked` over the
winner's `executing` row. A claim now first checks that the decision is still
claimable, and a completion is written only from the state it is made from
(a refusal only over `approved`, a result only over `executing`).

## Live RCA-to-remediation campaign (disposable cluster, 2026-10-17)

A fresh kind cluster with the `deploy/e2e` overlay: Keycloak 26.6.4 as the
identity provider, live healing on, approval required, test-only gates (see
`deploy/e2e/README.md`). `scripts/e2e-heal.sh` injected each fault, let the
deployed Chronicle diagnose it, tried to decide the proposal with no token
and with a non-approver, decided it as the approver, waited for the worker,
and checked recovery before the harness restored anything. Logs:
`chaos-results/heal-campaign-{1,2,3}.jsonl` (local, not committed).

| Fault | RCA (top cause, confidence) | Proposal | Decision | Final | What it shows |
|---|---|---|---|---|---|
| redis scaled to zero | scale:redis, 0.68 | restore_replicas | approved | **succeeded**; outage gone 66 s later, no harness help | the whole path works |
| redis scaled to zero again, 5 min later | scale:redis, **0.38 inconclusive** | blocked by the 0.65 floor | — | blocked | the floor refused an uncertain diagnosis. The uncertainty was a defect, since fixed (see *P0 hardening* below) |
| api bad release | deploy:api, 0.93 | rollback_deployment | approved | **succeeded**; outage gone 55 s later | template rollback works |
| redis deadlocked, strict PDB | became_unready:redis pod, **0.39** | blocked by the 0.80 floor | — | blocked | a bare failure 5 hops upstream scores low; the PDB path was not reached (tested on a real API server instead) |
| api two bad releases, 9.5 min after the rollback | deploy:api, 0.96 | rollback_deployment | approved | **blocked: cooldown** ("acted on 9m30s ago") | the per-workload cooldown, live |
| same, 25 min later | deploy:api (release **a**), 1.00 | rollback_deployment | approved | **failed: "refusing to roll back: api now runs release b"** | the executor's re-read refuses stale evidence |
| worker scaled to zero | scale:worker, 0.73 | restore_replicas | **denied** | denied; nothing ran | denial is final |
| api bad config | config_change:api, 0.92 | none ("no rule for config_change") | — | none | nothing is proposed without a rule (the harness first mislabelled this; fixed) |
| redis deadlocked | became_unready:redis pod, 0.39 | blocked by the 0.80 floor | — | blocked | as above |
| worker scaled to zero under a pod quota | scale:worker, 0.77 | restore_replicas | approved | **failed: "rollout did not complete: 0 of 1 replicas updated"** after 185 s | a remediation that runs and does not work is recorded as failed |

Every time a proposal was pending, the request without a token got **401** and
the non-approver (`vic`) **403**, and the proposal stayed pending; every
decision was recorded as `ana@chronicle.test` with her Keycloak subject,
whatever name the request body claimed. Every experiment's cluster returned to
its steady state. RCA ranked the true cause first in all ten.

**Final run, on the final build** (`chaos-results/heal-campaign-final.jsonl`,
the suite as it now stands): RCA top-1 correct in 8 of 8; 6 of 8 remediations
ended as pre-registered (restore and rollback succeeded and healed in 60 s
each; the quota-blocked restore and the stale-evidence rollback failed with
the expected reasons; the denial held; the config typo proposed nothing). The
2 mismatches are both deadlock steps, refused by the restart floor at 0.40 and
0.39. The `api-two-bad-releases` expectation was revised after the first live
run showed which release RCA names; that is recorded in `HealSuite`.

**What it did not show.** `restart_pod` never ran: its proposals were refused
by the floor. The controller's `ProgressDeadlineExceeded` fast path did not
fire (Kubernetes does not apply the progress deadline to a scale-up of a
complete Deployment), so the quota failure was caught by the verify timeout.
Outcome labels: Chronicle's own executions are labelled `unknown` by design
(finding 20), so no symptom check follows Chronicle's own actions yet.

## P0 hardening (2026-10-18)

**Quick safety fixes.** Findings 15, 24 and 25 (table above); an executor
refusal is recorded as `blocked`, releases its claim, and starts no cooldown;
every cluster-changing script and `make` target is pinned to a `kind-*` context
(`scripts/kube-context.sh`; `TestEveryClusterScriptPinsItsContext`).

**Symptom check** (migration `016`, `internal/heal/symptom.go`): described under
the safety layers. Live, on a disposable cluster:

| Fault | Execution | Symptom check |
|---|---|---|
| redis scaled to zero | restore succeeded | **succeeded**: "error_spike on frontend recovered at 21:53:08Z and stayed below 0.050 for 5m0s" |
| redis scaled to zero + a hidden deny-all NetworkPolicy (`restore-does-not-clear-symptom`) | restore succeeded, workload whole | **failed**: "no improvement within 2m0s of the action: error_spike on frontend is 1.000, limit 0.050" |

Without the check the second would have been recorded as a plain success.

**Repeat outages.** The same fault five minutes after the first scored 0.81,
then 0.38 (inconclusive), so the repeat was never proposed. The chaos log now
records each candidate's factors; the second analysis carried
`Remediation ×0.35: undoes an earlier change`, meaning it was taken for a repair
of the first outage. Three separate causes, each reproduced from real events
captured through the API (`internal/rca/testdata/repeat_redis_outage*.json`):

1. `failingAt` kept a pod "failing" for ever after a log line, because log lines
   have no recovery of their own. They now end at the next recovery in scope, as
   `recoveredAt` already treated them.
2. `collapseRepeats` merged identical log lines across episodes and kept the
   *earliest copy*, which was the earlier outage's: the new outage lost its own
   first effect. Copies further apart than the episode gap are now separate.
3. `revertingChanges` judged "was it already failing?" at the change's
   *ingestion* time. A scale to zero is stamped to the whole second and ingested
   0.66 s later, and the `Killing` warning for the pod it kills carries the same
   second, so the new fault looked like a repair of a failure it had just caused.
   It is now judged at the change's own time, less one second (Kubernetes
   timestamps are whole seconds).

A fourth rule, `markResolvedHistory`, sets aside alerts, log lines, failure
facts and statuses left over from an incident that had ended before this episode
began, so they no longer read as faint rivals. An incident ends at the last
recovery in the symptom's scope after which nothing there was failing and
nothing failed for the flap window; one alert resolving while another of the
same incident still fires is not the end. Changes are untouched: whether an old
change is still the cause stays `markRecovered`'s decision.

The test that matters is not "the repeat scores like the first" (each outage is
judged on its own timing, and they raised different alerts first) but "removing
the earlier outage does not change the diagnosis of the fresh one", within
0.005, on all three real captures (`internal/rca/testdata/repeat_redis_outage*.json`). Dev and held-out benchmark output is byte-identical
before and after, and 0 of 117 independent incidents changed.

## P1 hardening (2026-10-18)

**Rollback names an exact revision.** A rollback used to be judged by container
images, so a change to an env var, a sidecar or a resource limit could not be
rolled back, and a revert to the "previous" image could land on a different
template. Events now carry `from_template_hash` and `to_template_hash`
(`internal/revision`: sha256 of the whole pod template, without the
`pod-template-hash` label), and the executor restores the ReplicaSet whose
template has the hash the decision names. It refuses when that revision is no
longer retained, the Deployment is paused or not yet observed, a newer change
exists, or the template already is that revision. Events recorded before this
change carry no hashes and fall back to the image check.

**Which revision to return to.** The previous revision is used only if it ran
`HEAL_ROLLBACK_MIN_HEALTHY` (30 min) without a recorded failure
(`error_spike`, `latency_spike`, `became_unready`, `container_restart`,
`oom_kill`, `crash_loop`, `node_not_ready`, from Chronicle's own events; log
lines excluded; the first 2 minutes of a rollout are grace). If it did not, the
next older revision is checked, and so on through the retained history; if none
qualifies the decision is `blocked` with the reason. If the history cannot be
read, nothing is proposed. A revision that never failed is not proof that it
works with today's data (schemas, dependencies): that part stays with the
approver, which is why the proposal shows the chain it checked.

**Progressive delivery is left alone.** A Deployment owned by an Argo
Rollouts `Rollout`, or annotated or labelled for Flagger (including the
`-primary` copy), is recorded as managed by that controller and gets a GitOps-style proposal,
never a competing rollback. This detection is written from the controllers'
documented ownership and labels and is **not verified against a live Argo
Rollouts or Flagger installation**.

**`revert-bad-config`.** A `config_change` on a Deployment (its env or
mounted-object references changed) proposes `rollback_deployment`, confidence floor
0.90, one per hour, with the same revision rules. For a ConfigMap or Secret the
rule produces no proposal at all: Chronicle neither reads nor stores their
contents (a ConfigMap event holds key names and hashes; Secrets are not watched),
so there is nothing exact to restore. Benchmarks: the fixtures hold no revision evidence, so this
rule is not scored in the remedy sweep (it says so); on the independent set 4 of 117 incidents
now propose it, each a Deployment config change, and no RCA column changed.

**Restarts are proposal-only.** `HEAL_RESTART_EXECUTION_ENABLED` is `false` by
default: a restart is proposed, can be approved, and is refused at execution
("proposal-only") until the flag is set. The rules endpoint and console mark
such a rule. A proposal for a pod with no liveness probe recommends adding one.
When enabled, a restart is also *eligible* only when the pod has been unready
for `HEAL_RESTART_UNREADY_FOR` (5 min) and nothing in its container terminated
within `HEAL_RESTART_QUIET` (10 min); a pod in a Deployment that is mid-rollout
is rechecked in 30 seconds; a pod that is ready again or gone is a refusal. An
approved decision that is not yet eligible stays approved, claims nothing and
changes nothing, and runs when it becomes eligible if its approval has not
expired first.

**Failed rollouts fail fast.** When a Deployment cannot create replicas (a
`ReplicaFailure` condition, for example a quota or an admission rejection), the
rollout wait now stops with that message instead of running to the timeout.

**What the live run showed about restarts through RCA.** A deadlocked redis
(`CLIENT PAUSE`) is named correctly as its pod becoming unready, 14 seconds
before the symptom, but five hops upstream of it in the dependency graph, with
an event-type weight of 0.5 and a graph-distance weight of 0.33: confidence
**0.36**, "inconclusive". No corroboration weight can lift that to the 0.80
floor, so the RCA-to-restart proposal path stays closed for this class of
fault. It was not forced open: the floor is not lowered, and no extra scoring
was added to reach it. Raising it honestly needs outcome data (finding 22).

**Live, on the disposable cluster.** `api-bad-config`, decided by a real OIDC
approver (401 without a token, 403 for a non-approver): RCA 0.97 `config_change`,
`revert-bad-config` proposed, rolled back by the worker from revision
`e1816596` to `57d0b466`, rollout complete, and the symptom check **succeeded**
("error_spike on frontend recovered ... stayed below 0.050 for 5m0s"). Two earlier
attempts showed what the rule needs:

1. *Blocked*: the api's current revision had a recorded `latency_spike`, caused by
   redis being down in an earlier step, so no revision was "known to have run
   healthily". This is the safe answer, and also a coarse one: **any** recorded
   failure of the workload counts, including one an upstream outage caused, so a
   revision that lived through such an outage is never returned to. Attributing
   failures to their upstream cause would loosen it; that is not done.
2. *Ambiguous (0.49), nothing proposed*: a good release minutes before the bad one
   makes the RCA weigh two config changes. The suite now waits 16 minutes (past
   the longest alert lookback) after the good release. The RCA was not changed.

**Not done for P1:** restart execution through RCA live (blocked by the floor,
above); restart eligibility is tested against fake and real API servers, not a
live campaign; Argo Rollouts and Flagger detection is untested against a real
installation.

## Also fixed in this round

- **`TestPostgresRCAAndHistoricalGraph` failed against a real database.** Two
  causes. The RCA reader scanned the nullable `trace_id` and `correlation_key`
  columns into plain strings, so any event written without them broke analysis;
  the reads now `COALESCE` (production code). And the test was wired to a graph
  source production never uses, with a fixture that modelled an outgoing call
  where a dependent was meant; it now uses the production wiring and a faithful
  topology.
- **A Helm release appeared as an Argo CD application.** The graph read
  `app.kubernetes.io/instance` as Argo ownership, which Helm sets too (seen
  live: the `monitoring` chart became an Application node that never existed).
  `BuildArgoEdges` now reads Argo's own markers (the tracking-id annotation, the
  `argocd.argoproj.io/instance` label) and the instance label only when its value
  is an Application Argo CD reports. The graph builder, the Kubernetes collector
  and the healing executor share that one set (`graph.ApplicationSet`), so they
  agree. Without `ARGOCD_URL` no instance label is trusted at all. Checked on the
  kind cluster against a fake Argo CD API reporting one application: the graph
  gained that application and its workload, and the three `monitoring` Helm
  releases carrying the same label gained nothing.
- **A one-second blip tied with a real cause** (RCA episode onset), described in
  `internal/rca/benchdata/INDEPENDENT.md`.

## Tracked work

Not fixed, and not claimed as fixed.

| Item | Why it is open |
|---|---|
| Findings 5, 8, 9, 27 | partial; what remains is in the table above |
| Findings 14, 21-23, 30 | deferred; each needs data or a design decision |
| An Argo CD Application and a Helm release that share a *name* | the instance label cannot tell them apart; the workload is then treated as Argo-managed, which errs on the side of refusing to write it |
| The migration runner is a shell loop in an init container | a real migration tool with a version table would replace per-file idempotence |
| Kyverno policies are tested only on a one-node cluster | webhook latency, Kyverno outages and upgrades are untested |
| `restart_pod` has never run through RCA live | confirmed again in P1: a deadlocked redis scored 0.36 (five hops upstream, 0.5 type weight), under the 0.80 floor, so the floor refuses; the executor's eviction path is tested against a real API server instead. Lowering the floor is a calibration decision, not made here |
| Release-controller detection (Argo Rollouts, Flagger) | written from documented ownership and labels; no live installation was available to verify it |
| Live healing runs only in the disposable e2e cluster | `deploy/e2e` waives the observation and evidence gates there; the base manifests keep live off |

## Running the tests

| What | Command | Needs |
|---|---|---|
| everything that needs nothing | `go test ./...` | nothing |
| Postgres integration (heal, RCA) | `CHRONICLE_TEST_POSTGRES_URL=postgres://... go test ./internal/heal ./internal/rca` | a scratch database with all migrations applied |
| RCA-to-remediation campaign, live | `./scripts/e2e-heal.sh` | the `deploy/e2e` disposable cluster and its port-forwards |
| executors on a real cluster | `HEAL_TEST_KUBE_CONTEXT=kind-kyverno-test go test ./internal/heal -run RealCluster` | a disposable `kind-kyverno-test*` cluster |
| Kyverno policies, CLI | `make kyverno-test` | the `kyverno` CLI |
| Kyverno policies, real admission controller | `make kyverno-live-test` | docker, kind, kubectl, helm |
