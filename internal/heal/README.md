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
2. **Approval (HTTP).** Needs the approval token. It succeeds only for a
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
5. **Kubernetes RBAC.** The default deployment can write only its own Lease
   and checkpoint ConfigMap. Live write access comes from
   `deploy/heal-executor`, one namespaced Role per allowlisted namespace.
6. **Kyverno (optional, Audit).** `deploy/kyverno/chronicle-heal-guardrails.yaml`
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
| `HEAL_ACTION_TIMEOUT` / `HEAL_VERIFY_TIMEOUT` | `30s` / `3m` | Separate budgets for the write and its verification |
| `HEAL_OBSERVATION_STARTED_AT` | first decision, stored once | Overrides the stored start |
| `HEAL_MIN_DECISIVE_DECISIONS` / `HEAL_MIN_PRECISION` | `20` / `0.80` | Evidence gate |
| `HEAL_RULES` | built-in | JSON rules. `require_approve: false` is accepted and has no effect: automatic execution is not implemented |
| `ARGOCD_URL`, `ARGOCD_TOKEN` | unset | Optional Argo CD collector; its list of Applications also decides which `app.kubernetes.io/instance` labels mean Argo CD ownership |

**Secrets** are referenced by name and never logged:

- `chronicle-config/HEAL_APPROVAL_TOKEN` (approval; without it nothing can be approved);
- `ARGOCD_TOKEN`;
- `SLACK_WEBHOOK_URL`.

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
| 5 | Restart is a raw delete | **Partial** | Controller-owner, readiness and UID checks are done. Still deferred: the Eviction API (PDBs) and verifying the replacement became ready. |
| 6 | Execution inside HTTP | **Fixed** | Worker plus separate verify budget: `TestTheWorkerExecutesNothingWhileLiveIsOff`. Still deferred: post-action symptom check and auto-revert. |
| 7 | Image-only rollback | **Fixed** | `TestRollbackRestoresThePreviousPodTemplate` plus GitOps refusal |
| 8 | Memory bump unworkable | **Partial** | No longer planned without evidence, and the container is chosen by name. Still deferred: the collector recording the original limit, and a cumulative cap. |
| 9 | Restore ignores intent | **Partial** | GitOps-managed workloads are refused. Still deferred: using `changed_by` to recognise deliberate scale-to-zero. |
| 10 | Allowlist never matches | **Fixed** | `TestAllowlistMatchesTheOwningWorkload` |
| 11 | "Automatic" rules never run | **Fixed** | Every action requires approval, and this is reported truthfully: `TestEveryProposalNeedsApproval` |
| 12 | "Queued" was not a queue | **Fixed** | Worker plus truthful API and console states |
| 13 | Observation start drifts | **Fixed** | `TestPostgresObservationStartSurvivesPruning` |
| 14 | Evidence gate is global | Deferred | Needs labelled data per action type |
| 15 | Floor below the inconclusive line | Deferred | Latent with the defaults |
| 16 | Namespace-wide recovery | **Fixed** | `TestRecoveryElsewhereInTheNamespaceDoesNotConfirm`, `TestPostgresLabellingUsesTheSymptomAndOrder` |
| 17 | Event order ignored | **Fixed** | `TestRecoveryBeforeTheFixDoesNotConfirm` |
| 18 | Loose attribution | **Fixed** | `TestAttributionIsExact` |
| 19 | Unrelated changes contradict | **Fixed** | `TestAnUnrelatedChangeDoesNotContradict`. Relatedness comes from the entities the RCA weighed. |
| 20 | Self-confirming evidence | **Fixed** | `TestChroniclesOwnChangeDoesNotConfirm`, `TestPostgresEvidenceExcludesSelfExecutedOutages` |
| 21 | No rule for config/service/resource/node causes | Deferred | The template rollback could cover config and resource changes; that needs a rule design and evidence first. |
| 22 | 0.65 floor is uncalibrated | Deferred | Floors were deliberately not lowered. |
| 23 | Restart rule fires on any unready pod | Deferred | Needs a "running, unready, no restarts for N minutes" precondition. |
| 24 | Duplicate-insert race returns a phantom ID | Deferred | |
| 25 | Re-analysis hides the existing decision | Deferred | |
| 26 | Approver identity is self-declared | Deferred | Needs real authentication (OIDC) |
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
| Findings 14, 15, 21-26, 30 | deferred; each needs data or a design decision |
| An Argo CD Application and a Helm release that share a *name* | the instance label cannot tell them apart; the workload is then treated as Argo-managed, which errs on the side of refusing to write it |
| A failed precondition is recorded as `failed`, not `blocked` | a stale workload is a refusal, not an execution error; the state machine treats both as final |
| The migration runner is a shell loop in an init container | a real migration tool with a version table would replace per-file idempotence |
| Kyverno policies are tested only on a one-node cluster | webhook latency, Kyverno outages and upgrades are untested |
| Live execution is not enabled anywhere | the worker's real-cluster path (claim, limits, executor) has been tested in two halves, not together on a cluster |

## Running the tests

| What | Command | Needs |
|---|---|---|
| everything that needs nothing | `go test ./...` | nothing |
| Postgres integration (heal, RCA) | `CHRONICLE_TEST_POSTGRES_URL=postgres://... go test ./internal/heal ./internal/rca` | a scratch database with all migrations applied |
| executors on a real cluster | `HEAL_TEST_KUBE_CONTEXT=kind-kyverno-test go test ./internal/heal -run RealCluster` | a disposable `kind-kyverno-test*` cluster |
| Kyverno policies, CLI | `make kyverno-test` | the `kyverno` CLI |
| Kyverno policies, real admission controller | `make kyverno-live-test` | docker, kind, kubectl, helm |
