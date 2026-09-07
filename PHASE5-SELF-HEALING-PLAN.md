# Phase 5: Safe self-healing

Failure prediction is removed from the implementation roadmap. The former Phase 6 self-healing work becomes the new Phase 5.

## Goal

Given a Phase 4 RCA result, Chronicle should recommend and safely execute a recovery action, with dry-run mode enabled by default.

## Milestones

### 1. Action model and audit trail

- Add an `Action` model containing rule, target, confidence, reasoning, status, error, and timestamp.
- Store every proposed and completed action in PostgreSQL.
- Make actions idempotent and attach them to the triggering incident.

### 2. Rule engine

- Match rules against the top RCA candidate type.
- Require a minimum confidence threshold.
- Enforce one action per incident and per-rule hourly limits.
- Refuse to act when there are no candidates or confidence is inconclusive.

### 3. Dry-run actions

Start with recommendations only:

- Restart an unready pod.
- Increase memory after a high-confidence OOM kill.
- Recommend deployment rollback.

Each action must produce `WOULD HAVE RUN` output without changing Kubernetes.

### 4. Approval and safety controls

- Keep deployment rollbacks approval-gated.
- Add an explicit global dry-run flag.
- Add Kubernetes namespace and resource allowlists.
- Add rate limits, action timeouts, and loop protection.
- Record the proposed action before executing it.

### 5. Controlled live execution

- Enable only low-risk pod restarts first.
- Keep rollbacks and resource changes behind human approval.
- Provide a kill switch and verify the resulting cluster state.
- Run all actions against a test cluster before production use.

## Completion criteria

- Every action is visible in the audit log.
- Dry-run mode is safe for at least one observation period.
- No action loop occurs during repeated failures.
- Low-risk actions succeed on controlled incidents.
- Rollbacks remain approval-gated.

## Explicitly out of scope

- Failure prediction models and the former Phase 5 feature.
- Fully automatic deployment rollbacks.
- Unbounded or unrestricted Kubernetes writes.
