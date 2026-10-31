# GitOps end-to-end environment

**A throwaway one-node kind cluster only** (`kind-chronicle-gitops`). It runs
Argo CD, an in-cluster Gitea, a small Prometheus, the demo app (managed by Argo CD
from Gitea) and Chronicle with live healing and Git remediation on.

```bash
scripts/e2e-gitops.sh up     # about 10 minutes
scripts/e2e-gitops.sh run    # about 15 minutes
scripts/e2e-gitops.sh down
```

`run` checks the whole Git-based remediation path:

1. A bad release is committed to Git, and Argo CD syncs it; the api breaks.
2. Chronicle raises an alert and diagnoses the cause.
3. Because the api is managed by Argo CD, the remedy is a `git_revert`, not a write
   to the cluster, and it waits for approval.
4. Before approval nothing exists anywhere: no pull request, no change.
5. One person approves; the worker opens a pull request that changes only the file
   the bad release changed. `main` and the cluster are still untouched.
6. A reviewer merges it; Chronicle sees the merge, then Argo CD's sync of it, and
   the api runs the good release again.
7. The symptom check confirms the alert cleared and stayed clear.
8. The Deployment's field managers show that only Argo CD and Kubernetes wrote it.

Test-only settings (`chronicle/env.yaml`): live healing on, the observation and
evidence gates waived (a new cluster has no history), the shared approval token
instead of OIDC (a Git revert needs one approver), and Argo CD's admin session
token for Chronicle's read-only polling. Confidence floors, the allowlists, the
approval itself and every other gate are unchanged. Generated passwords and tokens
live in `$TMPDIR/chronicle-e2e-gitops` and are never printed; `down` removes them.
