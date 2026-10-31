# Chronicle documentation

| Document | Read it for |
|---|---|
| [architecture.md](architecture.md) | How a signal becomes a diagnosis, a proposal and, with approval, a remediation; and why each tool was kept, replaced or added |
| [deployment.md](deployment.md) | Taking Chronicle to a real cluster with GitOps, in dry-run, and what to change before turning on live healing |
| [production-readiness.md](production-readiness.md) | Service-level objectives, failure drills, scale measurements, and what is still open |

Detailed references live beside the code they describe:

| Document | Read it for |
|---|---|
| [internal/heal/README.md](../internal/heal/README.md) | Every healing safety gate, the configuration, the review findings and each hardening round |
| [internal/rca/benchdata/README.md](../internal/rca/benchdata/README.md) | How RCA accuracy is measured, and what the numbers cannot claim |
| [internal/chaos/README.md](../internal/chaos/README.md) | The fault-injection harness that tests RCA and healing on real faults |
| [deploy/e2e/README.md](../deploy/e2e/README.md) | The disposable cluster for live, approval-gated healing |
| [deploy/secrets/README.md](../deploy/secrets/README.md) | External secrets, rotation, and the AWS Secrets Manager setup |
