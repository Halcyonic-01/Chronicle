# Independent RCA benchmark: first run and findings

**These are controlled-benchmark results, not production accuracy.** The set was
written by the same engineer who wrote the analyzer, on a synthetic application.

## How independent it is, and how much

* Written after the RCA rules were settled, on an application (a streaming-media
  platform across two namespaces, with replicas and external dependencies) that
  shares no names, topology or code with the development and held-out sets.
* Never run through RCA while being written: only its structure was checked
  (ground truth present and consistent, every cause in its stream, strictly
  before its symptom, and reachable upstream).
* Not independent of its author. Scenario shapes come from the same mind that
  designed the analyzer. A set written by someone else, or from real incidents,
  would be stronger.
* One fixture was corrected before the run: a "90s late" delayed-observation
  incident was mislabeled outside the contract, found by a timing property test
  (not by RCA output on this set). It was relabeled and a 120s variant added.
* Between writing the set and running it, RCA gained the ambiguous verdict and a
  healing guard (justified from the development sets and code review, see below).
  Nothing was changed in RCA after seeing independent results.

## Frozen code and the single run

RCA source hash at the run: `e43c3425e9c4f0b9` (non-test files of `internal/rca`
plus `internal/heal/action.go` and `outcome.go`). Per-incident rows:
`internal/rca/testdata/independent_first_run.json`. Run it with
`RCA_INDEPENDENT=1 go test ./internal/rca/benchdata/eval -run TestIndependentBenchmark -v`.
**Any rerun after a fix is a "post-fix independent benchmark", not an untouched one.**

## Composition

117 incidents: 84 single-cause, 7 ambiguous, 26 with no recorded root cause. 111
sit inside the analyzer's documented contract and 6 outside it (a cause older than
the lookback window, a cause stamped at the same instant as its symptom, an event
ingested more than 30s after the symptom was). Categories: A known observable 31,
B previously unobserved 25, C rival causes 21, D root-versus-effect 8, E no root
cause 12, T timing/simultaneity/delayed 20.

## Results (first run)

Headline: the 111 incidents inside the documented contract.

| Metric | Result |
|---|---|
| Top-1, single-cause | 76/78 = 97.4% |
| Top-3, single-cause | 78/78 = 100.0% |
| No-root-cause declared | 22/26 = 84.6% (4 invented a cause) |
| Ambiguity recognised | 7/7 = 100.0% |
| Explanation communicates doubt and names every plausible cause | 7/7 = 100.0% |
| False confident diagnoses (verdict root_cause, confidence at least 0.7, wrong) | 1 of 51 confident verdicts |
| Mean confidence: correct / wrong / ambiguous | 0.772 (n=76) / 0.515 (n=2) / 0.497 (n=7) |

Counting all 117 (including the 6 outside the contract): Top-1 77/84 = 91.7%,
Top-3 79/84 = 94.0%, 3 false confident diagnoses. The 6 outside-contract incidents
alone: 1 of 6 correct.

The two in-contract Top-1 "misses" are `benign_deploy_then_oom_limit/A,B`: a
harmless deploy outranked the real limit change, but the verdict was `ambiguous`
and the real cause was second.

By category (Top-1 / no-root declared): A 31/31; B 11/11 and 14/14 declared;
C 13/15 (5 single-cause incidents called ambiguous, 6/6 ambiguity recognised);
D 8/8; E 8/12 declared; T 14/19.

### Ambiguous incidents

Each has two plausible roots. In all 7: the verdict is `ambiguous`, the rival is
listed as an alternative, the explanation says the evidence cannot separate them
and names both, and healing declines to act. Confidence is 0.45 to 0.53, against
0.842 for confident single-cause answers. Single-cause incidents with a harmless
decoy nearby are also called ambiguous in 5 cases (over-ambiguity; see F3).

## Healing (dry-run; no outcome is simulated here)

In-contract incidents: 29 would-run proposals (19 rollback, 8 restore replicas, 2
bump memory); 28 address the actual root, **1 is wrong** (F1); unsafe-action rate
1/29 = 3.4%. Coverage 28/53 remediable incidents = 52.8%. Correctly skipped: 57 (7
ambiguous, 25 with no root cause, 25 whose root has no remedy). Missed actionable
25: 7 inconclusive, 7 whose root has no rule, 6 below a rule floor, 5 declined as
ambiguous. The engine would have acted on 0 of 7 ambiguous incidents and on 1 of
26 no-root-cause incidents (the same wrong proposal).

## Findings of the first run (documented, not fixed at the time; see the post-fix section)

* **F1. A capacity increase is treated as a cause.** `capacity_increase_decoy/A,B`:
  a cache scale-up from 1 to 2, 15 seconds before unexplained errors, is reported
  as the root cause at confidence 0.61 and 0.74, and the healing rule
  `restore-scaled-down-workload` would run on it (wrong action). The recovery
  filter excludes only a scale up from zero, and the rule matches any `scale`.
* **F2. A missing cause turns an effect into a confident wrong root.** When the
  cause is not in the record (`delayed/120s-late`, `replayed-after-7min-gap`, both
  outside the contract), the downstream `container_restart` is reported as the root
  at confidence 0.70. A restart can be a genuine root (a crash with no change) or
  an effect of a change that was not recorded; the analyzer cannot tell which.
* **F3. No corroboration.** A harmless change near the real cause makes the verdict
  `ambiguous` even when only the real cause has failure events behind it
  (`benign_upstream_deploy`, `benign_deploy_then_oom_limit`). The ranking does not
  prefer a candidate with explained effects over a bare change.
* **F4. A recovered earlier incident resurfaces.** `resolved_incident_decoy/A,B`:
  a deploy from 8 minutes earlier, whose incident recorded a recovery, is still a
  candidate (confidence 0.06 to 0.20, verdict `inconclusive`).
* **F5. Confidence collapses near the edge of the lookback.** A correct cause
  600 seconds before an `error_spike` (or 890 before a `log_error`) is ranked first
  but at confidence 0.05 and verdict `inconclusive`, because of time decay.
* **F6. The contract labels were approximate.** `became_unready-after-330s` was
  labeled outside the contract by the lookback alone but was found, because
  restarts on the path extend the episode window.

## Post-fix independent benchmark (hardening pass)

**This is a post-fix rerun, not an untouched estimate.** F1 to F5 were fixed with
general rules after the findings above were known, so for those five behaviours
this set now measures a fix designed with them in mind. No fixture was changed,
no rule names a fixture, nothing was tuned on this set, and the code was frozen
before this single run: RCA source hash `38cf5a83b12ecbfa`, rows in
`internal/rca/testdata/independent_postfix_run.json`. The rules were checked
only on the development and held-out sets and on new regression tests with
their own topology (`internal/rca/hardening_test.go`,
`internal/heal/hardening_test.go`).

Inside the documented contract (111 incidents):

| Metric | First run (`e43c3425e9c4f0b9`) | Post-fix (`38cf5a83b12ecbfa`) |
|---|---|---|
| Top-1, single-cause | 76/78 | 76/78 |
| Top-3, single-cause | 78/78 | 78/78 |
| Single-cause called ambiguous | 5 | 2 |
| No-root-cause declared | 22/26 | 26/26 |
| Ambiguity recognised | 7/7 | 7/7 |
| False confident diagnoses | 1 of 51 | 0 of 59 |
| Mean confidence: correct / wrong / ambiguous | 0.772 / 0.515 / 0.497 | 0.825 / 0.535 / 0.521 |
| Healing would-run / wrong / unsafe | 29 / 1 / 1 | 31 / 0 / 0 |
| Healing coverage of remediable incidents | 28/53 | 31/53 |

All 117: false confident diagnoses 3 to 0; Top-1 77/84 unchanged. 12 incidents
changed and none got worse.

| Finding | Status | What the run shows |
|---|---|---|
| F1 scale-up as a cause | FIXED | `capacity_increase_decoy/A,B` now `no_root_cause`; the wrong `restore_replicas` is gone |
| F2 effect promoted when the cause is missing | PARTIALLY FIXED | `delayed/120s-late` and `replayed-after-7min-gap` now `no_root_cause` instead of a 0.70 wrong root; still only when the new pod's creation is inside the window |
| F3 no corroboration | PARTIALLY FIXED | `benign_upstream_deploy/A,B,D` now correct `root_cause`; `benign_deploy_then_oom_limit/A,B` stay ambiguous, because both changes are on the failing workload and either could explain its OOMs |
| F4 recovered incident resurfaces | FIXED | `resolved_incident_decoy/A,B` now `no_root_cause` |
| F5 confidence collapse near the lookback edge | PARTIALLY FIXED | `error_spike-after-600s` and `log_error-after-890s` go from 0.05 inconclusive to 0.99 and 1.00, because their effects began at once; a cause with nothing observed between it and the alert still decays, by design |
| F6 approximate contract labels | UNCHANGED | a labelling note, not an analyzer fault |

Still not as expected (7 of 117): the two `benign_deploy_then_oom_limit` above, and
five outside the contract. `error_spike-after-630s` and `log_error-after-930s` have
their cause beyond the lookback and their rollout's pod created outside the
window, so a restart is named at 0.04 (inconclusive). The two delayed incidents
and the same-instant one have no visible cause and are now declared
`no_root_cause` rather than guessed.

## What was not asked of this set

It does not measure healing outcomes (none are simulated), calibration against
real incidents, or behaviour under production event volume.
