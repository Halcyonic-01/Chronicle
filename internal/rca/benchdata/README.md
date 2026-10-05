# RCA benchmark

Controlled, deterministic incidents with a known injected cause, run through the
real `Analyzer.Analyze` (event store → dependency graph → episode onset →
candidate collection → scoring → chain collapsing → ranking → confidence).

**This measures accuracy on a synthetic benchmark written by the people who wrote
the analyzer. It is not production accuracy.** Variants of one fault class (same
cause, different symptom or timing) are not independent samples.

## Sets

| Set | Incidents | Purpose |
|---|---|---|
| main (`Incidents()`) | 85 (70 known cause, 15 unknown) | the set the rules were developed against |
| held-out (`HoldoutIncidents()`) | 66 (54 known, 12 unknown) | a different application, written after the rules were settled and run once per version |

The held-out set is only held out for the changes made before it was run. The
chain-linking and root-separation fixes that came after were motivated by
ambiguous incidents present in both sets, so for those two changes it is not a
clean estimate. `two_ledger_deploys/W` analyses its symptom before the second
deploy happens, so that variant is not actually ambiguous (a fixture flaw, left
visible).

## Definitions

* **Top-1**: the first candidate has the truth's type and entity. **Top-3**: any of
  the first three does. Strict by type and entity; no partial credit.
* **Unknown-cause, strict**: zero candidates (the console's "NO REACHABLE CAUSE").
  **Declared**: the analyzer says no root cause (`Verdict == no_root_cause`, which
  the original analyzer could only say by returning nothing). **Abstained**: none,
  or confidence below 50%.
* Ground truth for an incident with several acceptable causes is a set; Top-1
  accepts any member.
* Observability profiles: `baseline` drops the event types the collectors did not
  emit before the Service/Node/ConfigMap/HPA collectors; `improved` keeps them.

## Commands

```bash
go test ./internal/rca ./internal/heal -run 'TestRCABenchmark|TestRCAHoldout|TestHealingBenchmark' -v -count=1
make rca-benchmark                 # new events (what the collectors emit now)
make rca-benchmark-baseline        # only the events emitted before
RCA_SENSITIVITY=1 go test ./internal/rca -run TestRCASensitivity -v -count=1
RCA_BENCH_OUT=out.json RCA_HOLDOUT_OUT=hold.json go test ./internal/rca -run 'TestRCABenchmark$|TestRCAHoldout' -v
```

To reproduce the **before** numbers, check out the original commit (`44056ad`) in
a separate worktree, copy `internal/rca/benchdata/` and `internal/rca/benchmark_test.go`
(and `internal/heal/benchmark_test.go`) in, and run the same commands. The saved
results are in `internal/rca/testdata/` (`before_*`, `after_*`, `holdout_*`).

## Results (measured)

| | Original code, original events | New code, new events |
|---|---|---|
| main Top-1 | 52/70 = 74.3% | 70/70 = 100.0% |
| main Top-3 | 55/70 = 78.6% | 70/70 = 100.0% |
| main unknown declared | 5/15 = 33.3% | 15/15 = 100.0% |
| held-out Top-1 | 39/54 = 72.2% | 53/54 = 98.1% |
| held-out Top-3 | 41/54 = 75.9% | 54/54 = 100.0% |
| held-out unknown declared | 4/12 = 33.3% | 12/12 = 100.0% |

Strict unknown-cause (zero candidates) is unchanged at 5/15 and 4/12 by design: a
failure's first appearance is still shown, but labelled as not a cause.

## What the benchmark cannot tell you

* A sensitivity sweep of 40 variations (decay, graph-distance slope, propagation,
  remediation, blast radius, episode gaps, event weights) changes none of the
  scores. The benchmark cannot distinguish good values from bad ones for these
  constants; it only fails to refute the shipped ones. No existing constant was
  tuned on it.
* Confidence values are ranking scores, not probabilities. The calibration table in
  the healing benchmark is on synthetic data and is not a calibration.
* Healing floors (`MinConfidence`) remain provisional. There is no real outcome
  data behind them; the simulated outcomes in the healing benchmark are built from
  the same ground truth as the incidents, so they exercise the outcome labeller
  and nothing more.

## Parameters introduced or changed

| Parameter | Old | New | Reason | Benchmark effect |
|---|---|---|---|---|
| `service_change` base weight | none (generic 0.20) | 0.90 | a routing change breaks every caller at once; set by analogy to `resource_change` | the sweep is flat at 0.5 to 0.9, so the value is not validated |
| `node_not_ready` base weight | none (0.20) | 0.85 | a lost node takes its pods; set by analogy to `oom_kill` | flat, not validated |
| `hpa_change` base weight | none (0.20) | 0.70 | automatic limits explain a scale, below manual changes | flat, not validated |
| propagation horizon `service_change`, `node_not_ready` | 2 min (default) | 2 min | unchanged in effect, now explicit | none |
| propagation horizon `hpa_change` | 2 min (default) | 15 min | an autoscaler reacts over minutes | none on this benchmark |
| `SettleAfterChange` | 2 min | 20 s | a restored break outranked a real cause 40 s later in a live run | 5 s, 20 s, 1 m and 2 m score identically, so the benchmark neither supports nor contradicts it |

Everything else is unchanged and now lives in `rca.Config` (`DefaultConfig()`).

## Hardening pass (independent findings F1 to F5)

General rules, no fixture names, all tunable in `rca.Config`:

* **F1** A scale that adds replicas to a running workload is not a cause
  (`not_root`, ×`NonRootFactor` 0.25), and `restore_replicas` is planned only for
  a scale to zero, the executor's own precondition.
* **F2** A failure on a pod its controller created within `NewPodWindow` (15 m),
  or on a pod being shut down within `TerminationGrace` (30 s) of its deletion, is
  an effect of whatever made the controller act. It stays ranked as evidence but
  is never the root; with nothing else, the verdict is `no_root_cause`.
* **F3** A change that explains none of the observed failure facts is held back
  (×`UncorroboratedFactor` 0.5) when those facts point at another change, or at
  an unrecorded rollout. A rival is only an alternative if it could explain the
  leading change's failures. A failure with no change behind it holds nothing back.
* **F4** A change or failure whose own incident recovered (every alert or failure
  closed by its own recovery, held for `FlapWindow` and not a recurrence of the
  same crash loop) before the current failure began is history (`not_root`).
* **F5** Strength times the leading candidate to the first failure it explains
  when that came before the onset, so evidence that bridges the gap keeps
  confidence up; an unbridged gap still decays.
* The verdict now judges the reported leader: if it cannot be a cause, the
  answer is `no_root_cause` however many weaker changes sit behind it.

Measured on these sets, using the same commands as above:

| | Before the pass | After the pass |
|---|---|---|
| main Top-1 / single-cause called ambiguous | 66/66 / 3 | 66/66 / 0 |
| main ambiguity recognised / false confident | 4/4 / 0 of 43 | 4/4 / 0 of 47 |
| main healing would-run / wrong / coverage | 16 / 0 / 16 of 41 | 19 / 0 / 19 of 41 |
| held-out Top-1 / single-cause called ambiguous | 51/52 / 1 | 52/52 / 0 |
| held-out ambiguity recognised / false confident | 1/2 / 1 of 33 | 1/2 / 1 of 38 |
| held-out healing would-run / wrong / unsafe | 13 / 0 / 1 | 14 / 0 / 1 |

Both sets were used while designing these rules, so this is not a held-out
estimate. The remaining held-out miss and its unsafe action are the documented
`two_ledger_deploys/W` fixture flaw. In the sensitivity sweep
(`testdata/sensitivity_postfix.json`), all four new parameters are STABLE, which
mostly means these sets barely exercise them; the regression tests are where
they are exercised. The post-fix independent run is in `INDEPENDENT.md`.


## Independent set and the shared evaluation

The final-evaluation set lives in `benchdata/independent/` and is run by
`benchdata/eval`, which also scores the development and held-out sets and drives
the sensitivity analysis, all with the same logic (including the dry-run healing
model). See `INDEPENDENT.md` for its first run and findings,
`OBSERVABILITY.md` for what RCA can and cannot see, and
`internal/rca/testdata/sensitivity.json` for the parameter sweep.

```bash
RCA_INDEPENDENT=1 go test ./internal/rca/benchdata/eval -run TestIndependentBenchmark -v -count=1   # final set: do not tune against it
RCA_SENSITIVITY=1 go test ./internal/rca/benchdata/eval -run TestSensitivity -v -count=1            # development + held-out only
go test ./internal/rca/benchdata/eval -run TestDevelopmentAndHeldOutReport -v -count=1
```
