package chaos

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

func healHarness(t *testing.T, workerEnd string) *harness {
	h := newHarness(t, nil)
	h.api.signals = []rca.Signal{signal("alert-1", "error_spike", "frontend", 30*time.Second)}
	h.api.results["alert-1"] = answer(rca.VerdictRootCause, "scale", "redis", 0.82)
	h.api.proposal = &heal.Action{ID: "act-1", Rule: "restore-scaled-down-workload", ActionType: heal.ActionRestoreReplicas,
		Namespace: "default", Target: "redis", Proposed: true, Status: heal.StatusWouldRun, Approval: heal.ApprovalPending, State: "pending_approval"}
	h.api.workerEnd = workerEnd
	h.api.tokens = map[string]int{"approver": http.StatusOK, "viewer": http.StatusForbidden}
	h.runner.Healer = &Healer{Approver: staticToken("approver"), NonApprover: staticToken("viewer"), Terminal: 2 * time.Minute}
	return h
}

func (h *harness) healRun(t *testing.T, want HealExpectation) *HealOutcome {
	t.Helper()
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0))
	exp.Heal = &want
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{Seed: 1}); err != nil {
		t.Fatal(err)
	}
	recs := h.records(t)
	if recs[1].Heal == nil || recs[1].Heal.Expect != want.Expect {
		t.Fatal("the remediation expectation must be on record before the fault")
	}
	if recs[2].Result.Heal == nil {
		t.Fatal("no remediation outcome recorded")
	}
	return recs[2].Result.Heal
}

// The decision is attempted first with no identity and with a non-approver,
// both refused, and only then made by the approver; the worker's result is
// what is recorded.
func TestHealModeDecidesAsAnApproverAfterTheRefusals(t *testing.T) {
	h := healHarness(t, heal.StatusSucceeded)
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: heal.StatusSucceeded})
	if want := []string{"approve:", "approve:viewer", "approve:approver"}; len(h.api.attempts) != 3 ||
		h.api.attempts[0] != want[0] || h.api.attempts[1] != want[1] || h.api.attempts[2] != want[2] {
		t.Fatalf("decision attempts %v, want %v", h.api.attempts, want)
	}
	if got.NoTokenStatus != 401 || got.NonApproverStatus != 403 || !got.PendingAfterProbe {
		t.Fatalf("refusals not recorded: %+v", got)
	}
	if got.Final != heal.StatusSucceeded || !got.HealedWithoutRestore || !got.Matches || got.DecidedBy != "ana@chronicle.test" {
		t.Fatalf("unexpected outcome %+v", got)
	}
}

func TestHealModeReportsAnOutcomeThatDiffersFromTheExpectation(t *testing.T) {
	h := healHarness(t, heal.StatusFailed)
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: heal.StatusSucceeded})
	if got.Matches || got.Final != heal.StatusFailed {
		t.Fatalf("a failed remediation was reported as expected: %+v", got)
	}
}

// A non-approver whose decision is accepted is a security failure, whatever
// the remediation then does.
func TestHealModeFlagsANonApproverWhoWasAccepted(t *testing.T) {
	h := healHarness(t, heal.StatusSucceeded)
	h.api.tokens["viewer"] = http.StatusOK
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: heal.StatusSucceeded})
	if got.Matches || got.Problem == "" {
		t.Fatalf("an accepted non-approver went unnoticed: %+v", got)
	}
}

func TestHealModeWithoutAProposalIsNone(t *testing.T) {
	h := healHarness(t, heal.StatusSucceeded)
	h.api.proposal = nil
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: "none"})
	if got.Final != "none" || !got.Matches || len(h.api.attempts) != 0 {
		t.Fatalf("got %+v, attempts %v", got, h.api.attempts)
	}
}

func TestHealModeDenyIsRecorded(t *testing.T) {
	h := healHarness(t, heal.StatusSucceeded)
	got := h.healRun(t, HealExpectation{Decide: "deny", Expect: heal.StatusDenied})
	if got.Final != heal.StatusDenied || !got.Matches || got.HealedWithoutRestore {
		t.Fatalf("got %+v", got)
	}
	if replicas(t, h.client, "redis") != 1 {
		t.Fatal("the harness must still restore after a denial")
	}
}

func TestHealSuiteIsWellFormed(t *testing.T) {
	seen := 0
	for _, e := range HealSuite() {
		if e.Name == "" || e.Heal == nil || e.Inject == nil {
			t.Errorf("incomplete heal experiment %+v", e.Name)
		}
		if e.Heal.Decide != "approve" && e.Heal.Decide != "deny" {
			t.Errorf("%s: decide %q", e.Name, e.Heal.Decide)
		}
		seen++
	}
	if seen != 9 {
		t.Fatalf("suite has %d steps", seen)
	}
}

// The right final state for the wrong reason is not a pass.
func TestHealModeRequiresTheExpectedReason(t *testing.T) {
	h := healHarness(t, heal.StatusBlocked)
	h.api.proposal.Status, h.api.proposal.State, h.api.proposal.Result = heal.StatusBlocked, heal.StatusBlocked, "confidence 0.38 is below 0.65"
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: heal.StatusBlocked, Reason: "cooldown"})
	if got.Matches {
		t.Fatalf("blocked by the confidence floor was accepted as blocked by the cooldown: %+v", got)
	}
}

// The analysis records a skipped decision when no rule applies; that is
// "nothing proposed", with its reason kept.
func TestASkippedDecisionMeansNothingWasProposed(t *testing.T) {
	h := healHarness(t, heal.StatusSucceeded)
	h.api.proposal.Status, h.api.proposal.State, h.api.proposal.Proposed = heal.StatusSkipped, heal.StatusSkipped, false
	h.api.proposal.Result = `no rule for cause type "config_change"`
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: "none"})
	if got.Final != "none" || !got.Matches || got.Result == "" || len(h.api.attempts) != 0 {
		t.Fatalf("got %+v, attempts %v", got, h.api.attempts)
	}
}

// The quota holds the namespace at its other pods, so the excepted workload
// cannot come back, and its restore removes it.
func TestCapPodsCountsEverythingButTheTargetAndIsRemoved(t *testing.T) {
	clk := &clock{t: t0}
	c, client, _ := testCluster(clk, victim()...)
	ctx := context.Background()
	if err := CapPods("chaos-pod-cap", "worker").Do(ctx, c); err != nil {
		t.Fatal(err)
	}
	q, err := client.CoreV1().ResourceQuotas("default").Get(ctx, "chaos-pod-cap", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := q.Spec.Hard[corev1.ResourcePods]; got.Value() != 4 {
		t.Fatalf("five victim pods less the worker's should cap at 4, got %s", got.String())
	}
	if err := RemoveQuota("chaos-pod-cap").Do(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := RemoveQuota("chaos-pod-cap").Do(ctx, c); err != nil {
		t.Fatal("removing an absent quota must be harmless:", err)
	}
}

// The harness waits for the symptom check before it lets the run restore, and
// records the verdict against the pre-registered one.
func TestHealModeWaitsForTheSymptomCheckAndComparesItToTheExpectation(t *testing.T) {
	for _, c := range []struct {
		end, want string
		matches   bool
	}{
		{heal.SymptomFailed, heal.SymptomFailed, true},
		{heal.SymptomSucceeded, heal.SymptomSucceeded, true},
		{heal.SymptomFailed, heal.SymptomSucceeded, false}, // restored fine, but the alert did not clear
		{heal.SymptomUnknown, heal.SymptomSucceeded, false},
	} {
		h := healHarness(t, heal.StatusSucceeded)
		h.api.symptomEnd = c.end
		h.runner.Healer.SymptomWait = 10 * time.Minute
		got := h.healRun(t, HealExpectation{Decide: "approve", Expect: heal.StatusSucceeded, Symptom: c.want})
		if got.SymptomCheck != c.end || got.Matches != c.matches {
			t.Errorf("end %s want %s: recorded %q, matches %v (want %v) %+v", c.end, c.want, got.SymptomCheck, got.Matches, c.matches, got)
		}
	}
}

// A check that never concludes is reported as such, not as a pass.
func TestHealModeReportsACheckThatNeverConcludes(t *testing.T) {
	h := healHarness(t, heal.StatusSucceeded)
	h.api.symptomEnd = heal.SymptomSucceeded
	h.api.listed = -1000 // never reaches the final listing
	h.runner.Healer.SymptomWait = 30 * time.Second
	got := h.healRun(t, HealExpectation{Decide: "approve", Expect: heal.StatusSucceeded, Symptom: heal.SymptomSucceeded})
	if got.Matches || got.SymptomCheck != heal.SymptomPending {
		t.Fatalf("an unfinished check was accepted: %+v", got)
	}
}

// A failed symptom check does not also report a steady-state problem: the
// double-fault is expected to leave the system unhealthy until the harness restores.
func TestAFailedSymptomCheckIsNotAlsoASteadyStateProblem(t *testing.T) {
	broken := false
	h := healHarness(t, heal.StatusSucceeded)
	h.api.symptomEnd = heal.SymptomFailed
	h.runner.Healer.SymptomWait = 10 * time.Minute
	h.runner.Steady = steadyScript(func() (bool, string) { return !broken, "frontend error rate 0.40" })
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0))
	exp.Inject = append([]Action{step{"break", func(context.Context, *Cluster) error { broken = true; return nil }}}, exp.Inject...)
	exp.Restore = []Action{step{"fix", func(context.Context, *Cluster) error { broken = false; return nil }}}
	exp.Heal = &HealExpectation{Decide: "approve", Expect: heal.StatusSucceeded, Symptom: heal.SymptomFailed}
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{Seed: 1}); err != nil {
		t.Fatal(err)
	}
	got := h.records(t)[2].Result.Heal
	if got == nil || got.Problem != "" || !got.Matches || got.HealedWithoutRestore {
		t.Fatalf("got %+v", got)
	}
}

func TestTheDoubleFaultInjectsThePolicyBeforeTheScale(t *testing.T) {
	e := RestoreDoesNotClearSymptom()
	if len(e.Inject) != 2 || !strings.Contains(e.Inject[0].Describe(), "NetworkPolicy") && !strings.Contains(e.Inject[0].Describe(), "deny") {
		t.Fatalf("inject steps: %v", e.Inject)
	}
	if !strings.Contains(e.Inject[1].Describe(), "scale") {
		t.Fatalf("the scale must come second so it is the only recorded change: %v", e.Inject[1].Describe())
	}
}
