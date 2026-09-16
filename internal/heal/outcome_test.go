package heal

import (
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

func ev(typ, name, title, payload string) event.Event {
	return event.Event{
		Namespace: "default", EntityKind: "Deployment", EntityName: name,
		Type: typ, Title: title, Payload: []byte(payload), OccurredAt: time.Now(),
	}
}

func restoreDecision() *Action {
	return &Action{ActionType: ActionRestoreReplicas, Namespace: "default", Target: "redis"}
}

// The case the calibration data is for: Chronicle proposed restoring redis, a
// human did exactly that, and the symptom cleared.
func TestOutcomeConfirmedWhenTheProposedFixIsWhatWorked(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), []event.Event{
		ev("scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		ev("error_spike_resolved", "frontend", "high_error_rate resolved", `{}`),
	})
	if got != OutcomeConfirmed {
		t.Fatalf("expected confirmed, got %s (%s)", got, detail)
	}
}

// Recovery that followed a different change means the proposal was not what
// fixed it.
func TestOutcomeContradictedWhenSomethingElseFixedIt(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), []event.Event{
		ev("deploy", "api", "api image rolled back", `{}`),
		ev("error_spike_resolved", "frontend", "high_error_rate resolved", `{}`),
	})
	if got != OutcomeContradicted {
		t.Fatalf("expected contradicted, got %s (%s)", got, detail)
	}
}

// Reluctance is the point: an unattributable recovery is not evidence.
func TestOutcomeUnknownWhenRecoveryCannotBeAttributed(t *testing.T) {
	got, _ := classifyOutcome(restoreDecision(), []event.Event{
		ev("error_spike_resolved", "frontend", "high_error_rate resolved", `{}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("an unexplained recovery must not be counted as evidence, got %s", got)
	}
}

func TestOutcomeUnknownWhenNothingHappened(t *testing.T) {
	if got, _ := classifyOutcome(restoreDecision(), nil); got != OutcomeUnknown {
		t.Fatalf("expected unknown, got %s", got)
	}
}

// The fix was applied and the symptom did not clear. That is not a confirmation,
// but recovery detection is not reliable enough to call it a contradiction.
func TestOutcomeUnknownWhenTheFixDidNotVisiblyHelp(t *testing.T) {
	got, _ := classifyOutcome(restoreDecision(), []event.Event{
		ev("scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("expected unknown, got %s", got)
	}
}

// A scale in the wrong direction is the outage, not the remedy.
func TestAScaleToZeroIsNotARestore(t *testing.T) {
	if remediates(restoreDecision(), ev("scale", "redis", "redis scaled from 1 to 0", `{"old_replicas":1,"new_replicas":0}`)) {
		t.Fatal("scaling to zero must not count as performing the restore")
	}
}

func TestRemediationMustMatchTheNamedTarget(t *testing.T) {
	other := ev("scale", "worker", "worker scaled from 0 to 2", `{"old_replicas":0,"new_replicas":2}`)
	if remediates(restoreDecision(), other) {
		t.Fatal("a restore of a different workload is not this decision's fix")
	}
	wrongNamespace := ev("scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`)
	wrongNamespace.Namespace = "staging"
	if remediates(restoreDecision(), wrongNamespace) {
		t.Fatal("a restore in another namespace is not this decision's fix")
	}
}

// A decision names a workload; the follow-up may name a Pod it owns.
func TestRestartIsMatchedOnThePodItNamed(t *testing.T) {
	a := &Action{ActionType: ActionRestartPod, Namespace: "default", Target: "api-59cd-2sn5k"}
	if !remediates(a, ev("resource_deleted", "api-59cd-2sn5k", "pod deleted", `{}`)) {
		t.Fatal("deleting the named pod is the restart being performed")
	}
	if remediates(a, ev("resource_deleted", "api-other-pod", "pod deleted", `{}`)) {
		t.Fatal("deleting a different pod is not this decision's fix")
	}
}

// Both of these produced false contradictions on real data: a scale-down is the
// outage, and a vanishing short-lived Pod is churn.
func TestScalingDownIsNotAFix(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), []event.Event{
		ev("scale", "redis", "redis scaled from 1 to 0", `{"old_replicas":1,"new_replicas":0}`),
		ev("error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got == OutcomeContradicted {
		t.Fatalf("a scale to zero is the fault, not the remedy: %s", detail)
	}
}

func TestAVanishingThrowawayPodIsNotAFix(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), []event.Event{
		ev("resource_deleted", "healthcheck", "healthcheck deleted", `{}`),
		ev("error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got == OutcomeContradicted {
		t.Fatalf("pod churn must not be read as somebody fixing the problem: %s", detail)
	}
}

// Scaling up still counts, so a genuine alternative fix is not lost.
func TestScalingUpElsewhereStillCounts(t *testing.T) {
	got, _ := classifyOutcome(restoreDecision(), []event.Event{
		ev("scale", "worker", "worker scaled from 1 to 3", `{"old_replicas":1,"new_replicas":3}`),
		ev("error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got != OutcomeContradicted {
		t.Fatalf("a real alternative fix should still contradict, got %s", got)
	}
}
