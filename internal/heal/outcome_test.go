package heal

import (
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

var base = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func ev(typ, name, title, payload string) event.Event {
	return evAt(0, typ, name, title, payload)
}

func evAt(sec int, typ, name, title, payload string) event.Event {
	return event.Event{
		Namespace: "default", EntityKind: "Deployment", EntityName: name,
		Type: typ, Title: title, Payload: []byte(payload), OccurredAt: base.Add(time.Duration(sec) * time.Second),
	}
}

func restoreDecision() *Action {
	return &Action{ActionType: ActionRestoreReplicas, Namespace: "default", Target: "redis", Workload: "redis",
		Related: []string{"frontend", "api", "worker", "redis"}}
}

var frontend = SymptomRef{Namespace: "default", Name: "frontend"}

// The case the calibration data is for: Chronicle proposed restoring redis, a
// human did exactly that, and the symptom cleared.
func TestOutcomeConfirmedWhenTheProposedFixIsWhatWorked(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(40, "error_spike_resolved", "frontend", "high_error_rate resolved", `{}`),
	})
	if got != OutcomeConfirmed {
		t.Fatalf("expected confirmed, got %s (%s)", got, detail)
	}
}

// Recovery that followed a different change to something the analysis weighed
// means the proposal was not what fixed it.
func TestOutcomeContradictedWhenSomethingElseFixedIt(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "deploy", "api", "api image rolled back", `{}`),
		evAt(40, "error_spike_resolved", "frontend", "high_error_rate resolved", `{}`),
	})
	if got != OutcomeContradicted {
		t.Fatalf("expected contradicted, got %s (%s)", got, detail)
	}
}

// Reluctance is the point: an unattributable recovery is not evidence.
func TestOutcomeUnknownWhenRecoveryCannotBeAttributed(t *testing.T) {
	got, _ := classifyOutcome(restoreDecision(), frontend, []event.Event{
		ev("error_spike_resolved", "frontend", "high_error_rate resolved", `{}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("an unexplained recovery must not be counted as evidence, got %s", got)
	}
}

func TestOutcomeUnknownWhenNothingHappened(t *testing.T) {
	if got, _ := classifyOutcome(restoreDecision(), frontend, nil); got != OutcomeUnknown {
		t.Fatalf("expected unknown, got %s", got)
	}
}

// The fix was applied and the symptom did not clear. That is not a confirmation,
// but recovery detection is not reliable enough to call it a contradiction.
func TestOutcomeUnknownWhenTheFixDidNotVisiblyHelp(t *testing.T) {
	got, _ := classifyOutcome(restoreDecision(), frontend, []event.Event{
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
	pod := func(name string) event.Event {
		e := ev("resource_deleted", name, "pod deleted", `{}`)
		e.EntityKind = "Pod"
		return e
	}
	if !remediates(a, pod("api-59cd-2sn5k")) {
		t.Fatal("deleting the named pod is the restart being performed")
	}
	if remediates(a, pod("api-other-pod")) {
		t.Fatal("deleting a different pod is not this decision's fix")
	}
}

// Both of these produced false contradictions on real data: a scale-down is the
// outage, and a vanishing short-lived Pod is churn.
func TestScalingDownIsNotAFix(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "redis", "redis scaled from 1 to 0", `{"old_replicas":1,"new_replicas":0}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got == OutcomeContradicted {
		t.Fatalf("a scale to zero is the fault, not the remedy: %s", detail)
	}
}

func TestAVanishingThrowawayPodIsNotAFix(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "resource_deleted", "healthcheck", "healthcheck deleted", `{}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got == OutcomeContradicted {
		t.Fatalf("pod churn must not be read as somebody fixing the problem: %s", detail)
	}
}

// Scaling up something the analysis weighed still counts, so a genuine
// alternative fix is not lost.
func TestScalingUpARelatedWorkloadStillCounts(t *testing.T) {
	got, _ := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "worker", "worker scaled from 1 to 3", `{"old_replicas":1,"new_replicas":3}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got != OutcomeContradicted {
		t.Fatalf("a real alternative fix should still contradict, got %s", got)
	}
}

// Issue 16: a pod of some other service becoming ready is not this symptom
// recovering, however busy the namespace is.
func TestRecoveryElsewhereInTheNamespaceDoesNotConfirm(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(40, "became_ready", "billing-7d9f-abcde", "billing started serving", `{}`),
		evAt(50, "latency_spike_resolved", "billing", "billing latency resolved", `{}`),
	})
	if got == OutcomeConfirmed {
		t.Fatalf("an unrelated recovery confirmed the decision: %s", detail)
	}
}

// The workload the action restores coming back counts as its recovery.
func TestRecoveryOfTheTargetWorkloadCounts(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(30, "became_ready", "redis-6d79c4d8db-8jp47", "redis started serving", `{}`),
	})
	if got != OutcomeConfirmed {
		t.Fatalf("expected confirmed, got %s (%s)", got, detail)
	}
}

// Issue 17: a recovery that came before the fix was not caused by it.
func TestRecoveryBeforeTheFixDoesNotConfirm(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(5, "error_spike_resolved", "frontend", "resolved", `{}`),
		evAt(30, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("a recovery before the fix must not confirm it, got %s (%s)", got, detail)
	}
	got, detail = classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(5, "error_spike_resolved", "frontend", "resolved", `{}`),
		evAt(30, "deploy", "api", "api redeployed", `{}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("a recovery before another change must not contradict, got %s (%s)", got, detail)
	}
}

// Issue 18: only the change the decision proposed counts as it being performed.
func TestAttributionIsExact(t *testing.T) {
	rollback := &Action{ActionType: ActionRollbackDeployment, Namespace: "default", Target: "api",
		Payload: []byte(`{"old_image":"api:v1","new_image":"api:v2"}`)}
	if !remediates(rollback, ev("deploy", "api", "api deployed", `{"old_image":"api:v2","new_image":"api:v1"}`)) {
		t.Error("returning to the previous image is the rollback")
	}
	if remediates(rollback, ev("deploy", "api", "api deployed", `{"old_image":"api:v2","new_image":"api:v3"}`)) {
		t.Error("a roll-forward is not the proposed rollback")
	}
	bump := &Action{ActionType: ActionBumpMemory, Namespace: "default", Target: "api-1", Payload: []byte(`{"owner":"api"}`)}
	if !remediates(bump, ev("resource_change", "api", "limits changed", `{"old_mem_limit":100,"new_mem_limit":150}`)) {
		t.Error("raising the owner's limit is the bump")
	}
	if remediates(bump, ev("resource_change", "api", "limits changed", `{"old_mem_limit":150,"new_mem_limit":100}`)) {
		t.Error("lowering the limit is not the bump")
	}
	restart := &Action{ActionType: ActionRestartPod, Namespace: "default", Target: "api"}
	if remediates(restart, ev("resource_deleted", "api", "deployment deleted", `{}`)) {
		t.Error("deleting a Deployment that shares the name is not a pod restart")
	}
}

// Issue 19: a change to something the analysis never weighed cannot
// contradict, even if the symptom recovers afterwards.
func TestAnUnrelatedChangeDoesNotContradict(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "deploy", "billing", "billing deployed", `{}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("an unrelated deploy must not contradict, got %s (%s)", got, detail)
	}
}

// Issue 20: Chronicle's own write is not evidence that Chronicle was right.
func TestChroniclesOwnChangeDoesNotConfirm(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1,"changed_by":"chronicle-heal"}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("a self-performed action must not confirm itself, got %s (%s)", got, detail)
	}
	// Nor can Chronicle's write to a related workload contradict.
	got, detail = classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "worker", "worker scaled from 1 to 3", `{"old_replicas":1,"new_replicas":3,"changed_by":"chronicle-heal"}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got == OutcomeContradicted {
		t.Fatalf("a self-performed change must not contradict either: %s", detail)
	}
}

// When both the proposal and another change precede the recovery, the credit
// cannot be split, so neither side is counted.
func TestTwoCandidateFixesBeforeRecoveryIsUnknown(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "deploy", "api", "api redeployed", `{}`),
		evAt(20, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
	})
	if got != OutcomeUnknown {
		t.Fatalf("expected unknown, got %s (%s)", got, detail)
	}
}

// Found live: a chaos campaign runs faults minutes apart, so the 45-minute window
// of one decision reached the next fault's events and judged the decision
// against a change made long after its own incident had recovered.
func TestAChangeAfterTheIncidentRecoveredCannotContradict(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(40, "error_spike_resolved", "frontend", "resolved", `{}`),
		// The next, unrelated incident, four minutes later, on something the analysis weighed.
		evAt(280, "deploy", "api", "api redeployed", `{}`),
		evAt(320, "error_spike_resolved", "frontend", "resolved again", `{}`),
	})
	if got != OutcomeConfirmed {
		t.Fatalf("the fix preceded the recovery; a later incident must not change that: %s (%s)", got, detail)
	}
	got, detail = classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(40, "error_spike_resolved", "frontend", "recovered by itself", `{}`),
		evAt(280, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(320, "error_spike_resolved", "frontend", "resolved again", `{}`),
	})
	if got == OutcomeConfirmed {
		t.Fatalf("a fix performed after the incident had already recovered cannot be credited: %s (%s)", got, detail)
	}
}

// A recovery that does not hold is a flap, not the end of the incident.
func TestAFlapDoesNotEndTheIncidentWindow(t *testing.T) {
	got, detail := classifyOutcome(restoreDecision(), frontend, []event.Event{
		evAt(10, "error_spike_resolved", "frontend", "flap", `{}`),
		evAt(30, "error_spike", "frontend", "errors again", `{}`),
		evAt(50, "scale", "redis", "redis scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`),
		evAt(90, "error_spike_resolved", "frontend", "resolved for good", `{}`),
	})
	if got != OutcomeConfirmed {
		t.Fatalf("the real recovery came after the fix, past a flap: %s (%s)", got, detail)
	}
}
