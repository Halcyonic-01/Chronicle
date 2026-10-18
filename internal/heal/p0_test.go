package heal

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

// Finding 15: the analysis's own "inconclusive" is not overridden by a rule
// floor set below it. (With the shipped floors this never arises: the floors are
// above 0.5. The guard is for a configured floor under the line.)
func TestAnInconclusiveVerdictIsNeverProposedEvenUnderALowFloor(t *testing.T) {
	cause := event.Event{ID: "c1", Namespace: "default", EntityKind: "Deployment", EntityName: "cache", Type: "scale",
		Payload: []byte(`{"old_replicas":2,"new_replicas":0}`)}
	engine := NewEngine(&memoryAudit{})
	engine.Rules = []Rule{{Name: "restore-scaled-down-workload", CauseType: "scale", ActionType: ActionRestoreReplicas, MinConfidence: 0.30, MaxPerHour: 5}}
	result := &rca.Result{Symptom: event.Event{ID: "s1"}, Confidence: 0.42, Verdict: rca.VerdictInconclusive,
		Candidates: []rca.Candidate{{Event: cause}}}
	a, err := engine.Evaluate(context.Background(), result)
	if err != nil {
		t.Fatal(err)
	}
	if a.Proposed || a.Approval == ApprovalPending || a.Status != StatusBlocked || !strings.Contains(a.Result, "inconclusive") {
		t.Fatalf("an inconclusive analysis was proposed: %+v", a)
	}
	// The same confidence with a root-cause verdict, as a control, is proposed.
	result.Verdict = rca.VerdictRootCause
	result.Symptom.ID = "s2"
	if a, _ = engine.Evaluate(context.Background(), result); !a.Proposed {
		t.Fatalf("control: a root-cause verdict above the floor should be proposed: %+v", a)
	}
}

// indexedAudit adds the optional lookup a real store provides.
type indexedAudit struct {
	memoryAudit
	existing *Action
}

func (i *indexedAudit) ActionForIncident(context.Context, string) (*Action, error) {
	return i.existing, nil
}

// Finding 25: analysing an incident again shows the decision already made.
func TestReAnalysisReturnsTheExistingDecision(t *testing.T) {
	existing := &Action{ID: "01EXISTING", IncidentID: "s1", Status: StatusWouldRun, Approval: ApprovalPending, Proposed: true}
	store := &indexedAudit{memoryAudit: memoryAudit{seen: true}, existing: existing}
	got, err := NewEngine(store).Evaluate(context.Background(), &rca.Result{Symptom: event.Event{ID: "s1"}, Confidence: 0.9,
		Candidates: []rca.Candidate{{Event: event.Event{Type: "scale"}}}})
	if err != nil || got == nil || got.ID != "01EXISTING" {
		t.Fatalf("re-analysis hid the existing decision: %+v, %v", got, err)
	}
	if len(store.actions) != 0 {
		t.Fatal("re-analysis recorded another decision")
	}
}

// racingAudit loses the insert to another analysis of the same incident.
type racingAudit struct {
	memoryAudit
	winner Action
}

func (r *racingAudit) RecordAction(_ context.Context, a *Action) error {
	*a = r.winner
	return ErrAlreadyRecorded
}

type countingNotifier struct{ n int }

func (c *countingNotifier) Notify(context.Context, *Action) error { c.n++; return nil }

// Finding 24: the caller holds the row that exists, not the ID it generated,
// and a decision that lost the insert is not announced a second time.
func TestALostInsertReturnsTheWinnersDecisionAndDoesNotNotify(t *testing.T) {
	store := &racingAudit{winner: Action{ID: "01WINNER", IncidentID: "s1", Status: StatusWouldRun, Approval: ApprovalPending, Proposed: true}}
	notifier := &countingNotifier{}
	engine := NewEngine(store)
	engine.Notifier = notifier
	cause := event.Event{ID: "c1", Namespace: "default", EntityKind: "Deployment", EntityName: "cache", Type: "scale",
		Payload: []byte(`{"old_replicas":2,"new_replicas":0}`)}
	a, err := engine.Evaluate(context.Background(), leadingWith(cause, 0.9))
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != "01WINNER" {
		t.Fatalf("returned the ID of a row that was never written: %s", a.ID)
	}
	if notifier.n != 0 {
		t.Fatalf("a duplicate was announced %d time(s)", notifier.n)
	}
}

func leadingWith(cause event.Event, confidence float64) *rca.Result {
	return &rca.Result{Symptom: event.Event{ID: "s1"}, Confidence: confidence, Verdict: rca.VerdictRootCause,
		Candidates: []rca.Candidate{{Event: cause}}}
}

// refusingExecutor declines, as the real one does when the cluster has moved on.
type refusingExecutor struct{ err error }

func (r refusingExecutor) Execute(context.Context, *Action) (string, error) { return "", r.err }

// A refusal is recorded as blocked and is not retried; a real failure is failed.
func TestARefusalIsRecordedAsBlockedAndAFailureAsFailed(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"refusal", refuse("refusing to restore: cache is already at 3 replicas"), StatusBlocked},
		{"wrapped refusal", errors.Join(errors.New("context"), refuse("refusing to roll back")), StatusBlocked},
		{"api failure", errors.New("the server could not find the requested resource"), StatusFailed},
	} {
		store := &executionMemory{}
		a, err := liveController(store, refusingExecutor{c.err}).ExecuteApproved(context.Background(), approvedRestart("act-"+c.name))
		if err == nil || store.status != c.want || a.Status != c.want {
			t.Errorf("%s: recorded %q (action %q), want %q, err %v", c.name, store.status, a.Status, c.want, err)
		}
	}
}

// The real executor's "the cluster moved on" cases are refusals; a disruption
// budget saying no is not (it is transient and retried).
func TestTheExecutorsStaleStateCasesAreRefusals(t *testing.T) {
	ready := unreadyPod("api-5f-ready", "api-5f")
	ready.Status.Conditions[0].Status = corev1.ConditionTrue
	client, _ := evictingClient(nil, ready, scaledDeployment("redis", 5))
	e := NewKubernetesExecutor(client)
	ctx := context.Background()
	cases := map[string]error{}
	_, cases["pod is ready again"] = e.restartPod(ctx, &Action{ActionType: ActionRestartPod, Namespace: "default", Target: "api-5f-ready", Payload: []byte(`{"owner":"api"}`)})
	_, cases["workload already restored"] = e.restoreReplicas(ctx, restoreAction(`{"old_replicas":2,"new_replicas":0}`))
	for name, err := range cases {
		var r *Refusal
		if !errors.As(err, &r) {
			t.Errorf("%s: %v is not a Refusal", name, err)
		}
	}
	budget, _ := evictingClient(func(string) bool { return true }, unreadyPod("redis-6d-abc", "redis-6d"))
	_, err := NewKubernetesExecutor(budget).restartPod(ctx, restartAction("redis-6d-abc"))
	var r *Refusal
	if err == nil || errors.As(err, &r) {
		t.Fatalf("a budget refusal must stay a retryable error, not a Refusal: %v", err)
	}
}
