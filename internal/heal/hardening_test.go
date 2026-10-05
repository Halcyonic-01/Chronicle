package heal

// Healing side of the independent benchmark's findings: the engine must never
// plan an action for a cause the evidence rules out, and never plan one its
// executor would refuse. Dry-run only.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

func scaleResult(id string, from, to int) *rca.Result {
	return &rca.Result{
		Symptom: event.Event{ID: id}, Confidence: 0.95, Verdict: rca.VerdictRootCause,
		Candidates: []rca.Candidate{{Event: event.Event{Namespace: "default", EntityName: "cache", Type: "scale",
			Payload: []byte(fmt.Sprintf(`{"old_replicas":%d,"new_replicas":%d}`, from, to))}}},
	}
}

// F1: restoring replicas is planned only for a scale to zero, whatever the confidence.
func TestRestoreIsPlannedOnlyForAScaleToZero(t *testing.T) {
	for _, c := range []struct {
		from, to int
		run      bool
	}{{1, 2, false}, {3, 10, false}, {3, 1, false}, {2, 0, true}} {
		action, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), scaleResult(fmt.Sprintf("s-%d-%d", c.from, c.to), c.from, c.to))
		if err != nil {
			t.Fatal(err)
		}
		if got := action.Status == StatusWouldRun; got != c.run {
			t.Errorf("scale %d->%d: would run = %v, want %v (%s)", c.from, c.to, got, c.run, action.Result)
		}
		if !c.run && !strings.Contains(action.Result, "not a scale to zero") {
			t.Errorf("scale %d->%d: the refusal should say why: %q", c.from, c.to, action.Result)
		}
	}
}

// A leader the analysis ruled out is never acted on, even with a verdict and
// confidence that would otherwise clear the floor.
func TestHealingDeclinesALeaderRuledOutAsACause(t *testing.T) {
	r := &rca.Result{
		Symptom: event.Event{ID: "ruled-out"}, Confidence: 0.99, Verdict: rca.VerdictRootCause,
		Candidates: []rca.Candidate{{Event: event.Event{Namespace: "default", EntityName: "api-2", Type: "oom_kill"}, NotRoot: "a new pod"}},
	}
	action, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if action.Status == StatusWouldRun {
		t.Fatalf("acted on a ruled-out leader: %+v", action)
	}
}

// End to end with the real analyzer: a scale-up before unexplained errors, and
// an OOM on a rollout's new pod whose change was not recorded, plan nothing.
func TestHealingPlansNothingForRuledOutCauses(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	node := func(kind, name string) graph.Node { return graph.Node{Namespace: "default", Kind: kind, Name: name} }
	edges := []graph.Edge{
		{From: node("Deployment", "api"), To: node("Pod", "api-1"), Kind: "owns"},
		{From: node("Deployment", "api"), To: node("Pod", "api-2"), Kind: "owns"},
		{From: node("Service", "api"), To: node("Pod", "api-1"), Kind: "routes_to"},
		{From: node("Service", "api"), To: node("Pod", "api-2"), Kind: "routes_to"},
		{From: node("Deployment", "cache"), To: node("Pod", "cache-1"), Kind: "owns"},
		{From: node("Service", "cache"), To: node("Pod", "cache-1"), Kind: "routes_to"},
		{From: node("Pod", "api-1"), To: node("Service", "cache"), Kind: "calls"},
		{From: node("Pod", "api-2"), To: node("Service", "cache"), Kind: "calls"},
	}
	n := 0
	ev := func(sec float64, kind, name, typ, sev, payload string) event.Event {
		n++
		at := t0.Add(time.Duration(sec * float64(time.Second)))
		return event.Event{ID: fmt.Sprintf("e%d", n), OccurredAt: at, IngestedAt: at.Add(time.Second), Namespace: "default",
			EntityKind: kind, EntityName: name, Type: typ, Severity: sev, Title: typ + " " + name, Payload: []byte(payload)}
	}
	alert := func(sec float64) event.Event {
		e := ev(sec, "Service", "api", "error_spike", "critical", `{}`)
		e.Title = "high_error_rate on api"
		return e
	}
	cases := map[string][]event.Event{
		"scale-up": {ev(-15, "Deployment", "cache", "scale", "info", `{"old_replicas":1,"new_replicas":2}`)},
		"oom on a new pod, change unrecorded": {
			ev(0, "Pod", "api-2", "resource_created", "info", `{"owner":"api"}`),
			ev(1, "Pod", "api-1", "became_unready", "warning", `{"owner":"api"}`),
			ev(2, "Pod", "api-1", "resource_deleted", "info", `{}`),
			ev(20, "Pod", "api-2", "oom_kill", "critical", `{"owner":"api"}`),
			ev(60, "Pod", "api-2", "oom_kill", "critical", `{"owner":"api"}`),
		},
	}
	for name, events := range cases {
		symptom := alert(90)
		a := &rca.Analyzer{Events: &benchdata.Store{All: append(events, symptom)}, Graph: benchdata.Graph{EdgeList: edges}, MaxHops: 8}
		result, err := a.Analyze(context.Background(), symptom)
		if err != nil {
			t.Fatal(err)
		}
		action, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), result)
		if err != nil {
			t.Fatal(err)
		}
		if result.Verdict != rca.VerdictNoRootCause || action.Status == StatusWouldRun {
			t.Errorf("%s: verdict %s, action %s (%s); nothing should be planned", name, result.Verdict, action.Status, action.Result)
		}
	}
	if !NewEngine(&memoryAudit{}).DryRun {
		t.Fatal("the engine must default to dry-run")
	}
}
