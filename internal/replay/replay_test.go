package replay

import (
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

func TestReplayAppliesLifecycleAndMetricEvents(t *testing.T) {
	snapshot := &Snapshot{
		Objects: map[string]ObjectState{
			"prod/Deployment/api": {Kind: "Deployment", Name: "api", Namespace: "prod", Phase: "Running", Replicas: 2},
			"prod/Pod/api-1":      {Kind: "Pod", Name: "api-1", Namespace: "prod", Phase: "Running", ReadyCount: 1},
		},
		Edges:   []graph.Edge{{From: graph.Node{Kind: "Deployment", Name: "api", Namespace: "prod"}, To: graph.Node{Kind: "Pod", Name: "api-1", Namespace: "prod"}, Kind: "owns"}},
		Metrics: map[string]float64{},
	}

	applyEvent(snapshot, event.Event{Namespace: "prod", EntityKind: "Pod", EntityName: "api-1", Type: "oom_kill"})
	if got := snapshot.Objects["prod/Pod/api-1"]; got.Phase != "Restarting" || got.Restarts != 1 || got.ReadyCount != 0 {
		t.Fatalf("OOM replay did not update pod state: %#v", got)
	}
	applyEvent(snapshot, event.Event{Namespace: "prod", EntityKind: "Pod", EntityName: "api-1", Type: "became_ready"})
	if got := snapshot.Objects["prod/Pod/api-1"]; got.Phase != "Running" || got.ReadyCount != 1 {
		t.Fatalf("readiness replay did not restore pod state: %#v", got)
	}
	applyEvent(snapshot, event.Event{Namespace: "prod", EntityKind: "Deployment", EntityName: "api", Type: "scale", Payload: []byte(`{"new_replicas":3}`)})
	if got := snapshot.Objects["prod/Deployment/api"].Replicas; got != 3 {
		t.Fatalf("scale replay got %d replicas, want 3", got)
	}
	applyEvent(snapshot, event.Event{Namespace: "prod", EntityKind: "Service", EntityName: "api", Type: "error_spike", Payload: []byte(`{"value":0.42}`)})
	if got := snapshot.Metrics["error_spike{api}"]; got != 0.42 {
		t.Fatalf("metric replay got %v, want 0.42", got)
	}
	applyEvent(snapshot, event.Event{Namespace: "prod", EntityKind: "Service", EntityName: "api", Type: "error_spike_resolved"})
	if _, ok := snapshot.Metrics["error_spike{api}"]; ok {
		t.Fatal("resolved metric was not removed")
	}
}

func TestReplayGraphKeepsHistoricalEdgeSnapshot(t *testing.T) {
	before := time.Now().Add(-time.Hour)
	snapshot := Snapshot{TakenAt: before, Edges: []graph.Edge{{From: graph.Node{Kind: "Service", Name: "api", Namespace: "prod"}, To: graph.Node{Kind: "Pod", Name: "api-1", Namespace: "prod"}, Kind: "routes_to"}}}
	if len(snapshot.Edges) != 1 || snapshot.Edges[0].From.Namespace != "prod" {
		t.Fatalf("historical edge snapshot was not retained: %#v", snapshot.Edges)
	}
}
