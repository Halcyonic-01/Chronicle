package rca

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

type fakeEvents []event.Event

func (f fakeEvents) EventsBetween(context.Context, time.Time, time.Time) ([]event.Event, error) {
	return f, nil
}

type fakeGraph map[string]int

func (f fakeGraph) UpstreamAt(context.Context, time.Time, string, int) (map[string]int, error) {
	return f, nil
}
func TestAnalyzeFiltersRanksAndUsesFallback(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Title: "API errors"}
	r := event.Event{ID: "r", IngestedAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "container_restart", Title: "Redis restarted"}
	n := event.Event{ID: "n", IngestedAt: now.Add(-10 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "grafana", Type: "deploy", Title: "Grafana deployed"}
	got, err := (&Analyzer{Events: fakeEvents{r, n}, Graph: fakeGraph{"default/Service/api": 0, "default/Service/redis": 1}}).Analyze(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "r" {
		t.Fatalf("unexpected candidates: %+v", got.Candidates)
	}
	if got.Narrative == "" || got.Confidence <= 0 {
		t.Fatalf("missing fallback result: %+v", got)
	}
}
func TestAnalyzeRejectsSameTimestamp(t *testing.T) {
	now := time.Now().UTC()
	s := event.Event{ID: "s", IngestedAt: now, Namespace: "n", EntityKind: "Service", EntityName: "api"}
	e := event.Event{ID: "e", IngestedAt: now, Namespace: "n", EntityKind: "Service", EntityName: "redis"}
	got, err := (&Analyzer{Events: fakeEvents{e}, Graph: fakeGraph{"n/Service/api": 0, "n/Service/redis": 1}}).Analyze(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 0 {
		t.Fatalf("same-timestamp event was causal: %+v", got.Candidates)
	}
}

// countingGraph exposes its edge set, which is the path the analyzer takes in
// production: load the graph once, then walk it in memory.
type countingGraph struct {
	edges     []graph.Edge
	edgeLoads int
	walks     int
}

func (c *countingGraph) EdgesAt(context.Context, time.Time) ([]graph.Edge, error) {
	c.edgeLoads++
	return c.edges, nil
}
func (c *countingGraph) UpstreamAt(context.Context, time.Time, string, int) (map[string]int, error) {
	c.walks++
	return nil, nil
}
func (c *countingGraph) DownstreamAt(context.Context, time.Time, string, int) (map[string]int, error) {
	c.walks++
	return nil, nil
}
func (c *countingGraph) ImpactAt(context.Context, time.Time, string, int) (map[string]int, error) {
	c.walks++
	return nil, nil
}

func TestAnalyzeLoadsTheGraphOnceRegardlessOfCandidateCount(t *testing.T) {
	now := time.Now().UTC()
	node := func(name string) graph.Node {
		return graph.Node{Kind: "Service", Name: name, Namespace: "default"}
	}
	source := &countingGraph{edges: []graph.Edge{
		{From: node("api"), To: node("redis"), Kind: "calls", Weight: 1, Source: "static"},
		{From: node("api"), To: node("worker"), Kind: "calls", Weight: 1, Source: "static"},
		{From: node("worker"), To: node("db"), Kind: "calls", Weight: 1, Source: "static"},
	}}
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	upstream := []event.Event{
		{ID: "1", IngestedAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "container_restart"},
		{ID: "2", IngestedAt: now.Add(-40 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "worker", Type: "deploy"},
		{ID: "3", IngestedAt: now.Add(-50 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "db", Type: "scale"},
	}

	got, err := (&Analyzer{Events: fakeEvents(upstream), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 3 {
		t.Fatalf("expected every upstream event to be a candidate, got %d", len(got.Candidates))
	}
	if source.edgeLoads != 1 {
		t.Fatalf("the graph must be loaded once per analysis, not per candidate: %d loads", source.edgeLoads)
	}
	if source.walks != 0 {
		t.Fatalf("per-candidate graph queries should be replaced by in-memory walks: %d queries", source.walks)
	}
	if len(got.Evidence) == 0 {
		t.Fatal("the loaded edge set should still produce evidence edges")
	}
}

func TestScoreFactorsExplainTheScoreTheyProduce(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	candidate := Candidate{
		Event:            event.Event{ID: "c", IngestedAt: now.Add(-60 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "deploy"},
		Distance:         2,
		AffectedServices: 3,
	}
	score(&candidate, symptom)

	if len(candidate.Factors) != 4 {
		t.Fatalf("expected a factor per scoring step, got %d: %+v", len(candidate.Factors), candidate.Factors)
	}
	if !candidate.Factors[0].Base {
		t.Fatal("the first factor is the base weight, not a multiplier")
	}
	// The derivation shown in the UI must actually reproduce the score.
	product := 1.0
	for _, f := range candidate.Factors {
		product *= f.Multiplier
	}
	if math.Abs(product-candidate.Score) > 1e-9 {
		t.Fatalf("factors multiply to %.6f but the score is %.6f", product, candidate.Score)
	}
	// Reasons stay in sync because the heal audit trail stores them.
	if len(candidate.Reasons) != len(candidate.Factors) {
		t.Fatalf("every factor should still have a sentence: %d reasons, %d factors", len(candidate.Reasons), len(candidate.Factors))
	}
}
