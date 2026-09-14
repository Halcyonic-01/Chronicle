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

// A factor that saturates at five affected services is a constant, and a
// constant cannot rank anything.
func TestBlastRadiusFactorKeepsDiscriminatingAtScale(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api"}
	factorFor := func(affected int) float64 {
		c := Candidate{
			Event:            event.Event{IngestedAt: now.Add(-time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "deploy"},
			AffectedServices: affected,
		}
		score(&c, symptom)
		for _, f := range c.Factors {
			if f.Label == "Blast radius" {
				return f.Multiplier
			}
		}
		t.Fatalf("no blast radius factor for %d services", affected)
		return 0
	}
	small, medium, large := factorFor(5), factorFor(20), factorFor(45)
	if !(small < medium && medium < large) {
		t.Fatalf("blast radius must keep separating candidates: 5=%.4f 20=%.4f 45=%.4f", small, medium, large)
	}
	if large >= 1.25 {
		t.Fatalf("the factor must stay under its ceiling, got %.4f", large)
	}
}

func deployEvent(id string, at time.Time, from, to string) event.Event {
	return event.Event{
		ID: id, IngestedAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "deploy",
		Payload: []byte(`{"old_image":"` + from + `","new_image":"` + to + `"}`),
	}
}

// Fixing an outage puts a change into the causal window like any other, and
// recency alone ranks the fix above the break it was fixing.
func TestRevertingChangesAreIdentified(t *testing.T) {
	now := time.Now().UTC()
	broke := deployEvent("broke", now.Add(-90*time.Second), "latest", "nonexistent")
	fixed := deployEvent("fixed", now.Add(-5*time.Second), "nonexistent", "latest")
	unrelated := deployEvent("unrelated", now.Add(-60*time.Second), "v1", "v2")

	reverts := revertingChanges([]event.Event{broke, unrelated, fixed})
	if reverts["fixed"] != "broke" {
		t.Fatalf("the restore should be recognised as undoing the break, got %#v", reverts)
	}
	if _, marked := reverts["broke"]; marked {
		t.Error("the break undoes nothing that came before it")
	}
	if _, marked := reverts["unrelated"]; marked {
		t.Error("a forward change to a different image is not a revert")
	}
}

// The regression that prompted this: restoring an image at the moment the pod
// finally reported Failed outscored the deploy that broke it a minute earlier.
func TestTheFixDoesNotOutrankTheBreak(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "worker-1", Type: "resource_status"}
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{
		{From: node("Deployment", "worker"), To: node("Pod", "worker-1"), Kind: "owns", Weight: 1, Source: "static"},
	}}
	window := []event.Event{
		deployEvent("broke", now.Add(-47*time.Second), "latest", "nonexistent"),
		deployEvent("fixed", now.Add(-1*time.Second), "nonexistent", "latest"),
	}

	got, err := (&Analyzer{Events: fakeEvents(window), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 2 {
		t.Fatalf("both deploys should be graph-reachable candidates, got %d", len(got.Candidates))
	}
	if got.Candidates[0].Event.ID != "broke" {
		t.Fatalf("the break must rank above the fix, got %q first (scores: %.3f vs %.3f)",
			got.Candidates[0].Event.ID, got.Candidates[0].Score, got.Candidates[1].Score)
	}
	if got.Candidates[1].Reverts != "broke" {
		t.Errorf("the fix should be annotated with what it undid, got %q", got.Candidates[1].Reverts)
	}
	// Demoted, not hidden: a rollback to a bad older version is a real cause.
	if got.Candidates[1].Score <= 0 {
		t.Error("a remediation stays rankable rather than being excluded")
	}
}
