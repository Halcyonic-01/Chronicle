package rca

import (
	"context"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
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
