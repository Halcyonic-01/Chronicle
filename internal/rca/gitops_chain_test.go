package rca

import (
	"context"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// An Argo CD sync and the Deployment change it made are one intervention, not two
// rival causes: the confidence of a Git-delivered bad release must not be split
// between them (found live: 0.68 for an otherwise clear-cut release).
func TestAGitOpsSyncAndTheChangeItAppliedAreOneCause(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	app := graph.Node{Kind: "Application", Name: "victim", Namespace: graph.ArgoNamespace}
	deploy := graph.Node{Kind: "Deployment", Name: "api", Namespace: "default"}
	pod := graph.Node{Kind: "Pod", Name: "api-1", Namespace: "default"}
	g := &countingGraph{edges: []graph.Edge{
		{From: app, To: deploy, Kind: "owns", Weight: 1, Source: "static"},
		{From: deploy, To: pod, Kind: "owns", Weight: 1, Source: "static"},
	}}
	at := func(s int) time.Time { return now.Add(time.Duration(s) * time.Second) }
	sync := event.Event{ID: "sync", Source: "argocd", OccurredAt: at(-60), IngestedAt: at(-60), Namespace: graph.ArgoNamespace,
		EntityKind: "Application", EntityName: "victim", Type: "deploy", Title: "victim synced revision bad", Payload: []byte(`{"revision":"bad"}`)}
	change := event.Event{ID: "change", Source: "k8s", OccurredAt: at(-59), IngestedAt: at(-59), Namespace: "default",
		EntityKind: "Deployment", EntityName: "api", Type: "deploy", Title: "api deployed: v1 -> v2", Payload: []byte(`{"gitops":"argocd:victim"}`)}
	symptom := event.Event{ID: "s", Source: "prometheus", OccurredAt: now, IngestedAt: now, Namespace: "default",
		EntityKind: "Pod", EntityName: "api-1", Type: "error_spike", Severity: "critical"}

	got, err := (&Analyzer{Events: fakeEvents{sync, change}, Graph: g, MaxHops: 4}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	chains := map[string]string{}
	for _, c := range got.Candidates {
		chains[c.Event.ID] = c.Chain
	}
	if chains["sync"] == "" || chains["sync"] != chains["change"] {
		t.Fatalf("the sync and the change it applied are not one chain: %v", chains)
	}
	if got.Separation < 0.999 || len(got.Alternatives) != 0 {
		t.Fatalf("the sync still competes with its own change: separation %.3f, alternatives %d", got.Separation, len(got.Alternatives))
	}

	// Two different changes still compete: a manual deploy is not explained by a
	// sync of another application.
	other := sync
	other.ID, other.EntityName = "other-sync", "billing"
	got, err = (&Analyzer{Events: fakeEvents{other, change}, Graph: g, MaxHops: 4}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "other-sync" && c.Chain != "" && c.Chain == chains["change"] {
			t.Fatal("a sync of an unrelated application was linked to the change")
		}
	}
}
