package store

import (
	"testing"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

func edge(from, to, kind string, weight float64, source string) graph.Edge {
	return graph.Edge{
		From: graph.Node{Kind: "Deployment", Name: from, Namespace: "default"},
		To:   graph.Node{Kind: "Deployment", Name: to, Namespace: "default"},
		Kind: kind, Weight: weight, Source: source,
	}
}

func TestEdgeIdentityIgnoresWeightAndSource(t *testing.T) {
	a := edge("api", "redis", "calls", 1, "mesh")
	b := edge("api", "redis", "calls", 900, "static")
	if edgeIdentity(a) != edgeIdentity(b) {
		t.Fatal("the same relationship must keep one identity across weight and source changes")
	}
	if edgeIdentity(a) == edgeIdentity(edge("api", "redis", "routes_to", 1, "mesh")) {
		t.Fatal("edges of different kinds must not share an identity")
	}
}

func TestEdgeChangedIgnoresMeshJitterButNotRealMovement(t *testing.T) {
	cases := []struct {
		name      string
		current   graph.Edge
		wanted    graph.Edge
		expectNew bool
	}{
		{"identical", edge("api", "redis", "calls", 10, "mesh"), edge("api", "redis", "calls", 10, "mesh"), false},
		{"jitter", edge("api", "redis", "calls", 10, "mesh"), edge("api", "redis", "calls", 10.5, "mesh"), false},
		{"traffic doubled", edge("api", "redis", "calls", 10, "mesh"), edge("api", "redis", "calls", 20, "mesh"), true},
		{"promoted to observed", edge("api", "redis", "calls", 10, "static"), edge("api", "redis", "calls", 10, "mesh"), true},
		{"zero to non-zero", edge("api", "redis", "calls", 0, "mesh"), edge("api", "redis", "calls", 4, "mesh"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := edgeChanged(tc.current, tc.wanted); got != tc.expectNew {
				t.Fatalf("edgeChanged = %v, want %v", got, tc.expectNew)
			}
		})
	}
}
