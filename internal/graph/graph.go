package graph

import (
	"fmt"
	"strings"
	"sync"
)

type Node struct {
	Kind      string // "Deployment" | "Service" | "Pod" | "ConfigMap"
	Name      string
	Namespace string
}

func (n Node) Key() string {
	return fmt.Sprintf("%s/%s/%s", n.Namespace, n.Kind, n.Name)
}

func ParseKey(k string) (Node, error) {
	parts := strings.SplitN(k, "/", 3)
	if len(parts) != 3 {
		return Node{}, fmt.Errorf("invalid node key: %s", k)
	}
	return Node{Namespace: parts[0], Kind: parts[1], Name: parts[2]}, nil
}

type Edge struct {
	From   Node
	To     Node
	Kind   string  // "owns" | "routes_to" | "calls" | "mounts"
	Weight float64 // how strong is this dependency?
	Source string  // "static" | "mesh" | "trace"
}

// Graph is the in-memory representation for fast walks.
type Graph struct {
	mu       sync.RWMutex
	outgoing map[string][]Edge
	incoming map[string][]Edge // pre-built reverse index for upstream walks
}

func New() *Graph {
	return &Graph{
		outgoing: make(map[string][]Edge),
		incoming: make(map[string][]Edge),
	}
}

func (g *Graph) SetEdges(edges []Edge) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.outgoing = make(map[string][]Edge)
	g.incoming = make(map[string][]Edge)

	for _, e := range edges {
		from := e.From.Key()
		to := e.To.Key()
		g.outgoing[from] = append(g.outgoing[from], e)
		g.incoming[to] = append(g.incoming[to], e)
	}
}

// CurrentEdges returns a snapshot of all current active edges.
func (g *Graph) CurrentEdges() []Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var all []Edge
	for _, edges := range g.outgoing {
		all = append(all, edges...)
	}
	return all
}

// Upstream returns everything that could possibly have caused a failure at `start`.
// Returns node key -> number of hops away.
func (g *Graph) Upstream(start string, maxDepth int) map[string]int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	seen := map[string]int{start: 0}
	queue := []string{start}

	for depth := 1; depth <= maxDepth; depth++ {
		var next []string
		for _, node := range queue {
			for _, e := range g.incoming[node] {
				if _, ok := seen[e.From.Key()]; ok {
					continue // cycle guard
				}
				seen[e.From.Key()] = depth
				next = append(next, e.From.Key())
			}
		}
		queue = next
		if len(queue) == 0 {
			break
		}
	}
	return seen
}
