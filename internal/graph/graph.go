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

// causalWithEdge lists the edge kinds where a change at the From end can cause
// a failure at the To end, so causality runs the same way the edge points.
//
// Ownership is the only one: a Deployment's image or replica change breaks the
// Pods it owns. Every other edge is a dependency, and causality runs against
// it — a Pod "calls" a Service, so it is the Service failing that breaks the
// Pod, not the other way round. Walking only incoming edges therefore finds
// the caller when what you want is the callee, which is why a frontend error
// could never reach the Redis outage behind it.
var causalWithEdge = map[string]bool{"owns": true}

// Upstream returns everything that could possibly have caused a failure at
// `start`, keyed by node with its shortest hop distance. It follows ownership
// edges backwards and dependency edges forwards, because those are the two
// directions causality actually travels.
func (g *Graph) Upstream(start string, maxDepth int) map[string]int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	seen := map[string]int{start: 0}
	queue := []string{start}

	for depth := 1; depth <= maxDepth; depth++ {
		var next []string
		visit := func(key string) {
			if _, ok := seen[key]; ok {
				return // cycle guard
			}
			seen[key] = depth
			next = append(next, key)
		}
		for _, node := range queue {
			// Something that owns this node changed, breaking it.
			for _, e := range g.incoming[node] {
				if causalWithEdge[e.Kind] {
					visit(e.From.Key())
				}
			}
			// Something this node depends on failed, breaking it.
			for _, e := range g.outgoing[node] {
				if !causalWithEdge[e.Kind] {
					visit(e.To.Key())
				}
			}
		}
		queue = next
		if len(queue) == 0 {
			break
		}
	}
	return seen
}

// Downstream returns nodes affected by a change at start, keyed by node key
// and annotated with their shortest hop distance.
func (g *Graph) Downstream(start string, maxDepth int) map[string]int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	seen := map[string]int{start: 0}
	queue := []string{start}
	for depth := 1; depth <= maxDepth; depth++ {
		var next []string
		for _, node := range queue {
			for _, edge := range g.outgoing[node] {
				if _, ok := seen[edge.To.Key()]; ok {
					continue
				}
				seen[edge.To.Key()] = depth
				next = append(next, edge.To.Key())
			}
		}
		queue = next
		if len(queue) == 0 {
			break
		}
	}
	return seen
}

// Impact walks both directions because some infrastructure relationships are
// represented as Service -> Pod while the operational impact travels Pod ->
// Service. Causality still uses Upstream; this method is only for blast radius.
func (g *Graph) Impact(start string, maxDepth int) map[string]int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	seen := map[string]int{start: 0}
	queue := []string{start}
	for depth := 1; depth <= maxDepth; depth++ {
		var next []string
		for _, node := range queue {
			for _, edge := range g.outgoing[node] {
				if _, ok := seen[edge.To.Key()]; !ok {
					seen[edge.To.Key()] = depth
					next = append(next, edge.To.Key())
				}
			}
			for _, edge := range g.incoming[node] {
				if _, ok := seen[edge.From.Key()]; !ok {
					seen[edge.From.Key()] = depth
					next = append(next, edge.From.Key())
				}
			}
		}
		queue = next
		if len(queue) == 0 {
			break
		}
	}
	return seen
}
