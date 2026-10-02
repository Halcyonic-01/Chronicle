// Package independent is the final-evaluation incident set for Chronicle's RCA.
//
// It was written after the RCA rules were settled, on an application that shares
// no names, topology or code with the development and held-out sets, and it was
// never run against RCA while being written (only its structure was checked).
// It must not be used to tune anything. If it exposes a bug, document the bug;
// if the bug is then fixed, any rerun is a "post-fix" result, not an untouched
// baseline.
//
// What the numbers mean: accuracy on a synthetic benchmark authored by the same
// engineer who wrote the analyzer. It is not production accuracy.
package independent

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// The application: a media platform.
//
//	media: gateway(3) -> catalog(2) -> metadata-db
//	       gateway -> player-api(2) -> session-cache, entitlement -> metadata-db
//	       ingest -> queue <- transcoder(2) -> blob-proxy(2) -> [external object store]
//	auth:  auth-svc(2) -> auth-db        (called by gateway and player-api)
//	media: report-gen (nothing depends on it)
//
// Pods are named <deployment>-r<revision>-<index>; revision 2 is where a rollout
// lands. Services outside the cluster (object store, CDN, DNS, payment provider)
// have no node in the graph, so nothing can be recorded about them.

type dep struct {
	ns, name string
	replicas int
	nodes    []string
}

var deployments = []dep{
	{"media", "gateway", 3, []string{"node-a", "node-b", "node-c"}},
	{"media", "catalog", 2, []string{"node-b"}},
	{"media", "metadata-db", 1, []string{"node-d"}},
	{"media", "player-api", 2, []string{"node-a", "node-c"}},
	{"media", "session-cache", 1, []string{"node-c"}},
	{"media", "entitlement", 1, []string{"node-b"}},
	{"media", "ingest", 1, []string{"node-d"}},
	{"media", "queue", 1, []string{"node-d"}},
	{"media", "transcoder", 2, []string{"node-c", "node-d"}},
	{"media", "blob-proxy", 2, []string{"node-d", "node-a"}},
	{"media", "report-gen", 1, []string{"node-a"}},
	{"auth", "auth-svc", 2, []string{"node-a", "node-b"}},
	{"auth", "auth-db", 1, []string{"node-d"}},
}

var calls = [][2]string{
	{"media/gateway", "media/catalog"}, {"media/gateway", "media/player-api"}, {"media/gateway", "auth/auth-svc"},
	{"media/catalog", "media/metadata-db"},
	{"media/player-api", "media/session-cache"}, {"media/player-api", "media/entitlement"}, {"media/player-api", "auth/auth-svc"},
	{"media/entitlement", "media/metadata-db"},
	{"media/ingest", "media/queue"}, {"media/transcoder", "media/queue"}, {"media/transcoder", "media/blob-proxy"},
	{"auth/auth-svc", "auth/auth-db"},
}

func podName(name string, rev, i int) string { return fmt.Sprintf("%s-r%d-%d", name, rev, i) }

func node(ns, kind, name string) graph.Node { return graph.Node{Namespace: ns, Kind: kind, Name: name} }

func split(k string) (string, string) {
	p := strings.SplitN(k, "/", 2)
	return p[0], p[1]
}

func find(k string) dep {
	ns, name := split(k)
	for _, d := range deployments {
		if d.ns == ns && d.name == name {
			return d
		}
	}
	panic("unknown deployment " + k)
}

// Edges returns the dependency graph.
func Edges() []graph.Edge {
	var edges []graph.Edge
	add := func(from, to graph.Node, kind string) {
		edges = append(edges, graph.Edge{From: from, To: to, Kind: kind, Weight: 1, Source: "independent"})
	}
	for _, d := range deployments {
		for rev := 1; rev <= 2; rev++ {
			for i := 0; i < d.replicas; i++ {
				p := podName(d.name, rev, i)
				add(node(d.ns, "Deployment", d.name), node(d.ns, "Pod", p), "owns")
				add(node(d.ns, "Service", d.name), node(d.ns, "Pod", p), "routes_to")
				add(node(d.ns, "Pod", p), node("", "Node", d.nodes[i%len(d.nodes)]), "runs_on")
			}
		}
	}
	for _, c := range calls {
		from, to := find(c[0]), find(c[1])
		for rev := 1; rev <= 2; rev++ {
			for i := 0; i < from.replicas; i++ {
				add(node(from.ns, "Pod", podName(from.name, rev, i)), node(to.ns, "Service", to.name), "calls")
			}
		}
		add(node(from.ns, "Deployment", from.name), node(to.ns, "Deployment", to.name), "calls")
	}
	for _, cm := range [][2]string{{"catalog", "catalog-config"}, {"transcoder", "transcode-profiles"}} {
		d := find("media/" + cm[0])
		for rev := 1; rev <= 2; rev++ {
			for i := 0; i < d.replicas; i++ {
				add(node("media", "Pod", podName(cm[0], rev, i)), node("media", "ConfigMap", cm[1]), "uses")
			}
		}
	}
	add(node("media", "Ingress", "edge"), node("media", "Service", "gateway"), "routes_to")
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].From.Key()+edges[i].To.Key() < edges[j].From.Key()+edges[j].To.Key()
	})
	return edges
}

// --- timelines -------------------------------------------------------------

// Seconds are relative to the injected cause (t=0). Lags differ from the other
// sets on purpose.
var lag = map[string]float64{"k8s": 2, "prometheus": 7, "loki": 12}

type timeline struct {
	id  string
	n   int
	evs []event.Event
}

func (t *timeline) put(at float64, src, ns, kind, name, typ, sev, title, payload string) event.Event {
	return t.putLate(at, 0, src, ns, kind, name, typ, sev, title, payload)
}

// putLate records an event that Chronicle only saw `late` seconds after it
// happened, as after a collector gap.
func (t *timeline) putLate(at, late float64, src, ns, kind, name, typ, sev, title, payload string) event.Event {
	t.n++
	occurred := epoch.Add(secs(at))
	e := event.Event{
		ID: fmt.Sprintf("%s#%03d", t.id, t.n), OccurredAt: occurred, IngestedAt: occurred.Add(secs(lag[src] + late)),
		Source: src, Namespace: ns, EntityKind: kind, EntityName: name, Type: typ, Severity: sev, Title: title, Payload: []byte(payload),
	}
	t.evs = append(t.evs, e)
	return e
}
