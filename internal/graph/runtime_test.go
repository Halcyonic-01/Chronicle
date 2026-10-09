package graph

import (
	"testing"

	"github.com/prometheus/common/model"
)

func sample(labels map[string]string, v float64) *model.Sample {
	m := model.Metric{}
	for k, val := range labels {
		m[model.LabelName(k)] = model.LabelValue(val)
	}
	return &model.Sample{Metric: m, Value: model.SampleValue(v)}
}

// Linkerd's TCP connections are the only trace of a dependency named in code
// (found by a chaos run: a postgres outage had no edge to reach it).
func TestMeshEdgesReadLinkerdHTTPAndTCPTraffic(t *testing.T) {
	vec := model.Vector{
		sample(map[string]string{"deployment": "api", "namespace": "default", "dst_deployment": "worker", "dst_namespace": "default"}, 1.9),
		sample(map[string]string{"deployment": "worker", "namespace": "default", "dst_deployment": "postgres", "dst_namespace": "default"}, 1.9),
		sample(map[string]string{"deployment": "worker", "namespace": "default"}, 0.2), // no destination: unmeshed
		sample(map[string]string{"deployment": "api", "namespace": "default", "dst_deployment": "api", "dst_namespace": "default"}, 1),
	}
	got := map[string]bool{}
	for _, e := range meshEdges(vec) {
		if e.Kind != "calls" || e.Source != "mesh" {
			t.Fatalf("unexpected edge %+v", e)
		}
		got[e.From.Key()+" -> "+e.To.Key()] = true
	}
	for _, want := range []string{"default/Deployment/api -> default/Deployment/worker", "default/Deployment/worker -> default/Deployment/postgres"} {
		if !got[want] {
			t.Errorf("missing %s in %v", want, got)
		}
	}
	if len(got) != 2 {
		t.Errorf("self-calls and unknown destinations are not edges: %v", got)
	}
}

func TestMeshEdgesKeepOtherMeshesLabels(t *testing.T) {
	vec := model.Vector{sample(map[string]string{"source_workload": "a", "destination_workload": "b", "source_workload_namespace": "shop", "destination_workload_namespace": "shop"}, 1)}
	edges := meshEdges(vec)
	if len(edges) != 1 || edges[0].From.Key() != "shop/Deployment/a" || edges[0].To.Key() != "shop/Deployment/b" {
		t.Fatalf("got %+v", edges)
	}
}
