package collect

import (
	"testing"

	"github.com/prometheus/common/model"
)

// An event whose namespace/kind/name does not resolve to a graph node is
// silently dropped by the causal filter, so guessing here deletes signal.
func TestIdentifyResolvesToTheResourceTheMetricDescribes(t *testing.T) {
	cases := []struct {
		name                            string
		metric                          model.Metric
		wantName, wantKind, wantNamespc string
	}{
		{"service series", model.Metric{"service": "api", "namespace": "default"}, "api", "Service", "default"},
		{"cadvisor pod series", model.Metric{"pod": "chronicle-abc", "namespace": "chronicle"}, "chronicle-abc", "Pod", "chronicle"},
		{"service wins over pod", model.Metric{"service": "api", "pod": "api-1", "namespace": "shop"}, "api", "Service", "shop"},
		{"no namespace label", model.Metric{"pod": "loki-0"}, "loki-0", "Pod", "default"},
		{"nothing identifiable", model.Metric{"job": "kubelet"}, "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name, kind, namespace := identify(tc.metric)
			if tc.wantName == "" {
				// The caller skips a sample it cannot name; kind is then moot.
				if name != "" {
					t.Fatalf("unidentifiable series resolved to %q", name)
				}
				return
			}
			if name != tc.wantName || kind != tc.wantKind || namespace != tc.wantNamespc {
				t.Fatalf("got %s/%s/%s, want %s/%s/%s", namespace, kind, name, tc.wantNamespc, tc.wantKind, tc.wantName)
			}
		})
	}
}

// The regression that mattered: a Chronicle pod alert used to be filed as
// default/Service/<pod>, a node that does not exist anywhere in the graph.
func TestIdentifyDoesNotFileEveryAlertUnderDefaultService(t *testing.T) {
	name, kind, namespace := identify(model.Metric{"pod": "chronicle-7bdb-x", "namespace": "chronicle"})
	if namespace == "default" || kind == "Service" {
		t.Fatalf("a cAdvisor pod series resolved to %s/%s/%s", namespace, kind, name)
	}
}
