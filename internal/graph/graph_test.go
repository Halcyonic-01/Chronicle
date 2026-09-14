package graph

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func node(kind, name string) Node { return Node{Kind: kind, Name: name, Namespace: "default"} }

// The victim topology: frontend calls api, api calls redis, each Service routes
// to its Pod, each Deployment owns its Pod.
func victimGraph() *Graph {
	g := New()
	g.SetEdges([]Edge{
		{From: node("Pod", "frontend-1"), To: node("Service", "api"), Kind: "calls"},
		{From: node("Service", "api"), To: node("Pod", "api-1"), Kind: "routes_to"},
		{From: node("Pod", "api-1"), To: node("Service", "redis"), Kind: "calls"},
		{From: node("Service", "redis"), To: node("Pod", "redis-1"), Kind: "routes_to"},
		{From: node("Deployment", "frontend"), To: node("Pod", "frontend-1"), Kind: "owns"},
		{From: node("Pod", "frontend-1"), To: Node{Kind: "Node", Name: "worker-1"}, Kind: "runs_on"},
	})
	return g
}

// Causality runs against dependency edges: the frontend broke because something
// it depends on broke. Walking only incoming edges finds the caller, never the
// callee, so a frontend error could never reach the Redis outage behind it.
func TestUpstreamFollowsDependenciesTowardsTheirCause(t *testing.T) {
	upstream := victimGraph().Upstream("default/Pod/frontend-1", 4)
	for key, wantDepth := range map[string]int{
		"default/Service/api":   1,
		"default/Pod/api-1":     2,
		"default/Service/redis": 3,
		"default/Pod/redis-1":   4,
	} {
		got, reachable := upstream[key]
		if !reachable {
			t.Errorf("%s is a possible cause of a frontend failure but was not reachable", key)
			continue
		}
		if got != wantDepth {
			t.Errorf("%s is %d hop(s) away, want %d", key, got, wantDepth)
		}
	}
}

// Ownership is the one relationship where causality runs with the edge: a
// Deployment change breaks the Pods it owns.
func TestUpstreamStillFollowsOwnershipBackwards(t *testing.T) {
	upstream := victimGraph().Upstream("default/Pod/frontend-1", 2)
	if upstream["default/Deployment/frontend"] != 1 {
		t.Fatalf("owning deployment should be one hop upstream, got %#v", upstream["default/Deployment/frontend"])
	}
	if _, reachable := upstream["/Node/worker-1"]; !reachable {
		t.Fatal("the node hosting the pod can cause its failure and must be reachable")
	}
}

// A caller is not a cause of its callee's failure: redis did not break because
// the api called it.
func TestUpstreamDoesNotTreatCallersAsCauses(t *testing.T) {
	upstream := victimGraph().Upstream("default/Pod/redis-1", 4)
	for _, key := range []string{"default/Pod/api-1", "default/Pod/frontend-1"} {
		if _, reachable := upstream[key]; reachable {
			t.Errorf("%s calls redis; it cannot have caused redis to fail", key)
		}
	}
}

func podWithEnv(name string, env ...corev1.EnvVar) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Env: env}}},
	}
}

// Causality now runs along these edges, so a false match invents a false cause.
func TestInferCallEdgesMatchesTheHostNotASubstring(t *testing.T) {
	known := map[string]bool{"default/api": true, "default/redis": true, "other/shared": true}
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{"bare host and port", "http://api:8080", "default/Service/api"},
		{"cluster dns", "redis://redis.default.svc.cluster.local:6379", "default/Service/redis"},
		{"host only", "api", "default/Service/api"},
		{"credentials in url", "redis://user:pw@redis:6379/0", "default/Service/redis"},
		{"cross namespace", "http://shared.other.svc.cluster.local", "other/Service/shared"},
		{"name merely contained", "https://rapid-api-gateway.example.com", ""},
		{"name inside a sentence", "connect to the api when ready", ""},
		{"unknown service", "http://billing:8080", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			edges := InferCallEdges(podWithEnv("caller", corev1.EnvVar{Name: "URL", Value: tc.value}), known)
			if tc.want == "" {
				if len(edges) != 0 {
					t.Fatalf("%q should not infer a dependency, got %s", tc.value, edges[0].To.Key())
				}
				return
			}
			if len(edges) != 1 {
				t.Fatalf("%q should infer exactly one dependency, got %d", tc.value, len(edges))
			}
			if got := edges[0].To.Key(); got != tc.want {
				t.Fatalf("%q inferred %s, want %s", tc.value, got, tc.want)
			}
		})
	}
}

// Without these edges an Argo sync event has no node and can never be a cause.
func TestBuildArgoEdgesLinkApplicationsToWhatTheyDeploy(t *testing.T) {
	deployments := []appsv1.Deployment{
		{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "default", Labels: map[string]string{"argocd.argoproj.io/instance": "victim"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "default", Labels: map[string]string{"app.kubernetes.io/instance": "victim"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged", Namespace: "default"}},
	}
	edges := BuildArgoEdges(deployments)
	if len(edges) != 2 {
		t.Fatalf("expected one edge per managed deployment, got %d", len(edges))
	}
	for _, e := range edges {
		if e.From.Key() != ArgoNamespace+"/Application/victim" {
			t.Fatalf("application node is %s, want %s/Application/victim", e.From.Key(), ArgoNamespace)
		}
		if e.Kind != "owns" {
			t.Fatalf("Argo manages its deployments; edge kind is %q", e.Kind)
		}
	}

	// The whole point: the Application must now be causally upstream.
	g := New()
	g.SetEdges(append(edges, Edge{From: node("Deployment", "api"), To: node("Pod", "api-1"), Kind: "owns"}))
	if upstream := g.Upstream("default/Pod/api-1", 3); upstream[ArgoNamespace+"/Application/victim"] != 2 {
		t.Fatalf("an Argo sync should be reachable as a cause, got %#v", upstream)
	}
}

// Every meshed pod depends on the mesh control plane, so following those edges
// makes everything's blast radius the size of the cluster.
func meshedGraph() *Graph {
	g := New()
	g.SetEdges([]Edge{
		{From: node("Pod", "api-1"), To: node("Service", "redis"), Kind: "calls"},
		{From: node("Pod", "api-1"), To: Node{Kind: "Service", Name: "linkerd-identity", Namespace: "linkerd"}, Kind: EdgeCallsInfra},
		{From: node("Pod", "unrelated-1"), To: Node{Kind: "Service", Name: "linkerd-identity", Namespace: "linkerd"}, Kind: EdgeCallsInfra},
		{From: node("Pod", "stranger-1"), To: Node{Kind: "Service", Name: "linkerd-identity", Namespace: "linkerd"}, Kind: EdgeCallsInfra},
	})
	return g
}

func TestImpactFollowsWhatDependsOnTheFailure(t *testing.T) {
	impact := victimGraph().Impact("default/Service/redis", 4)
	for key, want := range map[string]int{"default/Pod/api-1": 1, "default/Service/api": 2, "default/Pod/frontend-1": 3} {
		if got, reached := impact[key]; !reached || got != want {
			t.Errorf("%s should be %d hop(s) into the blast radius, got %d (reached=%v)", key, want, got, reached)
		}
	}
}

// Fourteen pods run on one node. A bidirectional walk would hop
// pod -> node -> every other pod and make everything's blast radius the cluster.
func TestImpactDoesNotSpreadThroughSharedResources(t *testing.T) {
	g := New()
	node := Node{Kind: "Node", Name: "worker-1"}
	g.SetEdges([]Edge{
		{From: Node{Kind: "Pod", Name: "mine", Namespace: "default"}, To: node, Kind: "runs_on"},
		{From: Node{Kind: "Pod", Name: "stranger", Namespace: "other"}, To: node, Kind: "runs_on"},
	})
	if _, reached := g.Impact("default/Pod/mine", 4)["other/Pod/stranger"]; reached {
		t.Fatal("a co-tenant on the same node is not collateral damage")
	}
	// The node itself failing is a different matter entirely.
	if _, reached := g.Impact("/Node/worker-1", 2)["other/Pod/stranger"]; !reached {
		t.Fatal("a node failure must take its pods with it")
	}
}

// Causality still follows the edge: if the mesh control plane fails, the pods
// plugged into it really do break.
func TestUpstreamStillFollowsSharedInfrastructure(t *testing.T) {
	upstream := meshedGraph().Upstream("default/Pod/api-1", 3)
	if _, reachable := upstream["linkerd/Service/linkerd-identity"]; !reachable {
		t.Fatal("a mesh outage is a legitimate cause and must stay reachable")
	}
}
