package graph

import (
	"sync"
	"testing"
)

func TestGitOpsManagerReadsOnlyControllerSpecificMarkers(t *testing.T) {
	argo := func(names ...string) KnownApplication {
		return func(n string) bool {
			for _, k := range names {
				if k == n {
					return true
				}
			}
			return false
		}
	}
	cases := []struct {
		name        string
		labels      map[string]string
		annotations map[string]string
		known       KnownApplication
		want        string
	}{
		{"argo annotation tracking", nil, map[string]string{"argocd.argoproj.io/tracking-id": "shop:apps/Deployment:default/api"}, nil, "argocd:shop"},
		{"argo instance label", map[string]string{"argocd.argoproj.io/instance": "shop"}, nil, nil, "argocd:shop"},
		{"flux kustomization", map[string]string{"kustomize.toolkit.fluxcd.io/name": "apps"}, nil, nil, "flux:apps"},
		// Plain Helm sets this label too, so alone it is not evidence of GitOps.
		{"helm release, argo not in use", map[string]string{"app.kubernetes.io/instance": "monitoring"}, nil, nil, ""},
		// The case that remained ambiguous: Argo CD is installed AND a Helm release exists.
		{"helm release, argo in use, not an application", map[string]string{"app.kubernetes.io/instance": "monitoring"}, nil, argo("shop"), ""},
		{"instance label naming a real application", map[string]string{"app.kubernetes.io/instance": "shop"}, nil, argo("shop"), "argocd:shop"},
		{"unmanaged", map[string]string{"app": "api"}, nil, argo("shop"), ""},
	}
	for _, c := range cases {
		if got := GitOpsManager(c.labels, c.annotations, c.known); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// Until Argo CD has answered once nothing is known, so no label is trusted.
func TestApplicationSetKnowsNothingUntilArgoAnswers(t *testing.T) {
	var s ApplicationSet
	if s.Has("shop") {
		t.Fatal("an application was known before Argo CD answered")
	}
	s.Replace([]string{"shop", "billing"})
	if !s.Has("shop") || !s.Has("billing") || s.Has("monitoring") {
		t.Fatal("the set should hold exactly what Argo CD reported")
	}
	s.Replace([]string{"billing"}) // an application was deleted
	if s.Has("shop") || !s.Has("billing") {
		t.Fatal("Replace must drop applications Argo CD no longer reports")
	}
	s.Replace(nil) // Argo CD answered with none
	if s.Has("billing") {
		t.Fatal("an empty answer is an answer")
	}
	var missing *ApplicationSet
	if missing.Has("shop") {
		t.Fatal("a missing set knows nothing")
	}
}

func TestApplicationSetIsSafeForConcurrentUse(t *testing.T) {
	var s ApplicationSet
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s.Replace([]string{"a", "b"})
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = s.Has("a")
			}
		}()
	}
	wg.Wait()
}
