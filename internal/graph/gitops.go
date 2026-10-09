package graph

import (
	"strings"
	"sync"
)

// KnownApplication reports whether name is an Argo CD Application that Argo CD
// itself reported. Nil means Argo CD is not in use, or has not answered yet.
type KnownApplication func(name string) bool

// ApplicationSet is the Application names the Argo CD collector last read. It is
// safe for concurrent use: the collector replaces it while the graph builder,
// the Kubernetes collector and the healing executor read it.
type ApplicationSet struct {
	mu    sync.RWMutex
	names map[string]struct{}
	read  bool
}

// Replace swaps in the names Argo CD currently reports.
func (s *ApplicationSet) Replace(names []string) {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	s.mu.Lock()
	s.names, s.read = set, true
	s.mu.Unlock()
}

// Has reports whether Argo CD reports an Application of that name. Until Argo CD
// has answered once, nothing is known.
func (s *ApplicationSet) Has(name string) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.names[name]
	return s.read && ok
}

// GitOpsManager names the GitOps controller that owns an object's desired
// state ("argocd:<app>", "flux:<name>"), or "" when none is evident. A change
// Chronicle writes to such an object is reverted on the next reconcile, so
// remediation must go through Git instead.
//
// Only controller-specific markers count on their own: Argo CD's tracking-id
// annotation and instance label, Flux's name labels. app.kubernetes.io/instance
// is also set by plain Helm, so it counts as Argo CD ownership only when its
// value is an Application Argo CD reported (known). A Helm release is never an
// Argo CD Application just because Argo CD is installed.
func GitOpsManager(labels, annotations map[string]string, known KnownApplication) string {
	if id := annotations["argocd.argoproj.io/tracking-id"]; id != "" {
		app, _, _ := strings.Cut(id, ":")
		return "argocd:" + app
	}
	if app := labels["argocd.argoproj.io/instance"]; app != "" {
		return "argocd:" + app
	}
	for _, key := range []string{"kustomize.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/name"} {
		if name := labels[key]; name != "" {
			return "flux:" + name
		}
	}
	if app := labels["app.kubernetes.io/instance"]; app != "" && known != nil && known(app) {
		return "argocd:" + app
	}
	return ""
}
