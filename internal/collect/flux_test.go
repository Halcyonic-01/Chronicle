package collect

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

func fluxObject(group, version, kind, ns, name string, fields map[string]any) *unstructured.Unstructured {
	obj := map[string]any{"apiVersion": group + "/" + version, "kind": kind, "metadata": map[string]any{"name": name, "namespace": ns}}
	for k, v := range fields {
		obj[k] = v
	}
	return &unstructured.Unstructured{Object: obj}
}

func fluxClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		fluxKustomizations: "KustomizationList", fluxGitRepos: "GitRepositoryList"}, objs...)
}

// Each revision a Kustomization applies is recorded once, with the repository,
// branch and path behind it.
func TestFluxRevisionsAreRecordedWithTheirSource(t *testing.T) {
	repo := fluxObject("source.toolkit.fluxcd.io", "v1", "GitRepository", "flux-system", "gitops", map[string]any{
		"spec": map[string]any{"url": "https://git.example.com/acme/gitops.git", "ref": map[string]any{"branch": "main"}}})
	app := fluxObject("kustomize.toolkit.fluxcd.io", "v1", "Kustomization", "flux-system", "apps", map[string]any{
		"spec":   map[string]any{"path": "./apps/prod", "sourceRef": map[string]any{"kind": "GitRepository", "name": "gitops"}},
		"status": map[string]any{"lastAppliedRevision": "main@sha1:0123456789abcdef0123456789abcdef01234567"}})
	client := fluxClient(repo, app)
	out := make(chan event.Event, 8)
	c := NewFluxCollector(client, out)
	if err := c.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drain(out)
	if len(events) != 1 {
		t.Fatalf("events %+v", events)
	}
	e := events[0]
	if e.Source != "flux" || e.EntityKind != "Kustomization" || e.EntityName != "apps" || e.Type != "deploy" {
		t.Fatalf("%+v", e)
	}
	for path, want := range map[string]string{"revision": "0123456789abcdef0123456789abcdef01234567", "repo_url": "https://git.example.com/acme/gitops.git",
		"branch": "main", "path": "./apps/prod"} {
		if got := gjsonString(e, path); got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	// The same revision again is not news.
	if err := c.poll(context.Background()); err != nil || len(drain(out)) != 0 {
		t.Fatalf("a repeat was recorded: %v", err)
	}
}

func TestFluxRevisionFormats(t *testing.T) {
	for in, want := range map[string]string{"main@sha1:abc": "abc", "main/abc": "abc", "sha1:abc": "abc", "abc": "abc"} {
		if got := fluxCommit(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}
