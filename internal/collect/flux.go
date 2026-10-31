package collect

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

var (
	fluxKustomizations = schema.GroupVersionResource{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Resource: "kustomizations"}
	fluxGitRepos       = schema.GroupVersionResource{Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "gitrepositories"}
)

// FluxCollector records each revision a Flux Kustomization applies, with the
// repository, branch and path it came from: the history a Git revert is planned
// from, as the Argo CD collector does for Applications. Read-only. Where Flux is
// not installed it notices once and stays quiet.
type FluxCollector struct {
	BaseCollector
	client   dynamic.Interface
	seen     map[string]string
	interval time.Duration
	absent   bool
}

func NewFluxCollector(client dynamic.Interface, out chan<- event.Event) *FluxCollector {
	return &FluxCollector{BaseCollector: BaseCollector{Out: out}, client: client, seen: map[string]string{},
		interval: durationFromEnv("FLUX_POLL_INTERVAL", 30*time.Second)}
}

func (f *FluxCollector) Run(ctx context.Context) error {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		if err := f.poll(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("flux poll failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (f *FluxCollector) poll(ctx context.Context) error {
	list, err := f.client.Resource(fluxKustomizations).Namespace("").List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		if !f.absent {
			slog.Info("Flux is not installed; the Flux collector is idle")
			f.absent = true
		}
		return nil
	}
	if err != nil {
		return err
	}
	f.absent = false
	for i := range list.Items {
		k := &list.Items[i]
		raw, _, _ := unstructured.NestedString(k.Object, "status", "lastAppliedRevision")
		revision := fluxCommit(raw)
		key := k.GetNamespace() + "/" + k.GetName()
		if revision == "" || f.seen[key] == revision {
			continue
		}
		// After a restart the current revision is recorded again; the history
		// lookup skips repeats of the same revision.
		f.seen[key] = revision
		payload := map[string]any{"revision": revision, "flux_revision": raw}
		if p, _, _ := unstructured.NestedString(k.Object, "spec", "path"); p != "" {
			payload["path"] = p
		}
		if suspended, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); suspended {
			payload["suspended"] = true
		}
		if ns, _, _ := unstructured.NestedString(k.Object, "spec", "targetNamespace"); ns != "" {
			payload["destination_namespace"] = ns
		}
		kind, _, _ := unstructured.NestedString(k.Object, "spec", "sourceRef", "kind")
		if kind == "GitRepository" {
			name, _, _ := unstructured.NestedString(k.Object, "spec", "sourceRef", "name")
			ns, _, _ := unstructured.NestedString(k.Object, "spec", "sourceRef", "namespace")
			if ns == "" {
				ns = k.GetNamespace()
			}
			if repo, err := f.client.Resource(fluxGitRepos).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
				payload["repo_url"], _, _ = unstructured.NestedString(repo.Object, "spec", "url")
				payload["branch"], _, _ = unstructured.NestedString(repo.Object, "spec", "ref", "branch")
			}
		} else if kind != "" {
			payload["source_kind"] = kind // a bucket or OCI artifact: not a Git revert
		}
		f.Emit(event.Event{Source: "flux", EntityKind: "Kustomization", EntityName: k.GetName(), Namespace: k.GetNamespace(),
			Type: "deploy", Severity: "info", Title: fmt.Sprintf("%s applied revision %s", k.GetName(), shortRevision(revision)),
			Payload: mustJSON(payload)})
	}
	return nil
}

// fluxCommit reads the commit from Flux's revision formats: "main@sha1:<sha>"
// (current), "main/<sha>" (older), or a bare "sha1:<sha>".
func fluxCommit(revision string) string {
	if i := strings.LastIndex(revision, "sha1:"); i >= 0 {
		return revision[i+len("sha1:"):]
	}
	if i := strings.LastIndex(revision, "/"); i >= 0 {
		return revision[i+1:]
	}
	return revision
}
