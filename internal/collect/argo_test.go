package collect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// The Argo collector keeps the set of real Applications current, so the rest of
// Chronicle can tell an Application from a Helm release carrying the same label.
func TestTheArgoCollectorPublishesTheApplicationsArgoReports(t *testing.T) {
	body := `{"items":[
		{"metadata":{"name":"shop"},"status":{"sync":{"status":"Synced","revision":"abc123"},"health":{"status":"Healthy"}}},
		{"metadata":{"name":"billing"},"status":{"sync":{"status":"Synced","revision":"def456"},"health":{"status":"Healthy"}}},
		{"metadata":{"name":""},"status":{}}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/applications" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	t.Setenv("ARGOCD_URL", server.URL)
	t.Setenv("ARGOCD_TOKEN", "")

	var apps graph.ApplicationSet
	out := make(chan event.Event, 16)
	collector := NewArgoCollectorFromEnv(out, &apps)
	if collector == nil {
		t.Fatal("expected a collector when ARGOCD_URL is set")
	}
	if apps.Has("shop") {
		t.Fatal("nothing is known before the first poll")
	}
	if err := collector.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !apps.Has("shop") || !apps.Has("billing") || apps.Has("monitoring") || apps.Has("") {
		t.Fatal("the set should hold exactly the named applications Argo CD reported")
	}

	// A later poll that no longer lists an application removes it.
	body = `{"items":[{"metadata":{"name":"billing"},"status":{}}]}`
	if err := collector.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if apps.Has("shop") || !apps.Has("billing") {
		t.Fatal("a deleted application must leave the set")
	}

	// An unreachable Argo CD keeps the last answer instead of forgetting it.
	server.Close()
	_ = collector.poll(context.Background())
	if !apps.Has("billing") {
		t.Fatal("a failed poll must not empty the set")
	}
}

// The set is optional: without one the collector behaves as before.
func TestTheArgoCollectorWorksWithoutAnApplicationSet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"shop"},"status":{"sync":{"status":"Synced","revision":"abc"}}}]}`))
	}))
	defer server.Close()
	t.Setenv("ARGOCD_URL", server.URL)
	out := make(chan event.Event, 4)
	collector := NewArgoCollectorFromEnv(out, nil)
	if err := collector.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("the sync event should still be emitted")
	}
}

// A sync event says where the Application's desired state lives, so a revert can
// be prepared in the right repository, branch and path.
func TestAnArgoSyncRecordsWhereTheDesiredStateLives(t *testing.T) {
	body := `{"items":[
		{"metadata":{"name":"shop"},"spec":{"source":{"repoURL":"https://git.example.com/acme/gitops.git","path":"apps/shop","targetRevision":"main"},
		  "destination":{"namespace":"default"},"syncPolicy":{"automated":{"selfHeal":true}}},
		 "status":{"sync":{"status":"Synced","revision":"abc123"}}},
		{"metadata":{"name":"charts"},"spec":{"source":{"repoURL":"https://charts.example.com","chart":"redis","targetRevision":"1.2.3"}},
		 "status":{"sync":{"status":"Synced","revision":"1.2.3"}}},
		{"metadata":{"name":"multi"},"spec":{"sources":[{},{}]},"status":{"sync":{"status":"Synced","revision":"x"}}}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer server.Close()
	t.Setenv("ARGOCD_URL", server.URL)
	out := make(chan event.Event, 16)
	if err := NewArgoCollectorFromEnv(out, nil).poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	by := map[string]event.Event{}
	for _, e := range drain(out) {
		if e.Type == "deploy" {
			by[e.EntityName] = e
		}
	}
	shop := by["shop"]
	for path, want := range map[string]string{"revision": "abc123", "repo_url": "https://git.example.com/acme/gitops.git", "path": "apps/shop",
		"branch": "main", "destination_namespace": "default", "auto_sync": "true", "multi_source": "false"} {
		if got := gjsonString(shop, path); got != want {
			t.Errorf("shop %s = %q, want %q", path, got, want)
		}
	}
	if gjsonString(by["charts"], "chart") != "redis" || gjsonString(by["charts"], "auto_sync") != "false" {
		t.Errorf("a Helm chart source must be marked: %s", by["charts"].Payload)
	}
	if gjsonString(by["multi"], "multi_source") != "true" {
		t.Errorf("a multi-source Application must be marked: %s", by["multi"].Payload)
	}
}

// A sync is dated by Argo CD's own record, not by the poll that noticed it, so it
// precedes the changes it made.
func TestAnArgoSyncIsDatedWhenArgoSyncedIt(t *testing.T) {
	body := `{"items":[
		{"metadata":{"name":"op"},"status":{"sync":{"revision":"r2"},"operationState":{"startedAt":"2026-10-31T12:00:05Z","syncResult":{"revision":"r2"}}}},
		{"metadata":{"name":"hist"},"status":{"sync":{"revision":"r9"},"history":[{"revision":"r8","deployedAt":"2026-10-31T11:00:00Z"},{"revision":"r9","deployedAt":"2026-10-31T11:30:00Z"}]}},
		{"metadata":{"name":"unknown"},"status":{"sync":{"revision":"r1"}}}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer server.Close()
	t.Setenv("ARGOCD_URL", server.URL)
	out := make(chan event.Event, 16)
	if err := NewArgoCollectorFromEnv(out, nil).poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	when := map[string]string{}
	for _, e := range drain(out) {
		if e.Type == "deploy" {
			when[e.EntityName] = e.OccurredAt.UTC().Format(time.RFC3339)
		}
	}
	if when["op"] != "2026-10-31T12:00:05Z" || when["hist"] != "2026-10-31T11:30:00Z" {
		t.Fatalf("sync times %v", when)
	}
	if seen, _ := time.Parse(time.RFC3339, when["unknown"]); time.Since(seen) > time.Minute {
		t.Fatalf("without Argo's own date the event is dated when it was seen: %v", when["unknown"])
	}
}
