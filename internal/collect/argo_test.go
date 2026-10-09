package collect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

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
