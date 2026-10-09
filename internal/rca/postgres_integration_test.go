package rca

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
	"github.com/Halcyonic-01/Chronicle/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Run with CHRONICLE_TEST_POSTGRES_URL against a database containing the
// Chronicle migrations. The test is skipped by normal unit-test runs.
func TestPostgresRCAAndHistoricalGraph(t *testing.T) {
	url := os.Getenv("CHRONICLE_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set CHRONICLE_TEST_POSTGRES_URL to run the PostgreSQL integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UTC().Format("20060102150405.000000")
	candidateID, symptomID := "phase4-candidate-"+suffix, "phase4-symptom-"+suffix
	// redis is fronted by a Service; api calls that Service, so api is what a
	// failing redis pod takes down. The Service in front is the failure's own
	// entry point, which the blast radius deliberately leaves out.
	from := "default/Deployment/redis"
	pod := "default/Pod/redis-1"
	svc := "default/Service/redis"
	apiSvc, apiPod := "default/Service/api", "default/Pod/api-1"
	keys := []string{from, pod, svc, apiSvc, apiPod}
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM events WHERE id IN ($1,$2)`, candidateID, symptomID)
		_, _ = pool.Exec(ctx, `DELETE FROM graph_edges WHERE source = 'integration' AND (from_key = ANY($1) OR to_key = ANY($1))`, keys)
	}()

	now := time.Now().UTC()
	valid := now.Add(-time.Hour)
	for _, e := range [][3]string{{from, pod, "owns"}, {svc, pod, "routes_to"}, {apiSvc, apiPod, "routes_to"}, {apiPod, svc, "calls"}} {
		if _, err = pool.Exec(ctx, `INSERT INTO graph_edges (from_key,to_key,kind,weight,source,valid_from) VALUES ($1,$2,$3,1,'integration',$4)`, e[0], e[1], e[2], valid); err != nil {
			t.Fatal(err)
		}
	}
	_, err = pool.Exec(ctx, `INSERT INTO events (id,occurred_at,ingested_at,source,namespace,entity_kind,entity_name,type,severity,title,payload) VALUES ($1,$2,$2,'integration','default','Deployment','redis','deploy','info','redis deployed','{}'),($3,$4,$4,'integration','default','Pod','redis-1','became_unready','warning','redis stopped serving','{}')`, candidateID, now.Add(-time.Minute), symptomID, now)
	if err != nil {
		t.Fatal(err)
	}

	source := NewPostgresEventSource(pool)
	result, err := (&Analyzer{Events: source, Graph: &ReplayerGraphSource{Replayer: replay.NewReplayer(pool), Historical: store.NewGraphStore(pool)}, MaxHops: 3}).Analyze(ctx, event.Event{ID: symptomID, IngestedAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "redis-1", Type: "became_unready", Severity: "warning", Title: "redis stopped serving"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].Event.ID != candidateID {
		t.Fatalf("unexpected RCA candidates: %#v", result.Candidates)
	}
	if result.BlastRadius.AffectedServices != 1 {
		t.Fatalf("expected one affected service, got %#v", result.BlastRadius)
	}
	if len(result.Evidence) == 0 {
		t.Fatal("expected historical graph evidence")
	}

	graphStore := store.NewGraphStore(pool)
	edges, err := graphStore.At(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) < 2 {
		t.Fatalf("expected historical graph edges, got %d", len(edges))
	}
}
