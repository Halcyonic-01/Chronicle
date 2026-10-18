package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// Run with CHRONICLE_TEST_POSTGRES_URL against a database with the migrations.
func TestPostgresTransitionRecordedMatchesOnlyTheSameMove(t *testing.T) {
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
	name := fmt.Sprintf("catchup-%d", time.Now().UnixNano())
	defer pool.Exec(ctx, `DELETE FROM events WHERE entity_name=$1`, name)

	at := time.Now().UTC().Truncate(time.Second)
	mk := func(id, title, from, to string, missed bool, when time.Time) event.Event {
		e := moved(id+name, from, to, missed)
		e.Source, e.Namespace, e.EntityName, e.Title = "k8s", "default", name, title
		e.OccurredAt, e.IngestedAt, e.Severity = when, when, "info"
		return e
	}
	live := mk("live", "live", "A", "B", false, at)
	if _, err := insertEvents(ctx, pool, []event.Event{live,
		mk("caught", "caught", "B", "C", true, at),                               // a catch-up is not evidence of a move
		mk("stale", "stale", "X", "Y", false, at.Add(-time.Hour))}); err != nil { // far older than any catch-up
		t.Fatal(err)
	}
	cases := []struct {
		name string
		e    event.Event
		want bool
	}{
		{"the same move", mk("q1", "q1", "A", "B", true, at), true},
		{"another move", mk("q2", "q2", "A", "D", true, at), false},
		{"only a catch-up recorded it", mk("q3", "q3", "B", "C", true, at), false},
		{"recorded long before", mk("q4", "q4", "X", "Y", true, at), false},
	}
	for _, c := range cases {
		got, err := transitionRecorded(ctx, pool, c.e)
		if err != nil || got != c.want {
			t.Errorf("%s: got %v, %v want %v", c.name, got, err, c.want)
		}
	}
}
