package store

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/metrics"
)

func TestOnlyStoredEventsCountTowardsLagAndDuplicatesAreCountedApart(t *testing.T) {
	now := time.Now()
	batch := []event.Event{
		{ID: "a", Source: "lagtest", IngestedAt: now.Add(-4 * time.Second)},
		{ID: "b", Source: "lagtest", IngestedAt: now.Add(-9 * time.Second)},
		{ID: "c", Source: "lagtest", IngestedAt: now.Add(-1 * time.Second)},
	}
	dupBefore := testutil.ToFloat64(metrics.EventsDuplicate)
	recordStored(batch, map[string]struct{}{"a": {}, "c": {}}, now)

	if got := testutil.ToFloat64(metrics.EventsStored.WithLabelValues("lagtest")); got != 2 {
		t.Fatalf("stored %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.EventsDuplicate) - dupBefore; got != 1 {
		t.Fatalf("duplicates %v, want 1", got)
	}
	// The duplicate's 9 seconds must not appear in the lag: two samples, 5s in all.
	var m dto.Metric
	if err := metrics.EventLag.WithLabelValues("lagtest").(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	if n, sum := m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum(); n != 2 || sum < 4.9 || sum > 5.1 {
		t.Fatalf("lag samples %d summing to %.2fs, want 2 summing to 5s", n, sum)
	}
}

func moved(id, from, to string, missed bool) event.Event {
	payload := `{"from_template_hash":"` + from + `","to_template_hash":"` + to + `"`
	if missed {
		payload += `,"missed":true`
	}
	return event.Event{ID: id, Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "config_change", Payload: []byte(payload + "}")}
}

func ids(es []event.Event) string {
	out := ""
	for _, e := range es {
		out += e.ID + " "
	}
	return out
}

func TestACatchUpIsDroppedOnlyWhenThatExactMoveIsAlreadyRecorded(t *testing.T) {
	stored := func(event.Event) (bool, error) { return true, nil }
	unknown := func(event.Event) (bool, error) { return false, nil }
	broken := func(event.Event) (bool, error) { return false, context.DeadlineExceeded }

	if got := ids(dropSeenCatchUps([]event.Event{moved("c", "A", "B", true)}, stored)); got != "" {
		t.Errorf("a catch-up for a recorded move was kept: %q", got)
	}
	if got := ids(dropSeenCatchUps([]event.Event{moved("c", "A", "B", true)}, unknown)); got != "c " {
		t.Errorf("a catch-up for an unrecorded move was dropped: %q", got)
	}
	if got := ids(dropSeenCatchUps([]event.Event{moved("c", "A", "B", true)}, broken)); got != "c " {
		t.Errorf("a catch-up was dropped although the store could not be asked: %q", got)
	}
	// Arriving in one batch with the live event for the same move.
	if got := ids(dropSeenCatchUps([]event.Event{moved("live", "A", "B", false), moved("c", "A", "B", true)}, unknown)); got != "live " {
		t.Errorf("same-batch duplicate: %q", got)
	}
	// A different move, or a live event, is never touched.
	if got := ids(dropSeenCatchUps([]event.Event{moved("live", "A", "B", false), moved("c", "B", "C", true)}, unknown)); got != "live c " {
		t.Errorf("a different move was dropped: %q", got)
	}
	// A catch-up without hashes (an older checkpoint) cannot be matched, so stays.
	legacy := event.Event{ID: "old", Type: "config_change", Payload: []byte(`{"missed":true}`)}
	if got := ids(dropSeenCatchUps([]event.Event{legacy}, stored)); got != "old " {
		t.Errorf("a hashless catch-up was dropped: %q", got)
	}
}
