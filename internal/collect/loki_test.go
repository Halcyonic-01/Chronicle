package collect

import (
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

func TestLokiDedupeSetForgetsOldFingerprints(t *testing.T) {
	out := make(chan event.Event, 4)
	collector := NewLokiCollector(out)
	labels := map[string]string{"namespace": "default", "pod": "api-1"}

	collector.emitLog(labels, "1700000000000000000", "error: connection refused")
	if len(drain(out)) != 1 {
		t.Fatal("the first occurrence of a log line should be emitted")
	}
	collector.emitLog(labels, "1700000000000000000", "error: connection refused")
	if len(drain(out)) != 0 {
		t.Fatal("an identical line within the retention window is a duplicate")
	}
	if len(collector.seen) != 1 {
		t.Fatalf("expected one remembered fingerprint, got %d", len(collector.seen))
	}

	// Without expiry this set grows once per distinct log line, forever.
	collector.forgetStaleFingerprints(time.Now().Add(seenRetention + time.Minute))
	if len(collector.seen) != 0 {
		t.Fatalf("fingerprints past the retention window must be dropped, %d remain", len(collector.seen))
	}
	collector.emitLog(labels, "1700000000000000000", "error: connection refused")
	if len(drain(out)) != 1 {
		t.Fatal("a line seen again after the window has expired should be emitted")
	}
}
