package store

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

func TestCollectCommitBatchGroupsQueuedAcknowledgements(t *testing.T) {
	inFlight := make(chan pendingMessage, 3)
	acknowledgements := make(chan string, 3)
	for _, id := range []string{"a", "b", "c"} {
		inFlight <- pendingMessage{message: kafka.Message{Offset: int64(len(id))}, id: id}
	}
	// "a" is delivered as the triggering acknowledgement; the rest are already
	// queued, which is what a single writer flush of three events looks like.
	acknowledgements <- "b"
	acknowledgements <- "c"

	batch, err := (&KafkaBus{}).collectCommitBatch(context.Background(), inFlight, acknowledgements, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 3 {
		t.Fatalf("a flush of three events should commit in one batch, got %d commits", len(batch))
	}
}

func TestCollectCommitBatchStopsWhenNoFurtherAcknowledgementIsQueued(t *testing.T) {
	inFlight := make(chan pendingMessage, 2)
	inFlight <- pendingMessage{id: "a"}
	inFlight <- pendingMessage{id: "b"}

	batch, err := (&KafkaBus{}).collectCommitBatch(context.Background(), inFlight, make(chan string), "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("only the acknowledged message may be committed, got %d", len(batch))
	}
	if pending := <-inFlight; pending.id != "b" {
		t.Fatalf("unacknowledged message was consumed: %q", pending.id)
	}
}

func TestCollectCommitBatchSkipsAcknowledgementsFromAPreviousGeneration(t *testing.T) {
	// Consume restarted after an error: "stale" was persisted but its offset was
	// never committed, so Kafka redelivered it as the new pending message "a".
	inFlight := make(chan pendingMessage, 1)
	inFlight <- pendingMessage{message: kafka.Message{Offset: 7}, id: "a"}
	acknowledgements := make(chan string, 1)
	acknowledgements <- "a"

	batch, err := (&KafkaBus{}).collectCommitBatch(context.Background(), inFlight, acknowledgements, "stale")
	if err != nil {
		t.Fatalf("a stale acknowledgement must not fail the consumer: %v", err)
	}
	if len(batch) != 1 || batch[0].Offset != 7 {
		t.Fatalf("expected the redelivered message to be committed, got %+v", batch)
	}
}

func TestCollectCommitBatchStopsWhenAcknowledgementsNeverMatch(t *testing.T) {
	inFlight := make(chan pendingMessage, 1)
	inFlight <- pendingMessage{id: "a"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := (&KafkaBus{}).collectCommitBatch(ctx, inFlight, make(chan string), "z"); err == nil {
		t.Fatal("an unmatched acknowledgement must never commit an offset")
	}
}

// A synchronous write of one event waits for BatchTimeout; at the library's
// one-second default the publisher managed one event per second, and an
// incident's log flood queued alerts minutes behind (found by a chaos run).
func TestPublishingDoesNotWaitASecondPerEvent(t *testing.T) {
	bus := NewKafkaBus("localhost:9092", "t", "g")
	if bus.writer.BatchTimeout > 50*time.Millisecond {
		t.Fatalf("a lone event lingers %v before it is sent", bus.writer.BatchTimeout)
	}
}

func TestDrainBatchTakesWhatIsQueuedWithoutWaiting(t *testing.T) {
	queue := make(chan event.Event, 10)
	for _, id := range []string{"b", "c"} {
		queue <- event.Event{ID: id}
	}
	got := DrainBatch(event.Event{ID: "a"}, queue, 10)
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Fatalf("queued events should follow in order: %+v", got)
	}
	if one := DrainBatch(event.Event{ID: "x"}, queue, 10); len(one) != 1 {
		t.Fatalf("an empty queue sends the one event at once: %+v", one)
	}
}

func TestDrainBatchStopsAtItsLimit(t *testing.T) {
	queue := make(chan event.Event, 10)
	for i := 0; i < 5; i++ {
		queue <- event.Event{ID: string(rune('b' + i))}
	}
	if got := DrainBatch(event.Event{ID: "a"}, queue, 3); len(got) != 3 || len(queue) != 3 {
		t.Fatalf("took %d, left %d", len(got), len(queue))
	}
}
