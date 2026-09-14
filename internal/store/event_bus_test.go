package store

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
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
