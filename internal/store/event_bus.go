package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// maxInFlight bounds how many fetched-but-unacknowledged messages Chronicle
// keeps in memory. It must stay above the writer's batch size, otherwise the
// writer can never fill a batch and every event becomes its own INSERT.
const maxInFlight = 2000

// maxCommitBatch bounds how many offsets are committed in a single call.
const maxCommitBatch = 500

// KafkaBus provides the durable buffer between collectors and the database
// writer. A message contains the canonical Event JSON so replay and retries do
// not change the event shape.
type KafkaBus struct {
	writer *kafka.Writer
	reader *kafka.Reader
}

func NewKafkaBus(brokers, topic, group string) *KafkaBus {
	addresses := strings.FieldsFunc(brokers, func(r rune) bool { return r == ',' || r == ' ' })
	if len(addresses) == 0 {
		addresses = []string{"localhost:9092"}
	}
	return &KafkaBus{
		writer: &kafka.Writer{Addr: kafka.TCP(addresses...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true},
		reader: kafka.NewReader(kafka.ReaderConfig{Brokers: addresses, Topic: topic, GroupID: group, MinBytes: 1, MaxBytes: 10 << 20}),
	}
}

func (b *KafkaBus) Publish(ctx context.Context, e event.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := []byte(event.CorrelationKey(e))
	if len(key) == 0 {
		key = []byte(e.ID)
	}
	return b.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: payload})
}

// pendingMessage pairs a fetched Kafka message with the ID of the event it
// carries, so an acknowledgement can be matched to the offset it commits.
type pendingMessage struct {
	message kafka.Message
	id      string
}

// Consume streams events to the caller and commits Kafka offsets only after
// the caller acknowledges that an event was persisted. Up to maxInFlight
// messages may be outstanding at once: fetching and acknowledging run
// concurrently so the writer can assemble real batches instead of being
// throttled to one event per flush interval. Kafka's at-least-once delivery
// still never becomes data loss, because an unacknowledged offset is never
// committed.
func (b *KafkaBus) Consume(ctx context.Context, out chan<- event.Event, acknowledgements <-chan string) error {
	if acknowledgements == nil {
		return b.consumeAutoCommit(ctx, out)
	}

	// Messages enter inFlight before their event is delivered downstream, so
	// the nth acknowledgement always refers to the nth pending message.
	inFlight := make(chan pendingMessage, maxInFlight)
	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for {
			message, err := b.reader.FetchMessage(groupCtx)
			if err != nil {
				return err
			}
			var e event.Event
			if err := json.Unmarshal(message.Value, &e); err != nil {
				return fmt.Errorf("decode Kafka event: %w", err)
			}
			select {
			case inFlight <- pendingMessage{message: message, id: e.ID}:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
			select {
			case out <- e:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case acknowledgedID := <-acknowledgements:
				batch, err := b.collectCommitBatch(groupCtx, inFlight, acknowledgements, acknowledgedID)
				if err != nil {
					return err
				}
				if err := b.reader.CommitMessages(groupCtx, batch...); err != nil {
					return fmt.Errorf("committing Kafka messages: %w", err)
				}
			}
		}
	})

	return group.Wait()
}

// collectCommitBatch pairs one acknowledgement with its pending message, then
// drains any acknowledgements already queued so a writer flush of N events
// commits in a single round trip rather than N.
//
// Acknowledgements that do not match the oldest pending message are discarded.
// They belong to a previous consumer generation: Consume restarts after an
// error while the writer keeps its acknowledgement channel, so an event that
// was persisted but whose offset was never committed is simply redelivered.
// Treating that as fatal would restart Consume forever.
func (b *KafkaBus) collectCommitBatch(ctx context.Context, inFlight <-chan pendingMessage, acknowledgements <-chan string, first string) ([]kafka.Message, error) {
	batch := make([]kafka.Message, 0, maxCommitBatch)
	acknowledgedID := first
	discarded := 0
	for {
		var pending pendingMessage
		select {
		case pending = <-inFlight:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		for acknowledgedID != pending.id {
			discarded++
			if discarded > maxInFlight {
				return nil, fmt.Errorf("Kafka acknowledgements never resynchronised: got %q, want %q", acknowledgedID, pending.id)
			}
			slog.Warn("discarding acknowledgement from a previous Kafka consumer generation",
				"acknowledged", acknowledgedID, "pending", pending.id)
			select {
			case acknowledgedID = <-acknowledgements:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		batch = append(batch, pending.message)
		if len(batch) >= maxCommitBatch {
			return batch, nil
		}
		select {
		case acknowledgedID = <-acknowledgements:
		default:
			return batch, nil
		}
	}
}

func (b *KafkaBus) consumeAutoCommit(ctx context.Context, out chan<- event.Event) error {
	for {
		message, err := b.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var e event.Event
		if err := json.Unmarshal(message.Value, &e); err != nil {
			return fmt.Errorf("decode Kafka event: %w", err)
		}
		select {
		case out <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := b.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("committing Kafka message: %w", err)
		}
	}
}

func (b *KafkaBus) Close() error {
	writerErr := b.writer.Close()
	readerErr := b.reader.Close()
	if writerErr != nil {
		return writerErr
	}
	return readerErr
}
