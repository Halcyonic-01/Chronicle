package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

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

// Consume delivers one event at a time and commits its Kafka offset only after
// the caller acknowledges that the event was persisted. This keeps Kafka's
// at-least-once delivery from becoming data loss when PostgreSQL is down or a
// batch is rejected.
func (b *KafkaBus) Consume(ctx context.Context, out chan<- event.Event, acknowledgements <-chan string) error {
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

		if acknowledgements == nil {
			if err := b.reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("committing Kafka message: %w", err)
			}
			continue
		}

		select {
		case acknowledgedID := <-acknowledgements:
			if acknowledgedID != e.ID {
				return fmt.Errorf("Kafka acknowledgement out of order: got %q, want %q", acknowledgedID, e.ID)
			}
			if err := b.reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("committing Kafka message: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
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
