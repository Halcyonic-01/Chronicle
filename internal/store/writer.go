package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type Writer struct {
	pool *pgxpool.Pool
}

func NewWriter(pool *pgxpool.Pool) *Writer {
	return &Writer{pool: pool}
}

func (w *Writer) Run(ctx context.Context, in <-chan event.Event) error {
	buf := make([]event.Event, 0, 500)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}

		_, err := w.pool.CopyFrom(ctx,
			pgx.Identifier{"events"},
			[]string{"id", "occurred_at", "ingested_at", "source", "namespace",
				"entity_kind", "entity_name", "type", "severity", "title",
				"payload", "trace_id", "correlation_key"},
			pgx.CopyFromSlice(len(buf), func(i int) ([]any, error) {
				e := buf[i]
				return []any{e.ID, e.OccurredAt, e.IngestedAt, e.Source, e.Namespace,
					e.EntityKind, e.EntityName, e.Type, e.Severity, e.Title,
					e.Payload, e.TraceID, event.CorrelationKey(e)}, nil
			}),
		)

		if err != nil {
			slog.Error("batch insert failed", "n", len(buf), "err", err)
			// In a real app with Kafka, we'd nack or rely on Kafka offset retry
		}

		// Also push to Redis for the live dashboard in a real app
		// w.cacheRecent(ctx, buf)

		buf = buf[:0] // reset buffer
	}

	for {
		select {
		case e := <-in:
			buf = append(buf, e)
			if len(buf) >= 500 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush() // don't lose the tail on shutdown
			return ctx.Err()
		}
	}
}
