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
	pool  *pgxpool.Pool
	cache *RecentCache
}

func NewWriter(pool *pgxpool.Pool, caches ...*RecentCache) *Writer {
	var cache *RecentCache
	if len(caches) > 0 {
		cache = caches[0]
	}
	return &Writer{pool: pool, cache: cache}
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
			return
		}

		if w.cache != nil {
			for _, e := range buf {
				if err := w.cache.Add(ctx, e); err != nil {
					slog.Warn("failed to update recent event cache", "err", err)
				}
			}
		}

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
