package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

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

func (w *Writer) Run(ctx context.Context, in <-chan event.Event, acknowledgements ...chan<- string) error {
	buf := make([]event.Event, 0, 500)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var acknowledge chan<- string
	if len(acknowledgements) > 0 {
		acknowledge = acknowledgements[0]
	}

	flush := func(flushCtx context.Context) {
		if len(buf) == 0 {
			return
		}

		batch := uniqueEvents(buf)
		err := insertEvents(flushCtx, w.pool, batch)

		if err != nil {
			slog.Error("batch insert failed", "n", len(buf), "unique", len(batch), "err", err)
			return
		}
		if acknowledge != nil {
			for _, e := range buf {
				select {
				case acknowledge <- e.ID:
				case <-flushCtx.Done():
					return
				}
			}
		}

		if w.cache != nil {
			if err := w.cache.Add(flushCtx, batch...); err != nil {
				slog.Warn("failed to update recent event cache", "err", err)
			}
		}

		buf = buf[:0] // reset buffer
	}

	for {
		select {
		case e := <-in:
			buf = append(buf, e)
			if len(buf) >= 500 {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		case <-ctx.Done():
			// Don't lose the tail on shutdown. ctx is already cancelled here,
			// so the final write needs its own short-lived context.
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			flush(shutdownCtx)
			cancel()
			return ctx.Err()
		}
	}
}

const insertEventPrefix = `
INSERT INTO events (
    id, occurred_at, ingested_at, source, namespace,
    entity_kind, entity_name, type, severity, title,
    payload, trace_id, correlation_key
) VALUES `

// insertEvents is deliberately idempotent because Kafka delivery is
// at-least-once. A replayed message must not poison the whole batch or cause
// valid events behind it to be retried forever.
func insertEvents(ctx context.Context, pool *pgxpool.Pool, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}

	var query strings.Builder
	query.WriteString(insertEventPrefix)
	args := make([]any, 0, len(events)*13)
	for i, e := range events {
		if i > 0 {
			query.WriteString(",")
		}
		base := i*13 + 1
		_, _ = fmt.Fprintf(&query, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6,
			base+7, base+8, base+9, base+10, base+11, base+12)
		args = append(args,
			e.ID, e.OccurredAt, e.IngestedAt, e.Source, e.Namespace,
			e.EntityKind, e.EntityName, e.Type, e.Severity, e.Title,
			e.Payload, e.TraceID, event.CorrelationKey(e))
	}
	query.WriteString(" ON CONFLICT (id) DO NOTHING")
	_, err := pool.Exec(ctx, query.String(), args...)
	return err
}

func uniqueEvents(events []event.Event) []event.Event {
	unique := make([]event.Event, 0, len(events))
	seen := make(map[string]struct{}, len(events))
	for _, e := range events {
		if _, exists := seen[e.ID]; exists {
			continue
		}
		seen[e.ID] = struct{}{}
		unique = append(unique, e)
	}
	return unique
}
