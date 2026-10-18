package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tidwall/gjson"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/metrics"
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
		batch = dropSeenCatchUps(batch, func(e event.Event) (bool, error) { return transitionRecorded(flushCtx, w.pool, e) })
		stored, err := insertEvents(flushCtx, w.pool, batch)

		if err != nil {
			metrics.EventWriteErrors.Inc()
			slog.Error("batch insert failed", "n", len(buf), "unique", len(batch), "err", err)
			return
		}
		recordStored(batch, stored, time.Now())
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
			fresh := batch[:0:0]
			for _, e := range batch {
				if _, ok := stored[e.ID]; ok {
					fresh = append(fresh, e)
				}
			}
			if len(fresh) > 0 {
				if err := w.cache.Add(flushCtx, fresh...); err != nil {
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

// dropSeenCatchUps removes catch-up events for a change the store already holds.
// A new leader compares what it sees with the checkpoint the last one saved, and
// that checkpoint lags: a change reported just before a crash is reported again.
// The same template transition (from and to) is the same change. If the store
// cannot be asked, the event is kept: a repeat is better than a gap.
func dropSeenCatchUps(batch []event.Event, seen func(event.Event) (bool, error)) []event.Event {
	live := map[string]bool{}
	for _, e := range batch {
		if !isCatchUp(e) {
			live[transitionKey(e)] = true
		}
	}
	kept := batch[:0:0]
	for _, e := range batch {
		if isCatchUp(e) {
			if live[transitionKey(e)] {
				continue
			}
			if ok, err := seen(e); err == nil && ok {
				continue
			}
		}
		kept = append(kept, e)
	}
	return kept
}

func transitionKey(e event.Event) string {
	return strings.Join([]string{e.Namespace, e.EntityKind, e.EntityName, e.Type,
		gjson.GetBytes(e.Payload, "from_template_hash").String(), gjson.GetBytes(e.Payload, "to_template_hash").String()}, "|")
}

func isCatchUp(e event.Event) bool {
	return gjson.GetBytes(e.Payload, "missed").Bool() &&
		gjson.GetBytes(e.Payload, "from_template_hash").String() != "" &&
		gjson.GetBytes(e.Payload, "to_template_hash").String() != ""
}

// transitionRecorded asks whether an event the collector saw happen already
// describes the same move between the same two templates.
func transitionRecorded(ctx context.Context, pool *pgxpool.Pool, e event.Event) (bool, error) {
	var found bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM events
		  WHERE namespace=$1 AND entity_kind=$2 AND entity_name=$3 AND type=$4
		    AND occurred_at >= $5
		    AND payload->>'from_template_hash'=$6 AND payload->>'to_template_hash'=$7
		    AND COALESCE(payload->>'missed','false') <> 'true')`,
		e.Namespace, e.EntityKind, e.EntityName, e.Type, e.OccurredAt.Add(-10*time.Minute),
		gjson.GetBytes(e.Payload, "from_template_hash").String(), gjson.GetBytes(e.Payload, "to_template_hash").String()).Scan(&found)
	return found, err
}

// recordStored counts what a write kept and how late it was: a duplicate is a
// fact the store already had, and has no lag of its own.
func recordStored(batch []event.Event, stored map[string]struct{}, now time.Time) {
	for _, e := range batch {
		if _, ok := stored[e.ID]; !ok {
			metrics.EventsDuplicate.Inc()
			continue
		}
		metrics.EventsStored.WithLabelValues(e.Source).Inc()
		metrics.EventLag.WithLabelValues(e.Source).Observe(now.Sub(e.IngestedAt).Seconds())
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
// It reports which events it actually stored. A conflict means the fact was
// already recorded, and the recent-event cache must not advertise a row the
// store rejected: the dashboard lists from the cache and analyses from the
// store, so anything cached but not stored becomes a signal that cannot be
// opened.
func insertEvents(ctx context.Context, pool *pgxpool.Pool, events []event.Event) (map[string]struct{}, error) {
	if len(events) == 0 {
		return nil, nil
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
	// Untargeted so it covers both unique constraints: the id, which absorbs
	// Kafka redelivering one message, and the content key, which absorbs a
	// collector re-observing one fact under a new id. RETURNING then names the
	// rows that were genuinely new.
	query.WriteString(" ON CONFLICT DO NOTHING RETURNING id")
	rows, err := pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stored := make(map[string]struct{}, len(events))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		stored[id] = struct{}{}
	}
	return stored, rows.Err()
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
