package rca

import (
	"context"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresEventSource struct{ pool *pgxpool.Pool }

func NewPostgresEventSource(pool *pgxpool.Pool) *PostgresEventSource {
	return &PostgresEventSource{pool: pool}
}
func (s *PostgresEventSource) EventsBetween(ctx context.Context, from, to time.Time) ([]event.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, occurred_at, ingested_at, source, namespace, entity_kind, entity_name, type, severity, title, payload, trace_id, correlation_key FROM events WHERE ingested_at >= $1 AND ingested_at < $2 ORDER BY ingested_at ASC`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []event.Event
	for rows.Next() {
		var e event.Event
		if err := rows.Scan(&e.ID, &e.OccurredAt, &e.IngestedAt, &e.Source, &e.Namespace, &e.EntityKind, &e.EntityName, &e.Type, &e.Severity, &e.Title, &e.Payload, &e.TraceID, &e.CorrelationKey); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// RecentEvents returns the newest events in reverse chronological order for
// the operations console. Filters are deliberately kept server-side so the
// UI does not need to load an unbounded event table.
func (s *PostgresEventSource) RecentEvents(ctx context.Context, from, to time.Time, limit, offset int) ([]event.Event, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, ingested_at, source, namespace, entity_kind,
		       entity_name, type, severity, title, payload, trace_id, correlation_key
		FROM events
		WHERE ingested_at >= $1 AND ingested_at <= $2
		ORDER BY ingested_at DESC
		LIMIT $3 OFFSET $4`, from, to, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []event.Event
	for rows.Next() {
		var e event.Event
		if err := rows.Scan(&e.ID, &e.OccurredAt, &e.IngestedAt, &e.Source, &e.Namespace, &e.EntityKind, &e.EntityName, &e.Type, &e.Severity, &e.Title, &e.Payload, &e.TraceID, &e.CorrelationKey); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// CountEvents returns the number of events received in the requested window.
// It is kept separate from RecentEvents so the UI can show a real total while
// still loading a bounded page of event details.
func (s *PostgresEventSource) CountEvents(ctx context.Context, from, to time.Time) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM events
		WHERE ingested_at >= $1 AND ingested_at <= $2`, from, to).Scan(&count)
	return count, err
}

func (s *PostgresEventSource) GetEvent(ctx context.Context, id string) (*event.Event, error) {
	var e event.Event
	err := s.pool.QueryRow(ctx, `SELECT id, occurred_at, ingested_at, source, namespace, entity_kind, entity_name, type, severity, title, payload, trace_id, correlation_key FROM events WHERE id = $1`, id).Scan(&e.ID, &e.OccurredAt, &e.IngestedAt, &e.Source, &e.Namespace, &e.EntityKind, &e.EntityName, &e.Type, &e.Severity, &e.Title, &e.Payload, &e.TraceID, &e.CorrelationKey)
	if err != nil {
		return nil, err
	}
	return &e, nil
}
