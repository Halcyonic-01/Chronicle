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

func (s *PostgresEventSource) GetEvent(ctx context.Context, id string) (*event.Event, error) {
	var e event.Event
	err := s.pool.QueryRow(ctx, `SELECT id, occurred_at, ingested_at, source, namespace, entity_kind, entity_name, type, severity, title, payload, trace_id, correlation_key FROM events WHERE id = $1`, id).Scan(&e.ID, &e.OccurredAt, &e.IngestedAt, &e.Source, &e.Namespace, &e.EntityKind, &e.EntityName, &e.Type, &e.Severity, &e.Title, &e.Payload, &e.TraceID, &e.CorrelationKey)
	if err != nil {
		return nil, err
	}
	return &e, nil
}
