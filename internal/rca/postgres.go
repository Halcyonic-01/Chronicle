package rca

import (
	"context"
	"sort"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresEventSource struct{ pool *pgxpool.Pool }

func NewPostgresEventSource(pool *pgxpool.Pool) *PostgresEventSource {
	return &PostgresEventSource{pool: pool}
}
func (s *PostgresEventSource) EventsBetween(ctx context.Context, from, to time.Time) ([]event.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, occurred_at, ingested_at, source, namespace, entity_kind, entity_name, type, severity, title, payload, trace_id, correlation_key FROM events WHERE ingested_at >= $1 AND ingested_at < $2 ORDER BY ingested_at ASC, id ASC`, from, to)
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
// CountSignals is how many signals exist in the window, which is not the same
// as how many were fetched. Reporting the fetched count as the total made a
// capped page look like the whole story.
func (s *PostgresEventSource) CountSignals(ctx context.Context, from, to time.Time) (int, error) {
	var total int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM events
		WHERE ingested_at >= $1 AND ingested_at <= $2
		  AND severity IN ('warning','critical')`, from, to).Scan(&total)
	return total, err
}

// Signal is an event plus what its whole entity/type series looks like, so a
// capped page can still report how often the signal fired and when it began.
type Signal struct {
	event.Event
	Occurrences int       `json:"occurrences"`
	FirstSeen   time.Time `json:"first_seen"`
}

// signalsPerSeries bounds how many rows one entity/type series contributes.
const signalsPerSeries = 40

// RecentSignals returns warning and critical events. A chatty series -- a pod
// logging the same error every second -- used to fill the whole page and push
// every other incident out of view. Rows are now taken newest-first from each
// series in turn, so each series is represented before any one fills the page.
func (s *PostgresEventSource) RecentSignals(ctx context.Context, from, to time.Time, limit int) ([]Signal, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, ingested_at, source, namespace, entity_kind,
		       entity_name, type, severity, title, payload, trace_id, correlation_key,
		       total, first_seen
		FROM (
			SELECT id, occurred_at, ingested_at, source, namespace, entity_kind,
			       entity_name, type, severity, title, payload, trace_id, correlation_key,
			       row_number() OVER w AS rn,
			       count(*) OVER p AS total,
			       min(ingested_at) OVER p AS first_seen
			FROM events
			WHERE ingested_at >= $1 AND ingested_at <= $2
			  AND severity IN ('warning','critical')
			WINDOW p AS (PARTITION BY namespace, entity_kind, entity_name, type),
			       w AS (p ORDER BY ingested_at DESC, id DESC)
		) ranked
		WHERE rn <= $4
		ORDER BY rn ASC, ingested_at DESC
		LIMIT $3`, from, to, limit, signalsPerSeries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var signals []Signal
	for rows.Next() {
		var sig Signal
		e := &sig.Event
		if err := rows.Scan(&e.ID, &e.OccurredAt, &e.IngestedAt, &e.Source, &e.Namespace, &e.EntityKind, &e.EntityName, &e.Type, &e.Severity, &e.Title, &e.Payload, &e.TraceID, &e.CorrelationKey, &sig.Occurrences, &sig.FirstSeen); err != nil {
			return nil, err
		}
		signals = append(signals, sig)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(signals, func(i, j int) bool { return signals[i].IngestedAt.After(signals[j].IngestedAt) })
	return signals, nil
}

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
		ORDER BY ingested_at DESC, id DESC
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
