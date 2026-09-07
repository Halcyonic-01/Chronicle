package heal

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresAuditStore struct{ pool *pgxpool.Pool }

func NewPostgresAuditStore(pool *pgxpool.Pool) *PostgresAuditStore {
	return &PostgresAuditStore{pool: pool}
}

func (s *PostgresAuditStore) RecordAction(ctx context.Context, a *Action) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO heal_actions
		(id, incident_id, rule, cause_type, namespace, target, confidence,
		 reasoning, status, result, error, dry_run, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), $12, $13)
		ON CONFLICT (incident_id) DO NOTHING`,
		a.ID, a.IncidentID, a.Rule, a.CauseType, a.Namespace, a.Target,
		a.Confidence, a.Reasoning, a.Status, a.Result, a.Error, a.DryRun, a.CreatedAt)
	return err
}

func (s *PostgresAuditStore) CountRuleSince(ctx context.Context, rule string, since time.Time) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM heal_actions
		WHERE rule = $1 AND created_at >= $2 AND status = $3`, rule, since, StatusWouldRun).Scan(&count)
	return count, err
}

func (s *PostgresAuditStore) HasActionForIncident(ctx context.Context, incidentID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM heal_actions WHERE incident_id = $1)`, incidentID).Scan(&exists)
	return exists, err
}
