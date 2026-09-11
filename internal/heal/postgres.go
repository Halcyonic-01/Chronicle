package heal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresAuditStore struct{ pool *pgxpool.Pool }

type ActionStore interface {
	ListActions(context.Context, int) ([]Action, error)
	DecideAction(context.Context, string, bool, string, string) (*Action, error)
}

type ExecutionStore interface {
	ClaimExecution(context.Context, string, time.Time) (bool, error)
	CompleteExecution(context.Context, string, string, string, string, string) error
}

func NewPostgresAuditStore(pool *pgxpool.Pool) *PostgresAuditStore {
	return &PostgresAuditStore{pool: pool}
}

func (s *PostgresAuditStore) RecordAction(ctx context.Context, a *Action) error {
	reasoning := a.Reasoning
	if reasoning == nil {
		reasoning = []string{}
	}
	reasoningJSON, err := json.Marshal(reasoning)
	if err != nil {
		return fmt.Errorf("marshal healing reasoning: %w", err)
	}
	payload := a.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO heal_actions
		(id, incident_id, rule, cause_type, action_type, namespace, target, confidence,
		 reasoning, status, result, error, approval, payload, dry_run, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''), $13, $14, $15, $16)
		ON CONFLICT (incident_id) DO NOTHING`,
		a.ID, a.IncidentID, a.Rule, a.CauseType, a.ActionType, a.Namespace, a.Target,
		a.Confidence, reasoningJSON, a.Status, a.Result, a.Error, a.Approval, payload, a.DryRun, a.CreatedAt)
	return err
}

func (s *PostgresAuditStore) ListActions(ctx context.Context, limit int) ([]Action, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT id, incident_id, rule, cause_type, action_type, namespace, target, confidence, reasoning, status, result, COALESCE(error,''), COALESCE(approval,'not_required'), COALESCE(payload,'null'::jsonb), dry_run, created_at, COALESCE(decided_by,''), COALESCE(decision_reason,''), decided_at, started_at, finished_at, COALESCE(verification,''), attempts FROM heal_actions ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []Action
	for rows.Next() {
		var a Action
		var reasoningJSON []byte
		if err := rows.Scan(&a.ID, &a.IncidentID, &a.Rule, &a.CauseType, &a.ActionType, &a.Namespace, &a.Target, &a.Confidence, &reasoningJSON, &a.Status, &a.Result, &a.Error, &a.Approval, &a.Payload, &a.DryRun, &a.CreatedAt, &a.DecisionBy, &a.DecisionReason, &a.DecidedAt, &a.StartedAt, &a.FinishedAt, &a.Verification, &a.Attempts); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(reasoningJSON, &a.Reasoning); err != nil {
			return nil, fmt.Errorf("decode healing reasoning: %w", err)
		}
		a.Reasoning = append([]string{}, a.Reasoning...)
		a.Payload = append(json.RawMessage{}, a.Payload...)
		actions = append(actions, a)
	}
	return actions, rows.Err()
}

func (s *PostgresAuditStore) DecideAction(ctx context.Context, id string, approved bool, decidedBy, reason string) (*Action, error) {
	approval := ApprovalDenied
	status := StatusBlocked
	result := "DENIED BY REVIEWER"
	if approved {
		approval, status, result = ApprovalApproved, StatusWouldRun, "APPROVED (queued; live execution safety gates apply)"
	}
	var a Action
	var reasoningJSON []byte
	var decidedAt time.Time
	err := s.pool.QueryRow(ctx, `UPDATE heal_actions SET approval=$2, status=$3, result=$4, decided_by=NULLIF($5,''), decision_reason=NULLIF($6,''), decided_at=now() WHERE id=$1 AND approval='pending' RETURNING id, incident_id, rule, cause_type, action_type, namespace, target, confidence, reasoning, status, result, COALESCE(error,''), approval, COALESCE(payload,'null'::jsonb), dry_run, created_at, COALESCE(decided_by,''), COALESCE(decision_reason,''), decided_at`, id, approval, status, result, decidedBy, reason).Scan(&a.ID, &a.IncidentID, &a.Rule, &a.CauseType, &a.ActionType, &a.Namespace, &a.Target, &a.Confidence, &reasoningJSON, &a.Status, &a.Result, &a.Error, &a.Approval, &a.Payload, &a.DryRun, &a.CreatedAt, &a.DecisionBy, &a.DecisionReason, &decidedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(reasoningJSON, &a.Reasoning); err != nil {
		return nil, err
	}
	a.DecidedAt = &decidedAt
	return &a, nil
}

func (s *PostgresAuditStore) CountRuleSince(ctx context.Context, rule string, since time.Time) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM heal_actions
		WHERE rule = $1 AND created_at >= $2 AND status NOT IN ('skipped','blocked')`, rule, since).Scan(&count)
	return count, err
}

func (s *PostgresAuditStore) ClaimExecution(ctx context.Context, id string, started time.Time) (bool, error) {
	var claimed bool
	err := s.pool.QueryRow(ctx, `UPDATE heal_actions SET status=$2, dry_run=false, started_at=$3, attempts=attempts+1 WHERE id=$1 AND approval='approved' AND status IN ('would_run','approved') RETURNING true`, id, StatusExecuting, started).Scan(&claimed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return claimed, nil
}

func (s *PostgresAuditStore) CompleteExecution(ctx context.Context, id, status, result, executionError, verification string) error {
	_, err := s.pool.Exec(ctx, `UPDATE heal_actions SET status=$2, result=$3, error=NULLIF($4,''), verification=$5, finished_at=now() WHERE id=$1`, id, status, result, executionError, verification)
	return err
}

func (s *PostgresAuditStore) HasActionForIncident(ctx context.Context, incidentID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM heal_actions WHERE incident_id = $1)`, incidentID).Scan(&exists)
	return exists, err
}
