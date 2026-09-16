package heal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"

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
		 reasoning, status, result, error, approval, payload, dry_run, created_at, cause_event_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''), $13, $14, $15, $16, NULLIF($17, ''))
		ON CONFLICT (incident_id) DO NOTHING`,
		a.ID, a.IncidentID, a.Rule, a.CauseType, a.ActionType, a.Namespace, a.Target,
		a.Confidence, reasoningJSON, a.Status, a.Result, a.Error, a.Approval, payload, a.DryRun, a.CreatedAt, a.CauseEventID)
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

// Prune drops decision records that are past retention. Skipped records are the
// bulk of the table -- one per analysis -- and carry nothing once the window
// they describe has aged out. Anything a human decided on, or that reached a
// cluster, is kept regardless of age.
func (s *PostgresAuditStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM heal_actions
		WHERE created_at < $1
		  AND status = 'skipped'
		  AND approval = 'not_required'
		  AND started_at IS NULL`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// EarliestActionAt is when Chronicle first recorded a decision, which is when
// it actually began observing. Zero means it never has.
func (s *PostgresAuditStore) EarliestActionAt(ctx context.Context) (time.Time, error) {
	var at *time.Time
	if err := s.pool.QueryRow(ctx, `SELECT min(created_at) FROM heal_actions`).Scan(&at); err != nil {
		return time.Time{}, err
	}
	if at == nil {
		return time.Time{}, nil
	}
	return at.UTC(), nil
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

// pendingLabel is a decision waiting to be judged.
type pendingLabel struct {
	Action
	symptomNamespace  string
	symptomEntityName string
}

// LabelOutcomes judges decisions that have had time to settle, and records
// whether Chronicle's proposal turned out to be what fixed the problem. The
// audit store is the calibration data for the confidence floor; without this it
// records opinions and never learns whether they were right.
func (s *PostgresAuditStore) LabelOutcomes(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT a.id, a.incident_id, a.action_type, a.namespace, a.target,
		       COALESCE(a.payload,'null'::jsonb), a.created_at,
		       COALESCE(e.namespace,''), COALESCE(e.entity_name,'')
		FROM heal_actions a
		LEFT JOIN events e ON e.id = a.incident_id
		WHERE a.outcome IS NULL AND a.confidence > 0 AND a.created_at < $1
		ORDER BY a.created_at
		LIMIT $2`, now.Add(-SettleWindow), limit)
	if err != nil {
		return 0, err
	}
	var pending []pendingLabel
	for rows.Next() {
		var p pendingLabel
		if err := rows.Scan(&p.ID, &p.IncidentID, &p.ActionType, &p.Namespace, &p.Target,
			&p.Payload, &p.CreatedAt, &p.symptomNamespace, &p.symptomEntityName); err != nil {
			rows.Close()
			return 0, err
		}
		pending = append(pending, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	labelled := 0
	for i := range pending {
		p := pending[i]
		followUps, err := s.eventsAfter(ctx, p.CreatedAt, p.CreatedAt.Add(SettleWindow), p.Namespace, p.symptomNamespace)
		if err != nil {
			return labelled, err
		}
		outcome, detail := classifyOutcome(&p.Action, followUps)
		if _, err := s.pool.Exec(ctx,
			`UPDATE heal_actions SET outcome=$2, outcome_at=$3, outcome_detail=$4 WHERE id=$1 AND outcome IS NULL`,
			p.ID, outcome, now.UTC(), detail); err != nil {
			return labelled, err
		}
		labelled++
	}
	return labelled, nil
}

// eventsAfter reads what happened in the namespaces this decision touched,
// between the decision and the end of its settle window.
func (s *PostgresAuditStore) eventsAfter(ctx context.Context, from, to time.Time, namespaces ...string) ([]event.Event, error) {
	scope := make([]string, 0, len(namespaces))
	for _, n := range namespaces {
		if n != "" {
			scope = append(scope, n)
		}
	}
	if len(scope) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT namespace, entity_kind, entity_name, type, title, COALESCE(payload,'null'::jsonb), occurred_at
		FROM events
		WHERE occurred_at > $1 AND occurred_at <= $2 AND namespace = ANY($3)
		ORDER BY occurred_at`, from, to, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Event
	for rows.Next() {
		var e event.Event
		if err := rows.Scan(&e.Namespace, &e.EntityKind, &e.EntityName, &e.Type, &e.Title, &e.Payload, &e.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// OutcomeSummary is the calibration view: how decisions at each confidence
// band actually turned out.
func (s *PostgresAuditStore) OutcomeSummary(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT width_bucket(confidence, 0, 1, 10) AS band, outcome, count(*)
		FROM heal_actions
		WHERE outcome IS NOT NULL AND confidence > 0
		GROUP BY 1, 2 ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var band int
		var outcome string
		var count int
		if err := rows.Scan(&band, &outcome, &count); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"confidence_from": float64(band-1) / 10,
			"confidence_to":   float64(band) / 10,
			"outcome":         outcome,
			"count":           count,
		})
	}
	return out, rows.Err()
}

// EvidenceSummary weighs what the observation period proved, counted once per
// outage rather than once per decision. One failure analysed forty times is one
// piece of evidence, not forty.
//
// Only outages where a decision actually reached would_run are credited: the
// gate asks whether acting would have been right, and a decision the floor
// blocked never tested that. Outages Chronicle judged correctly but blocked are
// reported separately, because they measure the floor, not the engine.
func (s *PostgresAuditStore) EvidenceSummary(ctx context.Context) (EvidenceSummary, error) {
	var out EvidenceSummary
	err := s.pool.QueryRow(ctx, `
		WITH per_outage AS (
			SELECT cause_event_id,
			       bool_or(status = 'would_run') AS would_act,
			       (array_agg(outcome ORDER BY confidence DESC)
			          FILTER (WHERE outcome IN ('confirmed','contradicted')))[1] AS verdict
			FROM heal_actions
			WHERE outcome IS NOT NULL AND COALESCE(cause_event_id,'') <> ''
			GROUP BY cause_event_id
		)
		SELECT count(*) FILTER (WHERE would_act AND verdict = 'confirmed'),
		       count(*) FILTER (WHERE would_act AND verdict = 'contradicted'),
		       count(*) FILTER (WHERE verdict IS NULL),
		       count(*) FILTER (WHERE NOT would_act AND verdict = 'confirmed')
		FROM per_outage`).
		Scan(&out.Confirmed, &out.Contradicted, &out.Unknown, &out.MissedByFloor)
	return out, err
}
