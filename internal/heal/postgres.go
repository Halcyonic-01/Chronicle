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
	DecideAction(context.Context, Decision) (*Action, error)
}

// Decision is a reviewer's answer to a pending approval.
type Decision struct {
	ID       string
	Approved bool
	By       string
	// Issuer and Subject identify the approver as the identity provider
	// vouched for them; empty when the decision came from the shared token.
	Issuer  string
	Subject string
	// Email (verified) and Groups are kept with an approval so that, before
	// anything runs, the approver can be checked against today's allowlist.
	Email  string
	Groups []string
	Reason string
	Result string
	Now    time.Time
}

// ErrAlreadyRecorded: a decision for this incident exists. RecordAction then
// leaves the existing decision in the Action it was given, so a caller never
// holds the ID of a row that was not written.
var ErrAlreadyRecorded = errors.New("a decision for this incident is already recorded")

// Why a decision could not be recorded.
var (
	ErrActionNotFound = errors.New("healing action not found")
	ErrNotPending     = errors.New("healing action is not awaiting approval")
	ErrExpired        = errors.New("healing action expired before it was approved")
	// ErrAlreadyApproved: this person's approval is already counted. A second
	// click is not a second person.
	ErrAlreadyApproved = errors.New("you have already approved this action; it needs a different person")
	// ErrNeedsIdentity: the action needs several approvers, which a shared
	// token cannot tell apart.
	ErrNeedsIdentity = errors.New("this action needs more than one approver and the shared approval token cannot tell them apart; sign in with the identity provider")
)

// ClaimRequest carries the limits re-checked, atomically with the claim, before
// anything is executed.
type ClaimRequest struct {
	ID, Rule, Namespace, Workload string
	Now                           time.Time
	RuleMaxPerHour, MaxPerHour    int
	Cooldown                      time.Duration
}

type ExecutionStore interface {
	// ClaimExecution moves an approved action to executing if every limit
	// allows it. refusal names the limit that did not.
	ClaimExecution(context.Context, ClaimRequest) (claimed bool, refusal string, err error)
	CompleteExecution(context.Context, string, string, string, string, string) error
}

// WorkQueue is what the execution worker drains.
type WorkQueue interface {
	ApprovedActions(ctx context.Context, now time.Time, limit int) ([]Action, error)
	ExpireStale(ctx context.Context, now time.Time) (int64, error)
	FailInterrupted(ctx context.Context, startedBefore time.Time) (int64, error)
}

// healExecutionLock serialises claims, so concurrent workers cannot both pass
// a limit that only one of them should.
const healExecutionLock = 0x6865616c // "heal"

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
	related := a.Related
	if related == nil {
		related = []string{}
	}
	relatedJSON, err := json.Marshal(related)
	if err != nil {
		return fmt.Errorf("marshal related entities: %w", err)
	}
	payload := a.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO heal_actions
		(id, incident_id, rule, cause_type, action_type, namespace, target, confidence,
		 reasoning, status, result, error, approval, payload, dry_run, created_at, cause_event_id,
		 workload, proposed, expires_at, related, approvals_required)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''), $13, $14, $15, $16, NULLIF($17, ''),
		        $18, $19, $20, $21, $22)
		ON CONFLICT (incident_id) DO NOTHING`,
		a.ID, a.IncidentID, a.Rule, a.CauseType, a.ActionType, a.Namespace, a.Target,
		a.Confidence, reasoningJSON, a.Status, a.Result, a.Error, a.Approval, payload, a.DryRun, a.CreatedAt, a.CauseEventID,
		a.Workload, a.Proposed, a.ExpiresAt, relatedJSON, max(a.ApprovalsRequired, 1))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Lost a race with another analysis of the same incident: its row stands.
		existing, getErr := s.ActionForIncident(ctx, a.IncidentID)
		if getErr != nil {
			return fmt.Errorf("read the decision already recorded for incident %s: %w", a.IncidentID, getErr)
		}
		*a = *existing
		return ErrAlreadyRecorded
	}
	// The observation period starts with the first decision and is never
	// moved by pruning.
	_, err = s.pool.Exec(ctx, `INSERT INTO heal_settings (key, at) VALUES ('observation_started_at', $1) ON CONFLICT (key) DO NOTHING`, a.CreatedAt)
	return err
}

const actionColumns = `id, incident_id, rule, cause_type, action_type, namespace, target, confidence, reasoning, status, result,
	COALESCE(error,''), COALESCE(approval,'not_required'), COALESCE(payload,'null'::jsonb), dry_run, created_at,
	COALESCE(decided_by,''), COALESCE(decided_by_issuer,''), COALESCE(decided_by_subject,''), COALESCE(decision_reason,''), decided_at, started_at, finished_at, COALESCE(verification,''), attempts,
	COALESCE(cause_event_id,''), workload, proposed, expires_at, related,
	COALESCE(outcome,''), COALESCE(outcome_detail,''), COALESCE(outcome_labeller,''), outcome_at,
	COALESCE(symptom_check,''), COALESCE(symptom_detail,''), symptom_improved_at, symptom_due_at, symptom_checked_at,
	approvals_required,
	COALESCE((SELECT json_agg(json_build_object('issuer', p.issuer, 'subject', p.subject, 'display', p.display, 'email', p.email,
	                                           'groups', to_json(p.groups), 'reason', p.reason, 'at', p.approved_at) ORDER BY p.approved_at)
	          FROM heal_action_approvals p WHERE p.action_id = heal_actions.id), '[]'::json)`

func scanAction(row pgx.Row) (Action, error) {
	var a Action
	var reasoningJSON, relatedJSON, approvalsJSON []byte
	if err := row.Scan(&a.ID, &a.IncidentID, &a.Rule, &a.CauseType, &a.ActionType, &a.Namespace, &a.Target, &a.Confidence,
		&reasoningJSON, &a.Status, &a.Result, &a.Error, &a.Approval, &a.Payload, &a.DryRun, &a.CreatedAt,
		&a.DecisionBy, &a.DecisionIssuer, &a.DecisionSubject, &a.DecisionReason, &a.DecidedAt, &a.StartedAt, &a.FinishedAt, &a.Verification, &a.Attempts,
		&a.CauseEventID, &a.Workload, &a.Proposed, &a.ExpiresAt, &relatedJSON,
		&a.Outcome, &a.OutcomeDetail, &a.OutcomeLabeller, &a.OutcomeAt,
		&a.SymptomCheck, &a.SymptomDetail, &a.SymptomImprovedAt, &a.SymptomDueAt, &a.SymptomCheckedAt,
		&a.ApprovalsRequired, &approvalsJSON); err != nil {
		return a, err
	}
	if err := json.Unmarshal(approvalsJSON, &a.Approvals); err != nil {
		return a, fmt.Errorf("decode approvals: %w", err)
	}
	if err := json.Unmarshal(reasoningJSON, &a.Reasoning); err != nil {
		return a, fmt.Errorf("decode healing reasoning: %w", err)
	}
	if len(relatedJSON) > 0 {
		if err := json.Unmarshal(relatedJSON, &a.Related); err != nil {
			return a, fmt.Errorf("decode related entities: %w", err)
		}
	}
	a.Reasoning = append([]string{}, a.Reasoning...)
	a.Payload = append(json.RawMessage{}, a.Payload...)
	a.State = StateOf(&a)
	return a, nil
}

func (s *PostgresAuditStore) ListActions(ctx context.Context, limit int) ([]Action, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+actionColumns+` FROM heal_actions ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []Action
	for rows.Next() {
		a, err := scanAction(rows)
		if err != nil {
			return nil, err
		}
		actions = append(actions, a)
	}
	return actions, rows.Err()
}

// GetAction reads one decision.
func (s *PostgresAuditStore) GetAction(ctx context.Context, id string) (*Action, error) {
	a, err := scanAction(s.pool.QueryRow(ctx, `SELECT `+actionColumns+` FROM heal_actions WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrActionNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// DecideAction records a reviewer's answer. Only a decision the engine would
// have run, still pending and unexpired, can be decided; approval only queues
// it for the worker, which re-checks everything before executing.
//
// One denial ends it. An approval is counted once per person; the decision
// becomes approved only when approvals_required distinct people have approved,
// and the row is locked while that is counted so two people approving at the
// same moment cannot both see "one so far".
func (s *PostgresAuditStore) DecideAction(ctx context.Context, d Decision) (*Action, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var required int
	var approval, status string
	var expires *time.Time
	err = tx.QueryRow(ctx, `SELECT approvals_required, COALESCE(approval,'not_required'), status, expires_at
		FROM heal_actions WHERE id=$1 FOR UPDATE`, d.ID).Scan(&required, &approval, &status, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrActionNotFound
	}
	if err != nil {
		return nil, err
	}
	if status != StatusWouldRun || approval != ApprovalPending {
		current, getErr := s.GetAction(ctx, d.ID)
		if getErr != nil {
			return nil, getErr
		}
		return nil, fmt.Errorf("%w (it is %s)", ErrNotPending, current.State)
	}
	if expires == nil || !expires.After(d.Now) {
		if _, err := tx.Exec(ctx, `UPDATE heal_actions SET status='expired', approval='expired', result='EXPIRED: not approved before the deadline'
			WHERE id=$1 AND status='would_run' AND approval='pending'`, d.ID); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, ErrExpired
	}

	finalApproval, finalStatus, result := ApprovalDenied, StatusDenied, "DENIED BY REVIEWER"
	if d.Approved {
		if required > 1 && d.Subject == "" {
			return nil, ErrNeedsIdentity
		}
		have := 0
		if d.Subject != "" {
			tag, err := tx.Exec(ctx, `INSERT INTO heal_action_approvals (action_id, issuer, subject, display, email, groups, reason, approved_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`,
				d.ID, d.Issuer, d.Subject, d.By, d.Email, nonNil(d.Groups), d.Reason, d.Now)
			if err != nil {
				return nil, err
			}
			if tag.RowsAffected() == 0 {
				return nil, ErrAlreadyApproved
			}
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM heal_action_approvals WHERE action_id=$1`, d.ID).Scan(&have); err != nil {
				return nil, err
			}
		} else {
			have = required // the shared token: one approver, and required is 1
		}
		if have < required {
			// Counted, but not yet enough: still waiting, with a note of how far.
			if _, err := tx.Exec(ctx, `UPDATE heal_actions SET result=$2 WHERE id=$1`, d.ID,
				fmt.Sprintf("APPROVED BY %d OF %d REQUIRED; waiting for another approver", have, required)); err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return s.GetAction(ctx, d.ID)
		}
		finalApproval, finalStatus, result = ApprovalApproved, StatusApproved, d.Result
	}
	if _, err := tx.Exec(ctx, `
		UPDATE heal_actions
		SET approval=$2, status=$3, result=$4, decided_by=NULLIF($5,''), decided_by_issuer=NULLIF($8,''), decided_by_subject=NULLIF($9,''), decision_reason=NULLIF($6,''), decided_at=$7
		WHERE id=$1`, d.ID, finalApproval, finalStatus, result, d.By, d.Reason, d.Now, d.Issuer, d.Subject); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetAction(ctx, d.ID)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
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

// EarliestActionAt is when Chronicle began observing: stored once, so pruning
// old rows cannot move it. Zero means it never has.
func (s *PostgresAuditStore) EarliestActionAt(ctx context.Context) (time.Time, error) {
	var at *time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT at FROM heal_settings WHERE key = 'observation_started_at'),
			(SELECT min(created_at) FROM heal_actions))`).Scan(&at); err != nil {
		return time.Time{}, err
	}
	if at == nil {
		return time.Time{}, nil
	}
	return at.UTC(), nil
}

// CountRuleSince counts the outages a rule proposed acting on, not the
// decisions: one outage's symptoms must not spend the limit several times.
func (s *PostgresAuditStore) CountRuleSince(ctx context.Context, rule string, since time.Time) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT count(DISTINCT COALESCE(NULLIF(cause_event_id,''), id)) FROM heal_actions
		WHERE rule = $1 AND created_at >= $2 AND proposed`, rule, since).Scan(&count)
	return count, err
}

// ProposalForCause finds the decision already proposed for this outage. An
// expired proposal does not count: nobody acted on it, so a later symptom may
// propose again.
func (s *PostgresAuditStore) ProposalForCause(ctx context.Context, causeEventID string) (string, string, bool, error) {
	var a Action
	err := s.pool.QueryRow(ctx, `
		SELECT id, status, COALESCE(approval,'not_required') FROM heal_actions
		WHERE cause_event_id = $1 AND proposed AND status <> 'expired'
		ORDER BY created_at LIMIT 1`, causeEventID).Scan(&a.ID, &a.Status, &a.Approval)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	return a.ID, StateOf(&a), true, nil
}

func (s *PostgresAuditStore) ClaimExecution(ctx context.Context, req ClaimRequest) (bool, string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(healExecutionLock)); err != nil {
		return false, "", err
	}
	// Whoever holds the lock first wins. A late caller (a second worker, a slow
	// pass) finds the decision already claimed and must not be told it hit a
	// limit the winner just started, or it would block a decision it does not own.
	var status, approval string
	if err := tx.QueryRow(ctx, `SELECT status, approval FROM heal_actions WHERE id = $1`, req.ID).Scan(&status, &approval); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, "", nil
		}
		return false, "", err
	}
	if status != StatusApproved || approval != ApprovalApproved {
		return false, "", nil
	}
	var ruleRuns, allRuns int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE rule = $1), count(*)
		FROM heal_actions WHERE started_at >= $2`, req.Rule, req.Now.Add(-time.Hour)).Scan(&ruleRuns, &allRuns); err != nil {
		return false, "", err
	}
	if ruleRuns >= req.RuleMaxPerHour {
		return false, fmt.Sprintf("rule %s already executed %d time(s) in the last hour (limit %d)", req.Rule, ruleRuns, req.RuleMaxPerHour), nil
	}
	if allRuns >= req.MaxPerHour {
		return false, fmt.Sprintf("%d execution(s) in the last hour (global limit %d)", allRuns, req.MaxPerHour), nil
	}
	var last *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT max(started_at) FROM heal_actions
		WHERE namespace = $1 AND workload = $2 AND started_at IS NOT NULL`, req.Namespace, req.Workload).Scan(&last); err != nil {
		return false, "", err
	}
	if last != nil && req.Now.Sub(*last) < req.Cooldown {
		return false, fmt.Sprintf("%s/%s was acted on %s ago (cooldown %s)", req.Namespace, req.Workload, req.Now.Sub(*last).Round(time.Second), req.Cooldown), nil
	}
	var sibling string
	err = tx.QueryRow(ctx, `
		SELECT o.id FROM heal_actions o JOIN heal_actions a ON a.id = $1
		WHERE o.id <> a.id AND COALESCE(a.cause_event_id,'') <> '' AND o.cause_event_id = a.cause_event_id
		  AND o.started_at IS NOT NULL
		LIMIT 1`, req.ID).Scan(&sibling)
	if err == nil {
		return false, fmt.Sprintf("this outage was already acted on by decision %s", sibling), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, "", err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE heal_actions SET status=$2, dry_run=false, started_at=$3, attempts=attempts+1
		WHERE id=$1 AND approval='approved' AND status='approved' AND expires_at > $3`, req.ID, StatusExecuting, req.Now)
	if err != nil {
		return false, "", err
	}
	if tag.RowsAffected() == 0 {
		return false, "", nil
	}
	return true, "", tx.Commit(ctx)
}

// CompleteExecution records how a decision ended, from the one state each ending
// is legitimately made from: a refusal ("blocked") only while it is still merely
// approved, anything else only while it is executing. A worker that lost the
// claim therefore cannot overwrite the one that won it.
func (s *PostgresAuditStore) CompleteExecution(ctx context.Context, id, status, result, executionError, verification string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE heal_actions
		SET status=$2, result=$3, error=NULLIF($4,''), verification=$5, finished_at=now()
		WHERE id=$1
		  AND (($2 = 'blocked' AND status = 'approved') OR ($2 <> 'blocked' AND status = 'executing'))`,
		id, status, result, executionError, verification)
	return err
}

// RefuseExecution ends a claimed decision the executor declined to carry out.
// Nothing was changed, so the claim is undone: the refusal must not spend the
// hourly caps or start a cooldown, and the outage stays open to being judged as
// one Chronicle did not act on.
func (s *PostgresAuditStore) RefuseExecution(ctx context.Context, id, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE heal_actions
		SET status='blocked', result=$2, error=$2, started_at=NULL, finished_at=now()
		WHERE id=$1 AND status='executing'`, id, reason)
	return err
}

// BeginSymptomCheck marks a succeeded action as waiting for its symptom check.
// Only a succeeded action starts one, and only once.
func (s *PostgresAuditStore) BeginSymptomCheck(ctx context.Context, id string, startedAt, dueAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE heal_actions
		SET symptom_check='pending', symptom_started_at=$2, symptom_due_at=$3, symptom_detail=NULL,
		    symptom_improved_at=NULL, symptom_checked_at=NULL
		WHERE id=$1 AND status='succeeded' AND symptom_check IS NULL`, id, startedAt, dueAt)
	return err
}

// PendingSymptomChecks are the checks in progress, with the symptom each is about.
func (s *PostgresAuditStore) PendingSymptomChecks(ctx context.Context, limit int) ([]SymptomTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.id, a.action_type, a.target, COALESCE(e.type,''), COALESCE(e.namespace,''), COALESCE(e.entity_name,''),
		       COALESCE(e.severity,''), a.symptom_started_at, a.symptom_improved_at, a.symptom_due_at
		FROM heal_actions a LEFT JOIN events e ON e.id = a.incident_id
		WHERE a.symptom_check = 'pending'
		ORDER BY a.symptom_due_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SymptomTask
	for rows.Next() {
		var t SymptomTask
		if err := rows.Scan(&t.ID, &t.ActionType, &t.Target, &t.SymptomType, &t.SymptomNamespace, &t.SymptomEntity,
			&t.Severity, &t.StartedAt, &t.ImprovedAt, &t.DueAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// RecordSymptomImprovement keeps the first time the symptom was seen below its
// threshold, so a leader change resumes the hold instead of restarting it.
func (s *PostgresAuditStore) RecordSymptomImprovement(ctx context.Context, id string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE heal_actions SET symptom_improved_at = COALESCE(symptom_improved_at, $2)
		WHERE id=$1 AND symptom_check='pending'`, id, at)
	return err
}

// FinishSymptomCheck ends a pending check. It returns nil when there was nothing
// pending (another pass finished it first).
func (s *PostgresAuditStore) FinishSymptomCheck(ctx context.Context, id, verdict, detail string, at time.Time) (*Action, error) {
	a, err := scanAction(s.pool.QueryRow(ctx, `
		UPDATE heal_actions SET symptom_check=$2, symptom_detail=$3, symptom_checked_at=$4
		WHERE id=$1 AND symptom_check='pending'
		RETURNING `+actionColumns, id, verdict, detail, at))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a.State = StateOf(&a)
	return &a, nil
}

// ApprovedActions are the decisions waiting for the worker, oldest approval first.
func (s *PostgresAuditStore) ApprovedActions(ctx context.Context, now time.Time, limit int) ([]Action, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+actionColumns+` FROM heal_actions
		WHERE status = 'approved' AND approval = 'approved' AND expires_at > $1
		ORDER BY decided_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Action
	for rows.Next() {
		a, err := scanAction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ExpireStale ends decisions that waited past their deadline, approved or not.
func (s *PostgresAuditStore) ExpireStale(ctx context.Context, now time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE heal_actions
		SET status = 'expired',
		    approval = CASE WHEN approval = 'pending' THEN 'expired' ELSE approval END,
		    result = CASE WHEN approval = 'pending'
		                  THEN 'EXPIRED: not approved before the deadline'
		                  ELSE 'EXPIRED: approved but not executed before the deadline' END
		WHERE status IN ('would_run','approved')
		  AND (expires_at IS NULL OR expires_at <= $1)`, now)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// FailInterrupted closes executions a crashed worker left behind. Whether the
// change landed is unknown, so they are never retried.
func (s *PostgresAuditStore) FailInterrupted(ctx context.Context, startedBefore time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE heal_actions
		SET status = 'failed', error = 'interrupted', finished_at = now(),
		    result = 'execution was interrupted; whether the change was applied is unknown, so it is not retried'
		WHERE status = 'executing' AND started_at < $1`, startedBefore)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ActionForIncident returns the decision recorded for an incident.
func (s *PostgresAuditStore) ActionForIncident(ctx context.Context, incidentID string) (*Action, error) {
	a, err := scanAction(s.pool.QueryRow(ctx, `SELECT `+actionColumns+` FROM heal_actions WHERE incident_id=$1`, incidentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrActionNotFound
	}
	if err != nil {
		return nil, err
	}
	a.State = StateOf(&a)
	return &a, nil
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
		       COALESCE(a.payload,'null'::jsonb), a.created_at, a.workload, a.related,
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
		var relatedJSON []byte
		if err := rows.Scan(&p.ID, &p.IncidentID, &p.ActionType, &p.Namespace, &p.Target,
			&p.Payload, &p.CreatedAt, &p.Workload, &relatedJSON, &p.symptomNamespace, &p.symptomEntityName); err != nil {
			rows.Close()
			return 0, err
		}
		_ = json.Unmarshal(relatedJSON, &p.Related)
		pending = append(pending, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	labelled := 0
	for i := range pending {
		p := pending[i]
		followUps, err := s.eventsAfter(ctx, p.CreatedAt.Add(-EventClockSkew), p.CreatedAt.Add(SettleWindow), p.Namespace, p.symptomNamespace)
		if err != nil {
			return labelled, err
		}
		outcome, detail := classifyOutcome(&p.Action, SymptomRef{Namespace: p.symptomNamespace, Name: p.symptomEntityName}, followUps)
		if _, err := s.pool.Exec(ctx,
			`UPDATE heal_actions SET outcome=$2, outcome_at=$3, outcome_detail=$4, outcome_labeller=$5 WHERE id=$1 AND outcome IS NULL`,
			p.ID, outcome, now.UTC(), detail, LabellerVersion); err != nil {
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

// LabellerVersion names the logic that judges outcomes. Labels written by an
// older version are preserved in heal_outcome_history and re-judged by a
// migration (012 for v1, 013 for v2), never silently overwritten. Changing how
// outcomes are judged means a new version and a migration like those.
const LabellerVersion = "v3-incident-bounded"

// SupersededOutcome is a label an older labeller wrote and the current one replaced.
type SupersededOutcome struct {
	ActionID     string     `json:"action_id"`
	Outcome      string     `json:"outcome"`
	OutcomeAt    *time.Time `json:"outcome_at,omitempty"`
	Detail       string     `json:"detail,omitempty"`
	Labeller     string     `json:"labeller"`
	SupersededBy string     `json:"superseded_by"`
	SupersededAt time.Time  `json:"superseded_at"`
}

// OutcomeHistory returns the preserved earlier labels of one decision, oldest first.
func (s *PostgresAuditStore) OutcomeHistory(ctx context.Context, actionID string) ([]SupersededOutcome, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT action_id, outcome, outcome_at, COALESCE(outcome_detail,''), labeller, superseded_by, superseded_at
		FROM heal_outcome_history WHERE action_id = $1 ORDER BY superseded_at, id`, actionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SupersededOutcome
	for rows.Next() {
		var o SupersededOutcome
		if err := rows.Scan(&o.ActionID, &o.Outcome, &o.OutcomeAt, &o.Detail, &o.Labeller, &o.SupersededBy, &o.SupersededAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// PreservedLabelCount is how many earlier labels are kept in the history.
func (s *PostgresAuditStore) PreservedLabelCount(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM heal_outcome_history`).Scan(&n)
	return n, err
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
// Only outages where a decision was proposed are credited: the gate asks
// whether acting would have been right, and a decision the floor blocked never
// tested that. Outages Chronicle judged correctly but blocked are reported
// separately, because they measure the floor, not the engine. Outages
// Chronicle acted on itself are left out entirely: its own action cannot be
// the evidence that its actions are safe.
func (s *PostgresAuditStore) EvidenceSummary(ctx context.Context) (EvidenceSummary, error) {
	var out EvidenceSummary
	err := s.pool.QueryRow(ctx, `
		WITH per_outage AS (
			SELECT cause_event_id,
			       bool_or(proposed) AS would_act,
			       (array_agg(outcome ORDER BY confidence DESC)
			          FILTER (WHERE outcome IN ('confirmed','contradicted')))[1] AS verdict
			FROM heal_actions
			WHERE outcome IS NOT NULL AND COALESCE(cause_event_id,'') <> ''
			  AND cause_event_id NOT IN (
			      SELECT cause_event_id FROM heal_actions
			      WHERE started_at IS NOT NULL AND cause_event_id IS NOT NULL)
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

// failureTypes are the recorded signals that a workload was unwell. Logs are left
// out: applications log errors in perfectly healthy periods.
var failureTypes = []string{"error_spike", "latency_spike", "became_unready", "container_restart", "oom_kill", "crash_loop", "node_not_ready"}

// PriorRevisions rebuilds the workload's revision history from the change events
// it recorded (each carries the whole template before and after).
func (s *PostgresAuditStore) PriorRevisions(ctx context.Context, namespace, workload, bad string, limit int) ([]RevisionPeriod, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT occurred_at, payload->>'from_template_hash', payload->>'to_template_hash'
		FROM events
		WHERE namespace=$1 AND entity_kind='Deployment' AND entity_name=$2
		  AND type IN ('deploy','config_change','resource_change')
		  AND COALESCE(payload->>'to_template_hash','') <> ''
		ORDER BY occurred_at`, namespace, workload)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var steps []RevisionStep
	for rows.Next() {
		var st RevisionStep
		var from, to *string
		if err := rows.Scan(&st.At, &from, &to); err != nil {
			return nil, err
		}
		if from != nil {
			st.From = *from
		}
		if to != nil {
			st.To = *to
		}
		steps = append(steps, st)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return priorRevisions(steps, bad, limit), nil
}

// RanHealthily judges one revision from the events recorded for the workload and
// its pods while it was the running one.
func (s *PostgresAuditStore) RanHealthily(ctx context.Context, namespace, workload string, p RevisionPeriod, min time.Duration) (bool, string, error) {
	var observedFrom *time.Time
	if p.Since.IsZero() {
		// Chronicle did not see this revision begin: its record starts with the
		// first event it has for the workload or any of its pods.
		if err := s.pool.QueryRow(ctx, `
			SELECT min(occurred_at) FROM events
			WHERE namespace=$1 AND (entity_name=$2 OR entity_name LIKE $2 || '-%')`, namespace, workload).Scan(&observedFrom); err != nil {
			return false, "", err
		}
	}
	from := p.Since
	if from.IsZero() && observedFrom != nil {
		from = *observedFrom
	}
	var failures []time.Time
	if !from.IsZero() {
		rows, err := s.pool.Query(ctx, `
			SELECT occurred_at FROM events
			WHERE namespace=$1 AND (entity_name=$2 OR entity_name LIKE $2 || '-%')
			  AND severity IN ('warning','critical') AND type = ANY($3)
			  AND occurred_at >= $4 AND occurred_at < $5
			ORDER BY occurred_at`, namespace, workload, failureTypes, from, p.Until)
		if err != nil {
			return false, "", err
		}
		defer rows.Close()
		for rows.Next() {
			var at time.Time
			if err := rows.Scan(&at); err != nil {
				return false, "", err
			}
			failures = append(failures, at)
		}
		if err := rows.Err(); err != nil {
			return false, "", err
		}
	}
	var observed time.Time
	if observedFrom != nil {
		observed = *observedFrom
	}
	ok, why := judgeRevision(p, observed, failures, min)
	return ok, why, nil
}
