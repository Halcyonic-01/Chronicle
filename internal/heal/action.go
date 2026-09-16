// Package heal contains the safety-first self-healing rule engine.
// The initial implementation is dry-run only: it plans and audits actions
// without changing Kubernetes resources.
package heal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/oklog/ulid/v2"
)

const (
	StatusWouldRun      = "would_run"
	StatusExecuting     = "executing"
	StatusSucceeded     = "succeeded"
	StatusFailed        = "failed"
	StatusSkipped       = "skipped"
	StatusBlocked       = "blocked"
	ApprovalNotRequired = "not_required"
	ApprovalPending     = "pending"
	ApprovalApproved    = "approved"
	ApprovalDenied      = "denied"

	ActionRestartPod         = "restart_pod"
	ActionBumpMemory         = "bump_memory"
	ActionRollbackDeployment = "rollback_deployment"
	ActionRestoreReplicas    = "restore_replicas"
)

type Action struct {
	ID         string `json:"id"`
	IncidentID string `json:"incident_id"`
	Rule       string `json:"rule"`
	CauseType  string `json:"cause_type"`
	// CauseEventID identifies the outage, so decisions about one failure are
	// not counted as independent evidence.
	CauseEventID   string          `json:"cause_event_id,omitempty"`
	ActionType     string          `json:"action_type"`
	Namespace      string          `json:"namespace"`
	Target         string          `json:"target"`
	Confidence     float64         `json:"confidence"`
	Reasoning      []string        `json:"reasoning"`
	Status         string          `json:"status"`
	Result         string          `json:"result"`
	Error          string          `json:"error,omitempty"`
	Approval       string          `json:"approval"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	DryRun         bool            `json:"dry_run"`
	CreatedAt      time.Time       `json:"created_at"`
	DecisionBy     string          `json:"decision_by,omitempty"`
	DecisionReason string          `json:"decision_reason,omitempty"`
	DecidedAt      *time.Time      `json:"decided_at,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	Verification   string          `json:"verification,omitempty"`
	Attempts       int             `json:"attempts"`
}

type Rule struct {
	Name           string  `json:"name"`
	CauseType      string  `json:"cause_type"`
	MinConfidence  float64 `json:"min_confidence"`
	MaxPerHour     int     `json:"max_per_hour"`
	RequireApprove bool    `json:"require_approve"`
	ActionType     string  `json:"action_type"`
}

var defaultRules = []Rule{
	{Name: "restart-deadlocked-pod", CauseType: "became_unready", ActionType: ActionRestartPod, MinConfidence: 0.80, MaxPerHour: 3, RequireApprove: true},
	{Name: "bump-memory-on-oom", CauseType: "oom_kill", ActionType: ActionBumpMemory, MinConfidence: 0.85, MaxPerHour: 2},
	{Name: "rollback-bad-deploy", CauseType: "deploy", ActionType: ActionRollbackDeployment, MinConfidence: 0.90, MaxPerHour: 1, RequireApprove: true},
	// A scale to zero is the outage RCA identifies most often here, and nothing
	// could act on it. The executor restores only from zero: a smaller non-zero
	// count is capacity management, not a fault.
	{Name: "restore-scaled-down-workload", CauseType: "scale", ActionType: ActionRestoreReplicas, MinConfidence: 0.65, MaxPerHour: 2, RequireApprove: true},
}

// formatBelow renders a value and the floor it missed at the least precision
// that still tells them apart: 0.6496 against a 0.65 floor read as
// "confidence 0.65 is below 0.65" at two places.
func formatBelow(value, floor float64) (string, string) {
	for _, places := range []int{2, 3, 4, 5} {
		got := strconv.FormatFloat(value, 'f', places, 64)
		want := strconv.FormatFloat(floor, 'f', places, 64)
		if got != want {
			return got, want
		}
	}
	// Shortest form that round-trips: two different float64s cannot collide.
	return strconv.FormatFloat(value, 'g', -1, 64), strconv.FormatFloat(floor, 'g', -1, 64)
}

type AuditStore interface {
	RecordAction(context.Context, *Action) error
	CountRuleSince(context.Context, string, time.Time) (int, error)
	HasActionForIncident(context.Context, string) (bool, error)
}

type Engine struct {
	Store    AuditStore
	Rules    []Rule
	DryRun   bool
	Now      func() time.Time
	Notifier Notifier
}

func NewEngine(store AuditStore) *Engine {
	return &Engine{Store: store, Rules: RulesFromEnv(), DryRun: true, Now: time.Now, Notifier: NewSlackNotifierFromEnv()}
}

// RulesFromEnv returns the rules to enforce. HEAL_RULES, when set, is a JSON
// array replacing the defaults, so a threshold can be retuned without a
// rebuild. An unparseable value keeps the defaults rather than disarming the
// engine silently.
func RulesFromEnv() []Rule {
	raw := strings.TrimSpace(os.Getenv("HEAL_RULES"))
	if raw == "" {
		return append([]Rule(nil), defaultRules...)
	}
	var parsed []Rule
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil || len(parsed) == 0 {
		slog.Error("ignoring invalid HEAL_RULES; keeping the built-in rules", "err", err)
		return append([]Rule(nil), defaultRules...)
	}
	for _, r := range parsed {
		if r.Name == "" || r.CauseType == "" || !executableActions[r.ActionType] {
			slog.Error("ignoring HEAL_RULES: a rule is incomplete or names an unknown action", "rule", r.Name, "action", r.ActionType)
			return append([]Rule(nil), defaultRules...)
		}
	}
	return parsed
}

// Evaluate converts a completed RCA result into at most one audited action.
// It deliberately has no Kubernetes client: live execution is a later,
// separately reviewed milestone.
func (e *Engine) Evaluate(ctx context.Context, result *rca.Result) (*Action, error) {
	if e == nil || e.Store == nil {
		return nil, fmt.Errorf("heal engine requires an audit store")
	}
	if result == nil {
		return nil, fmt.Errorf("heal engine requires an RCA result")
	}
	if !e.DryRun {
		return nil, fmt.Errorf("live healing is not enabled in this milestone")
	}
	now := time.Now
	if e.Now != nil {
		now = e.Now
	}
	action := &Action{
		ID:         ulid.Make().String(),
		IncidentID: result.Symptom.ID,
		Reasoning:  []string{},
		Approval:   ApprovalNotRequired,
		DryRun:     true,
		CreatedAt:  now().UTC(),
		Status:     StatusSkipped,
	}
	seen, err := e.Store.HasActionForIncident(ctx, result.Symptom.ID)
	if err != nil {
		return nil, fmt.Errorf("check incident idempotency: %w", err)
	}
	if seen {
		return nil, nil
	}
	if len(result.Candidates) == 0 {
		action.Result = "no actionable RCA candidate"
		return action, e.Store.RecordAction(ctx, action)
	}
	top := result.Candidates[0]
	var rule *Rule
	for i := range e.Rules {
		if e.Rules[i].CauseType == top.Event.Type {
			rule = &e.Rules[i]
			break
		}
	}
	if rule == nil {
		action.Result = fmt.Sprintf("no rule for cause type %q", top.Event.Type)
		return action, e.Store.RecordAction(ctx, action)
	}
	action.Rule, action.CauseType = rule.Name, rule.CauseType
	action.CauseEventID = top.Event.ID
	action.ActionType = rule.ActionType
	action.Namespace, action.Target = top.Event.Namespace, top.Event.EntityName
	action.Payload = append(json.RawMessage(nil), top.Event.Payload...)
	action.Confidence, action.Reasoning = result.Confidence, append([]string(nil), top.Reasons...)
	if rule.RequireApprove {
		action.Approval = ApprovalPending
	}
	if result.Confidence < rule.MinConfidence {
		action.Status = StatusBlocked
		got, floor := formatBelow(result.Confidence, rule.MinConfidence)
		action.Result = fmt.Sprintf("confidence %s is below %s", got, floor)
		return action, e.Store.RecordAction(ctx, action)
	}
	count, err := e.Store.CountRuleSince(ctx, rule.Name, now().Add(-time.Hour))
	if err != nil {
		return nil, fmt.Errorf("check heal rate limit: %w", err)
	}
	if count >= rule.MaxPerHour {
		action.Status = StatusBlocked
		action.Result = fmt.Sprintf("rule limit reached (%d per hour)", rule.MaxPerHour)
		return action, e.Store.RecordAction(ctx, action)
	}
	action.Status = StatusWouldRun
	action.Result = "WOULD HAVE RUN (dry-run)"
	if rule.RequireApprove {
		action.Result = "WOULD HAVE RUN (approval required)"
	}
	if err := e.Store.RecordAction(ctx, action); err != nil {
		return action, err
	}
	// An approval queue nobody is told about is not a queue. Notification is
	// best-effort: a failed webhook must not lose the audited decision.
	if e.Notifier != nil && action.Approval == ApprovalPending {
		_ = e.Notifier.Notify(ctx, action)
	}
	return action, nil
}
