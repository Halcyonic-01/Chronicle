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
	"github.com/tidwall/gjson"
)

// A decision's life: skipped or blocked (terminal, never executable), or
// would_run awaiting approval -> approved | denied | expired, then
// approved -> executing -> succeeded | failed, or blocked by an execution gate.
const (
	StatusWouldRun      = "would_run"
	StatusApproved      = "approved"
	StatusExecuting     = "executing"
	StatusSucceeded     = "succeeded"
	StatusFailed        = "failed"
	StatusSkipped       = "skipped"
	StatusBlocked       = "blocked"
	StatusDenied        = "denied"
	StatusExpired       = "expired"
	ApprovalNotRequired = "not_required"
	ApprovalPending     = "pending"
	ApprovalApproved    = "approved"
	ApprovalDenied      = "denied"
	ApprovalExpired     = "expired"

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
	// Workload is what the action changes: a Pod action names its owner. The
	// allowlist and the per-target cooldown key on it.
	Workload string `json:"workload,omitempty"`
	// Proposed: the engine would have acted. It stays true through approval,
	// expiry and execution, so evidence does not depend on later states.
	Proposed  bool       `json:"proposed"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// Related are the entities the analysis weighed; a change to one of them
	// can explain a recovery, a change elsewhere cannot.
	Related []string `json:"related,omitempty"`
	State   string   `json:"state"`
	// The judgement of how the decision turned out (see classifyOutcome), by
	// which version of the labeller. Empty until the settle window has passed.
	Outcome         string     `json:"outcome,omitempty"`
	OutcomeDetail   string     `json:"outcome_detail,omitempty"`
	OutcomeLabeller string     `json:"outcome_labeller,omitempty"`
	OutcomeAt       *time.Time `json:"outcome_at,omitempty"`
}

// StateOf names where a decision is in its life, for display and the API.
func StateOf(a *Action) string {
	switch {
	case a.Status == StatusWouldRun && a.Approval == ApprovalPending:
		return "pending_approval"
	case a.Status == StatusWouldRun && a.Approval == ApprovalApproved, a.Status == StatusApproved:
		return "approved"
	case a.Status == StatusWouldRun:
		return "planned"
	}
	return a.Status
}

// workloadOf names the workload an action would change.
func workloadOf(actionType, entity string, payload json.RawMessage) string {
	if actionType == ActionRestartPod || actionType == ActionBumpMemory {
		if owner := gjson.GetBytes(payload, "owner").String(); owner != "" {
			return owner
		}
	}
	return entity
}

type Rule struct {
	Name          string  `json:"name"`
	CauseType     string  `json:"cause_type"`
	MinConfidence float64 `json:"min_confidence"`
	MaxPerHour    int     `json:"max_per_hour"`
	// RequireApprove is kept for configuration compatibility. Automatic
	// execution is not implemented: every executable action needs approval.
	RequireApprove bool   `json:"require_approve"`
	ActionType     string `json:"action_type"`
}

var defaultRules = []Rule{
	{Name: "restart-deadlocked-pod", CauseType: "became_unready", ActionType: ActionRestartPod, MinConfidence: 0.80, MaxPerHour: 3, RequireApprove: true},
	{Name: "bump-memory-on-oom", CauseType: "oom_kill", ActionType: ActionBumpMemory, MinConfidence: 0.85, MaxPerHour: 2, RequireApprove: true},
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

// precondition says why an action does not fit its cause, or "" when it does.
// It is checked when planning and again before executing, so the engine never
// plans what the executor would refuse.
func precondition(actionType string, payload json.RawMessage) string {
	if manager := gjson.GetBytes(payload, "gitops").String(); manager != "" && actionType != ActionRestartPod {
		return gitopsProposal(manager)
	}
	switch actionType {
	case ActionRestoreReplicas:
		// Any other scale is capacity management; undoing it would fight
		// whoever made it.
		from, to := gjson.GetBytes(payload, "old_replicas").Int(), gjson.GetBytes(payload, "new_replicas").Int()
		if from <= 0 || to != 0 {
			return fmt.Sprintf("not a scale to zero (%d to %d); restoring replicas does not apply", from, to)
		}
	case ActionBumpMemory:
		if gjson.GetBytes(payload, "owner").String() == "" || gjson.GetBytes(payload, "original_mem_bytes").Int() <= 0 {
			return "the OOM evidence lacks the owner and original memory limit; a bump could not be bounded"
		}
	case ActionRollbackDeployment:
		if gjson.GetBytes(payload, "old_image").String() == "" || gjson.GetBytes(payload, "new_image").String() == "" {
			return "the deploy evidence lacks the previous and new image; there is no verified revision to return to"
		}
	}
	return ""
}

// gitopsProposal is the remediation Chronicle proposes but never performs for a
// workload whose desired state lives in Git: a write here would be reverted on
// the next reconcile, or would hide drift from it.
func gitopsProposal(manager string) string {
	kind, name, _ := strings.Cut(manager, ":")
	if kind == "argocd" {
		return fmt.Sprintf("managed by Argo CD application %q: Chronicle will not change it; revert the change in Git, or run `argocd app rollback %s` with auto-sync paused", name, name)
	}
	return fmt.Sprintf("managed by %s: Chronicle will not change it; revert the change in Git", manager)
}

type AuditStore interface {
	RecordAction(context.Context, *Action) error
	CountRuleSince(context.Context, string, time.Time) (int, error)
	HasActionForIncident(context.Context, string) (bool, error)
}

// causeIndex finds an earlier proposal for the same outage. Optional, so test
// and benchmark stores need not implement it.
type causeIndex interface {
	ProposalForCause(ctx context.Context, causeEventID string) (id, state string, found bool, err error)
}

// DefaultApprovalTTL bounds how long a decision may wait for approval and then
// for execution. The evidence is a snapshot; recovery signals arrive a median
// of ~23 minutes later, so a proposal older than this describes the past.
const DefaultApprovalTTL = 30 * time.Minute

type Engine struct {
	Store       AuditStore
	Rules       []Rule
	DryRun      bool
	Now         func() time.Time
	Notifier    Notifier
	ApprovalTTL time.Duration
}

func NewEngine(store AuditStore) *Engine {
	return &Engine{Store: store, Rules: RulesFromEnv(), DryRun: true, Now: time.Now, Notifier: NewSlackNotifierFromEnv(),
		ApprovalTTL: durationEnv("HEAL_APPROVAL_TTL", DefaultApprovalTTL)}
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
	// Acting on one of several causes the record cannot tell apart is a guess.
	switch result.Verdict {
	case rca.VerdictAmbiguous:
		action.Result = fmt.Sprintf("ambiguous: %d causes are about equally plausible", 1+len(result.Alternatives))
		return action, e.Store.RecordAction(ctx, action)
	case rca.VerdictNoRootCause:
		action.Result = "no root cause observed"
		return action, e.Store.RecordAction(ctx, action)
	}
	// The root cause leads. A rule fires on a root, never on one of its effects:
	// a node failure restarts no pod, and an autoscaler limit is not undone by
	// restoring replicas.
	top := result.Candidates[0]
	if top.NotRoot != "" {
		action.Result = "the leading candidate is not a cause: " + top.NotRoot
		return action, e.Store.RecordAction(ctx, action)
	}
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
	action.Workload = workloadOf(rule.ActionType, top.Event.EntityName, top.Event.Payload)
	action.Related = relatedEntities(result)
	action.Confidence, action.Reasoning = result.Confidence, append([]string(nil), top.Reasons...)
	// Approval is requested only below, once every check has passed: a blocked
	// or refused decision is never approvable.
	if top.Event.EntityKind == "Application" && rule.ActionType == ActionRollbackDeployment {
		action.Result = gitopsProposal("argocd:" + top.Event.EntityName)
		return action, e.Store.RecordAction(ctx, action)
	}
	// Never plan what the executor would refuse.
	if reason := precondition(rule.ActionType, top.Event.Payload); reason != "" {
		action.Result = reason
		return action, e.Store.RecordAction(ctx, action)
	}
	if result.Confidence < rule.MinConfidence {
		action.Status = StatusBlocked
		got, floor := formatBelow(result.Confidence, rule.MinConfidence)
		action.Result = fmt.Sprintf("confidence %s is below %s", got, floor)
		return action, e.Store.RecordAction(ctx, action)
	}
	// One proposal per outage: every symptom of one failure names the same
	// cause, and each used to queue its own approval and spend the rule limit.
	if index, ok := e.Store.(causeIndex); ok && action.CauseEventID != "" {
		id, state, found, err := index.ProposalForCause(ctx, action.CauseEventID)
		if err != nil {
			return nil, fmt.Errorf("check for an earlier proposal: %w", err)
		}
		if found {
			action.Result = fmt.Sprintf("same outage as decision %s (%s); one proposal per cause", id, state)
			return action, e.Store.RecordAction(ctx, action)
		}
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
	ttl := e.ApprovalTTL
	if ttl <= 0 {
		ttl = DefaultApprovalTTL
	}
	expires := action.CreatedAt.Add(ttl)
	action.Status, action.Approval, action.Proposed, action.ExpiresAt = StatusWouldRun, ApprovalPending, true, &expires
	action.Result = "WOULD HAVE RUN (approval required)"
	action.State = StateOf(action)
	if err := e.Store.RecordAction(ctx, action); err != nil {
		return action, err
	}
	// An approval queue nobody is told about is not a queue. Notification is
	// best-effort: a failed webhook must not lose the audited decision.
	if e.Notifier != nil && action.Approval == ApprovalPending {
		if err := e.Notifier.Notify(ctx, action); err != nil {
			slog.Warn("approval notification failed", "action", action.ID, "err", err)
		}
	}
	return action, nil
}

// maxRelated bounds the entities kept per decision.
const maxRelated = 20

// relatedEntities are the symptom and every entity the analysis ranked: the
// only places a change could plausibly explain this symptom's recovery.
func relatedEntities(result *rca.Result) []string {
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		if name != "" && !seen[name] && len(out) < maxRelated {
			seen[name] = true
			out = append(out, name)
		}
	}
	add(result.Symptom.EntityName)
	for _, c := range result.Candidates {
		add(c.Event.EntityName)
		add(gjson.GetBytes(c.Event.Payload, "owner").String())
	}
	return out
}
