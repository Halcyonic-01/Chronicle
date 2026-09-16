package heal

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type Notifier interface {
	Notify(context.Context, *Action) error
}

type Policy struct {
	LiveEnabled       bool
	KillSwitch        bool
	AllowedNamespaces map[string]bool
	AllowedActions    map[string]bool
	AllowedTargets    map[string]bool
	Timeout           time.Duration
	ObservationSince  time.Time
	MinDecisive       int
	MinPrecision      float64
	Evidence          EvidenceSummary
}

// EvidenceSummary is what the observation period actually produced. Unknown
// outcomes are counted but never credited: a decision nobody could judge is not
// evidence that the engine was right.
type EvidenceSummary struct {
	Confirmed    int `json:"confirmed"`
	Contradicted int `json:"contradicted"`
	Unknown      int `json:"unknown"`
	// MissedByFloor counts outages Chronicle read correctly but declined to act
	// on. They are evidence the confidence floor is too high, not evidence that
	// acting would have been right, so they are reported and never credited.
	MissedByFloor int `json:"missed_by_floor"`
}

// Decisive is the number of decisions that actually resolved either way.
func (e EvidenceSummary) Decisive() int { return e.Confirmed + e.Contradicted }

// Precision is how often a decisive decision turned out to be right.
func (e EvidenceSummary) Precision() float64 {
	if e.Decisive() == 0 {
		return 0
	}
	return float64(e.Confirmed) / float64(e.Decisive())
}

func PolicyFromEnv() Policy {
	return Policy{
		LiveEnabled:       os.Getenv("HEAL_LIVE_ENABLED") == "true",
		KillSwitch:        os.Getenv("HEAL_KILL_SWITCH") != "false",
		AllowedNamespaces: csvSet(os.Getenv("HEAL_ALLOWED_NAMESPACES")),
		AllowedActions:    csvSet(os.Getenv("HEAL_ALLOWED_ACTIONS")),
		AllowedTargets:    csvSet(os.Getenv("HEAL_ALLOWED_TARGETS")),
		Timeout:           durationEnv("HEAL_ACTION_TIMEOUT", 30*time.Second),
		ObservationSince:  timeEnv("HEAL_OBSERVATION_STARTED_AT"),
		MinDecisive:       intEnv("HEAL_MIN_DECISIVE_DECISIONS", 20),
		MinPrecision:      floatEnv("HEAL_MIN_PRECISION", 0.80),
	}
}

type Controller struct {
	Store    ExecutionStore
	Executor Executor
	Notifier Notifier
	Policy   Policy
	mu       sync.Mutex
}

func NewController(store ExecutionStore, executor Executor) *Controller {
	return &Controller{Store: store, Executor: executor, Notifier: NewSlackNotifierFromEnv(), Policy: PolicyFromEnv()}
}

// ExecuteApproved is the only live execution entry point. It is default-deny:
// live mode, the kill switch, the observation gate, and all allowlists must
// permit the action before Kubernetes is called.
func (c *Controller) ExecuteApproved(ctx context.Context, action *Action) (*Action, error) {
	if c == nil || c.Store == nil || c.Executor == nil {
		return action, fmt.Errorf("healing execution is not configured")
	}
	if action == nil {
		return nil, fmt.Errorf("action is nil")
	}
	if action.Approval != ApprovalApproved {
		return action, fmt.Errorf("action is not approved")
	}
	policy := c.Policy
	policy.ObservationSince = c.ObservationStart(ctx)
	policy.Evidence = c.Evidence(ctx)
	if err := policy.Allows(action); err != nil {
		action.Status = StatusBlocked
		action.Result = err.Error()
		action.Error = err.Error()
		_ = c.Store.CompleteExecution(ctx, action.ID, StatusBlocked, err.Error(), err.Error(), "")
		return action, err
	}
	c.mu.Lock()
	claimed, err := c.Store.ClaimExecution(ctx, action.ID, time.Now().UTC())
	c.mu.Unlock()
	if err != nil || !claimed {
		return action, err
	}
	action.Status, action.DryRun = StatusExecuting, false

	execCtx, cancel := context.WithTimeout(ctx, c.Policy.Timeout)
	defer cancel()

	var (
		result       string
		execErr      error
		verification string
	)
	for attempt := 1; attempt <= maxExecutionAttempts; attempt++ {
		action.Attempts = attempt
		result, execErr = c.Executor.Execute(execCtx, action)
		if execErr == nil {
			break
		}
		// Only a plausibly transient fault is worth repeating. A refusal from
		// the action's own preconditions is a decision, and repeating it just
		// asks the same question again.
		if !retryableExecution(execErr) || attempt == maxExecutionAttempts {
			break
		}
		select {
		case <-execCtx.Done():
			execErr = execCtx.Err()
			attempt = maxExecutionAttempts
		case <-time.After(executionBackoff(attempt)):
		}
	}
	if execErr == nil {
		if verifier, ok := c.Executor.(interface {
			Verify(context.Context, *Action) (string, error)
		}); ok {
			verification, execErr = verifier.Verify(execCtx, action)
		} else {
			verification = "executor completed; no verifier configured"
		}
	}
	if execErr != nil {
		action.Status, action.Error, action.Result = StatusFailed, execErr.Error(), "execution failed"
		_ = c.Store.CompleteExecution(ctx, action.ID, StatusFailed, action.Result, action.Error, verification)
	} else {
		action.Status, action.Result, action.Verification = StatusSucceeded, result, verification
		_ = c.Store.CompleteExecution(ctx, action.ID, StatusSucceeded, result, "", verification)
	}
	if c.Notifier != nil {
		_ = c.Notifier.Notify(context.Background(), action)
	}
	return action, execErr
}

// executableActions are the action types with an executor and a verifier. The
// allowlist narrows this further; it can never widen it.
var executableActions = map[string]bool{
	ActionRestartPod:         true,
	ActionRestoreReplicas:    true,
	ActionBumpMemory:         true,
	ActionRollbackDeployment: true,
}

// ObservationStart resolves when the dry-run observation period began.
// HEAL_OBSERVATION_STARTED_AT wins when set; otherwise it is the first decision
// Chronicle ever recorded. Leaving it to the environment alone meant an unset
// variable read as "never", so the gate looked like a countdown that was in
// fact permanently shut.
func (c *Controller) ObservationStart(ctx context.Context) time.Time {
	if !c.Policy.ObservationSince.IsZero() {
		return c.Policy.ObservationSince
	}
	source, ok := c.Store.(interface {
		EarliestActionAt(context.Context) (time.Time, error)
	})
	if !ok {
		return time.Time{}
	}
	at, err := source.EarliestActionAt(ctx)
	if err != nil {
		return time.Time{}
	}
	return at
}

// ObservationRemaining is how much of the period is left; zero means complete
// and ok is false while nothing has been observed at all.
func (c *Controller) ObservationRemaining(ctx context.Context) (remaining time.Duration, ok bool) {
	start := c.ObservationStart(ctx)
	if start.IsZero() {
		return 0, false
	}
	if elapsed := time.Since(start); elapsed < ObservationPeriod {
		return ObservationPeriod - elapsed, true
	}
	return 0, true
}

const ObservationPeriod = 30 * 24 * time.Hour

// Evidence reports what the observation period has produced so far.
func (c *Controller) Evidence(ctx context.Context) EvidenceSummary {
	source, ok := c.Store.(interface {
		EvidenceSummary(context.Context) (EvidenceSummary, error)
	})
	if !ok {
		return EvidenceSummary{}
	}
	summary, err := source.EvidenceSummary(ctx)
	if err != nil {
		return EvidenceSummary{}
	}
	return summary
}

func intEnv(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
		return v
	}
	return fallback
}

func floatEnv(name string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v >= 0 && v <= 1 {
		return v
	}
	return fallback
}

const maxExecutionAttempts = 3

// executionBackoff spaces the retries out so a struggling API server is not
// asked again immediately.
func executionBackoff(attempt int) time.Duration {
	return time.Duration(attempt) * 500 * time.Millisecond
}

// retryableExecution reports whether a failure is worth another attempt. The
// API server being briefly unavailable is; a conflict on optimistic concurrency
// is, because the next attempt re-reads. Anything else -- not found, forbidden,
// invalid, or a precondition this package refused on -- will fail the same way
// however many times it is asked.
func retryableExecution(err error) bool {
	if err == nil {
		return false
	}
	switch {
	case apierrors.IsConflict(err),
		apierrors.IsServerTimeout(err),
		apierrors.IsTimeout(err),
		apierrors.IsTooManyRequests(err),
		apierrors.IsInternalError(err),
		apierrors.IsServiceUnavailable(err):
		return true
	}
	return false
}

func (p Policy) Allows(a *Action) error {
	if a == nil {
		return fmt.Errorf("action is nil")
	}
	// Default-deny on capability, then on configuration. The hard-coded single
	// action this replaced made HEAL_ALLOWED_ACTIONS look like the control when
	// it was not: an operator could allowlist an action and never learn that a
	// line above ignored the list entirely.
	if !executableActions[a.ActionType] {
		return fmt.Errorf("action type %q cannot be executed: it has no verified executor", a.ActionType)
	}
	if !p.LiveEnabled {
		return fmt.Errorf("live healing is disabled")
	}
	if p.KillSwitch {
		return fmt.Errorf("global healing kill switch is enabled")
	}
	if p.ObservationSince.IsZero() || time.Since(p.ObservationSince) < ObservationPeriod {
		return fmt.Errorf("30-day dry-run observation period is not complete")
	}
	// Elapsed time alone is a calendar, not a standard: the period could run out
	// with every decision unjudged and the gate would still open.
	if decisive := p.Evidence.Decisive(); decisive < p.MinDecisive {
		return fmt.Errorf("only %d decision(s) have a confirmed or contradicted outcome, %d required (%d were unjudged)",
			decisive, p.MinDecisive, p.Evidence.Unknown)
	}
	if got := p.Evidence.Precision(); got < p.MinPrecision {
		return fmt.Errorf("decisions have been right %.0f%% of the time, below the %.0f%% required",
			got*100, p.MinPrecision*100)
	}
	if !p.AllowedActions[a.ActionType] {
		return fmt.Errorf("action type %q is not allowlisted", a.ActionType)
	}
	if !p.AllowedNamespaces[a.Namespace] {
		return fmt.Errorf("namespace %q is not allowlisted", a.Namespace)
	}
	if len(p.AllowedTargets) == 0 || (!p.AllowedTargets[a.Namespace+"/"+a.Target] && !p.AllowedTargets[a.Target]) {
		return fmt.Errorf("target %s/%s is not allowlisted", a.Namespace, a.Target)
	}
	return nil
}

// ApprovalAuthorized checks the reviewer's approval token. It is read from
// X-Chronicle-Heal-Token first, because Authorization may already carry the
// separate API token; a bearer token there is still accepted so existing
// scripts keep working.
func ApprovalAuthorized(r *http.Request) bool {
	want := os.Getenv("HEAL_APPROVAL_TOKEN")
	if want == "" {
		return false
	}
	have := r.Header.Get("X-Chronicle-Heal-Token")
	if have == "" {
		have = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	return subtle.ConstantTimeCompare([]byte(have), []byte(want)) == 1
}

func csvSet(raw string) map[string]bool {
	result := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		if value := strings.TrimSpace(item); value != "" {
			result[value] = true
		}
	}
	return result
}
func durationEnv(name string, fallback time.Duration) time.Duration {
	if value, err := time.ParseDuration(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}
func timeEnv(name string) time.Time {
	value := os.Getenv(name)
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339, value)
	return parsed
}
