package heal

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
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
	if err := c.Policy.Allows(action); err != nil {
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
	result, execErr := c.Executor.Execute(execCtx, action)
	verification := ""
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

func (p Policy) Allows(a *Action) error {
	if a == nil {
		return fmt.Errorf("action is nil")
	}
	if a.ActionType != ActionRestartPod {
		return fmt.Errorf("only pod restart execution is enabled")
	}
	if !p.LiveEnabled {
		return fmt.Errorf("live healing is disabled")
	}
	if p.KillSwitch {
		return fmt.Errorf("global healing kill switch is enabled")
	}
	if p.ObservationSince.IsZero() || time.Since(p.ObservationSince) < 30*24*time.Hour {
		return fmt.Errorf("30-day dry-run observation period is not complete")
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

func ApprovalAuthorized(r *http.Request) bool {
	want := os.Getenv("HEAL_APPROVAL_TOKEN")
	if want == "" {
		return false
	}
	have := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
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
