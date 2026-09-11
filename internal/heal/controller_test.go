package heal

import (
	"context"
	"testing"
	"time"
)

type executionMemory struct {
	claimed bool
	status  string
}

func (m *executionMemory) ClaimExecution(context.Context, string, time.Time) (bool, error) {
	if m.claimed {
		return false, nil
	}
	m.claimed = true
	return true, nil
}
func (m *executionMemory) CompleteExecution(_ context.Context, _ string, status, _, _, _ string) error {
	m.status = status
	return nil
}

type recordingExecutor struct{ calls int }

func (e *recordingExecutor) Execute(context.Context, *Action) (string, error) {
	e.calls++
	return "deleted", nil
}
func (e *recordingExecutor) Verify(context.Context, *Action) (string, error) { return "verified", nil }

func livePolicy() Policy {
	return Policy{
		LiveEnabled:       true,
		AllowedActions:    map[string]bool{ActionRestartPod: true},
		AllowedNamespaces: map[string]bool{"default": true},
		AllowedTargets:    map[string]bool{"default/redis": true},
		ObservationSince:  time.Now().Add(-31 * 24 * time.Hour),
		Timeout:           time.Second,
	}
}

func TestPolicyAllowsOnlyAllowlistedPodRestart(t *testing.T) {
	p := livePolicy()
	if err := p.Allows(&Action{ActionType: ActionRollbackDeployment, Namespace: "default", Target: "redis"}); err == nil {
		t.Fatal("rollback should remain disabled")
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "monitoring", Target: "redis"}); err == nil {
		t.Fatal("unexpected namespace was allowed")
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis"}); err != nil {
		t.Fatal(err)
	}
}

func TestControllerExecutesAndVerifiesApprovedRestart(t *testing.T) {
	store := &executionMemory{}
	executor := &recordingExecutor{}
	controller := &Controller{Store: store, Executor: executor, Policy: livePolicy()}
	action := &Action{ID: "action-1", ActionType: ActionRestartPod, Namespace: "default", Target: "redis", Approval: ApprovalApproved, DryRun: true}
	got, err := controller.ExecuteApproved(context.Background(), action)
	if err != nil {
		t.Fatal(err)
	}
	if executor.calls != 1 || store.status != StatusSucceeded || got.Verification != "verified" || got.DryRun {
		t.Fatalf("unexpected execution: %+v", got)
	}
}

func TestControllerKillSwitchBlocksWithoutExecutorCall(t *testing.T) {
	store := &executionMemory{}
	executor := &recordingExecutor{}
	policy := livePolicy()
	policy.KillSwitch = true
	controller := &Controller{Store: store, Executor: executor, Policy: policy}
	action := &Action{ID: "action-2", ActionType: ActionRestartPod, Namespace: "default", Target: "redis", Approval: ApprovalApproved}
	if _, err := controller.ExecuteApproved(context.Background(), action); err == nil {
		t.Fatal("kill switch did not block")
	}
	if executor.calls != 0 || store.status != StatusBlocked {
		t.Fatalf("unexpected blocked execution: calls=%d status=%s", executor.calls, store.status)
	}
}
