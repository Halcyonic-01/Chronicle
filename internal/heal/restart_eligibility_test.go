package heal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var restartNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

func stuckPod(unreadyFor time.Duration) *corev1.Pod {
	p := unreadyPod("redis-6d-abc", "redis-6d")
	p.Status.Conditions[0].LastTransitionTime = metav1.NewTime(restartNow.Add(-unreadyFor))
	return p
}

func TestARestartWaitsUntilThePodHasBeenUnreadyForFiveMinutes(t *testing.T) {
	e := NewKubernetesExecutor(fake.NewSimpleClientset(stuckPod(2 * time.Minute)))
	e.Now = func() time.Time { return restartNow }
	got, err := e.NotBefore(context.Background(), restartAction("redis-6d-abc"))
	if err != nil || !got.Equal(restartNow.Add(3*time.Minute)) {
		t.Fatalf("a pod unready for 2 minutes is eligible at %v, %v", got, err)
	}
	e = NewKubernetesExecutor(fake.NewSimpleClientset(stuckPod(8 * time.Minute)))
	e.Now = func() time.Time { return restartNow }
	if got, err := e.NotBefore(context.Background(), restartAction("redis-6d-abc")); err != nil || got.After(restartNow) {
		t.Fatalf("a pod unready for 8 minutes is eligible now, got %v, %v", got, err)
	}
}

func TestARestartWaitsOutAContainerThatWasJustRestarted(t *testing.T) {
	p := stuckPod(8 * time.Minute)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "redis", RestartCount: 1, LastTerminationState: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(restartNow.Add(-4 * time.Minute))}}}}
	e := NewKubernetesExecutor(fake.NewSimpleClientset(p))
	e.Now = func() time.Time { return restartNow }
	got, err := e.NotBefore(context.Background(), restartAction("redis-6d-abc"))
	if err != nil || !got.Equal(restartNow.Add(6*time.Minute)) {
		t.Fatalf("a restart 4 minutes ago leaves 6 of the 10 to wait: %v, %v", got, err)
	}
}

func TestARestartWaitsWhileItsDeploymentIsMidRollout(t *testing.T) {
	one := int32(1)
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "default", Generation: 3},
		Spec: appsv1.DeploymentSpec{Replicas: &one}, Status: appsv1.DeploymentStatus{ObservedGeneration: 3, UpdatedReplicas: 0}}
	e := NewKubernetesExecutor(fake.NewSimpleClientset(stuckPod(8*time.Minute), d))
	e.Now = func() time.Time { return restartNow }
	got, err := e.NotBefore(context.Background(), restartAction("redis-6d-abc"))
	if err != nil || !got.After(restartNow) {
		t.Fatalf("a rollout in progress should defer the restart: %v, %v", got, err)
	}
}

func TestARestartIsRefusedOutrightWhenThePodRecoveredOrIsGone(t *testing.T) {
	ready := stuckPod(8 * time.Minute)
	ready.Status.Conditions[0].Status = corev1.ConditionTrue
	for name, e := range map[string]*KubernetesExecutor{
		"ready again": NewKubernetesExecutor(fake.NewSimpleClientset(ready)),
		"gone":        NewKubernetesExecutor(fake.NewSimpleClientset()),
	} {
		_, err := e.NotBefore(context.Background(), restartAction("redis-6d-abc"))
		var r *Refusal
		if !errors.As(err, &r) {
			t.Errorf("%s: %v is not a refusal", name, err)
		}
	}
}

func TestOnlyARestartIsHeldBack(t *testing.T) {
	e := NewKubernetesExecutor(fake.NewSimpleClientset())
	if got, err := e.NotBefore(context.Background(), restoreAction(`{"old_replicas":2,"new_replicas":0}`)); err != nil || !got.IsZero() {
		t.Fatalf("a restore was held back: %v %v", got, err)
	}
}

// ---- the controller --------------------------------------------------------

type gatedExecutor struct {
	recordingExecutor
	until time.Time
	err   error
}

func (g *gatedExecutor) NotBefore(context.Context, *Action) (time.Time, error) { return g.until, g.err }

func TestAnIneligibleDecisionStaysApprovedAndClaimsNothing(t *testing.T) {
	store, executor := &executionMemory{}, &gatedExecutor{until: time.Now().Add(time.Hour)}
	_, err := liveController(store, executor).ExecuteApproved(context.Background(), approvedRestart("waiting"))
	var waiting *NotYet
	if !errors.As(err, &waiting) {
		t.Fatalf("got %v", err)
	}
	if store.claimed || store.status != "" || executor.calls != 0 {
		t.Fatalf("a decision that is not yet eligible was acted on: claimed=%v status=%q calls=%d", store.claimed, store.status, executor.calls)
	}
	// Once eligible it runs, with the same approval.
	executor.until = time.Now().Add(-time.Second)
	if _, err := liveController(store, executor).ExecuteApproved(context.Background(), approvedRestart("waiting")); err != nil || executor.calls != 1 {
		t.Fatalf("an eligible decision did not run: %v calls=%d", err, executor.calls)
	}
}

func TestARefusalFromTheEligibilityCheckBlocksTheDecision(t *testing.T) {
	store, executor := &executionMemory{}, &gatedExecutor{err: refuse("refusing to restart: pod redis-6d-abc is ready again")}
	_, err := liveController(store, executor).ExecuteApproved(context.Background(), approvedRestart("recovered"))
	if err == nil || store.status != StatusBlocked || executor.calls != 0 || !strings.Contains(err.Error(), "ready again") {
		t.Fatalf("err %v status %q calls %d", err, store.status, executor.calls)
	}
	// An error that is not a refusal (the API was unreachable) must not burn the approval.
	store, executor = &executionMemory{}, &gatedExecutor{err: errors.New("connection refused")}
	if _, err := liveController(store, executor).ExecuteApproved(context.Background(), approvedRestart("flaky")); err == nil || store.status != "" {
		t.Fatalf("a transient error blocked the decision: %v status %q", err, store.status)
	}
}

func restartResult(payload string) *rca.Result {
	return &rca.Result{Symptom: event.Event{ID: "incident-r"}, Confidence: 0.91, Verdict: rca.VerdictRootCause,
		Candidates: []rca.Candidate{{Event: event.Event{ID: "c1", Namespace: "default", EntityKind: "Pod", EntityName: "redis-6d-abc",
			Type: "became_unready", Payload: []byte(payload)}, Reasons: []string{"redis stopped answering"}}}}
}

func TestARestartProposalRecommendsALivenessProbeOnlyWhenThereIsNone(t *testing.T) {
	for payload, want := range map[string]bool{
		`{"owner":"redis","has_liveness_probe":false}`: true,
		`{"owner":"redis","has_liveness_probe":true}`:  false,
		`{"owner":"redis"}`:                            false, // an older event: unknown, not absent
	} {
		action, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), restartResult(payload))
		if err != nil || action.ActionType != ActionRestartPod {
			t.Fatalf("%s: %v %+v", payload, err, action)
		}
		if got := strings.Contains(strings.Join(action.Reasoning, "\n"), "livenessProbe"); got != want {
			t.Errorf("%s: recommendation present=%v, want %v (%v)", payload, got, want, action.Reasoning)
		}
	}
}

func TestAPodWithNoControllerIsJudgedOnItsOwnConditions(t *testing.T) {
	p := stuckPod(8 * time.Minute) // owned by nothing
	p.OwnerReferences = nil
	e := NewKubernetesExecutor(fake.NewSimpleClientset(p))
	e.Now = func() time.Time { return restartNow }
	if got, err := e.NotBefore(context.Background(), restartAction("redis-6d-abc")); err != nil || got.After(restartNow) {
		t.Fatalf("got %v, %v", got, err)
	}
}
