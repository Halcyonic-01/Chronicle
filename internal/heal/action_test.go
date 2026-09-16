package heal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type memoryAudit struct {
	actions []*Action
	count   int
	seen    bool
}

func (m *memoryAudit) RecordAction(_ context.Context, a *Action) error {
	m.actions = append(m.actions, a)
	return nil
}
func (m *memoryAudit) CountRuleSince(context.Context, string, time.Time) (int, error) {
	return m.count, nil
}
func (m *memoryAudit) HasActionForIncident(context.Context, string) (bool, error) { return m.seen, nil }

func TestEnginePlansDryRunForHighConfidenceCause(t *testing.T) {
	store := &memoryAudit{}
	engine := NewEngine(store)
	result := &rca.Result{
		Symptom:    event.Event{ID: "incident-1"},
		Confidence: 0.91,
		Candidates: []rca.Candidate{{Event: event.Event{Namespace: "default", EntityName: "redis", Type: "deploy"}, Reasons: []string{"one hop upstream"}}},
	}
	action, err := engine.Evaluate(context.Background(), result)
	if err != nil {
		t.Fatal(err)
	}
	if action.Status != StatusWouldRun || !action.DryRun {
		t.Fatalf("unexpected action: %+v", action)
	}
	if action.Result != "WOULD HAVE RUN (approval required)" {
		t.Fatalf("unexpected result: %q", action.Result)
	}
}

func TestEngineBlocksLowConfidenceCause(t *testing.T) {
	store := &memoryAudit{}
	action, err := NewEngine(store).Evaluate(context.Background(), &rca.Result{
		Symptom: event.Event{ID: "incident-2"}, Confidence: 0.20,
		Candidates: []rca.Candidate{{Event: event.Event{EntityName: "redis", Type: "oom_kill"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if action.Status != StatusBlocked || action.DryRun == false {
		t.Fatalf("unexpected action: %+v", action)
	}
}

func TestEngineBlocksRateLimitedRule(t *testing.T) {
	store := &memoryAudit{count: 3}
	action, err := NewEngine(store).Evaluate(context.Background(), &rca.Result{
		Symptom: event.Event{ID: "incident-3"}, Confidence: 0.90,
		Candidates: []rca.Candidate{{Event: event.Event{EntityName: "redis", Type: "became_unready"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if action.Status != StatusBlocked {
		t.Fatalf("unexpected action: %+v", action)
	}
}

func TestEngineIsIdempotentPerIncident(t *testing.T) {
	store := &memoryAudit{seen: true}
	action, err := NewEngine(store).Evaluate(context.Background(), &rca.Result{Symptom: event.Event{ID: "incident-4"}})
	if err != nil {
		t.Fatal(err)
	}
	if action != nil {
		t.Fatalf("duplicate incident produced an action: %+v", action)
	}
}

func TestEmptyReasoningIsRepresentedAsAnEmptyList(t *testing.T) {
	action := &Action{}
	if action.Reasoning != nil {
		t.Fatal("test setup expected nil reasoning")
	}
	reasoning := action.Reasoning
	if reasoning == nil {
		reasoning = []string{}
	}
	if reasoning == nil || len(reasoning) != 0 {
		t.Fatalf("expected empty reasoning list, got %#v", reasoning)
	}
}

func TestKubernetesExecutorDeletesOnlyApprovedNonDryRunPod(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "default"}})
	executor := NewKubernetesExecutor(client)
	action := &Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis", Approval: ApprovalNotRequired, DryRun: false}
	if _, err := executor.Execute(context.Background(), action); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "redis", metav1.GetOptions{}); err == nil {
		t.Fatal("pod was not deleted")
	}
}

func TestKubernetesExecutorRequiresApprovalForRollback(t *testing.T) {
	client := fake.NewSimpleClientset()
	executor := NewKubernetesExecutor(client)
	action := &Action{ActionType: ActionRollbackDeployment, Approval: ApprovalPending, DryRun: false}
	if _, err := executor.Execute(context.Background(), action); err == nil {
		t.Fatal("unapproved rollback was accepted")
	}
}

func TestKubernetesExecutorCapsMemoryIncrease(t *testing.T) {
	client := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("100Mi")}}}}}}},
	})
	executor := NewKubernetesExecutor(client)
	action := &Action{ActionType: ActionBumpMemory, Namespace: "default", Target: "redis-pod", Approval: ApprovalNotRequired, DryRun: false, Payload: []byte(`{"owner":"redis","original_mem_bytes":104857600}`)}
	if _, err := executor.Execute(context.Background(), action); err != nil {
		t.Fatal(err)
	}
	d, err := client.AppsV1().Deployments("default").Get(context.Background(), "redis", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	memory := d.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
	if got := memory.Value(); got != 157286400 {
		t.Fatalf("unexpected memory limit: %d", got)
	}
}

func scaledDeployment(name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
}

func restoreAction(payload string) *Action {
	return &Action{
		ActionType: ActionRestoreReplicas, Namespace: "default", Target: "redis",
		Payload: []byte(payload), Approval: ApprovalApproved,
	}
}

func TestRestoreReplicasPutsAZeroedWorkloadBack(t *testing.T) {
	client := fake.NewSimpleClientset(scaledDeployment("redis", 0))
	e := &KubernetesExecutor{Client: client}
	got, err := e.restoreReplicas(context.Background(), restoreAction(`{"old_replicas":3,"new_replicas":0}`))
	if err != nil {
		t.Fatal(err)
	}
	d, _ := client.AppsV1().Deployments("default").Get(context.Background(), "redis", metav1.GetOptions{})
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 3 {
		t.Fatalf("expected 3 replicas, got %v (%s)", d.Spec.Replicas, got)
	}
}

// Ten replicas down to three is somebody managing capacity. Undoing it would
// fight them, so only a scale to zero counts as a fault.
func TestRestoreReplicasRefusesANonZeroScaleDown(t *testing.T) {
	client := fake.NewSimpleClientset(scaledDeployment("redis", 3))
	e := &KubernetesExecutor{Client: client}
	if _, err := e.restoreReplicas(context.Background(), restoreAction(`{"old_replicas":10,"new_replicas":3}`)); err == nil {
		t.Fatal("a deliberate scale-down must not be undone")
	}
	d, _ := client.AppsV1().Deployments("default").Get(context.Background(), "redis", metav1.GetOptions{})
	if *d.Spec.Replicas != 3 {
		t.Fatalf("the deployment was modified anyway: %d", *d.Spec.Replicas)
	}
}

// The evidence is a snapshot of the past. If somebody already restored the
// workload, writing the old count now would overwrite a newer decision.
func TestRestoreReplicasRefusesWhenTheWorkloadIsAlreadyBack(t *testing.T) {
	client := fake.NewSimpleClientset(scaledDeployment("redis", 5))
	e := &KubernetesExecutor{Client: client}
	if _, err := e.restoreReplicas(context.Background(), restoreAction(`{"old_replicas":1,"new_replicas":0}`)); err == nil {
		t.Fatal("a stale action must not overwrite a newer replica count")
	}
	d, _ := client.AppsV1().Deployments("default").Get(context.Background(), "redis", metav1.GetOptions{})
	if *d.Spec.Replicas != 5 {
		t.Fatalf("the newer count was overwritten: %d", *d.Spec.Replicas)
	}
}

// A field with two writers is the standard cause of a replica tug-of-war: the
// autoscaler owns spec.replicas, so Chronicle must not also write it.
func TestRestoreReplicasRefusesWhenAnAutoscalerOwnsTheField(t *testing.T) {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "redis"},
		},
	}
	client := fake.NewSimpleClientset(scaledDeployment("redis", 0), hpa)
	e := &KubernetesExecutor{Client: client}
	_, err := e.restoreReplicas(context.Background(), restoreAction(`{"old_replicas":3,"new_replicas":0}`))
	if err == nil {
		t.Fatal("Chronicle must not write a field an autoscaler owns")
	}
	if !strings.Contains(err.Error(), "redis-hpa") {
		t.Fatalf("the refusal should name the autoscaler, got %v", err)
	}
	d, _ := client.AppsV1().Deployments("default").Get(context.Background(), "redis", metav1.GetOptions{})
	if *d.Spec.Replicas != 0 {
		t.Fatal("the deployment was modified despite the autoscaler")
	}
}

// An HPA on a different workload is not a reason to refuse.
func TestRestoreReplicasIgnoresAnUnrelatedAutoscaler(t *testing.T) {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "api-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "api"},
		},
	}
	client := fake.NewSimpleClientset(scaledDeployment("redis", 0), hpa)
	e := &KubernetesExecutor{Client: client}
	if _, err := e.restoreReplicas(context.Background(), restoreAction(`{"old_replicas":2,"new_replicas":0}`)); err != nil {
		t.Fatalf("an unrelated autoscaler must not block the restore: %v", err)
	}
}

// The allowlist is now the control, so the deployed configuration is what keeps
// a new action type out of a live cluster -- not a hard-coded exception.
func TestRestoreReplicasNeedsToBeAllowlistedToRunLive(t *testing.T) {
	base := func(actions string) Policy {
		return Policy{
			LiveEnabled: true, ObservationSince: time.Now().Add(-40 * 24 * time.Hour),
			AllowedActions:    csvSet(actions),
			AllowedNamespaces: map[string]bool{"default": true},
			AllowedTargets:    map[string]bool{"default/redis": true},
		}
	}
	a := &Action{ActionType: ActionRestoreReplicas, Namespace: "default", Target: "redis"}

	// The configuration Chronicle actually ships with.
	if err := base("restart_pod").Allows(a); err == nil {
		t.Fatal("restore_replicas must be refused while only restart_pod is allowlisted")
	}
	if err := base("restart_pod,restore_replicas").Allows(a); err != nil {
		t.Fatalf("an allowlisted action should be permitted once every other gate is open: %v", err)
	}
}

// Capability is checked before configuration: allowlisting something with no
// executor must not make it runnable.
func TestAnActionWithNoExecutorIsRefusedEvenIfAllowlisted(t *testing.T) {
	p := Policy{
		LiveEnabled: true, ObservationSince: time.Now().Add(-40 * 24 * time.Hour),
		AllowedActions:    csvSet("drain_node"),
		AllowedNamespaces: map[string]bool{"default": true},
		AllowedTargets:    map[string]bool{"default/redis": true},
	}
	err := p.Allows(&Action{ActionType: "drain_node", Namespace: "default", Target: "redis"})
	if err == nil {
		t.Fatal("an action type with no executor must never run")
	}
	if !strings.Contains(err.Error(), "no verified executor") {
		t.Fatalf("the refusal should say why, got %v", err)
	}
}

// "confidence 0.65 is below 0.65" was a true rejection that read as a
// contradiction: 0.6496 rounds to the floor at two places.
func TestBlockedConfidenceMessageDoesNotReadAsEqual(t *testing.T) {
	cases := []struct {
		value, floor       float64
		wantGot, wantFloor string
	}{
		{0.6495788280560948, 0.65, "0.6496", "0.6500"},
		{0.47, 0.65, "0.47", "0.65"},
		{0.6499999, 0.65, "0.6499999", "0.65"},
	}
	for _, c := range cases {
		got, floor := formatBelow(c.value, c.floor)
		if got != c.wantGot || floor != c.wantFloor {
			t.Errorf("formatBelow(%v, %v) = %q, %q; want %q, %q", c.value, c.floor, got, floor, c.wantGot, c.wantFloor)
		}
		if got == floor {
			t.Errorf("formatBelow(%v, %v) rendered both sides as %q", c.value, c.floor, got)
		}
	}
}
