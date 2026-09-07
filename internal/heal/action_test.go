package heal

import (
	"context"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	appsv1 "k8s.io/api/apps/v1"
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
