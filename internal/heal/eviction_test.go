package heal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// evictingClient handles evictions as the API server does: the pod is
// deleted, subject to its UID precondition, unless refuse says a
// PodDisruptionBudget forbids it. The fake clientset on its own ignores them.
func evictingClient(refuse func(pod string) bool, objects ...runtime.Object) (*fake.Clientset, *[]string) {
	client := fake.NewSimpleClientset(objects...)
	var evicted []string
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok || create.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		ev := create.GetObject().(*policyv1.Eviction)
		if refuse != nil && refuse(ev.Name) {
			return true, nil, apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10)
		}
		obj, err := client.Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, ev.Namespace, ev.Name)
		if err != nil {
			return true, nil, err
		}
		if pre := ev.DeleteOptions; pre != nil && pre.Preconditions != nil && pre.Preconditions.UID != nil &&
			*pre.Preconditions.UID != obj.(*corev1.Pod).UID {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, ev.Name, errors.New("UID precondition failed"))
		}
		evicted = append(evicted, ev.Name)
		return true, nil, client.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, ev.Namespace, ev.Name)
	})
	return client, &evicted
}

func restartAction(pod string) *Action {
	return &Action{ActionType: ActionRestartPod, Namespace: "default", Target: pod, Approval: ApprovalApproved,
		Payload: []byte(`{"owner":"redis"}`)}
}

// Finding 5: a restart goes through the Eviction API, never a bare delete, so
// a PodDisruptionBudget is honoured.
func TestRestartEvictsAndNeverDeletes(t *testing.T) {
	client, evicted := evictingClient(nil, unreadyPod("redis-6d-abc", "redis-6d"))
	got, err := NewKubernetesExecutor(client).Execute(context.Background(), restartAction("redis-6d-abc"))
	if err != nil || !strings.Contains(got, "evicted") {
		t.Fatalf("got %q, %v", got, err)
	}
	if len(*evicted) != 1 {
		t.Fatalf("evicted %v", *evicted)
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatal("the pod was deleted directly, bypassing any disruption budget")
		}
	}
}

func TestADisruptionBudgetRefusalIsReportedAndRetryable(t *testing.T) {
	client, evicted := evictingClient(func(string) bool { return true }, unreadyPod("redis-6d-abc", "redis-6d"))
	_, err := NewKubernetesExecutor(client).Execute(context.Background(), restartAction("redis-6d-abc"))
	if err == nil || !strings.Contains(err.Error(), "PodDisruptionBudget") {
		t.Fatalf("got %v", err)
	}
	// A budget can free up, so the worker's bounded retry applies.
	if !retryableExecution(err) {
		t.Fatal("a budget refusal should be retried within the action timeout")
	}
	if len(*evicted) != 0 {
		t.Fatal("evicted despite the budget")
	}
	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "redis-6d-abc", metav1.GetOptions{}); err != nil {
		t.Fatal("the pod is gone despite the refusal")
	}
}

func deploymentStatus(name string, replicas int32, available int32, conds ...appsv1.DeploymentCondition) *appsv1.Deployment {
	d := rolledOut(name, 2, 2, replicas, replicas, replicas, available)
	d.Status.Conditions = conds
	return d
}

func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// Recovery means the workload is whole again, not that the request was
// accepted. Each of these accepted the change and did not recover.
func TestVerificationRequiresTheWorkloadToRecover(t *testing.T) {
	cases := map[string]struct {
		action *Action
		objs   []runtime.Object
	}{
		"restore: replicas set, none available": {restoreAction(`{"old_replicas":2,"new_replicas":0}`),
			[]runtime.Object{deploymentStatus("redis", 2, 0)}},
		"restore: still zero": {restoreAction(`{"old_replicas":2,"new_replicas":0}`),
			[]runtime.Object{deploymentStatus("redis", 0, 0)}},
		"restart: pod replaced, workload not ready": {restartAction("redis-6d-abc"),
			[]runtime.Object{deploymentStatus("redis", 1, 0)}},
		"restart: old pod still there": {restartAction("redis-6d-abc"),
			[]runtime.Object{unreadyPod("redis-6d-abc", "redis-6d"), deploymentStatus("redis", 1, 1)}},
	}
	for name, c := range cases {
		e := &KubernetesExecutor{Client: fake.NewSimpleClientset(c.objs...)}
		if got, err := e.Verify(cancelled(), c.action); err == nil {
			t.Errorf("%s: reported recovered: %q", name, got)
		}
	}
}

func TestVerificationPassesOnceTheWorkloadIsWhole(t *testing.T) {
	e := &KubernetesExecutor{Client: fake.NewSimpleClientset(deploymentStatus("redis", 1, 1))}
	if got, err := e.Verify(context.Background(), restartAction("redis-6d-abc")); err != nil || !strings.Contains(got, "replaced") {
		t.Fatalf("restart: %q, %v", got, err)
	}
	restore := restoreAction(`{"old_replicas":1,"new_replicas":0}`)
	restore.Target = "redis"
	if got, err := e.Verify(context.Background(), restore); err != nil || !strings.Contains(got, "available") {
		t.Fatalf("restore: %q, %v", got, err)
	}
}

// The Deployment controller's own verdict ends verification at once; a stale
// one, from the rollout being fixed, does not.
func TestARolloutTheControllerDeclaresFailedFailsAtOnce(t *testing.T) {
	stalled := func(at time.Time) appsv1.DeploymentCondition {
		return appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason: "ProgressDeadlineExceeded", Message: `ReplicaSet "api-7c" has timed out progressing.`,
			LastUpdateTime: metav1.NewTime(at)}
	}
	now := time.Now()
	fresh := &KubernetesExecutor{Client: fake.NewSimpleClientset(deploymentStatus("api", 2, 0, stalled(now)))}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := fresh.awaitRollout(ctx, "default", "api", now.Add(-time.Second))
	if err == nil || !strings.Contains(err.Error(), "timed out progressing") || time.Since(start) > 2*time.Second {
		t.Fatalf("expected an immediate failure naming the controller's verdict, got %v after %s", err, time.Since(start))
	}
	stale := &KubernetesExecutor{Client: fake.NewSimpleClientset(deploymentStatus("api", 2, 0, stalled(now.Add(-5*time.Minute))))}
	_, err = stale.awaitRollout(cancelled(), "default", "api", now.Add(-time.Second))
	if err == nil || strings.Contains(err.Error(), "timed out progressing") {
		t.Fatalf("a stale verdict from before the action must not decide: %v", err)
	}
}

// Evidence comes from an event; a count past the guardrail's ceiling is
// refused, never narrowed into a wrapped int32.
func TestRestoreRefusesACountAboveTheCeiling(t *testing.T) {
	zero := int32(0)
	d := scaledDeployment("redis", 0)
	d.Spec.Replicas = &zero
	client := fake.NewSimpleClientset(d)
	for _, old := range []string{"21", "4294967297"} {
		_, err := NewKubernetesExecutor(client).restoreReplicas(context.Background(), restoreAction(`{"old_replicas":`+old+`,"new_replicas":0}`))
		if err == nil || !strings.Contains(err.Error(), "ceiling") {
			t.Errorf("old_replicas %s: %v", old, err)
		}
	}
	got, _ := client.AppsV1().Deployments("default").Get(context.Background(), "redis", metav1.GetOptions{})
	if *got.Spec.Replicas != 0 {
		t.Fatalf("wrote %d replicas", *got.Spec.Replicas)
	}
}
