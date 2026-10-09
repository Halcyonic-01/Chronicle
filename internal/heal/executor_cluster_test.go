package heal

// The executor against a REAL API server and Deployment controller. The unit
// tests use a fake client, which cannot show that the revision bookkeeping the
// rollback depends on is what Kubernetes really writes.
//
// Opt-in, and only ever against a disposable cluster:
//
//	HEAL_TEST_KUBE_CONTEXT=kind-kyverno-test go test ./internal/heal -run RealCluster -v
//
// Any context not named kind-kyverno-test* is refused. Each test works in its
// own namespace and deletes it afterwards.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func realCluster(t *testing.T) (kubernetes.Interface, string) {
	t.Helper()
	name := os.Getenv("HEAL_TEST_KUBE_CONTEXT")
	if name == "" {
		t.Skip("set HEAL_TEST_KUBE_CONTEXT=kind-kyverno-test to run against a disposable cluster")
	}
	if !strings.HasPrefix(name, "kind-kyverno-test") {
		t.Fatalf("refusing context %q: only a disposable kind-kyverno-test* cluster may be used", name)
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{CurrentContext: name}).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("heal-it-%d", time.Now().UnixNano()%1_000_000)
	ctx := context.Background()
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}) })
	return client, ns
}

func realDeployment(ns, name, image, envValue, memory string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "api", Image: image, Env: []corev1.EnvVar{{Name: "REDIS_URL", Value: envValue}},
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(memory)}}},
					{Name: "metrics", Image: image + "-metrics"},
				}},
			},
		},
	}
}

// execute runs an executor step with the controller's retry rule: a conflict
// on optimistic concurrency is retried, because every step re-reads first.
func execute(step func() (string, error)) (string, error) {
	var out string
	var err error
	for attempt := 1; attempt <= maxExecutionAttempts; attempt++ {
		if out, err = step(); err == nil || !retryableExecution(err) {
			break
		}
		time.Sleep(executionBackoff(attempt))
	}
	return out, err
}

// waitFor polls until the condition holds; the Deployment controller is asynchronous.
func waitFor(t *testing.T, what string, cond func() (bool, string)) {
	t.Helper()
	deadline, last := time.Now().Add(90*time.Second), ""
	for time.Now().Before(deadline) {
		ok, why := cond()
		if ok {
			return
		}
		last = why
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %s", what, last)
}

// The rollback against the real revision history: a bad release changes the
// image, env and memory of two containers; the rollback restores all of it, a
// repeat is a no-op, and a later deploy is never overwritten.
func TestRealClusterRollbackRestoresTheWholeTemplate(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	deployments := client.AppsV1().Deployments(ns)

	// Nothing here needs to start: the revision annotations are written when
	// the controller creates each ReplicaSet, which is what is under test.
	if _, err := deployments.Create(ctx, realDeployment(ns, "api", "api:v1", "redis:6379", "256Mi", 1), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	revision := func(want string) func() (bool, string) {
		return func() (bool, string) {
			d, err := deployments.Get(ctx, "api", metav1.GetOptions{})
			if err != nil {
				return false, err.Error()
			}
			return d.Annotations[revisionAnnotation] == want && d.Status.ObservedGeneration == d.Generation, "revision " + d.Annotations[revisionAnnotation]
		}
	}
	waitFor(t, "revision 1", revision("1"))

	bad, _ := deployments.Get(ctx, "api", metav1.GetOptions{})
	bad.Spec.Template = realDeployment(ns, "api", "api:v2", "redis-typo:6379", "2Mi", 1).Spec.Template
	if _, err := deployments.Update(ctx, bad, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "revision 2", revision("2"))

	executor := &KubernetesExecutor{Client: client}
	action := &Action{ActionType: ActionRollbackDeployment, Namespace: ns, Target: "api", Approval: ApprovalApproved,
		Payload: []byte(`{"old_image":"api:v1","new_image":"api:v2"}`)}
	if got, err := executor.rollbackDeployment(ctx, action); err != nil {
		t.Fatalf("rollback failed: %v", err)
	} else {
		t.Log(got)
	}

	d, _ := deployments.Get(ctx, "api", metav1.GetOptions{})
	c := d.Spec.Template.Spec.Containers
	mem := c[0].Resources.Limits[corev1.ResourceMemory]
	if len(c) != 2 || c[0].Image != "api:v1" || c[0].Env[0].Value != "redis:6379" || mem.String() != "256Mi" || c[1].Image != "api:v1-metrics" {
		t.Fatalf("the previous template was not restored: %+v (memory %s)", c, mem.String())
	}
	if _, polluted := d.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey]; polluted {
		t.Fatal("the ReplicaSet's hash label leaked into the Deployment template")
	}
	// The controller must accept it as a normal rollout, reusing the old ReplicaSet.
	waitFor(t, "the rollback to be observed as revision 3", revision("3"))

	// A retry, or a second approval of the same decision, changes nothing.
	generation := d.Generation
	if got, err := executor.rollbackDeployment(ctx, action); err != nil || !strings.Contains(got, "no change") {
		t.Fatalf("a repeated rollback should be a no-op, got %q, %v", got, err)
	}
	if again, _ := deployments.Get(ctx, "api", metav1.GetOptions{}); again.Generation != generation {
		t.Fatal("a repeated rollback modified the Deployment")
	}

	// A later deploy since the decision must never be overwritten.
	later, _ := deployments.Get(ctx, "api", metav1.GetOptions{})
	later.Spec.Template = realDeployment(ns, "api", "api:v4", "redis:6379", "256Mi", 1).Spec.Template
	if _, err := deployments.Update(ctx, later, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "revision 4", revision("4"))
	if _, err := executor.rollbackDeployment(ctx, action); err == nil {
		t.Fatal("a stale rollback overwrote a later deploy")
	}
	if final, _ := deployments.Get(ctx, "api", metav1.GetOptions{}); final.Spec.Template.Spec.Containers[0].Image != "api:v4" {
		t.Fatalf("the later deploy was overwritten: %s", final.Spec.Template.Spec.Containers[0].Image)
	}
}

// Restoring replicas, and the refusal when the workload is no longer at zero.
func TestRealClusterRestoreReplicas(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	deployments := client.AppsV1().Deployments(ns)
	if _, err := deployments.Create(ctx, realDeployment(ns, "cache", "cache:v1", "x", "64Mi", 0), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	executor := &KubernetesExecutor{Client: client}
	action := &Action{ActionType: ActionRestoreReplicas, Namespace: ns, Target: "cache", Approval: ApprovalApproved,
		Payload: []byte(`{"old_replicas":3,"new_replicas":0}`)}
	if _, err := execute(func() (string, error) { return executor.restoreReplicas(ctx, action) }); err != nil {
		t.Fatal(err)
	}
	if d, _ := deployments.Get(ctx, "cache", metav1.GetOptions{}); d.Spec.Replicas == nil || *d.Spec.Replicas != 3 {
		t.Fatalf("expected 3 replicas, got %v", d.Spec.Replicas)
	}
	// Somebody already restored it: a second decision must not write.
	if _, err := executor.restoreReplicas(ctx, action); err == nil {
		t.Fatal("a stale restore wrote over a workload that is no longer at zero")
	}
}

// Restarting an unready pod: deleted by UID, recreated by its ReplicaSet; a
// repeat for the deleted pod is refused rather than hitting its replacement.
func TestRealClusterRestartPod(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	// An image that cannot be pulled keeps the pod permanently unready.
	d := realDeployment(ns, "stuck", "registry.invalid/stuck:v1", "x", "64Mi", 1)
	d.Spec.Template.Spec.Containers = d.Spec.Template.Spec.Containers[:1]
	if _, err := client.AppsV1().Deployments(ns).Create(ctx, d, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	waitFor(t, "a pod", func() (bool, string) {
		list, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=stuck"})
		if err != nil || len(list.Items) == 0 {
			return false, "no pod yet"
		}
		pod = list.Items[0]
		return true, ""
	})
	executor := &KubernetesExecutor{Client: client}
	action := &Action{ActionType: ActionRestartPod, Namespace: ns, Target: pod.Name, Approval: ApprovalApproved,
		Payload: []byte(`{"owner":"stuck"}`)}
	if got, err := executor.restartPod(ctx, action); err != nil {
		t.Fatalf("restart failed: %v", err)
	} else {
		t.Log(got)
	}
	waitFor(t, "a replacement pod", func() (bool, string) {
		list, _ := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=stuck"})
		for _, p := range list.Items {
			if p.Name != pod.Name && p.DeletionTimestamp == nil {
				return true, ""
			}
		}
		return false, "not recreated yet"
	})
	// Repeating the decision is either a no-op (still terminating) or refused
	// (gone); it must never delete the replacement.
	if got, err := executor.restartPod(ctx, action); err == nil && !strings.Contains(got, "already being deleted") {
		t.Fatalf("a repeat restart claimed a fresh deletion: %q", got)
	}
	list, _ := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=stuck"})
	alive := 0
	for _, p := range list.Items {
		if p.Name != pod.Name && p.DeletionTimestamp == nil {
			alive++
		}
	}
	if alive != 1 {
		t.Fatalf("expected the replacement pod to survive the repeat, %d alive", alive)
	}
	// A pod owned by another workload is never restarted under this decision.
	wrong := &Action{ActionType: ActionRestartPod, Namespace: ns, Target: pod.Name, Payload: []byte(`{"owner":"somebody-else"}`)}
	list, _ = client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=stuck"})
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil {
			wrong.Target = p.Name
		}
	}
	if _, err := executor.restartPod(ctx, wrong); err == nil {
		t.Fatal("deleted a pod owned by a different workload than the decision named")
	}
}
