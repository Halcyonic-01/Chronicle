package heal

// Revision identity and restart eligibility against a REAL API server. The fake
// client cannot show that a ReplicaSet's template hashes to the same value as
// the Deployment template it came from, which the whole exact-revision rollback
// depends on. Opt-in, disposable cluster only (see executor_cluster_test.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/revision"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

func templateHash(t *testing.T, d *appsv1.Deployment) string {
	t.Helper()
	return revision.TemplateHash(d.Spec.Template)
}

// updateTemplate edits the Deployment and waits for the controller to see it.
func updateTemplate(t *testing.T, client kubernetes.Interface, ns, name string, edit func(*corev1.PodTemplateSpec)) *appsv1.Deployment {
	t.Helper()
	ctx := context.Background()
	var d *appsv1.Deployment
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := client.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		edit(&cur.Spec.Template)
		d, err = client.AppsV1().Deployments(ns).Update(ctx, cur, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the change to be observed", func() (bool, string) {
		cur, _ := client.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		return cur.Status.ObservedGeneration >= d.Generation, "not observed"
	})
	cur, _ := client.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	return cur
}

func revisionRollback(ns, name, from, to string) *Action {
	// A decision about the change from -> to: it returns to from.
	payload, _ := json.Marshal(map[string]string{"from_template_hash": from, "to_template_hash": to})
	return &Action{ActionType: ActionRollbackDeployment, Namespace: ns, Target: name, Approval: ApprovalApproved, Payload: payload}
}

func TestRealClusterReplicaSetTemplatesHashLikeTheDeploymentTheyCameFrom(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	if _, err := client.AppsV1().Deployments(ns).Create(ctx, realDeployment(ns, "api", "api:v1", "redis:6379", "256Mi", 1), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first ReplicaSet", func() (bool, string) {
		l, _ := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		return len(l.Items) == 1, "no ReplicaSet"
	})
	d1, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	d2 := updateTemplate(t, client, ns, "api", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Env[0].Value = "redis:6380" })
	d3 := updateTemplate(t, client, ns, "api", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[1].Image = "metrics:v9" })
	want := map[string]bool{templateHash(t, d1): true, templateHash(t, d2): true, templateHash(t, d3): true}
	if len(want) != 3 {
		t.Fatal("an env-only change and a sidecar-only change must each be a new revision")
	}
	waitFor(t, "three ReplicaSets", func() (bool, string) {
		l, _ := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		return len(l.Items) == 3, fmt.Sprint(len(l.Items))
	})
	l, _ := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
	for _, rs := range l.Items {
		if h := revision.TemplateHash(rs.Spec.Template); !want[h] {
			t.Errorf("ReplicaSet %s hashes to %s, which is none of the Deployment's revisions", rs.Name, h)
		}
	}
}

// An env-only change, then a sidecar-only change, rolled back to an exact
// earlier revision, including one that is not the previous.
func TestRealClusterRollbackByRevisionIdentity(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	if _, err := client.AppsV1().Deployments(ns).Create(ctx, realDeployment(ns, "api", "api:v1", "redis:6379", "256Mi", 1), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first ReplicaSet", func() (bool, string) {
		l, _ := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		return len(l.Items) == 1, "none"
	})
	d1, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	h1 := templateHash(t, d1)
	d2 := updateTemplate(t, client, ns, "api", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Env[0].Value = "redis-typo:6379" })
	h2 := templateHash(t, d2)
	d3 := updateTemplate(t, client, ns, "api", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[1].Image = "metrics:v9" })
	h3 := templateHash(t, d3)
	executor := &KubernetesExecutor{Client: client}

	// The image never changed, so an image-based rollback could not have seen
	// either release. The decision names revisions 2 -> 3; undo it to 2.
	if got, err := executor.rollbackDeployment(ctx, revisionRollback(ns, "api", h2, h3)); err != nil {
		t.Fatalf("sidecar-only rollback: %v", err)
	} else {
		t.Log(got)
	}
	cur, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	if templateHash(t, cur) != h2 || cur.Spec.Template.Spec.Containers[1].Image != "api:v1-metrics" {
		t.Fatalf("revision 2 was not restored: %s", templateHash(t, cur))
	}
	if _, leaked := cur.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey]; leaked {
		t.Fatal("the ReplicaSet's hash label leaked into the Deployment template")
	}
	// A repeat is a no-op.
	generation := cur.Generation
	if got, err := executor.rollbackDeployment(ctx, revisionRollback(ns, "api", h2, h3)); err != nil || !strings.Contains(got, "no change") {
		t.Fatalf("a repeat should change nothing, got %q, %v", got, err)
	}
	if again, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{}); again.Generation != generation {
		t.Fatal("a repeated rollback modified the Deployment")
	}
	// Now running revision 2. A decision about revision 3 is stale and refused.
	if _, err := executor.rollbackDeployment(ctx, revisionRollback(ns, "api", h1, h3)); err == nil {
		t.Fatal("a decision about a revision that is no longer running was applied")
	}
	// An older revision than the previous one: 2 -> 1 (env-only).
	waitFor(t, "revision 2 observed", func() (bool, string) {
		c, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
		return c.Status.ObservedGeneration >= c.Generation, "not observed"
	})
	if _, err := executor.rollbackDeployment(ctx, revisionRollback(ns, "api", h1, h2)); err != nil {
		t.Fatalf("env-only rollback: %v", err)
	}
	cur, _ = client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	if templateHash(t, cur) != h1 || cur.Spec.Template.Spec.Containers[0].Env[0].Value != "redis:6379" {
		t.Fatalf("revision 1 was not restored: %+v", cur.Spec.Template.Spec.Containers[0].Env)
	}
}

func TestRealClusterRollbackRefusesARevisionThatIsNoLongerRetained(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	// No pods: a stuck rollout never scales the old ReplicaSets down, and only
	// those with no replicas are pruned.
	d := realDeployment(ns, "api", "api:v1", "a", "64Mi", 0)
	limit := int32(1)
	d.Spec.RevisionHistoryLimit = &limit
	if _, err := client.AppsV1().Deployments(ns).Create(ctx, d, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first ReplicaSet", func() (bool, string) {
		l, _ := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		return len(l.Items) == 1, "none"
	})
	first, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	h1 := templateHash(t, first)
	for _, v := range []string{"b", "c", "d"} {
		v := v
		updateTemplate(t, client, ns, "api", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Env[0].Value = v })
	}
	last, _ := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	waitFor(t, "old ReplicaSets to be pruned", func() (bool, string) {
		l, _ := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		return len(l.Items) <= 2, fmt.Sprint(len(l.Items), " ReplicaSets")
	})
	_, err := (&KubernetesExecutor{Client: client}).rollbackDeployment(ctx, revisionRollback(ns, "api", h1, templateHash(t, last)))
	if _, ok := err.(*Refusal); !ok {
		t.Fatalf("a pruned revision should be refused, got %v", err)
	}
}

// Restart eligibility from a real pod's conditions. The pause image is on every
// kind node; a failing exec readiness probe keeps the pod running but unready.
func TestRealClusterRestartEligibility(t *testing.T) {
	client, ns := realCluster(t)
	ctx := context.Background()
	pause := func(name string, probe *corev1.Probe) {
		_, err := client.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "p", Image: "registry.k8s.io/pause:3.10", ReadinessProbe: probe}}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	pause("healthy", nil)
	pause("stuck", &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/nonexistent"}}}, PeriodSeconds: 1})
	waitFor(t, "healthy to be ready and stuck to be running but unready", func() (bool, string) {
		h, _ := client.CoreV1().Pods(ns).Get(ctx, "healthy", metav1.GetOptions{})
		s, _ := client.CoreV1().Pods(ns).Get(ctx, "stuck", metav1.GetOptions{})
		return podReady(h) && s.Status.Phase == corev1.PodRunning && !podReady(s), fmt.Sprint(h.Status.Phase, s.Status.Phase)
	})
	restart := func(pod string) *Action {
		return &Action{ActionType: ActionRestartPod, Namespace: ns, Target: pod, Approval: ApprovalApproved}
	}

	executor := &KubernetesExecutor{Client: client, RestartUnreadyFor: time.Hour, RestartQuiet: 10 * time.Minute}
	// Unready for seconds against an hour: eligible far in the future.
	if at, err := executor.NotBefore(ctx, restart("stuck")); err != nil || time.Until(at) < 50*time.Minute {
		t.Fatalf("a freshly unready pod must wait: %v, %v", at, err)
	}
	// With a short wait it is eligible once that has passed.
	executor.RestartUnreadyFor = 2 * time.Second
	waitFor(t, "the pod to have been unready for 2s", func() (bool, string) {
		at, err := executor.NotBefore(ctx, restart("stuck"))
		return err == nil && !at.After(time.Now()), fmt.Sprint(at, err)
	})
	// A pod that is ready is refused outright.
	if _, err := executor.NotBefore(ctx, restart("healthy")); err == nil {
		t.Fatal("a ready pod was eligible for a restart")
	} else if _, ok := err.(*Refusal); !ok {
		t.Fatalf("want a refusal, got %v", err)
	}
	// A pod that is gone is refused too.
	if _, err := executor.NotBefore(ctx, restart("nope")); err == nil {
		t.Fatal("a missing pod was eligible")
	}
}
