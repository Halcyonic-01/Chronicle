package heal

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Halcyonic-01/Chronicle/internal/revision"
)

// A world of numbered revisions of one Deployment, each a distinct pod template.
type revisions struct {
	d      *appsv1.Deployment
	client *fake.Clientset
	hash   map[string]string // label -> template hash
}

func containersOf(env, sidecar string) []corev1.Container {
	return []corev1.Container{
		{Name: "api", Image: "api:v1", Env: []corev1.EnvVar{{Name: "REDIS_URL", Value: env}}},
		{Name: "sidecar", Image: sidecar},
	}
}

// world builds the Deployment running `running` and a ReplicaSet for every
// revision given as label=containers.
func world(t *testing.T, running string, defs map[string][]corev1.Container) *revisions {
	t.Helper()
	w := &revisions{hash: map[string]string{}}
	w.d = deploymentWithRevision("9", defs[running]...)
	objs := []runtime.Object{w.d}
	n := 0
	for label, containers := range defs {
		n++
		rs := replicaSetFor(w.d, "api-"+label, fmt.Sprint(n), containers...)
		w.hash[label] = revision.TemplateHash(rs.Spec.Template)
		objs = append(objs, rs)
	}
	w.client = fake.NewSimpleClientset()
	for _, o := range objs {
		if err := w.client.Tracker().Add(o); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func (w *revisions) action(to, from, target string) *Action {
	payload := fmt.Sprintf(`{"to_template_hash":%q,"from_template_hash":%q`, w.hash[to], w.hash[from])
	if target != "" {
		payload += fmt.Sprintf(`,"target_template_hash":%q`, w.hash[target])
	}
	return &Action{ActionType: ActionRollbackDeployment, Namespace: "default", Target: "api", Approval: ApprovalApproved, Payload: []byte(payload + "}")}
}

func (w *revisions) current(t *testing.T) corev1.PodTemplateSpec {
	t.Helper()
	d, err := w.client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return d.Spec.Template
}

// The case the image check could not see: the release changed only an
// environment variable, so the first image is the same in both revisions.
func TestRollbackByRevisionSeesAReleaseThatChangedOnlyAnEnvVar(t *testing.T) {
	w := world(t, "bad", map[string][]corev1.Container{
		"good": containersOf("redis:6379", "side:1"),
		"bad":  containersOf("redis-typo:6379", "side:1"),
	})
	if got, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "good", "")); err != nil || !strings.Contains(got, "rolled back") {
		t.Fatalf("%q, %v", got, err)
	}
	if revision.TemplateHash(w.current(t)) != w.hash["good"] {
		t.Fatal("the deployment does not run the previous revision")
	}
	if _, ok := w.current(t).Labels[revision.ControllerLabel]; ok {
		t.Fatal("the controller's own label was written back into the Deployment")
	}
}

func TestRollbackByRevisionSeesAReleaseThatChangedOnlyASidecar(t *testing.T) {
	w := world(t, "bad", map[string][]corev1.Container{
		"good": containersOf("redis:6379", "side:1"),
		"bad":  containersOf("redis:6379", "side:2"),
	})
	if _, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "good", "")); err != nil {
		t.Fatal(err)
	}
	if got := w.current(t).Spec.Containers[1].Image; got != "side:1" {
		t.Fatalf("the sidecar is %s", got)
	}
}

// Another change, of any kind, since the decision: not ours to undo.
func TestRollbackByRevisionRefusesWhenSomethingNewerIsRunning(t *testing.T) {
	w := world(t, "newer", map[string][]corev1.Container{
		"good":  containersOf("redis:6379", "side:1"),
		"bad":   containersOf("redis-typo:6379", "side:1"),
		"newer": containersOf("redis-typo:6379", "side:3"), // a later, sidecar-only change
	})
	e := &KubernetesExecutor{Client: w.client}
	_, err := e.rollbackDeployment(context.Background(), w.action("bad", "good", ""))
	var r *Refusal
	if !asRefusal(err, &r) || !strings.Contains(err.Error(), "not the") {
		t.Fatalf("got %v", err)
	}
	if revision.TemplateHash(w.current(t)) != w.hash["newer"] {
		t.Fatal("the newer change was overwritten")
	}
}

func TestRollbackByRevisionRefusesWhenTheTargetIsNoLongerRetained(t *testing.T) {
	w := world(t, "bad", map[string][]corev1.Container{"bad": containersOf("redis-typo:6379", "side:1")})
	a := w.action("bad", "bad", "")
	a.Payload = []byte(fmt.Sprintf(`{"to_template_hash":%q,"from_template_hash":"0123456789abcdef"}`, w.hash["bad"]))
	_, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), a)
	if err == nil || !strings.Contains(err.Error(), "no longer retained") {
		t.Fatalf("got %v", err)
	}
}

func TestRollbackByRevisionRefusesAPausedOrUnobservedDeployment(t *testing.T) {
	defs := map[string][]corev1.Container{"good": containersOf("redis:6379", "side:1"), "bad": containersOf("redis-typo:6379", "side:1")}
	for name, mutate := range map[string]func(*appsv1.Deployment){
		"paused":     func(d *appsv1.Deployment) { d.Spec.Paused = true },
		"unobserved": func(d *appsv1.Deployment) { d.Generation, d.Status.ObservedGeneration = 5, 4 },
	} {
		w := world(t, "bad", defs)
		d, _ := w.client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
		mutate(d)
		if _, err := w.client.AppsV1().Deployments("default").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "good", "")); err == nil {
			t.Errorf("%s: rolled back", name)
		}
		if revision.TemplateHash(w.current(t)) != w.hash["bad"] {
			t.Errorf("%s: the deployment was changed", name)
		}
	}
}

func TestRollbackByRevisionIsANoOpWhenAlreadyOnTheTarget(t *testing.T) {
	w := world(t, "good", map[string][]corev1.Container{"good": containersOf("redis:6379", "side:1"), "bad": containersOf("x", "side:1")})
	got, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "good", ""))
	if err != nil || !strings.Contains(got, "no change") {
		t.Fatalf("%q, %v", got, err)
	}
}

// When the previous revision is not known to have been healthy, the engine
// names an older one; the executor then returns to exactly that.
func TestRollbackByRevisionCanReturnToANamedOlderRevision(t *testing.T) {
	w := world(t, "bad", map[string][]corev1.Container{
		"older":    containersOf("redis:6379", "side:1"),
		"previous": containersOf("redis:6379", "side:2"),
		"bad":      containersOf("redis-typo:6379", "side:2"),
	})
	if _, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "previous", "older")); err != nil {
		t.Fatal(err)
	}
	if revision.TemplateHash(w.current(t)) != w.hash["older"] {
		t.Fatal("did not return to the named older revision")
	}
}

func TestRollbackByRevisionRefusesATemplateThatNoLongerFitsTheSelector(t *testing.T) {
	w := world(t, "bad", map[string][]corev1.Container{"good": containersOf("redis:6379", "side:1"), "bad": containersOf("redis-typo:6379", "side:1")})
	rs, _ := w.client.AppsV1().ReplicaSets("default").Get(context.Background(), "api-good", metav1.GetOptions{})
	rs.Spec.Template.Labels = map[string]string{"app": "renamed", revision.ControllerLabel: "x"}
	if _, err := w.client.AppsV1().ReplicaSets("default").Update(context.Background(), rs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	w.hash["good"] = revision.TemplateHash(rs.Spec.Template)
	if _, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "good", "")); err == nil ||
		!strings.Contains(err.Error(), "selector") {
		t.Fatalf("got %v", err)
	}
}

// A workload a release controller owns is never rolled back by Chronicle.
func TestRollbackRefusesAWorkloadAReleaseControllerOwns(t *testing.T) {
	w := world(t, "bad", map[string][]corev1.Container{"good": containersOf("redis:6379", "side:1"), "bad": containersOf("redis-typo:6379", "side:1")})
	d, _ := w.client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
	d.OwnerReferences = []metav1.OwnerReference{{APIVersion: "argoproj.io/v1alpha1", Kind: "Rollout", Name: "api"}}
	if _, err := w.client.AppsV1().Deployments("default").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err := (&KubernetesExecutor{Client: w.client}).rollbackDeployment(context.Background(), w.action("bad", "good", ""))
	if err == nil || !strings.Contains(err.Error(), "rollout controller owns") {
		t.Fatalf("got %v", err)
	}
	if revision.TemplateHash(w.current(t)) != w.hash["bad"] {
		t.Fatal("a rollout-controller-owned workload was changed")
	}
}

func asRefusal(err error, target **Refusal) bool {
	r, ok := err.(*Refusal)
	if ok {
		*target = r
	}
	return ok
}
