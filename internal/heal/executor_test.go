package heal

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func rolledOut(name string, gen, observed int64, desired, updated, total, available int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: gen},
		Spec:       appsv1.DeploymentSpec{Replicas: &desired},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: observed, UpdatedReplicas: updated,
			Replicas: total, AvailableReplicas: available,
		},
	}
}

// status.conditions alone is not dependable, so verification checks the same
// fields kubectl does. Each case below is a rollout that has not finished.
func TestAwaitRolloutRejectsAnIncompleteRollout(t *testing.T) {
	cases := map[string]*appsv1.Deployment{
		"controller has not observed the change": rolledOut("api", 3, 2, 2, 2, 2, 2),
		"not every replica updated":              rolledOut("api", 2, 2, 2, 1, 2, 2),
		"old replicas still running":             rolledOut("api", 2, 2, 2, 2, 3, 2),
		"updated but not yet available":          rolledOut("api", 2, 2, 2, 2, 2, 1),
	}
	for name, d := range cases {
		e := &KubernetesExecutor{Client: fake.NewSimpleClientset(d)}
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // the deployment never progresses, so do not wait on it
		if _, err := e.awaitRollout(ctx, "default", "api"); err == nil {
			t.Errorf("%s: verification should not report success", name)
		}
	}
}

func TestAwaitRolloutAcceptsACompletedRollout(t *testing.T) {
	e := &KubernetesExecutor{Client: fake.NewSimpleClientset(rolledOut("api", 2, 2, 2, 2, 2, 2))}
	got, err := e.awaitRollout(context.Background(), "default", "api")
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Fatal("a completed rollout should describe what it verified")
	}
}
