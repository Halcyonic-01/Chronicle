package revision

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func template() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "api", Image: "api:v1", Env: []corev1.EnvVar{{Name: "REDIS_URL", Value: "redis:6379"}}},
			{Name: "sidecar", Image: "side:v1"},
		}},
	}
}

// What the first container's image would miss: every one of these changes what
// the pods run, and each must change the hash.
func TestAnyChangeToWhatThePodsRunChangesTheHash(t *testing.T) {
	base := TemplateHash(template())
	changes := map[string]func(*corev1.PodTemplateSpec){
		"env var":          func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Env[0].Value = "redis-typo:6379" },
		"second container": func(p *corev1.PodTemplateSpec) { p.Spec.Containers[1].Image = "side:v2" },
		"first image":      func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Image = "api:v2" },
		"resource limit": func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Mi")}
		},
		"command":           func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Command = []string{"/other"} },
		"service account":   func(p *corev1.PodTemplateSpec) { p.Spec.ServiceAccountName = "other" },
		"template label":    func(p *corev1.PodTemplateSpec) { p.Labels["tier"] = "x" },
		"container removed": func(p *corev1.PodTemplateSpec) { p.Spec.Containers = p.Spec.Containers[:1] },
	}
	for name, change := range changes {
		p := template()
		change(&p)
		if TemplateHash(p) == base {
			t.Errorf("%s did not change the hash", name)
		}
	}
}

// A ReplicaSet's template is the Deployment's plus the controller's own label;
// they must identify the same revision.
func TestAReplicaSetAndItsDeploymentHashTheSame(t *testing.T) {
	deployment := template()
	rs := *deployment.DeepCopy()
	rs.Labels[ControllerLabel] = "6d79c4d8db"
	if TemplateHash(deployment) != TemplateHash(rs) {
		t.Fatal("the controller's label changed the identity of the revision")
	}
	if _, ok := Template(rs).Labels[ControllerLabel]; ok {
		t.Fatal("Template left the controller's label in")
	}
	if rs.Labels[ControllerLabel] == "" {
		t.Fatal("Template must not modify its argument")
	}
}

func TestTheHashIsStableAndNeverEmpty(t *testing.T) {
	a, b := TemplateHash(template()), TemplateHash(template())
	if a == "" || a != b || len(a) != 16 {
		t.Fatalf("got %q and %q", a, b)
	}
}
