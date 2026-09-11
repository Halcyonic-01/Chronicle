package graph

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildInfrastructureGraph(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-abc", Namespace: "prod", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-7d9f8"}}}, Spec: corev1.PodSpec{
		NodeName: "kind-worker",
		Volumes: []corev1.Volume{
			{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "api-config"}}}},
			{Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "api-secret"}}},
			{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "api-data"}}},
		},
		Containers: []corev1.Container{{Name: "api", EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "api-config"}}}}, Env: []corev1.EnvVar{{Name: "REDIS_URL", Value: "redis.prod.svc.cluster.local:6379"}}}},
	}}
	svc := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "prod"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}}}
	pod.Labels = map[string]string{"app": "api"}
	ingress := networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "public", Namespace: "prod"}, Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: "api"}}}}}}}}}}

	edges := append(BuildServiceEdges([]corev1.Service{svc}, []corev1.Pod{pod}), BuildOwnerEdges([]corev1.Pod{pod})...)
	edges = append(edges, BuildReferenceEdges([]corev1.Pod{pod})...)
	edges = append(edges, BuildIngressEdges([]networkingv1.Ingress{ingress})...)
	g := New()
	g.SetEdges(edges)

	want := []string{
		"prod/Service/api->prod/Pod/api-abc",
		"prod/Deployment/api->prod/Pod/api-abc",
		"prod/Pod/api-abc->prod/ConfigMap/api-config",
		"prod/Pod/api-abc->prod/Secret/api-secret",
		"prod/Pod/api-abc->prod/PersistentVolumeClaim/api-data",
		"prod/Pod/api-abc->/Node/kind-worker",
		"prod/Ingress/public->prod/Service/api",
	}
	got := map[string]bool{}
	for _, edge := range edges {
		got[edge.From.Key()+"->"+edge.To.Key()] = true
	}
	for _, key := range want {
		if !got[key] {
			t.Errorf("missing graph edge %s", key)
		}
	}
	if upstream := g.Upstream("prod/Pod/api-abc", 2); upstream["prod/Deployment/api"] != 1 {
		t.Fatalf("expected deployment upstream at one hop, got %#v", upstream)
	}
}
