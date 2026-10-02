package collect

import (
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func service(selector map[string]string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports:    []corev1.ServicePort{{Port: port, TargetPort: intstr.FromInt(int(port))}},
		},
	}
}

func extraOut(f func(k *K8sCollector)) []string {
	k, out := collectorWithOut()
	f(k)
	var got []string
	for _, e := range drain(out) {
		got = append(got, e.Type+":"+e.EntityKind+"/"+e.EntityName)
	}
	return got
}

// A selector edit orphans every caller of the service without touching any
// Deployment, so nothing recorded it.
func TestAServiceSelectorChangeIsRecorded(t *testing.T) {
	k, out := collectorWithOut()
	k.diffServices(service(map[string]string{"app": "redis"}, 6379), service(map[string]string{"app": "redis-x"}, 6379))
	events := drain(out)
	if len(events) != 1 || events[0].Type != "service_change" || events[0].EntityKind != "Service" {
		t.Fatalf("expected one service_change, got %+v", events)
	}
	if gjsonString(events[0], "changed.0") != "selector" || gjsonString(events[0], "from_hash") == gjsonString(events[0], "to_hash") {
		t.Fatalf("the change should be named and fingerprinted: %s", events[0].Payload)
	}
	if contains(events[0].Payload, "redis-x") {
		t.Fatalf("selector values must not be recorded: %s", events[0].Payload)
	}
}

func TestAnUnchangedServiceEmitsNothing(t *testing.T) {
	got := extraOut(func(k *K8sCollector) {
		k.diffServices(service(map[string]string{"app": "redis"}, 6379), service(map[string]string{"app": "redis"}, 6379))
	})
	if len(got) != 0 {
		t.Fatalf("a resync must not emit: %v", got)
	}
}

func node(ready corev1.ConditionStatus, pressure ...corev1.NodeConditionType) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}}
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready, Reason: "KubeletNotReady"}}
	for _, p := range pressure {
		n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: p, Status: corev1.ConditionTrue})
	}
	return n
}

func TestANodeGoingNotReadyIsRecordedOnceAndItsRecoveryToo(t *testing.T) {
	down := extraOut(func(k *K8sCollector) { k.diffNodes(node(corev1.ConditionTrue), node(corev1.ConditionFalse)) })
	if len(down) != 1 || down[0] != "node_not_ready:Node/node-2" {
		t.Fatalf("expected node_not_ready, got %v", down)
	}
	// Ready -> Unknown is how a vanished kubelet looks.
	if got := extraOut(func(k *K8sCollector) { k.diffNodes(node(corev1.ConditionTrue), node(corev1.ConditionUnknown)) }); len(got) != 1 {
		t.Fatalf("an unreachable node counts as not ready: %v", got)
	}
	if got := extraOut(func(k *K8sCollector) { k.diffNodes(node(corev1.ConditionFalse), node(corev1.ConditionFalse)) }); len(got) != 0 {
		t.Fatalf("staying down must not repeat the event: %v", got)
	}
	up := extraOut(func(k *K8sCollector) { k.diffNodes(node(corev1.ConditionFalse), node(corev1.ConditionTrue)) })
	if len(up) != 1 || up[0] != "became_ready:Node/node-2" {
		t.Fatalf("recovery should be recorded so episodes can end: %v", up)
	}
}

func TestNodePressureIsRecordedWhenItStartsAndEnds(t *testing.T) {
	on := extraOut(func(k *K8sCollector) {
		k.diffNodes(node(corev1.ConditionTrue), node(corev1.ConditionTrue, corev1.NodeMemoryPressure))
	})
	if len(on) != 1 || on[0] != "node_pressure:Node/node-2" {
		t.Fatalf("expected node_pressure, got %v", on)
	}
	off := extraOut(func(k *K8sCollector) {
		k.diffNodes(node(corev1.ConditionTrue, corev1.NodeMemoryPressure), node(corev1.ConditionTrue))
	})
	if len(off) != 1 || off[0] != "node_pressure_resolved:Node/node-2" {
		t.Fatalf("expected the resolution, got %v", off)
	}
}

func configMap(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Data: data}
}

func podUsing(configMapName string) *corev1.Pod {
	p := livePod("api-1", true, 0)
	p.Spec.Volumes = []corev1.Volume{{Name: "c", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configMapName}}}}}
	return p
}

func TestAConfigMapChangeNamesKeysNeverValues(t *testing.T) {
	k, out := collectorWithOut()
	k.diffConfigMaps(configMap("api-config", map[string]string{"REDIS_ADDR": "redis:6379", "A": "1"}), configMap("api-config", map[string]string{"REDIS_ADDR": "redis-typo:6379", "A": "1"}))
	events := drain(out)
	if len(events) != 1 || events[0].Type != "config_change" || events[0].EntityKind != "ConfigMap" {
		t.Fatalf("expected one config_change on the ConfigMap, got %+v", events)
	}
	if gjsonString(events[0], "changed.#") != "1" || gjsonString(events[0], "changed.0") != "REDIS_ADDR" {
		t.Fatalf("only the altered key should be named: %s", events[0].Payload)
	}
	if contains(events[0].Payload, "typo") || contains([]byte(events[0].Title), "typo") {
		t.Fatalf("values must never be recorded: %s %s", events[0].Title, events[0].Payload)
	}
}

// Most ConfigMaps in a cluster are nobody's configuration; recording them would
// bury the ones that matter.
func TestOnlyConfigMapsAPodReadsAreWatched(t *testing.T) {
	if !usedByAPod(configMap("api-config", nil), []*corev1.Pod{podUsing("api-config")}) {
		t.Error("a ConfigMap a pod mounts should be watched")
	}
	if usedByAPod(configMap("orphan", nil), []*corev1.Pod{podUsing("api-config")}) {
		t.Error("a ConfigMap nothing uses should be ignored")
	}
	for _, name := range []string{checkpointName, "kube-root-ca.crt", "kube-scheduler-lock"} {
		if !ignoredConfigMap(configMap(name, nil)) {
			t.Errorf("%s changes on its own and must be ignored", name)
		}
	}
}

func hpa(max int32) *autoscalingv2.HorizontalPodAutoscaler {
	min := int32(1)
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "worker"},
			MinReplicas:    &min, MaxReplicas: max,
		},
	}
}

func TestAnAutoscalerLimitChangeIsRecordedOnTheWorkloadItScales(t *testing.T) {
	k, out := collectorWithOut()
	k.diffHPAs(hpa(5), hpa(1))
	events := drain(out)
	if len(events) != 1 || events[0].Type != "hpa_change" || events[0].EntityKind != "Deployment" || events[0].EntityName != "worker" {
		t.Fatalf("expected hpa_change on Deployment/worker, got %+v", events)
	}
	if gjsonString(events[0], "changed.0") != "maxReplicas" {
		t.Fatalf("the limit should be named: %s", events[0].Payload)
	}
	if got := extraOut(func(k *K8sCollector) { k.diffHPAs(hpa(5), hpa(5)) }); len(got) != 0 {
		t.Fatalf("an unchanged autoscaler must not emit: %v", got)
	}
}
