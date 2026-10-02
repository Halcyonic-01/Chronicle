package collect

import (
	"fmt"
	"sort"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// Services, Nodes, ConfigMaps and HPAs are watched because each can break a
// dependency without touching any Deployment: a selector edit orphans every
// caller, a dead node takes its pods with it, a changed ConfigMap alters what a
// pod does on its next start, and an autoscaler limit explains a scale that
// nothing else does. Secrets are not watched: Chronicle is deliberately denied
// read access to them, and Ingresses sit downstream of the services they route
// to, so a change there has no symptom signal to be matched against.

// watchRelated registers handlers for the four kinds. Only updates and deletes
// are handled, so the informers' initial lists emit nothing.
func (k *K8sCollector) watchRelated(factory informers.SharedInformerFactory) {
	services := factory.Core().V1().Services().Informer()
	_, _ = services.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			o, okOld := old.(*corev1.Service)
			n, okNew := new.(*corev1.Service)
			if okOld && okNew {
				k.diffServices(o, n)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if s, ok := obj.(*corev1.Service); ok {
				k.emitResource("Service", s.Namespace, s.Name, "resource_deleted", "info", fmt.Sprintf("%s deleted", s.Name), time.Now().UTC(), map[string]any{"phase": "Deleted"})
			}
		},
	})

	nodes := factory.Core().V1().Nodes().Informer()
	_, _ = nodes.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			o, okOld := old.(*corev1.Node)
			n, okNew := new.(*corev1.Node)
			if okOld && okNew {
				k.diffNodes(o, n)
			}
		},
	})

	pods := factory.Core().V1().Pods().Lister()
	configMaps := factory.Core().V1().ConfigMaps().Informer()
	_, _ = configMaps.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			o, okOld := old.(*corev1.ConfigMap)
			n, okNew := new.(*corev1.ConfigMap)
			if !okOld || !okNew || ignoredConfigMap(n) {
				return
			}
			if refs, err := pods.List(labels.Everything()); err == nil && usedByAPod(n, refs) {
				k.diffConfigMaps(o, n)
			}
		},
	})

	hpas := factory.Autoscaling().V2().HorizontalPodAutoscalers().Informer()
	_, _ = hpas.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			o, okOld := old.(*autoscalingv2.HorizontalPodAutoscaler)
			n, okNew := new.(*autoscalingv2.HorizontalPodAutoscaler)
			if okOld && okNew {
				k.diffHPAs(o, n)
			}
		},
	})
}

func serviceFingerprint(s *corev1.Service) string {
	return fingerprint(struct {
		Selector map[string]string
		Ports    []corev1.ServicePort
		Type     corev1.ServiceType
	}{s.Spec.Selector, s.Spec.Ports, s.Spec.Type})
}

// serviceChanges names what changed on a Service, never values.
func serviceChanges(old, new *corev1.Service) []string {
	var changed []string
	if fingerprint(old.Spec.Selector) != fingerprint(new.Spec.Selector) {
		changed = append(changed, "selector")
	}
	if fingerprint(old.Spec.Ports) != fingerprint(new.Spec.Ports) {
		changed = append(changed, "ports")
	}
	if old.Spec.Type != new.Spec.Type {
		changed = append(changed, "type")
	}
	return changed
}

func (k *K8sCollector) diffServices(old, new *corev1.Service) {
	changed := serviceChanges(old, new)
	if len(changed) == 0 {
		return
	}
	k.emitResource("Service", new.Namespace, new.Name, "service_change", "info",
		fmt.Sprintf("%s service changed (%s)", new.Name, strings.Join(changed, ", ")), time.Now().UTC(),
		map[string]any{"changed": changed, "from_hash": serviceFingerprint(old), "to_hash": serviceFingerprint(new)})
}

func nodeCondition(n *corev1.Node, t corev1.NodeConditionType) (corev1.ConditionStatus, string, string) {
	for _, c := range n.Status.Conditions {
		if c.Type == t {
			return c.Status, c.Reason, c.Message
		}
	}
	return corev1.ConditionUnknown, "", ""
}

var nodePressure = []corev1.NodeConditionType{corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure, corev1.NodeNetworkUnavailable}

func (k *K8sCollector) diffNodes(old, new *corev1.Node) {
	was, _, _ := nodeCondition(old, corev1.NodeReady)
	now, reason, message := nodeCondition(new, corev1.NodeReady)
	switch {
	case was == corev1.ConditionTrue && now != corev1.ConditionTrue:
		k.emitResource("Node", "", new.Name, "node_not_ready", "critical", fmt.Sprintf("%s is NotReady", new.Name), time.Now().UTC(),
			map[string]any{"reason": reason, "message": message, "status": string(now)})
	case was != corev1.ConditionTrue && now == corev1.ConditionTrue:
		k.emitResource("Node", "", new.Name, "became_ready", "info", fmt.Sprintf("%s is Ready", new.Name), time.Now().UTC(), map[string]any{})
	}
	for _, t := range nodePressure {
		before, _, _ := nodeCondition(old, t)
		after, why, msg := nodeCondition(new, t)
		switch {
		case before != corev1.ConditionTrue && after == corev1.ConditionTrue:
			k.emitResource("Node", "", new.Name, "node_pressure", "warning", fmt.Sprintf("%s has %s", new.Name, t), time.Now().UTC(),
				map[string]any{"condition": string(t), "reason": why, "message": msg})
		case before == corev1.ConditionTrue && after != corev1.ConditionTrue:
			k.emitResource("Node", "", new.Name, "node_pressure_resolved", "info", fmt.Sprintf("%s no longer has %s", new.Name, t), time.Now().UTC(),
				map[string]any{"condition": string(t)})
		}
	}
}

// ignoredConfigMap skips ConfigMaps that change on their own: Chronicle's own
// checkpoint, leader-election records and the injected CA bundle.
func ignoredConfigMap(c *corev1.ConfigMap) bool {
	if c.Name == checkpointName || c.Name == "kube-root-ca.crt" || strings.HasSuffix(c.Name, "-lock") {
		return true
	}
	_, leader := c.Annotations["control-plane.alpha.kubernetes.io/leader"]
	return leader
}

// usedByAPod keeps ConfigMap events to the ones a workload actually reads. The
// rest are noise, and the dependency graph has no edge to explain them.
func usedByAPod(c *corev1.ConfigMap, pods []*corev1.Pod) bool {
	list := make([]corev1.Pod, 0, len(pods))
	for _, p := range pods {
		if p.Namespace == c.Namespace {
			list = append(list, *p)
		}
	}
	for _, e := range graph.BuildReferenceEdges(list) {
		if e.To.Kind == "ConfigMap" && e.To.Name == c.Name {
			return true
		}
	}
	return false
}

func configMapFingerprint(c *corev1.ConfigMap) string {
	return fingerprint(struct {
		Data       map[string]string
		BinaryData map[string][]byte
	}{c.Data, c.BinaryData})
}

// changedKeys lists the keys added, removed or altered. Values are never
// reported: a ConfigMap routinely holds connection strings.
func changedKeys(old, new *corev1.ConfigMap) []string {
	seen := map[string]bool{}
	var keys []string
	note := func(k string) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k, v := range new.Data {
		if ov, ok := old.Data[k]; !ok || ov != v {
			note(k)
		}
	}
	for k := range old.Data {
		if _, ok := new.Data[k]; !ok {
			note(k)
		}
	}
	for k, v := range new.BinaryData {
		if ov, ok := old.BinaryData[k]; !ok || string(ov) != string(v) {
			note(k)
		}
	}
	for k := range old.BinaryData {
		if _, ok := new.BinaryData[k]; !ok {
			note(k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (k *K8sCollector) diffConfigMaps(old, new *corev1.ConfigMap) {
	keys := changedKeys(old, new)
	if len(keys) == 0 {
		return
	}
	k.emitResource("ConfigMap", new.Namespace, new.Name, "config_change", "info",
		fmt.Sprintf("%s changed (keys: %s)", new.Name, strings.Join(keys, ", ")), time.Now().UTC(),
		map[string]any{"changed": keys, "from_hash": configMapFingerprint(old), "to_hash": configMapFingerprint(new)})
}

func hpaFingerprint(h *autoscalingv2.HorizontalPodAutoscaler) string {
	return fingerprint(struct {
		Min      *int32
		Max      int32
		Metrics  []autoscalingv2.MetricSpec
		Behavior *autoscalingv2.HorizontalPodAutoscalerBehavior
	}{h.Spec.MinReplicas, h.Spec.MaxReplicas, h.Spec.Metrics, h.Spec.Behavior})
}

func hpaChanges(old, new *autoscalingv2.HorizontalPodAutoscaler) []string {
	var changed []string
	if fingerprint(old.Spec.MinReplicas) != fingerprint(new.Spec.MinReplicas) {
		changed = append(changed, "minReplicas")
	}
	if old.Spec.MaxReplicas != new.Spec.MaxReplicas {
		changed = append(changed, "maxReplicas")
	}
	if fingerprint(old.Spec.Metrics) != fingerprint(new.Spec.Metrics) {
		changed = append(changed, "metrics")
	}
	if fingerprint(old.Spec.Behavior) != fingerprint(new.Spec.Behavior) {
		changed = append(changed, "behavior")
	}
	return changed
}

// diffHPAs records an autoscaler change on the workload it scales, so it sits
// next to the scale events it explains.
func (k *K8sCollector) diffHPAs(old, new *autoscalingv2.HorizontalPodAutoscaler) {
	changed := hpaChanges(old, new)
	target := new.Spec.ScaleTargetRef
	if len(changed) == 0 || target.Kind != "Deployment" {
		return
	}
	k.Emit(event.Event{
		Source: "k8s", Namespace: new.Namespace, EntityKind: "Deployment", EntityName: target.Name,
		Type: "hpa_change", Severity: "info",
		Title:   fmt.Sprintf("%s autoscaler changed (%s)", target.Name, strings.Join(changed, ", ")),
		Payload: mustJSON(map[string]any{"changed": changed, "autoscaler": new.Name, "from_hash": hpaFingerprint(old), "to_hash": hpaFingerprint(new)}),
	})
}
