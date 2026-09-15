package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type K8sCollector struct {
	BaseCollector
	client kubernetes.Interface
}

func NewK8sCollector(client kubernetes.Interface, out chan<- event.Event) *K8sCollector {
	return &K8sCollector{
		BaseCollector: BaseCollector{Out: out},
		client:        client,
	}
}

func (k *K8sCollector) Run(ctx context.Context) error {
	// Resync every 30s: a safety net in case we miss a watch event.
	factory := informers.NewSharedInformerFactory(k.client, 30*time.Second)
	var informersReady atomic.Bool

	// --- Source 1: Kubernetes Events (the "why did this pod die" source) ---
	evInformer := factory.Core().V1().Events().Informer()
	_, _ = evInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if !informersReady.Load() {
				return
			}
			ev := obj.(*corev1.Event)
			k.fromK8sEvent(ev)
		},
		UpdateFunc: func(old, new interface{}) {
			if !informersReady.Load() {
				return
			}
			previous, okPrevious := old.(*corev1.Event)
			current, okCurrent := new.(*corev1.Event)
			if okPrevious && okCurrent && current.Count > previous.Count {
				k.fromK8sEvent(current)
			}
		},
	})

	// --- Source 2: Pod lifecycle (restarts, OOM kills, crash loops) ---
	podInformer := factory.Core().V1().Pods().Informer()
	_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if !informersReady.Load() {
				return
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				k.emitResourceEvent("Pod", pod.Namespace, pod.Name, "resource_created", "info", fmt.Sprintf("%s created", pod.Name), podLifecyclePayload(pod))
			}
		},
		UpdateFunc: func(old, new interface{}) {
			oldPod, okOld := old.(*corev1.Pod)
			newPod, okNew := new.(*corev1.Pod)
			if okOld && okNew {
				k.diffPods(oldPod, newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if !informersReady.Load() {
				return
			}
			if pod, ok := deletedPod(obj); ok {
				k.emitResourceEvent("Pod", pod.Namespace, pod.Name, "resource_deleted", "info", fmt.Sprintf("%s deleted", pod.Name), podLifecyclePayload(pod))
			}
		},
	})

	// --- Source 3: Deployments (image changes, replica scaling) ---
	deployInformer := factory.Apps().V1().Deployments().Informer()
	_, _ = deployInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if !informersReady.Load() {
				return
			}
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				k.emitResourceEvent("Deployment", deployment.Namespace, deployment.Name, "resource_created", "info", fmt.Sprintf("%s created", deployment.Name), deploymentLifecyclePayload(deployment))
			}
		},
		UpdateFunc: func(old, new interface{}) {
			oldDeployment, okOld := old.(*appsv1.Deployment)
			newDeployment, okNew := new.(*appsv1.Deployment)
			if okOld && okNew {
				k.diffDeployments(oldDeployment, newDeployment)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if !informersReady.Load() {
				return
			}
			if deployment, ok := deletedDeployment(obj); ok {
				k.emitResourceEvent("Deployment", deployment.Namespace, deployment.Name, "resource_deleted", "info", fmt.Sprintf("%s deleted", deployment.Name), deploymentLifecyclePayload(deployment))
			}
		},
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done()) {
		return ctx.Err()
	}
	informersReady.Store(true)
	<-ctx.Done()
	return ctx.Err()
}

func (k *K8sCollector) fromK8sEvent(ev *corev1.Event) {
	if ev.Type == "Normal" {
		return // we mostly care about warnings
	}

	// Create event representation
	e := event.Event{
		Source:     "k8s",
		OccurredAt: k8sEventTime(ev),
		Namespace:  ev.InvolvedObject.Namespace,
		EntityKind: ev.InvolvedObject.Kind,
		EntityName: ev.InvolvedObject.Name,
		Type:       "k8s_event",
		Severity:   "warning",
		Title:      fmt.Sprintf("%s: %s", ev.Reason, ev.Message),
		Payload: mustJSON(map[string]any{
			"reason":               ev.Reason,
			"message":              ev.Message,
			"count":                ev.Count,
			"action":               ev.Action,
			"event_type":           ev.Type,
			"reporting_controller": ev.ReportingController,
			"source_component":     ev.Source.Component,
			"involved_object_kind": ev.InvolvedObject.Kind,
			"involved_object_name": ev.InvolvedObject.Name,
			"involved_object_uid":  string(ev.InvolvedObject.UID),
			"first_timestamp":      ev.FirstTimestamp,
			"last_timestamp":       ev.LastTimestamp,
			"event_time":           ev.EventTime,
		}),
	}
	k.Emit(e)
}

func (k *K8sCollector) diffPods(old, new *corev1.Pod) {
	// Kubernetes does not guarantee a stable order for container statuses
	// across updates, so restarts must be attributed by container name.
	previous := make(map[string]corev1.ContainerStatus, len(old.Status.ContainerStatuses))
	for _, status := range old.Status.ContainerStatuses {
		previous[status.Name] = status
	}

	for _, cs := range new.Status.ContainerStatuses {
		oldCS, known := previous[cs.Name]
		if !known {
			continue
		}

		// --- A restart happened ---
		if cs.RestartCount > oldCS.RestartCount {
			reason, exitCode := "Unknown", int32(0)
			if t := cs.LastTerminationState.Terminated; t != nil {
				reason, exitCode = t.Reason, t.ExitCode
			}

			evType := "container_restart"
			severity := "warning"
			if reason == "OOMKilled" {
				evType, severity = "oom_kill", "critical"
			}

			k.Emit(event.Event{
				Source:     "k8s",
				Namespace:  new.Namespace,
				EntityKind: "Pod",
				EntityName: new.Name,
				Type:       evType,
				Severity:   severity,
				Title:      fmt.Sprintf("%s restarted (%s, exit %d)", cs.Name, reason, exitCode),
				Payload: mustJSON(map[string]any{
					"container": cs.Name,
					"reason":    reason,
					"exit_code": exitCode,
					"owner":     ownerRef(new), // stable deployment name!
				}),
			})
		}
	}

	// --- Readiness flipped ---
	oldReady := isPodReady(old)
	newReady := isPodReady(new)
	if old.Status.Phase != new.Status.Phase || oldReady != newReady {
		k.emitResourceEvent("Pod", new.Namespace, new.Name, "resource_status", podStatusSeverity(new),
			fmt.Sprintf("%s status is %s", new.Name, podReplayPhase(new)), map[string]any{
				"phase":       podReplayPhase(new),
				"ready_count": podReadyCount(new),
				"reason":      new.Status.Reason,
				"message":     new.Status.Message,
			})
	}

	if oldReady && !newReady {
		k.Emit(event.Event{
			Source:     "k8s",
			EntityKind: "Pod",
			EntityName: new.Name,
			Namespace:  new.Namespace,
			Type:       "became_unready",
			Severity:   "warning",
			Title:      fmt.Sprintf("%s stopped serving traffic", new.Name),
			Payload: mustJSON(map[string]any{
				"owner":       ownerRef(new),
				"ready_count": podReadyCount(new),
			}),
		})
	}
	if !oldReady && newReady {
		k.Emit(event.Event{Source: "k8s", EntityKind: "Pod", EntityName: new.Name, Namespace: new.Namespace, Type: "became_ready", Severity: "info", Title: fmt.Sprintf("%s started serving traffic", new.Name), Payload: mustJSON(map[string]any{"owner": ownerRef(new), "ready_count": podReadyCount(new)})})
	}
}

// maxSpecChangeAge bounds how old a field-manager timestamp may be and still
// describe the change being emitted right now.
const maxSpecChangeAge = 2 * time.Minute

// specChangeTime reports when a Deployment's spec was last mutated and by which
// client. The API server records this per field manager, so it is the moment
// the change was made rather than the moment our informer noticed it.
//
// Two entries must be ignored. Status writes are the controller reporting
// progress, not someone changing the deployment. And a timestamp far older
// than now cannot describe the change we are emitting: "kubectl scale" goes
// through the /scale subresource, which leaves no managed-field entry of its
// own, so the newest remaining entry is whatever last applied the manifest —
// hours earlier. Reporting that would date every scale to the original deploy.
// In both cases a zero time is returned and the caller falls back to ingestion
// time, which is late but true.
func specChangeTime(d *appsv1.Deployment) (time.Time, string) {
	var at time.Time
	var manager string
	for _, field := range d.ManagedFields {
		if field.Time == nil {
			continue
		}
		if field.Subresource != "" && field.Subresource != "scale" {
			continue
		}
		if field.Time.Time.After(at) {
			at, manager = field.Time.Time.UTC(), field.Manager
		}
	}
	if at.IsZero() || time.Since(at) > maxSpecChangeAge {
		return time.Time{}, ""
	}
	return at, manager
}

func (k *K8sCollector) diffDeployments(old, new *appsv1.Deployment) {
	changedAt, changedBy := specChangeTime(new)

	if old.Status.ReadyReplicas != new.Status.ReadyReplicas || old.Status.AvailableReplicas != new.Status.AvailableReplicas {
		k.emitResourceEvent("Deployment", new.Namespace, new.Name, "resource_status", deploymentStatusSeverity(new),
			fmt.Sprintf("%s status is %d/%d replicas ready", new.Name, new.Status.ReadyReplicas, deploymentReplicas(new)), map[string]any{
				"phase":       deploymentReplayPhase(new),
				"ready_count": new.Status.ReadyReplicas,
				"reason":      deploymentStatusReason(new),
				"message":     fmt.Sprintf("%d/%d replicas ready", new.Status.ReadyReplicas, deploymentReplicas(new)),
			})
	}
	if len(old.Spec.Template.Spec.Containers) == 0 || len(new.Spec.Template.Spec.Containers) == 0 {
		return
	}

	oldImg := old.Spec.Template.Spec.Containers[0].Image
	newImg := new.Spec.Template.Spec.Containers[0].Image

	if oldImg != newImg {
		k.Emit(event.Event{
			Source:     "k8s",
			OccurredAt: changedAt,
			EntityKind: "Deployment",
			EntityName: new.Name,
			Namespace:  new.Namespace,
			Type:       "deploy",
			Severity:   "info",
			Title:      fmt.Sprintf("%s deployed: %s -> %s", new.Name, shortTag(oldImg), shortTag(newImg)),
			Payload: mustJSON(map[string]any{
				"old_image":  oldImg,
				"new_image":  newImg,
				"commit_sha": extractSHA(newImg),
				"changed_by": changedBy,
			}),
		})
	}

	oldRes := old.Spec.Template.Spec.Containers[0].Resources
	newRes := new.Spec.Template.Spec.Containers[0].Resources
	if !reflect.DeepEqual(oldRes, newRes) {
		k.Emit(event.Event{
			Source:     "k8s",
			EntityKind: "Deployment",
			EntityName: new.Name,
			Namespace:  new.Namespace,
			OccurredAt: changedAt,
			Type:       "resource_change",
			Severity:   "info",
			Title:      fmt.Sprintf("%s resource limits changed", new.Name),
			Payload:    mustJSON(map[string]any{"new_mem_limit": memoryLimit(new.Spec.Template.Spec.Containers[0]), "old_mem_limit": memoryLimit(old.Spec.Template.Spec.Containers[0]), "changed_by": changedBy}),
		})
	}

	if (old.Spec.Replicas != nil && new.Spec.Replicas != nil) && *old.Spec.Replicas != *new.Spec.Replicas {
		k.Emit(event.Event{
			Source:     "k8s",
			EntityKind: "Deployment",
			EntityName: new.Name,
			Namespace:  new.Namespace,
			OccurredAt: changedAt,
			Type:       "scale",
			Severity:   "info",
			Title:      fmt.Sprintf("%s scaled from %d to %d", new.Name, *old.Spec.Replicas, *new.Spec.Replicas),
			Payload:    mustJSON(map[string]any{"old_replicas": *old.Spec.Replicas, "new_replicas": *new.Spec.Replicas, "changed_by": changedBy}),
		})
	}
}

func (k *K8sCollector) emitResourceEvent(kind, namespace, name, eventType, severity, title string, payload map[string]any) {
	if payload == nil {
		payload = map[string]any{}
	}
	k.Emit(event.Event{
		Source:     "k8s",
		Namespace:  namespace,
		EntityKind: kind,
		EntityName: name,
		Type:       eventType,
		Severity:   severity,
		Title:      title,
		Payload:    mustJSON(payload),
	})
}

func deletedPod(obj interface{}) (*corev1.Pod, bool) {
	switch value := obj.(type) {
	case *corev1.Pod:
		return value, true
	case cache.DeletedFinalStateUnknown:
		pod, ok := value.Obj.(*corev1.Pod)
		return pod, ok
	case *cache.DeletedFinalStateUnknown:
		pod, ok := value.Obj.(*corev1.Pod)
		return pod, ok
	default:
		return nil, false
	}
}

func deletedDeployment(obj interface{}) (*appsv1.Deployment, bool) {
	switch value := obj.(type) {
	case *appsv1.Deployment:
		return value, true
	case cache.DeletedFinalStateUnknown:
		deployment, ok := value.Obj.(*appsv1.Deployment)
		return deployment, ok
	case *cache.DeletedFinalStateUnknown:
		deployment, ok := value.Obj.(*appsv1.Deployment)
		return deployment, ok
	default:
		return nil, false
	}
}

func podReadyCount(p *corev1.Pod) int32 {
	var ready int32
	for _, status := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
		if status.Ready {
			ready++
		}
	}
	return ready
}

func podReplayPhase(p *corev1.Pod) string {
	switch {
	case p.Status.Phase == corev1.PodFailed:
		return "Failed"
	case p.Status.Phase == corev1.PodSucceeded:
		return "Completed"
	case !isPodReady(p):
		return "Pending"
	default:
		return "Running"
	}
}

func podStatusSeverity(p *corev1.Pod) string {
	if p.Status.Phase == corev1.PodFailed {
		return "critical"
	}
	if !isPodReady(p) {
		return "warning"
	}
	return "info"
}

func deploymentReplicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}

func deploymentReplayPhase(d *appsv1.Deployment) string {
	if d.Status.ReadyReplicas >= deploymentReplicas(d) {
		return "Running"
	}
	return "Pending"
}

func deploymentStatusSeverity(d *appsv1.Deployment) string {
	if d.Status.ReadyReplicas < deploymentReplicas(d) {
		return "warning"
	}
	return "info"
}

func deploymentStatusReason(d *appsv1.Deployment) string {
	if d.Status.ReadyReplicas < deploymentReplicas(d) {
		return "NotReady"
	}
	return ""
}

func podLifecyclePayload(pod *corev1.Pod) map[string]any {
	return map[string]any{
		"phase":       podReplayPhase(pod),
		"ready_count": podReadyCount(pod),
		"restarts":    podRestartCount(pod),
		"owner":       ownerRef(pod),
		"reason":      pod.Status.Reason,
		"message":     pod.Status.Message,
	}
}

func deploymentLifecyclePayload(deployment *appsv1.Deployment) map[string]any {
	return map[string]any{
		"phase":       deploymentReplayPhase(deployment),
		"ready_count": deployment.Status.ReadyReplicas,
		"replicas":    deploymentReplicas(deployment),
		"reason":      deploymentStatusReason(deployment),
		"message":     fmt.Sprintf("%d/%d replicas ready", deployment.Status.ReadyReplicas, deploymentReplicas(deployment)),
	}
}

func podRestartCount(pod *corev1.Pod) int32 {
	var restarts int32
	for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		restarts += status.RestartCount
	}
	return restarts
}

func memoryLimit(container corev1.Container) int64 {
	if value, ok := container.Resources.Limits[corev1.ResourceMemory]; ok {
		return value.Value()
	}
	return 0
}

// Helpers

func mustJSON(m map[string]any) json.RawMessage {
	b, _ := json.Marshal(m)
	return b
}

func ownerRef(p *corev1.Pod) string {
	if len(p.OwnerReferences) > 0 {
		name := p.OwnerReferences[0].Name
		// trim replicaset hash if present
		if parts := strings.Split(name, "-"); len(parts) > 1 {
			return strings.Join(parts[:len(parts)-1], "-")
		}
		return name
	}
	return p.Name
}

func isPodReady(p *corev1.Pod) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func shortTag(img string) string {
	parts := strings.Split(img, ":")
	if len(parts) > 1 {
		tag := parts[len(parts)-1]
		if len(tag) > 7 {
			return tag[:7]
		}
		return tag
	}
	return img
}

// extractSHA returns the commit SHA an image tag encodes, if it plausibly
// encodes one. CorrelationKey groups every event sharing a SHA prefix, so
// returning an ordinary tag like "v1.2.3-alpine12" would silently correlate
// unrelated deployments.
func extractSHA(img string) string {
	parts := strings.Split(img, ":")
	if len(parts) < 2 {
		return ""
	}
	tag := parts[len(parts)-1]
	if digest := strings.TrimPrefix(tag, "sha256-"); digest != tag {
		tag = digest
	}
	if !isHex(tag) || len(tag) < 12 {
		return ""
	}
	return tag
}

func isHex(value string) bool {
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return value != ""
}

// k8sEventTime recovers when this occurrence of a Kubernetes Event happened.
//
// Field order matters and is not obvious: a repeating Event keeps its original
// EventTime and FirstTimestamp for the whole series while Kubernetes re-emits
// it with an incremented count. Reading those first dates an occurrence to when
// the series began — a warning still firing now looks fifteen minutes old, and
// its causal window then excludes the change that caused it.
func k8sEventTime(ev *corev1.Event) time.Time {
	if ev.Series != nil && !ev.Series.LastObservedTime.IsZero() {
		return ev.Series.LastObservedTime.Time.UTC()
	}
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time.UTC()
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time.UTC()
	}
	if !ev.FirstTimestamp.IsZero() {
		return ev.FirstTimestamp.Time.UTC()
	}
	return time.Time{}
}
