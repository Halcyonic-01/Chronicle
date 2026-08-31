package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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

	// --- Source 1: Kubernetes Events (the "why did this pod die" source) ---
	evInformer := factory.Core().V1().Events().Informer()
	_, _ = evInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ev := obj.(*corev1.Event)
			k.fromK8sEvent(ev)
		},
	})

	// --- Source 2: Pod lifecycle (restarts, OOM kills, crash loops) ---
	podInformer := factory.Core().V1().Pods().Informer()
	_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			k.diffPods(old.(*corev1.Pod), new.(*corev1.Pod))
		},
	})

	// --- Source 3: Deployments (image changes, replica scaling) ---
	deployInformer := factory.Apps().V1().Deployments().Informer()
	_, _ = deployInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			k.diffDeployments(old.(*appsv1.Deployment), new.(*appsv1.Deployment))
		},
	})

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
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
		Namespace:  ev.InvolvedObject.Namespace,
		EntityKind: ev.InvolvedObject.Kind,
		EntityName: ev.InvolvedObject.Name,
		Type:       "k8s_event",
		Severity:   "warning",
		Title:      fmt.Sprintf("%s: %s", ev.Reason, ev.Message),
		Payload:    mustJSON(map[string]any{"reason": ev.Reason, "message": ev.Message, "count": ev.Count}),
	}
	k.Emit(e)
}

func (k *K8sCollector) diffPods(old, new *corev1.Pod) {
	for i, cs := range new.Status.ContainerStatuses {
		if i >= len(old.Status.ContainerStatuses) {
			continue
		}
		oldCS := old.Status.ContainerStatuses[i]

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
				"owner": ownerRef(new),
			}),
		})
	}
}

func (k *K8sCollector) diffDeployments(old, new *appsv1.Deployment) {
	if len(old.Spec.Template.Spec.Containers) == 0 || len(new.Spec.Template.Spec.Containers) == 0 {
		return
	}

	oldImg := old.Spec.Template.Spec.Containers[0].Image
	newImg := new.Spec.Template.Spec.Containers[0].Image

	if oldImg != newImg {
		k.Emit(event.Event{
			Source:     "k8s",
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
			Type:       "resource_change",
			Severity:   "info",
			Title:      fmt.Sprintf("%s resource limits changed", new.Name),
		})
	}

	if (old.Spec.Replicas != nil && new.Spec.Replicas != nil) && *old.Spec.Replicas != *new.Spec.Replicas {
		k.Emit(event.Event{
			Source:     "k8s",
			EntityKind: "Deployment",
			EntityName: new.Name,
			Namespace:  new.Namespace,
			Type:       "scale",
			Severity:   "info",
			Title:      fmt.Sprintf("%s scaled from %d to %d", new.Name, *old.Spec.Replicas, *new.Spec.Replicas),
		})
	}
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

func extractSHA(img string) string {
	parts := strings.Split(img, ":")
	if len(parts) > 1 {
		return parts[len(parts)-1] // Assuming tag is the SHA
	}
	return ""
}
