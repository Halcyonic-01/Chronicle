package heal

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/tidwall/gjson"
)

// Executor is deliberately separate from Engine. Planning and auditing can
// run in dry-run mode without granting the process Kubernetes write access.
type Executor interface {
	Execute(context.Context, *Action) (string, error)
}

type KubernetesExecutor struct {
	Client              kubernetes.Interface
	MaxMemoryMultiplier float64
}

func NewKubernetesExecutor(client kubernetes.Interface) *KubernetesExecutor {
	return &KubernetesExecutor{Client: client, MaxMemoryMultiplier: 2.0}
}

func (e *KubernetesExecutor) Execute(ctx context.Context, action *Action) (string, error) {
	if e == nil || e.Client == nil {
		return "", fmt.Errorf("kubernetes executor requires a client")
	}
	if action == nil {
		return "", fmt.Errorf("action is nil")
	}
	if action.DryRun {
		return "", fmt.Errorf("refusing to execute a dry-run action")
	}
	if action.Approval == ApprovalPending || action.Approval == ApprovalDenied {
		return "", fmt.Errorf("action is not approved")
	}
	switch action.ActionType {
	case ActionRestartPod:
		return e.restartPod(ctx, action)
	case ActionBumpMemory:
		return e.bumpMemory(ctx, action)
	case ActionRollbackDeployment:
		return e.rollbackDeployment(ctx, action)
	case ActionRestoreReplicas:
		return e.restoreReplicas(ctx, action)
	default:
		return "", fmt.Errorf("unsupported action type %q", action.ActionType)
	}
}

func (e *KubernetesExecutor) restartPod(ctx context.Context, action *Action) (string, error) {
	if action.Namespace == "" || action.Target == "" {
		return "", fmt.Errorf("pod namespace and target are required")
	}
	if err := e.Client.CoreV1().Pods(action.Namespace).Delete(ctx, action.Target, metav1.DeleteOptions{}); err != nil {
		return "", err
	}
	return fmt.Sprintf("deleted pod %s/%s; Kubernetes should recreate it", action.Namespace, action.Target), nil
}

func (e *KubernetesExecutor) Verify(ctx context.Context, action *Action) (string, error) {
	if action.ActionType == ActionRestoreReplicas {
		d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if d.Spec.Replicas == nil || *d.Spec.Replicas == 0 {
			return "", fmt.Errorf("%s is still scaled to zero after the restore", action.Target)
		}
		return fmt.Sprintf("%s is at %d replica(s)", action.Target, *d.Spec.Replicas), nil
	}
	if action.ActionType == ActionBumpMemory || action.ActionType == ActionRollbackDeployment {
		target := action.Target
		if action.ActionType == ActionBumpMemory {
			target = gjson.GetBytes(action.Payload, "owner").String()
		}
		return e.awaitRollout(ctx, action.Namespace, target)
	}
	if action.ActionType != ActionRestartPod {
		return "not applicable", nil
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := e.Client.CoreV1().Pods(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return "pod deletion verified", nil
		}
		if err != nil {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// awaitRollout waits until a Deployment has actually adopted the change.
// status.conditions alone is not dependable, so this checks the same set of
// fields kubectl rollout status does: the controller has observed this
// generation, every replica is on the new template, all of them are available,
// and no replicas from the previous version are still running.
func (e *KubernetesExecutor) awaitRollout(ctx context.Context, namespace, name string) (string, error) {
	if namespace == "" || name == "" {
		return "", fmt.Errorf("rollout verification requires a namespace and deployment")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := "no status observed yet"
	for {
		d, err := e.Client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		desired := int32(1)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		switch {
		case d.Status.ObservedGeneration < d.Generation:
			last = "the controller has not observed the change yet"
		case d.Status.UpdatedReplicas < desired:
			last = fmt.Sprintf("%d of %d replicas updated", d.Status.UpdatedReplicas, desired)
		case d.Status.Replicas > d.Status.UpdatedReplicas:
			last = fmt.Sprintf("%d replicas from the previous version are still running", d.Status.Replicas-d.Status.UpdatedReplicas)
		case d.Status.AvailableReplicas < desired:
			last = fmt.Sprintf("%d of %d replicas available", d.Status.AvailableReplicas, desired)
		default:
			return fmt.Sprintf("rollout complete: %d/%d replicas updated and available", d.Status.AvailableReplicas, desired), nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("rollout did not complete: %s", last)
		case <-ticker.C:
		}
	}
}

func (e *KubernetesExecutor) bumpMemory(ctx context.Context, action *Action) (string, error) {
	owner := gjson.GetBytes(action.Payload, "owner").String()
	original := gjson.GetBytes(action.Payload, "original_mem_bytes").Int()
	if owner == "" || original <= 0 {
		return "", fmt.Errorf("OOM action requires owner and original_mem_bytes evidence")
	}
	d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, owner, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(d.Spec.Template.Spec.Containers) == 0 {
		return "", fmt.Errorf("deployment %s has no containers", owner)
	}
	container := &d.Spec.Template.Spec.Containers[0]
	current := container.Resources.Limits[corev1.ResourceMemory]
	if current.IsZero() {
		return "", fmt.Errorf("deployment %s has no memory limit", owner)
	}
	capBytes := int64(float64(original) * e.MaxMemoryMultiplier)
	newBytes := int64(float64(current.Value()) * 1.5)
	if newBytes > capBytes {
		newBytes = capBytes
	}
	if newBytes <= current.Value() {
		return "", fmt.Errorf("memory increase would exceed the configured cap")
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	container.Resources.Limits[corev1.ResourceMemory] = *resource.NewQuantity(newBytes, resource.BinarySI)
	if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		return "", err
	}
	return fmt.Sprintf("memory limit %d -> %d bytes", current.Value(), newBytes), nil
}

// restoreReplicas puts a workload that was scaled to zero back to the count it
// had. Writing spec.replicas makes Chronicle a writer of that field, and a
// field with more than one writer is the standard cause of a replica tug-of-war
// between an autoscaler and whatever else reconciles the object -- so the
// preconditions below are refusals, not advice.
func (e *KubernetesExecutor) restoreReplicas(ctx context.Context, action *Action) (string, error) {
	if action.Namespace == "" || action.Target == "" {
		return "", fmt.Errorf("restore requires a namespace and target")
	}
	previous := gjson.GetBytes(action.Payload, "old_replicas").Int()
	current := gjson.GetBytes(action.Payload, "new_replicas").Int()
	if previous <= 0 {
		return "", fmt.Errorf("restore requires old_replicas evidence above zero")
	}
	// Only a scale to zero is a fault. Going from ten replicas to three is
	// somebody managing capacity, and undoing it would fight them.
	if current != 0 {
		return "", fmt.Errorf("refusing to restore: the workload was scaled to %d, not zero", current)
	}

	d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	// The evidence is a snapshot of the past. If the workload is no longer at
	// zero somebody has already dealt with it, and writing the old count now
	// would overwrite a newer, deliberate decision with a stale one.
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 0 {
		have := int32(-1)
		if d.Spec.Replicas != nil {
			have = *d.Spec.Replicas
		}
		return "", fmt.Errorf("refusing to restore: %s is already at %d replicas", action.Target, have)
	}
	if owner, err := e.autoscalerFor(ctx, action.Namespace, action.Target); err != nil {
		return "", err
	} else if owner != "" {
		return "", fmt.Errorf("refusing to restore: HorizontalPodAutoscaler %q owns the replica count of %s", owner, action.Target)
	}

	want := int32(previous)
	d.Spec.Replicas = &want
	if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		return "", err
	}
	return fmt.Sprintf("restored %s/%s to %d replica(s)", action.Namespace, action.Target, want), nil
}

// autoscalerFor names the HPA that scales this Deployment, if any.
func (e *KubernetesExecutor) autoscalerFor(ctx context.Context, namespace, name string) (string, error) {
	list, err := e.Client.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("check for an autoscaler on %s/%s: %w", namespace, name, err)
	}
	for _, h := range list.Items {
		if h.Spec.ScaleTargetRef.Kind == "Deployment" && h.Spec.ScaleTargetRef.Name == name {
			return h.Name, nil
		}
	}
	return "", nil
}

func (e *KubernetesExecutor) rollbackDeployment(ctx context.Context, action *Action) (string, error) {
	oldImage := gjson.GetBytes(action.Payload, "old_image").String()
	if oldImage == "" {
		return "", fmt.Errorf("rollback requires old_image evidence")
	}
	d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(d.Spec.Template.Spec.Containers) == 0 {
		return "", fmt.Errorf("deployment %s has no containers", action.Target)
	}
	d.Spec.Template.Spec.Containers[0].Image = oldImage
	if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		return "", err
	}
	return fmt.Sprintf("deployment %s rolled back to %s", action.Target, oldImage), nil
}
