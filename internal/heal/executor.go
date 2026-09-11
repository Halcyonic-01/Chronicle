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
