package chaos

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// Action is one step of an experiment.
type Action interface {
	Describe() string
	Do(ctx context.Context, c *Cluster) error
}

type step struct {
	desc string
	do   func(ctx context.Context, c *Cluster) error
}

func (s step) Describe() string                         { return s.desc }
func (s step) Do(ctx context.Context, c *Cluster) error { return s.do(ctx, c) }

// chaosLabel marks objects the harness created, so restore can find them.
const chaosLabel = "chronicle.io/chaos"

// execTimeout bounds one command in a pod, so a hung target cannot hang the run.
const execTimeout = 30 * time.Second

// updateDeployment changes a deployment, retrying when it moved underneath us.
func updateDeployment(ctx context.Context, c *Cluster, name string, change func(*appsv1.Deployment) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		d, err := c.Client.AppsV1().Deployments(c.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := change(d); err != nil {
			return err
		}
		_, err = c.Client.AppsV1().Deployments(c.Namespace).Update(ctx, d, metav1.UpdateOptions{})
		return err
	})
}

// firstContainer is the one Chronicle's collector watches for image, resources and config.
func firstContainer(d *appsv1.Deployment) (*corev1.Container, error) {
	if len(d.Spec.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("deployment %s has no containers", d.Name)
	}
	return &d.Spec.Template.Spec.Containers[0], nil
}

// Scale sets a deployment's replica count.
func Scale(dep string, replicas int32) Action {
	return step{fmt.Sprintf("scale deployment/%s to %d", dep, replicas), func(ctx context.Context, c *Cluster) error {
		return updateDeployment(ctx, c, dep, func(d *appsv1.Deployment) error {
			d.Spec.Replicas = &replicas
			return nil
		})
	}}
}

// SetEnv sets one environment variable on the first container.
func SetEnv(dep, name, value string) Action {
	return step{fmt.Sprintf("set env %s on deployment/%s", name, dep), func(ctx context.Context, c *Cluster) error {
		return updateDeployment(ctx, c, dep, func(d *appsv1.Deployment) error {
			ct, err := firstContainer(d)
			if err != nil {
				return err
			}
			for i := range ct.Env {
				if ct.Env[i].Name == name {
					ct.Env[i].Value, ct.Env[i].ValueFrom = value, nil
					return nil
				}
			}
			ct.Env = append(ct.Env, corev1.EnvVar{Name: name, Value: value})
			return nil
		})
	}}
}

// SetImage changes the first container's image.
func SetImage(dep, image string) Action {
	return step{fmt.Sprintf("set image of deployment/%s to %s", dep, image), func(ctx context.Context, c *Cluster) error {
		return updateDeployment(ctx, c, dep, func(d *appsv1.Deployment) error {
			ct, err := firstContainer(d)
			if err != nil {
				return err
			}
			ct.Image = image
			return nil
		})
	}}
}

// Recreate switches a deployment to the Recreate strategy, which Chronicle does
// not record: a rollout then replaces the old pod before the new one is proven.
func Recreate(dep string) Action {
	return step{fmt.Sprintf("set deployment/%s strategy to Recreate", dep), func(ctx context.Context, c *Cluster) error {
		return updateDeployment(ctx, c, dep, func(d *appsv1.Deployment) error {
			d.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
			return nil
		})
	}}
}

// SetMemoryLimit sets the first container's memory limit (and request, so the
// request never exceeds it).
func SetMemoryLimit(dep, limit string) Action {
	return step{fmt.Sprintf("set memory limit of deployment/%s to %s", dep, limit), func(ctx context.Context, c *Cluster) error {
		q, err := resource.ParseQuantity(limit)
		if err != nil {
			return err
		}
		return updateDeployment(ctx, c, dep, func(d *appsv1.Deployment) error {
			ct, err := firstContainer(d)
			if err != nil {
				return err
			}
			if ct.Resources.Limits == nil {
				ct.Resources.Limits = corev1.ResourceList{}
			}
			if ct.Resources.Requests == nil {
				ct.Resources.Requests = corev1.ResourceList{}
			}
			ct.Resources.Limits[corev1.ResourceMemory] = q
			ct.Resources.Requests[corev1.ResourceMemory] = q
			return nil
		})
	}}
}

// SetSelector replaces a Service's selector.
func SetSelector(svc string, selector map[string]string) Action {
	return step{fmt.Sprintf("set service/%s selector to %s", svc, labelString(selector)), func(ctx context.Context, c *Cluster) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			s, err := c.Client.CoreV1().Services(c.Namespace).Get(ctx, svc, metav1.GetOptions{})
			if err != nil {
				return err
			}
			s.Spec.Selector = selector
			_, err = c.Client.CoreV1().Services(c.Namespace).Update(ctx, s, metav1.UpdateOptions{})
			return err
		})
	}}
}

// DenyIngress blocks all traffic into a deployment's pods with a NetworkPolicy.
func DenyIngress(dep string) Action {
	return step{fmt.Sprintf("deny all ingress to deployment/%s pods (NetworkPolicy)", dep), func(ctx context.Context, c *Cluster) error {
		d, err := c.Client.AppsV1().Deployments(c.Namespace).Get(ctx, dep, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if d.Spec.Selector == nil || len(d.Spec.Selector.MatchLabels) == 0 {
			return fmt.Errorf("deployment %s has no label selector to target", dep)
		}
		policy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "chaos-deny-" + dep, Namespace: c.Namespace, Labels: map[string]string{chaosLabel: "true"}},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: d.Spec.Selector.MatchLabels},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			},
		}
		_, err = c.Client.NetworkingV1().NetworkPolicies(c.Namespace).Create(ctx, policy, metav1.CreateOptions{})
		return err
	}}
}

// Exec runs a command in a container of a deployment's pod.
func Exec(dep, container string, command ...string) Action {
	return step{fmt.Sprintf("exec in deployment/%s (%s): %s", dep, container, strings.Join(command, " ")), func(ctx context.Context, c *Cluster) error {
		pod, err := c.Pod(ctx, dep)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, execTimeout)
		defer cancel()
		args := append([]string{"exec", pod.Name, "-c", container, "--"}, command...)
		_, err = c.kubectl(ctx, args...)
		return err
	}}
}

// PauseNodeOf freezes the kind node running a deployment's pod, as a lost node.
// The snapshot restore unpauses it.
func PauseNodeOf(dep string) Action {
	return step{fmt.Sprintf("pause the node running deployment/%s (docker pause)", dep), func(ctx context.Context, c *Cluster) error {
		node, err := c.NodeOf(ctx, dep)
		if err != nil {
			return err
		}
		if _, err := c.Run(ctx, "docker", "pause", node); err != nil {
			return err
		}
		c.markPaused(node)
		return nil
	}}
}

// Wait pauses between steps.
func Wait(d time.Duration) Action {
	return step{fmt.Sprintf("wait %s", d), func(ctx context.Context, c *Cluster) error {
		return c.Sleep(ctx, d)
	}}
}

// Try makes a step best effort: a command that kills its own session, such as
// shutting a server down, reports failure even when it worked.
func Try(a Action) Action {
	return step{a.Describe() + " (best effort)", func(ctx context.Context, c *Cluster) error {
		_ = a.Do(ctx, c)
		return nil
	}}
}

func labelString(m map[string]string) string {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
