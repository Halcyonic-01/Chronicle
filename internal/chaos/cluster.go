package chaos

import (
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Cluster is the live cluster an experiment runs against, held to one namespace.
type Cluster struct {
	Client    kubernetes.Interface
	Namespace string
	// Context is the kube context, passed to kubectl and checked for safety.
	Context string
	// Run executes a command (kubectl, docker); tests replace it.
	Run func(ctx context.Context, name string, args ...string) ([]byte, error)
	// Sleep waits; tests replace it.
	Sleep func(ctx context.Context, d time.Duration) error

	paused map[string]bool // kind node containers this run paused
}

// NewCluster wires a cluster with real commands.
func NewCluster(client kubernetes.Interface, namespace, kubeContext string) *Cluster {
	return &Cluster{Client: client, Namespace: namespace, Context: kubeContext, Run: runCommand, Sleep: sleep}
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Cluster) kubectl(ctx context.Context, args ...string) ([]byte, error) {
	return c.Run(ctx, "kubectl", append([]string{"--context", c.Context, "-n", c.Namespace}, args...)...)
}

func (c *Cluster) markPaused(node string) {
	if c.paused == nil {
		c.paused = map[string]bool{}
	}
	c.paused[node] = true
}

// Pod returns a running pod of a deployment, oldest first so repeated calls agree.
func (c *Cluster) Pod(ctx context.Context, dep string) (*corev1.Pod, error) {
	d, err := c.Client.AppsV1().Deployments(c.Namespace).Get(ctx, dep, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := c.Client.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	var running []corev1.Pod
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			running = append(running, p)
		}
	}
	if len(running) == 0 {
		return nil, fmt.Errorf("deployment %s has no running pod", dep)
	}
	sort.Slice(running, func(i, j int) bool {
		return running[i].CreationTimestamp.Before(&running[j].CreationTimestamp) ||
			(running[i].CreationTimestamp.Equal(&running[j].CreationTimestamp) && running[i].Name < running[j].Name)
	})
	return &running[0], nil
}

// NodeOf returns the node running a deployment's pod.
func (c *Cluster) NodeOf(ctx context.Context, dep string) (string, error) {
	pod, err := c.Pod(ctx, dep)
	if err != nil {
		return "", err
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("pod %s is not scheduled", pod.Name)
	}
	return pod.Spec.NodeName, nil
}

// NodeIsDedicated refuses a node fault when the node runs anything outside the
// victim namespace other than per-node agents: pausing it would also blind
// Chronicle, or take down its storage, and the run would measure nothing.
func (c *Cluster) NodeIsDedicated(ctx context.Context, dep string) error {
	node, err := c.NodeOf(ctx, dep)
	if err != nil {
		return err
	}
	pods, err := c.Client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return err
	}
	var others []string
	for _, p := range pods.Items {
		if p.Spec.NodeName != node {
			continue // the field selector is advisory for some clients
		}
		if p.Namespace == c.Namespace || ownedByDaemonSet(p) || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		others = append(others, p.Namespace+"/"+p.Name)
	}
	if len(others) > 0 {
		sort.Strings(others)
		return fmt.Errorf("node %s also runs %s; give the target a dedicated node (see scripts/setup.sh)", node, strings.Join(others, ", "))
	}
	return nil
}

func ownedByDaemonSet(p corev1.Pod) bool {
	for _, o := range p.OwnerReferences {
		if o.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// Snapshot is the state an experiment must leave behind.
type Snapshot struct {
	Deployments map[string]appsv1.DeploymentSpec
	Services    map[string]corev1.ServiceSpec
}

// Snapshot records every Deployment and Service spec in the namespace.
func (c *Cluster) Snapshot(ctx context.Context) (*Snapshot, error) {
	s := &Snapshot{Deployments: map[string]appsv1.DeploymentSpec{}, Services: map[string]corev1.ServiceSpec{}}
	deps, err := c.Client.AppsV1().Deployments(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, d := range deps.Items {
		s.Deployments[d.Name] = *d.Spec.DeepCopy()
	}
	svcs, err := c.Client.CoreV1().Services(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, svc := range svcs.Items {
		s.Services[svc.Name] = *svc.Spec.DeepCopy()
	}
	return s, nil
}

// Restore puts back every spec the snapshot holds, removes the NetworkPolicies
// the harness created and unpauses the nodes it paused. It returns what it
// changed, and keeps going past errors so one failure leaves nothing else broken.
func (c *Cluster) Restore(ctx context.Context, s *Snapshot) ([]string, error) {
	var changed, failed []string
	note := func(what string, err error) {
		if err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", what, err))
		} else {
			changed = append(changed, what)
		}
	}
	// Nodes first: nothing on a frozen node can be put back.
	nodes := make([]string, 0, len(c.paused))
	for n := range c.paused {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	for _, n := range nodes {
		_, err := c.Run(ctx, "docker", "unpause", n)
		note("unpaused node "+n, err)
		if err == nil {
			delete(c.paused, n)
		}
	}
	policies, err := c.Client.NetworkingV1().NetworkPolicies(c.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set{chaosLabel: "true"}).String()})
	if err != nil {
		note("list network policies", err)
	} else {
		for _, p := range policies.Items {
			note("deleted networkpolicy/"+p.Name, c.Client.NetworkingV1().NetworkPolicies(c.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}))
		}
	}
	for _, name := range sortedKeys(s.Services) {
		want := s.Services[name]
		var did bool
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			svc, err := c.Client.CoreV1().Services(c.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if equality.Semantic.DeepEqual(svc.Spec.Selector, want.Selector) && equality.Semantic.DeepEqual(svc.Spec.Ports, want.Ports) {
				return nil
			}
			svc.Spec.Selector, svc.Spec.Ports = want.Selector, want.Ports
			_, err = c.Client.CoreV1().Services(c.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
			did = err == nil
			return err
		})
		if did || err != nil {
			note("restored service/"+name, err)
		}
	}
	for _, name := range sortedKeys(s.Deployments) {
		want := s.Deployments[name]
		var did bool
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			d, err := c.Client.AppsV1().Deployments(c.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if sameSpec(d.Spec, want) {
				return nil
			}
			d.Spec.Replicas, d.Spec.Template, d.Spec.Strategy = want.Replicas, *want.Template.DeepCopy(), want.Strategy
			_, err = c.Client.AppsV1().Deployments(c.Namespace).Update(ctx, d, metav1.UpdateOptions{})
			did = err == nil
			return err
		})
		if did || err != nil {
			note("restored deployment/"+name, err)
		}
	}
	if len(failed) > 0 {
		return changed, fmt.Errorf("restore incomplete: %s", strings.Join(failed, "; "))
	}
	return changed, nil
}

func sameSpec(have, want appsv1.DeploymentSpec) bool {
	return equality.Semantic.DeepEqual(have.Replicas, want.Replicas) &&
		equality.Semantic.DeepEqual(have.Template, want.Template) &&
		equality.Semantic.DeepEqual(have.Strategy, want.Strategy)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Rolled reports whether every deployment in the namespace is fully rolled out
// and ready, and names the first that is not.
func (c *Cluster) Rolled(ctx context.Context) (bool, string, error) {
	deps, err := c.Client.AppsV1().Deployments(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, "", err
	}
	sort.Slice(deps.Items, func(i, j int) bool { return deps.Items[i].Name < deps.Items[j].Name })
	for _, d := range deps.Items {
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		st := d.Status
		if d.Generation > st.ObservedGeneration || st.UpdatedReplicas != want || st.ReadyReplicas != want || st.AvailableReplicas != want || st.Replicas != want {
			return false, fmt.Sprintf("deployment/%s is %d/%d ready (%d updated)", d.Name, st.ReadyReplicas, want, st.UpdatedReplicas), nil
		}
	}
	return true, "", nil
}
