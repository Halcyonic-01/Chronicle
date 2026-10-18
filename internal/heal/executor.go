package heal

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/tidwall/gjson"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/revision"
)

// Executor is deliberately separate from Engine. Planning and auditing can
// run in dry-run mode without granting the process Kubernetes write access.
type Executor interface {
	Execute(context.Context, *Action) (string, error)
}

type KubernetesExecutor struct {
	Client              kubernetes.Interface
	MaxMemoryMultiplier float64
	// A restart is for a pod that is stuck, not one that is momentarily unready:
	// it must have been unready for RestartUnreadyFor, with no container restart in
	// the last RestartQuiet (HEAL_RESTART_UNREADY_FOR, HEAL_RESTART_QUIET). Initial
	// values, not universal truths.
	RestartUnreadyFor, RestartQuiet time.Duration
	// Now is the clock; time.Now when nil.
	Now func() time.Time
	// Applications says which app.kubernetes.io/instance values are Argo CD
	// Applications; nil when Argo CD is not in use (see graph.GitOpsManager).
	Applications graph.KnownApplication
}

func NewKubernetesExecutor(client kubernetes.Interface) *KubernetesExecutor {
	return &KubernetesExecutor{Client: client, MaxMemoryMultiplier: 2.0,
		RestartUnreadyFor: durationEnv("HEAL_RESTART_UNREADY_FOR", DefaultRestartUnreadyFor),
		RestartQuiet:      durationEnv("HEAL_RESTART_QUIET", DefaultRestartQuiet)}
}

var updateOptions = metav1.UpdateOptions{FieldManager: FieldManager}

// Defaults for when a pod counts as stuck.
const (
	DefaultRestartUnreadyFor = 5 * time.Minute
	DefaultRestartQuiet      = 10 * time.Minute
	rolloutRecheck           = 30 * time.Second
)

func (e *KubernetesExecutor) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// NotBefore is the earliest time an approved decision may be carried out. It
// is the zero time for every action but a restart. A restart waits (it is not
// refused) until the pod has been unready for long enough and has not just been
// restarted, so an approval is not spent on a pod that is merely slow, and is
// refused outright when the pod has recovered or is gone.
func (e *KubernetesExecutor) NotBefore(ctx context.Context, action *Action) (time.Time, error) {
	if e == nil || e.Client == nil || action == nil || action.ActionType != ActionRestartPod {
		return time.Time{}, nil
	}
	pod, err := e.Client.CoreV1().Pods(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return time.Time{}, refuse("refusing to restart: pod %s no longer exists", action.Target)
	}
	if err != nil {
		return time.Time{}, err
	}
	if pod.DeletionTimestamp != nil {
		return time.Time{}, nil // restartPod reports it as already being deleted
	}
	var unreadySince time.Time
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			if c.Status == corev1.ConditionTrue {
				return time.Time{}, refuse("refusing to restart: pod %s is ready again", action.Target)
			}
			unreadySince = c.LastTransitionTime.Time
		}
	}
	if unreadySince.IsZero() {
		unreadySince = pod.CreationTimestamp.Time
	}
	notBefore := unreadySince.Add(e.RestartUnreadyFor)
	// A container the kubelet restarted recently has had its chance: wait out the
	// quiet period and look again rather than restart on top of it.
	for _, cs := range pod.Status.ContainerStatuses {
		if t := cs.LastTerminationState.Terminated; t != nil {
			if until := t.FinishedAt.Add(e.RestartQuiet); until.After(notBefore) {
				notBefore = until
			}
		}
	}
	// A pod that is unready because its Deployment is mid-rollout will be replaced
	// by the rollout.
	if name := ownerWorkload(metav1.GetControllerOf(pod)); name != "" {
		if d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, name, metav1.GetOptions{}); err == nil &&
			(d.Status.ObservedGeneration < d.Generation || (d.Spec.Replicas != nil && d.Status.UpdatedReplicas < *d.Spec.Replicas)) {
			if until := e.now().Add(rolloutRecheck); until.After(notBefore) {
				notBefore = until
			}
		}
	}
	return notBefore, nil
}

// Refusal is the executor declining to act because the cluster no longer
// matches the decision (or the decision lacks what a safe action needs). Nothing
// was changed, so it is recorded as blocked, not as a failed execution.
type Refusal struct{ Reason string }

func (r *Refusal) Error() string { return r.Reason }

func refuse(format string, args ...any) error { return &Refusal{Reason: fmt.Sprintf(format, args...)} }

// MaxRestoreReplicas is the most replicas a restore writes: the same ceiling
// the Kyverno replica guardrail enforces, and a bound on evidence read from an
// event before it is narrowed to int32.
const MaxRestoreReplicas = 20

// refuseGitOps stops a write a GitOps controller would revert, or that would
// hide drift from it. The engine checks the event's evidence; this checks the
// object as it is now.
func (e *KubernetesExecutor) refuseGitOps(d *appsv1.Deployment) error {
	if manager := graph.GitOpsManager(d.Labels, d.Annotations, e.Applications); manager != "" {
		return &Refusal{Reason: gitopsProposal(manager)}
	}
	if manager := graph.ReleaseController(d.Name, d.OwnerReferences, d.Labels, d.Annotations); manager != "" {
		return &Refusal{Reason: gitopsProposal(manager)}
	}
	return nil
}

func (e *KubernetesExecutor) Execute(ctx context.Context, action *Action) (string, error) {
	if e == nil || e.Client == nil {
		return "", fmt.Errorf("kubernetes executor requires a client")
	}
	if action == nil {
		return "", fmt.Errorf("action is nil")
	}
	if action.DryRun {
		return "", refuse("refusing to execute a dry-run action")
	}
	if action.Approval == ApprovalPending || action.Approval == ApprovalDenied {
		return "", refuse("action is not approved")
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
		return "", refuse("unsupported action type %q", action.ActionType)
	}
}

// restartPod deletes a pod only while it is still the one the decision was
// about: present, still unready, and owned by a controller that recreates it.
func (e *KubernetesExecutor) restartPod(ctx context.Context, action *Action) (string, error) {
	if action.Namespace == "" || action.Target == "" {
		return "", refuse("pod namespace and target are required")
	}
	pod, err := e.Client.CoreV1().Pods(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", refuse("refusing to restart: pod %s no longer exists", action.Target)
	}
	if err != nil {
		return "", err
	}
	if pod.DeletionTimestamp != nil {
		// A repeat of this decision, or one already carried out: nothing to do.
		return fmt.Sprintf("no change: pod %s is already being deleted", action.Target), nil
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil {
		return "", refuse("refusing to restart: pod %s has no controller, so nothing would recreate it", action.Target)
	}
	if want := gjson.GetBytes(action.Payload, "owner").String(); want != "" && ownerWorkload(owner) != want {
		return "", refuse("refusing to restart: pod %s is now owned by %s, not %s", action.Target, owner.Name, want)
	}
	if podReady(pod) {
		return "", refuse("refusing to restart: pod %s is ready again", action.Target)
	}
	// The Eviction API, not a delete: a PodDisruptionBudget can refuse it. A
	// budget with unhealthyPodEvictionPolicy: AlwaysAllow still lets an unready
	// pod go, which is the case this action is for.
	uid := pod.UID
	eviction := &policyv1.Eviction{
		ObjectMeta:    metav1.ObjectMeta{Name: action.Target, Namespace: action.Namespace},
		DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	}
	if err := e.Client.CoreV1().Pods(action.Namespace).EvictV1(ctx, eviction); err != nil {
		if apierrors.IsTooManyRequests(err) {
			return "", fmt.Errorf("a PodDisruptionBudget refused the eviction of %s: %w", action.Target, err)
		}
		return "", err
	}
	return fmt.Sprintf("evicted pod %s/%s; %s %s recreates it", action.Namespace, action.Target, owner.Kind, owner.Name), nil
}

// ownerWorkload names the workload behind a pod's controller: a ReplicaSet
// name minus its template hash.
func ownerWorkload(ref *metav1.OwnerReference) string {
	if ref == nil {
		return ""
	}
	if ref.Kind == "ReplicaSet" {
		if i := strings.LastIndex(ref.Name, "-"); i > 0 {
			return ref.Name[:i]
		}
	}
	return ref.Name
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Verify checks that the workload recovered, not merely that the change was
// accepted: every replica updated, ready and available.
func (e *KubernetesExecutor) Verify(ctx context.Context, action *Action) (string, error) {
	// A progress-deadline failure older than this is the fault being fixed.
	since := time.Now().Add(-time.Second)
	switch action.ActionType {
	case ActionRestoreReplicas:
		// Zero of zero replicas available would otherwise read as recovered.
		d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if d.Spec.Replicas == nil || *d.Spec.Replicas == 0 {
			return "", fmt.Errorf("%s is still scaled to zero after the restore", action.Target)
		}
		return e.awaitRollout(ctx, action.Namespace, action.Target, since)
	case ActionRollbackDeployment:
		return e.awaitRollout(ctx, action.Namespace, action.Target, since)
	case ActionBumpMemory:
		return e.awaitRollout(ctx, action.Namespace, gjson.GetBytes(action.Payload, "owner").String(), since)
	case ActionRestartPod:
		owner := gjson.GetBytes(action.Payload, "owner").String()
		if owner == "" {
			return "", fmt.Errorf("cannot verify recovery: the decision does not name the pod's workload")
		}
		if err := e.awaitGone(ctx, action.Namespace, action.Target); err != nil {
			return "", err
		}
		got, err := e.awaitRollout(ctx, action.Namespace, owner, since)
		if err != nil {
			return "", fmt.Errorf("pod %s is gone, but %s did not recover: %w", action.Target, owner, err)
		}
		return "pod " + action.Target + " replaced; " + got, nil
	}
	return "not applicable", nil
}

func (e *KubernetesExecutor) awaitGone(ctx context.Context, namespace, name string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := e.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pod %s was not removed: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

// awaitRollout waits until a Deployment has actually adopted the change.
// status.conditions alone is not dependable, so this checks the same set of
// fields kubectl rollout status does: the controller has observed this
// generation, every replica is on the new template, all of them are available,
// and no replicas from the previous version are still running. A rollout the
// Deployment controller itself declares failed (progressDeadlineSeconds) after
// since fails at once instead of at the timeout.
func (e *KubernetesExecutor) awaitRollout(ctx context.Context, namespace, name string, since time.Time) (string, error) {
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
		if c := progressFailure(d, since); c != "" {
			return "", fmt.Errorf("rollout failed: %s", c)
		}
		// Pods that cannot be created (a quota, an admission webhook) never reach
		// the progress deadline of a scale-up; the controller reports them here.
		if d.Status.AvailableReplicas < desired {
			if c := replicaFailure(d); c != "" {
				return "", fmt.Errorf("rollout failed: replicas cannot be created: %s", c)
			}
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

// progressFailure is the controller's own verdict that a rollout stalled, if
// it reached it for the current generation after since.
func progressFailure(d *appsv1.Deployment, since time.Time) string {
	if d.Status.ObservedGeneration < d.Generation {
		return ""
	}
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse &&
			c.Reason == "ProgressDeadlineExceeded" && !c.LastUpdateTime.Time.Before(since) {
			return c.Message
		}
	}
	return ""
}

// replicaFailure is the Deployment controller's report that its ReplicaSet could
// not create pods, with the reason it gave.
func replicaFailure(d *appsv1.Deployment) string {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue {
			if c.Message != "" {
				return c.Message
			}
			return c.Reason
		}
	}
	return ""
}

// baseMemoryAnnotation remembers, per container, the memory limit from before
// Chronicle first raised it.
const baseMemoryAnnotation = "chronicle.io/base-memory-limit-"

func (e *KubernetesExecutor) bumpMemory(ctx context.Context, action *Action) (string, error) {
	owner := gjson.GetBytes(action.Payload, "owner").String()
	original := gjson.GetBytes(action.Payload, "original_mem_bytes").Int()
	if owner == "" || original <= 0 {
		return "", refuse("OOM action requires owner and original_mem_bytes evidence")
	}
	d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, owner, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if err := e.refuseGitOps(d); err != nil {
		return "", err
	}
	container, err := oomContainer(d, gjson.GetBytes(action.Payload, "container").String())
	if err != nil {
		return "", err
	}
	current := container.Resources.Limits[corev1.ResourceMemory]
	if current.IsZero() {
		return "", refuse("deployment %s has no memory limit", owner)
	}
	// Growth is bounded against the limit before Chronicle first raised it, which
	// is remembered on the Deployment. The limit in the OOM event is the current
	// one, so after a bump it already includes the bump, and a cap taken from it
	// would move up with every OOM.
	base, annotated := current.Value(), false
	if raw, ok := d.Annotations[baseMemoryAnnotation+container.Name]; ok {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			base, annotated = v, true
		}
	}
	capBytes := int64(float64(base) * e.MaxMemoryMultiplier)
	newBytes := int64(float64(current.Value()) * 1.5)
	if newBytes > capBytes {
		newBytes = capBytes
	}
	if newBytes <= current.Value() {
		return "", refuse("memory increase would exceed the configured cap")
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	container.Resources.Limits[corev1.ResourceMemory] = *resource.NewQuantity(newBytes, resource.BinarySI)
	if !annotated {
		if d.Annotations == nil {
			d.Annotations = map[string]string{}
		}
		d.Annotations[baseMemoryAnnotation+container.Name] = strconv.FormatInt(base, 10)
	}
	if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, updateOptions); err != nil {
		return "", err
	}
	return fmt.Sprintf("memory limit of %s %d -> %d bytes", container.Name, current.Value(), newBytes), nil
}

// oomContainer is the container that was killed, by name. The first container
// is used only when it is the only one, never as a guess.
func oomContainer(d *appsv1.Deployment, name string) (*corev1.Container, error) {
	containers := d.Spec.Template.Spec.Containers
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i], nil
		}
	}
	if name == "" && len(containers) == 1 {
		return &containers[0], nil
	}
	return nil, refuse("deployment %s has no container %q to resize", d.Name, name)
}

// restoreReplicas puts a workload that was scaled to zero back to the count it
// had. Writing spec.replicas makes Chronicle a writer of that field, and a
// field with more than one writer is the standard cause of a replica tug-of-war
// between an autoscaler and whatever else reconciles the object -- so the
// preconditions below are refusals, not advice.
func (e *KubernetesExecutor) restoreReplicas(ctx context.Context, action *Action) (string, error) {
	if action.Namespace == "" || action.Target == "" {
		return "", refuse("restore requires a namespace and target")
	}
	previous := gjson.GetBytes(action.Payload, "old_replicas").Int()
	current := gjson.GetBytes(action.Payload, "new_replicas").Int()
	if previous <= 0 {
		return "", refuse("restore requires old_replicas evidence above zero")
	}
	if previous > MaxRestoreReplicas {
		return "", refuse("refusing to restore %d replicas: above the ceiling of %d", previous, MaxRestoreReplicas)
	}
	// Only a scale to zero is a fault. Going from ten replicas to three is
	// somebody managing capacity, and undoing it would fight them.
	if current != 0 {
		return "", refuse("refusing to restore: the workload was scaled to %d, not zero", current)
	}

	d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	// The evidence is a snapshot of the past. If the workload is no longer at
	// zero somebody has already dealt with it, and writing the old count now
	// would overwrite a newer, deliberate decision with a stale one.
	if err := e.refuseGitOps(d); err != nil {
		return "", err
	}
	// Somebody may have marked it for maintenance since the scale happened.
	if reason := graph.Maintenance(d.Annotations); reason != "" {
		return "", refuse("refusing to restore: %s is marked for maintenance (%s)", action.Target, reason)
	}
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 0 {
		have := int32(-1)
		if d.Spec.Replicas != nil {
			have = *d.Spec.Replicas
		}
		return "", refuse("refusing to restore: %s is already at %d replicas", action.Target, have)
	}
	if owner, err := e.autoscalerFor(ctx, action.Namespace, action.Target); err != nil {
		return "", err
	} else if owner != "" {
		return "", refuse("refusing to restore: HorizontalPodAutoscaler %q owns the replica count of %s", owner, action.Target)
	}

	want := int32(previous)
	d.Spec.Replicas = &want
	if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, updateOptions); err != nil {
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

// rollbackDeployment returns a Deployment to its previous revision's whole pod
// template, as `kubectl rollout undo` does: images, environment, resources and
// every container. Swapping one image left the rest of a bad release in place.
// It acts only while the bad revision is still the one running, so a newer
// deploy is never overwritten.
func (e *KubernetesExecutor) rollbackDeployment(ctx context.Context, action *Action) (string, error) {
	oldImage := gjson.GetBytes(action.Payload, "old_image").String()
	newImage := gjson.GetBytes(action.Payload, "new_image").String()
	hasRevision := gjson.GetBytes(action.Payload, "to_template_hash").String() != ""
	if !hasRevision && (oldImage == "" || newImage == "") {
		return "", refuse("rollback requires the revisions before and after the change (template hashes, or images for older events)")
	}
	d, err := e.Client.AppsV1().Deployments(action.Namespace).Get(ctx, action.Target, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if err := e.refuseGitOps(d); err != nil {
		return "", err
	}
	if len(d.Spec.Template.Spec.Containers) == 0 {
		return "", refuse("deployment %s has no containers", action.Target)
	}
	// A decision that names the whole template before and after is judged by it:
	// the image alone cannot see a release that changed anything else.
	if to := gjson.GetBytes(action.Payload, "to_template_hash").String(); to != "" {
		target := gjson.GetBytes(action.Payload, "target_template_hash").String()
		if target == "" {
			target = gjson.GetBytes(action.Payload, "from_template_hash").String()
		}
		return e.rollbackToRevision(ctx, action, d, to, target)
	}
	running := d.Spec.Template.Spec.Containers[0].Image
	if running == oldImage {
		// A retry after the update landed, or someone already rolled back.
		return fmt.Sprintf("no change: %s already runs %s", action.Target, oldImage), nil
	}
	if running != newImage {
		return "", refuse("refusing to roll back: %s now runs %s, not the %s the decision was about", action.Target, running, newImage)
	}
	previous, err := e.previousTemplate(ctx, d)
	if err != nil {
		return "", err
	}
	if len(previous.Spec.Containers) == 0 || previous.Spec.Containers[0].Image != oldImage {
		return "", refuse("refusing to roll back: the previous revision does not run %s", oldImage)
	}
	d.Spec.Template = *previous
	if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, updateOptions); err != nil {
		return "", err
	}
	return fmt.Sprintf("deployment %s rolled back to its previous pod template (%s)", action.Target, oldImage), nil
}

// previousTemplate is the pod template of the revision before the current one,
// read from the ReplicaSets the Deployment owns.
// rollbackToRevision returns a Deployment to the exact revision a decision named,
// and only while the revision the decision was about is still the one running.
// Every difference is a refusal, not a guess:
//   - paused, or a change the controller has not yet seen: someone is mid-edit;
//   - running something other than the revision the decision was about: a newer
//     change (even one that touched only an env var or a sidecar) is not ours to undo;
//   - the target revision is no longer retained (revisionHistoryLimit), or its
//     template no longer fits the Deployment's selector.
func (e *KubernetesExecutor) rollbackToRevision(ctx context.Context, action *Action, d *appsv1.Deployment, to, target string) (string, error) {
	if d.Spec.Paused {
		return "", refuse("refusing to roll back: deployment %s is paused, so someone is mid-edit", d.Name)
	}
	if d.Status.ObservedGeneration < d.Generation {
		return "", refuse("refusing to roll back: the controller has not yet observed the latest change to %s", d.Name)
	}
	running := revision.TemplateHash(d.Spec.Template)
	if running == target {
		return fmt.Sprintf("no change: %s already runs the target revision %s", d.Name, target), nil
	}
	if running != to {
		return "", refuse("refusing to roll back: %s now runs revision %s, not the %s the decision was about", d.Name, running, to)
	}
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return "", err
	}
	sets, err := e.Client.AppsV1().ReplicaSets(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return "", err
	}
	for i := range sets.Items {
		rs := &sets.Items[i]
		if !metav1.IsControlledBy(rs, d) || revision.TemplateHash(rs.Spec.Template) != target {
			continue
		}
		template := revision.Template(rs.Spec.Template)
		if len(template.Spec.Containers) == 0 || !selector.Matches(labels.Set(template.Labels)) {
			return "", refuse("refusing to roll back: revision %s no longer fits the deployment's selector", target)
		}
		d.Spec.Template = template
		if _, err := e.Client.AppsV1().Deployments(action.Namespace).Update(ctx, d, updateOptions); err != nil {
			return "", err
		}
		return fmt.Sprintf("deployment %s rolled back from revision %s to revision %s", d.Name, to, target), nil
	}
	return "", refuse("refusing to roll back: revision %s is no longer retained by %s (revisionHistoryLimit), so there is nothing exact to return to", target, d.Name)
}

func (e *KubernetesExecutor) previousTemplate(ctx context.Context, d *appsv1.Deployment) (*corev1.PodTemplateSpec, error) {
	current, err := strconv.ParseInt(d.Annotations[revisionAnnotation], 10, 64)
	if err != nil {
		return nil, refuse("deployment %s has no revision to roll back from", d.Name)
	}
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, err
	}
	sets, err := e.Client.AppsV1().ReplicaSets(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	var best *appsv1.ReplicaSet
	var bestRevision int64
	for i := range sets.Items {
		rs := &sets.Items[i]
		if !metav1.IsControlledBy(rs, d) {
			continue
		}
		revision, err := strconv.ParseInt(rs.Annotations[revisionAnnotation], 10, 64)
		if err != nil || revision >= current {
			continue
		}
		if best == nil || revision > bestRevision {
			best, bestRevision = rs, revision
		}
	}
	if best == nil {
		return nil, refuse("deployment %s has no earlier revision to roll back to", d.Name)
	}
	template := best.Spec.Template.DeepCopy()
	delete(template.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	return template, nil
}

const revisionAnnotation = "deployment.kubernetes.io/revision"
