package collect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// A checkpoint is what the collector last saw, so a new leader can report what
// changed while nobody was watching.
type checkpoint struct {
	At          time.Time                  `json:"at"`
	Deployments map[string]deploymentState `json:"deployments"`
	Pods        map[string]podState        `json:"pods"`
}

type deploymentState struct {
	Replicas  int32  `json:"replicas"`
	Ready     int32  `json:"ready"`
	Image     string `json:"image"`
	Memory    int64  `json:"memory"`
	Resources string `json:"resources"`
	Config    string `json:"config"`
}

type podState struct {
	Phase    string           `json:"phase"`
	Ready    bool             `json:"ready"`
	Restarts map[string]int32 `json:"restarts,omitempty"`
}

const (
	checkpointName = "chronicle-collector-state"
	checkpointKey  = "state.json"
	// A ConfigMap holds 1MiB; stay well under it.
	maxCheckpointBytes = 900 * 1024
	checkpointEvery    = 15 * time.Second
	// A resource created this long before the checkpoint can still be missing
	// from it, because the two were saved a moment apart.
	creationSlack = 30 * time.Second
)

type checkpointStore interface {
	Load(context.Context) (*checkpoint, error)
	Save(context.Context, *checkpoint) error
}

type configMapCheckpoint struct {
	client    kubernetes.Interface
	namespace string
}

func (s configMapCheckpoint) Load(ctx context.Context) (*checkpoint, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, checkpointName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cp checkpoint
	if err := json.Unmarshal([]byte(cm.Data[checkpointKey]), &cp); err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	return &cp, nil
}

func (s configMapCheckpoint) Save(ctx context.Context, cp *checkpoint) error {
	raw, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	if len(raw) > maxCheckpointBytes {
		return fmt.Errorf("checkpoint is %d bytes, over the %d limit", len(raw), maxCheckpointBytes)
	}
	cms := s.client.CoreV1().ConfigMaps(s.namespace)
	cm, err := cms.Get(ctx, checkpointName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: checkpointName, Namespace: s.namespace},
			Data:       map[string]string{checkpointKey: string(raw)},
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[checkpointKey] = string(raw)
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

// snapshotOf records the current state of every Deployment and Pod.
func snapshotOf(deployments []*appsv1.Deployment, pods []*corev1.Pod, at time.Time) *checkpoint {
	cp := &checkpoint{At: at, Deployments: map[string]deploymentState{}, Pods: map[string]podState{}}
	for _, d := range deployments {
		cp.Deployments[d.Namespace+"/"+d.Name] = deploymentStateOf(d)
	}
	for _, p := range pods {
		restarts := make(map[string]int32, len(p.Status.ContainerStatuses))
		for _, cs := range p.Status.ContainerStatuses {
			restarts[cs.Name] = cs.RestartCount
		}
		cp.Pods[p.Namespace+"/"+p.Name] = podState{Phase: string(p.Status.Phase), Ready: isPodReady(p), Restarts: restarts}
	}
	return cp
}

func deploymentStateOf(d *appsv1.Deployment) deploymentState {
	st := deploymentState{Replicas: deploymentReplicas(d), Ready: d.Status.ReadyReplicas, Config: configFingerprint(d)}
	if cs := d.Spec.Template.Spec.Containers; len(cs) > 0 {
		st.Image, st.Memory = cs[0].Image, memoryLimit(cs[0])
		st.Resources = fingerprint(cs[0].Resources)
	}
	return st
}

// fingerprint is a short stable hash, enough to tell two versions apart. It is
// not reversible in practice, which is why it can stand in for values that must
// not be stored.
func fingerprint(v any) string {
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:12]
}

// configFingerprint covers everything configChanges looks at.
func configFingerprint(d *appsv1.Deployment) string {
	type container struct {
		Name    string
		Env     []corev1.EnvVar
		EnvFrom []corev1.EnvFromSource
		Command []string
		Args    []string
	}
	var cs []container
	for _, c := range d.Spec.Template.Spec.Containers {
		cs = append(cs, container{c.Name, c.Env, c.EnvFrom, c.Command, c.Args})
	}
	return fingerprint(struct {
		Containers []container
		Volumes    []corev1.Volume
	}{cs, d.Spec.Template.Spec.Volumes})
}

// catchUp reports what changed between the last checkpoint and now. Without it
// a change made while no leader was running is never recorded, and neither is
// the recovery that would have closed an episode.
func (k *K8sCollector) catchUp(prior *checkpoint, deployments []*appsv1.Deployment, pods []*corev1.Pod) {
	for _, d := range deployments {
		key := d.Namespace + "/" + d.Name
		was, known := prior.Deployments[key]
		if !known {
			if d.CreationTimestamp.Time.After(prior.At.Add(-creationSlack)) {
				k.emitResource("Deployment", d.Namespace, d.Name, "resource_created", "info", fmt.Sprintf("%s created", d.Name), d.CreationTimestamp.Time, missed(deploymentLifecyclePayload(d)))
			}
			continue
		}
		k.catchUpDeployment(was, d)
	}
	for key, was := range prior.Deployments {
		if !hasDeployment(deployments, key) {
			ns, name := splitKey(key)
			k.emitResourceEvent("Deployment", ns, name, "resource_deleted", "info", fmt.Sprintf("%s deleted", name), time.Now().UTC(), map[string]any{"phase": "Deleted", "replicas": was.Replicas, "missed": true})
		}
	}

	for _, p := range pods {
		key := p.Namespace + "/" + p.Name
		was, known := prior.Pods[key]
		if !known {
			if p.CreationTimestamp.Time.After(prior.At.Add(-creationSlack)) {
				k.emitResource("Pod", p.Namespace, p.Name, "resource_created", "info", fmt.Sprintf("%s created", p.Name), p.CreationTimestamp.Time, missed(podLifecyclePayload(p)))
			}
			continue
		}
		k.diffPods(priorPod(p, was), p)
	}
	for key, was := range prior.Pods {
		if !hasPod(pods, key) {
			ns, name := splitKey(key)
			k.emitResourceEvent("Pod", ns, name, "resource_deleted", "info", fmt.Sprintf("%s deleted", name), time.Now().UTC(), map[string]any{"phase": was.Phase, "missed": true})
		}
	}
}

func (k *K8sCollector) catchUpDeployment(was deploymentState, d *appsv1.Deployment) {
	now := deploymentStateOf(d)
	changedAt, changedBy := specChangeTime(d)
	emit := func(typ, title string, payload map[string]any) {
		payload["changed_by"], payload["missed"] = changedBy, true
		k.Emit(event.Event{Source: "k8s", OccurredAt: changedAt, EntityKind: "Deployment", EntityName: d.Name, Namespace: d.Namespace, Type: typ, Severity: "info", Title: title, Payload: mustJSON(payload)})
	}
	if was.Image != now.Image {
		emit("deploy", fmt.Sprintf("%s deployed: %s -> %s", d.Name, shortTag(was.Image), shortTag(now.Image)),
			map[string]any{"old_image": was.Image, "new_image": now.Image, "commit_sha": extractSHA(now.Image)})
	}
	if was.Resources != now.Resources {
		emit("resource_change", fmt.Sprintf("%s resource limits changed", d.Name),
			map[string]any{"old_mem_limit": was.Memory, "new_mem_limit": now.Memory})
	}
	if was.Config != now.Config {
		emit("config_change", fmt.Sprintf("%s configuration changed", d.Name),
			map[string]any{"changed": []string{"configuration"}, "from_hash": was.Config, "to_hash": now.Config})
	}
	if was.Replicas != now.Replicas {
		emit("scale", fmt.Sprintf("%s scaled from %d to %d", d.Name, was.Replicas, now.Replicas),
			map[string]any{"old_replicas": was.Replicas, "new_replicas": now.Replicas})
	}
	if was.Ready != now.Ready {
		k.emitDeploymentStatus(d)
	}
}

// priorPod rebuilds the earlier pod from its checkpoint, so diffPods can say
// what changed with the rules it already has.
func priorPod(now *corev1.Pod, was podState) *corev1.Pod {
	old := now.DeepCopy()
	old.Status.Phase = corev1.PodPhase(was.Phase)
	ready := corev1.ConditionFalse
	if was.Ready {
		ready = corev1.ConditionTrue
	}
	conds := make([]corev1.PodCondition, 0, len(old.Status.Conditions)+1)
	for _, c := range old.Status.Conditions {
		if c.Type != corev1.PodReady {
			conds = append(conds, c)
		}
	}
	old.Status.Conditions = append(conds, corev1.PodCondition{Type: corev1.PodReady, Status: ready})
	for i, cs := range old.Status.ContainerStatuses {
		if n, ok := was.Restarts[cs.Name]; ok {
			old.Status.ContainerStatuses[i].RestartCount = n
		}
	}
	return old
}

// missed marks a payload as reported after the fact.
func missed(payload map[string]any) map[string]any {
	payload["missed"] = true
	return payload
}

func hasDeployment(ds []*appsv1.Deployment, key string) bool {
	for _, d := range ds {
		if d.Namespace+"/"+d.Name == key {
			return true
		}
	}
	return false
}

func hasPod(ps []*corev1.Pod, key string) bool {
	for _, p := range ps {
		if p.Namespace+"/"+p.Name == key {
			return true
		}
	}
	return false
}

func splitKey(key string) (namespace, name string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

// keepCheckpoint restores the previous state, reports what changed since, then
// keeps the checkpoint current until the context ends.
func (k *K8sCollector) keepCheckpoint(ctx context.Context, factory informers.SharedInformerFactory) {
	if k.checkpoints == nil {
		return
	}
	list := func() ([]*appsv1.Deployment, []*corev1.Pod) {
		ds, _ := factory.Apps().V1().Deployments().Lister().List(labels.Everything())
		ps, _ := factory.Core().V1().Pods().Lister().List(labels.Everything())
		return ds, ps
	}
	if prior, err := k.checkpoints.Load(ctx); err != nil {
		slog.Warn("could not read the collector checkpoint; changes made while Chronicle was down will be missed", "err", err)
	} else if prior != nil {
		ds, ps := list()
		k.catchUp(prior, ds, ps)
	}
	save := func() {
		ds, ps := list()
		if err := k.checkpoints.Save(ctx, snapshotOf(ds, ps, time.Now().UTC())); err != nil && ctx.Err() == nil {
			slog.Warn("could not save the collector checkpoint", "err", err)
		}
	}
	save()
	ticker := time.NewTicker(checkpointEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			save()
		}
	}
}
