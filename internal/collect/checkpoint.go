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
	autoscalingv2 "k8s.io/api/autoscaling/v2"
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
	// Fingerprints of the objects that can break a dependency from outside a
	// Deployment; see watchRelated.
	Services   map[string]string   `json:"services,omitempty"`
	ConfigMaps map[string]string   `json:"configmaps,omitempty"`
	HPAs       map[string]hpaState `json:"hpas,omitempty"`
	Nodes      map[string]bool     `json:"nodes,omitempty"` // name -> Ready
}

type hpaState struct {
	Target string `json:"target"`
	Hash   string `json:"hash"`
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

// related records the Services, used ConfigMaps, HPAs and Nodes.
func (cp *checkpoint) related(services []*corev1.Service, configMaps []*corev1.ConfigMap, hpas []*autoscalingv2.HorizontalPodAutoscaler, nodes []*corev1.Node, pods []*corev1.Pod) {
	cp.Services, cp.ConfigMaps = map[string]string{}, map[string]string{}
	cp.HPAs, cp.Nodes = map[string]hpaState{}, map[string]bool{}
	for _, s := range services {
		cp.Services[s.Namespace+"/"+s.Name] = serviceFingerprint(s)
	}
	for _, c := range configMaps {
		if !ignoredConfigMap(c) && usedByAPod(c, pods) {
			cp.ConfigMaps[c.Namespace+"/"+c.Name] = configMapFingerprint(c)
		}
	}
	for _, h := range hpas {
		cp.HPAs[h.Namespace+"/"+h.Name] = hpaState{Target: h.Spec.ScaleTargetRef.Name, Hash: hpaFingerprint(h)}
	}
	for _, n := range nodes {
		ready, _, _ := nodeCondition(n, corev1.NodeReady)
		cp.Nodes[n.Name] = ready == corev1.ConditionTrue
	}
}

// catchUpRelated reports changes to the objects recorded by related.
func (k *K8sCollector) catchUpRelated(prior *checkpoint, services []*corev1.Service, configMaps []*corev1.ConfigMap, hpas []*autoscalingv2.HorizontalPodAutoscaler, nodes []*corev1.Node) {
	for _, s := range services {
		if was, ok := prior.Services[s.Namespace+"/"+s.Name]; ok && was != serviceFingerprint(s) {
			k.emitResource("Service", s.Namespace, s.Name, "service_change", "info", fmt.Sprintf("%s service changed", s.Name), time.Now().UTC(),
				missed(map[string]any{"changed": []string{"service"}, "from_hash": was, "to_hash": serviceFingerprint(s)}))
		}
	}
	for _, c := range configMaps {
		if was, ok := prior.ConfigMaps[c.Namespace+"/"+c.Name]; ok && was != configMapFingerprint(c) {
			k.emitResource("ConfigMap", c.Namespace, c.Name, "config_change", "info", fmt.Sprintf("%s changed", c.Name), time.Now().UTC(),
				missed(map[string]any{"changed": []string{"configuration"}, "from_hash": was, "to_hash": configMapFingerprint(c)}))
		}
	}
	for _, h := range hpas {
		if was, ok := prior.HPAs[h.Namespace+"/"+h.Name]; ok && was.Hash != hpaFingerprint(h) && h.Spec.ScaleTargetRef.Kind == "Deployment" {
			k.Emit(event.Event{Source: "k8s", Namespace: h.Namespace, EntityKind: "Deployment", EntityName: h.Spec.ScaleTargetRef.Name,
				Type: "hpa_change", Severity: "info", Title: fmt.Sprintf("%s autoscaler changed", h.Spec.ScaleTargetRef.Name),
				Payload: mustJSON(missed(map[string]any{"changed": []string{"autoscaler"}, "autoscaler": h.Name, "from_hash": was.Hash, "to_hash": hpaFingerprint(h)}))})
		}
	}
	for _, n := range nodes {
		was, ok := prior.Nodes[n.Name]
		if !ok {
			continue
		}
		ready, reason, message := nodeCondition(n, corev1.NodeReady)
		switch {
		case was && ready != corev1.ConditionTrue:
			at, _ := k.nodeFailedAt(n.Name)
			k.emitResource("Node", "", n.Name, "node_not_ready", "critical", fmt.Sprintf("%s is NotReady", n.Name), at,
				missed(map[string]any{"reason": reason, "message": message, "status": string(ready)}))
		case !was && ready == corev1.ConditionTrue:
			k.emitResource("Node", "", n.Name, "became_ready", "info", fmt.Sprintf("%s is Ready", n.Name), time.Now().UTC(), missed(map[string]any{}))
		}
	}
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
		payload = k.withGitOps(d, payload)
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
	relatedObjects := func() ([]*corev1.Service, []*corev1.ConfigMap, []*autoscalingv2.HorizontalPodAutoscaler, []*corev1.Node) {
		svcs, _ := factory.Core().V1().Services().Lister().List(labels.Everything())
		cms, _ := factory.Core().V1().ConfigMaps().Lister().List(labels.Everything())
		hpas, _ := factory.Autoscaling().V2().HorizontalPodAutoscalers().Lister().List(labels.Everything())
		nodes, _ := factory.Core().V1().Nodes().Lister().List(labels.Everything())
		return svcs, cms, hpas, nodes
	}
	if prior, err := k.checkpoints.Load(ctx); err != nil {
		slog.Warn("could not read the collector checkpoint; changes made while Chronicle was down will be missed", "err", err)
	} else if prior != nil {
		ds, ps := list()
		k.catchUp(prior, ds, ps)
		svcs, cms, hpas, nodes := relatedObjects()
		k.catchUpRelated(prior, svcs, cms, hpas, nodes)
	}
	save := func() {
		ds, ps := list()
		cp := snapshotOf(ds, ps, time.Now().UTC())
		svcs, cms, hpas, nodes := relatedObjects()
		cp.related(svcs, cms, hpas, nodes, ps)
		if err := k.checkpoints.Save(ctx, cp); err != nil && ctx.Err() == nil {
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
