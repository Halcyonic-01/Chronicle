package heal

// Regression tests for the healing safety review (issues 1-4, 7, 8, 10-13 and
// the GitOps handling). Dry-run and fake clients only: nothing here touches a
// cluster.

import (
	"context"
	"strings"
	"testing"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func unreadyPod(name, replicaSet string) *corev1.Pod {
	yes := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid"),
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: replicaSet, Controller: &yes}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}},
	}
}

func leading(e event.Event, confidence float64) *rca.Result {
	return &rca.Result{Symptom: event.Event{ID: "sym-" + e.ID, EntityName: "frontend"}, Confidence: confidence,
		Verdict: rca.VerdictRootCause, Candidates: []rca.Candidate{{Event: e}}}
}

// Issue 1: a refused decision is never approvable, whatever the rule says.
func TestARefusedDecisionIsNeverAwaitingApproval(t *testing.T) {
	cause := event.Event{ID: "c1", Namespace: "default", EntityKind: "Deployment", EntityName: "cache", Type: "scale",
		Payload: []byte(`{"old_replicas":3,"new_replicas":1}`)}
	a, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), leading(cause, 0.95))
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != StatusSkipped || a.Approval != ApprovalNotRequired || a.Proposed || a.ExpiresAt != nil {
		t.Fatalf("a refused decision must not await approval: %+v", a)
	}
}

// Issue 11: there is no automatic execution, so a rule that does not ask for
// approval still gets one.
func TestEveryProposalNeedsApproval(t *testing.T) {
	engine := NewEngine(&memoryAudit{})
	for i := range engine.Rules {
		engine.Rules[i].RequireApprove = false
	}
	cause := event.Event{ID: "c2", Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale",
		Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	a, err := engine.Evaluate(context.Background(), leading(cause, 0.95))
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != StatusWouldRun || a.Approval != ApprovalPending {
		t.Fatalf("a proposal must wait for approval: %+v", a)
	}
	for _, r := range defaultRules {
		if !r.RequireApprove {
			t.Errorf("default rule %s claims it runs without approval", r.Name)
		}
	}
}

type causeMemory struct {
	memoryAudit
	byCause map[string]string
}

func (c *causeMemory) ProposalForCause(_ context.Context, cause string) (string, string, bool, error) {
	id, ok := c.byCause[cause]
	return id, "pending_approval", ok, nil
}

// Issue 3: the second symptom of one outage does not queue a second approval.
func TestOneProposalPerOutage(t *testing.T) {
	store := &causeMemory{byCause: map[string]string{"scale-1": "first-decision"}}
	cause := event.Event{ID: "scale-1", Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale",
		Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	a, err := NewEngine(store).Evaluate(context.Background(), leading(cause, 0.95))
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != StatusSkipped || a.Proposed || !strings.Contains(a.Result, "first-decision") {
		t.Fatalf("a duplicate proposal for one outage was planned: %+v", a)
	}
	other := cause
	other.ID = "scale-2"
	if a, _ := NewEngine(store).Evaluate(context.Background(), leading(other, 0.95)); a.Status != StatusWouldRun {
		t.Fatalf("a different outage must still be proposed: %+v", a)
	}
}

// Issue 8: the collector never records the original limit, so the executor
// would always refuse; the engine must not plan it.
func TestAnUnboundedMemoryBumpIsNotPlanned(t *testing.T) {
	cause := event.Event{ID: "oom-1", Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "oom_kill",
		Payload: []byte(`{"container":"api","reason":"OOMKilled","owner":"api"}`)}
	a, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), leading(cause, 0.99))
	if err != nil {
		t.Fatal(err)
	}
	if a.Status == StatusWouldRun {
		t.Fatalf("planned a bump the executor must refuse: %+v", a)
	}
}

// GitOps: a workload whose desired state lives in Git gets a proposal to
// revert there, never a write.
func TestGitOpsManagedCausesGetAProposalNotAnAction(t *testing.T) {
	cases := []event.Event{
		{ID: "d1", Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "deploy",
			Payload: []byte(`{"old_image":"api:v1","new_image":"api:v2","gitops":"argocd:shop"}`)},
		{ID: "d2", Namespace: "argocd", EntityKind: "Application", EntityName: "shop", Type: "deploy",
			Payload: []byte(`{"revision":"abc123"}`)},
		{ID: "s1", Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale",
			Payload: []byte(`{"old_replicas":1,"new_replicas":0,"gitops":"flux:apps"}`)},
	}
	for _, cause := range cases {
		a, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), leading(cause, 0.99))
		if err != nil {
			t.Fatal(err)
		}
		if a.Status == StatusWouldRun || !strings.Contains(a.Result, "Git") {
			t.Errorf("%s: GitOps-managed cause was planned for execution: %s (%s)", cause.ID, a.Status, a.Result)
		}
	}
}

func deploymentWithRevision(revision string, containers ...corev1.Container) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "default", UID: "api-uid",
			Annotations: map[string]string{revisionAnnotation: revision}},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec: corev1.PodSpec{Containers: containers}}},
	}
}

func replicaSetFor(d *appsv1.Deployment, name, revision string, containers ...corev1.Container) *appsv1.ReplicaSet {
	yes := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": "api"},
			Annotations:     map[string]string{revisionAnnotation: revision},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: d.Name, UID: d.UID, Controller: &yes}}},
		Spec: appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api", appsv1.DefaultDeploymentUniqueLabelKey: "h-" + revision}},
			Spec:       corev1.PodSpec{Containers: containers}}},
	}
}

func rollbackAction() *Action {
	return &Action{ActionType: ActionRollbackDeployment, Namespace: "default", Target: "api", Approval: ApprovalApproved,
		Payload: []byte(`{"old_image":"api:v1","new_image":"api:v2"}`)}
}

// Issue 7: the rollback restores the whole previous pod template -- env,
// resources and every container -- not just the first image.
func TestRollbackRestoresThePreviousPodTemplate(t *testing.T) {
	good := []corev1.Container{
		{Name: "api", Image: "api:v1", Env: []corev1.EnvVar{{Name: "REDIS_URL", Value: "redis:6379"}},
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}}},
		{Name: "metrics", Image: "exporter:1"},
	}
	bad := []corev1.Container{
		{Name: "api", Image: "api:v2", Env: []corev1.EnvVar{{Name: "REDIS_URL", Value: "redis-typo:6379"}},
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Mi")}}},
		{Name: "metrics", Image: "exporter:2"},
	}
	d := deploymentWithRevision("2", bad...)
	client := fake.NewSimpleClientset(d, replicaSetFor(d, "api-old", "1", good...), replicaSetFor(d, "api-new", "2", bad...))
	e := &KubernetesExecutor{Client: client}
	if _, err := e.rollbackDeployment(context.Background(), rollbackAction()); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
	c := got.Spec.Template.Spec.Containers
	if len(c) != 2 || c[0].Image != "api:v1" || c[0].Env[0].Value != "redis:6379" || c[1].Image != "exporter:1" {
		t.Fatalf("the previous template was not restored: %+v", c)
	}
	if mem := c[0].Resources.Limits[corev1.ResourceMemory]; mem.String() != "256Mi" {
		t.Fatalf("resources were not restored: %s", mem.String())
	}
	if _, ok := got.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey]; ok {
		t.Fatal("the ReplicaSet's hash label must not be copied into the Deployment")
	}
}

// Issue 2/7: a newer deploy since the decision is never overwritten.
func TestRollbackRefusesWhenANewerRevisionIsRunning(t *testing.T) {
	d := deploymentWithRevision("3", corev1.Container{Name: "api", Image: "api:v3"})
	client := fake.NewSimpleClientset(d,
		replicaSetFor(d, "api-1", "1", corev1.Container{Name: "api", Image: "api:v1"}),
		replicaSetFor(d, "api-2", "2", corev1.Container{Name: "api", Image: "api:v2"}))
	e := &KubernetesExecutor{Client: client}
	if _, err := e.rollbackDeployment(context.Background(), rollbackAction()); err == nil {
		t.Fatal("rolled back over a newer deploy")
	}
	got, _ := client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
	if got.Spec.Template.Spec.Containers[0].Image != "api:v3" {
		t.Fatal("the newer desired state was overwritten")
	}
}

// The history v1 -> v2 (bad) -> v1 (someone rolled back) -> v4 leaves v1 as the
// previous revision, so only the "still running the bad image" check stops a
// stale decision from replacing v4 with v1.
func TestRollbackNeverOverwritesALaterDeploy(t *testing.T) {
	d := deploymentWithRevision("4", corev1.Container{Name: "api", Image: "api:v4"})
	client := fake.NewSimpleClientset(d,
		replicaSetFor(d, "api-v2", "2", corev1.Container{Name: "api", Image: "api:v2"}),
		replicaSetFor(d, "api-v1", "3", corev1.Container{Name: "api", Image: "api:v1"}),
		replicaSetFor(d, "api-v4", "4", corev1.Container{Name: "api", Image: "api:v4"}))
	if _, err := (&KubernetesExecutor{Client: client}).rollbackDeployment(context.Background(), rollbackAction()); err == nil {
		t.Fatal("a stale rollback replaced a later deploy")
	}
	got, _ := client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
	if got.Spec.Template.Spec.Containers[0].Image != "api:v4" {
		t.Fatalf("the later deploy was overwritten with %s", got.Spec.Template.Spec.Containers[0].Image)
	}
}

// A retry after the update landed must not fail or change anything.
func TestRollbackIsIdempotent(t *testing.T) {
	d := deploymentWithRevision("3", corev1.Container{Name: "api", Image: "api:v1"})
	client := fake.NewSimpleClientset(d)
	got, err := (&KubernetesExecutor{Client: client}).rollbackDeployment(context.Background(), rollbackAction())
	if err != nil || !strings.Contains(got, "no change") {
		t.Fatalf("expected a no-op, got %q, %v", got, err)
	}
}

// The executor re-reads the object: a GitOps marker added since the event is
// still honoured.
func TestExecutorRefusesGitOpsManagedWorkloads(t *testing.T) {
	d := deploymentWithRevision("2", corev1.Container{Name: "api", Image: "api:v2"})
	d.Annotations["argocd.argoproj.io/tracking-id"] = "shop:apps/Deployment:default/api"
	zero := int32(0)
	scaled := scaledDeployment("redis", 0)
	scaled.Labels = map[string]string{"kustomize.toolkit.fluxcd.io/name": "apps"}
	scaled.Spec.Replicas = &zero
	client := fake.NewSimpleClientset(d, scaled)
	e := &KubernetesExecutor{Client: client}
	if _, err := e.rollbackDeployment(context.Background(), rollbackAction()); err == nil || !strings.Contains(err.Error(), "Argo CD") {
		t.Fatalf("rolled back an Argo CD application's workload: %v", err)
	}
	if _, err := e.restoreReplicas(context.Background(), restoreAction(`{"old_replicas":2,"new_replicas":0}`)); err == nil {
		t.Fatal("restored replicas on a Flux-managed workload")
	}
}

// Issue 2/5: a restart re-checks the pod as it is now.
func TestRestartRevalidatesThePod(t *testing.T) {
	ready := unreadyPod("api-5f-ready", "api-5f")
	ready.Status.Conditions[0].Status = corev1.ConditionTrue
	bare := unreadyPod("debug", "")
	bare.OwnerReferences = nil
	client := fake.NewSimpleClientset(ready, bare, unreadyPod("api-5f-moved", "billing-7c"))
	e := &KubernetesExecutor{Client: client}
	cases := map[string]*Action{
		"gone":        {Target: "api-5f-missing", Payload: []byte(`{"owner":"api"}`)},
		"ready again": {Target: "api-5f-ready", Payload: []byte(`{"owner":"api"}`)},
		"bare pod":    {Target: "debug"},
		"new owner":   {Target: "api-5f-moved", Payload: []byte(`{"owner":"api"}`)},
	}
	for name, a := range cases {
		a.ActionType, a.Namespace = ActionRestartPod, "default"
		if _, err := e.restartPod(context.Background(), a); err == nil {
			t.Errorf("%s: the pod was deleted", name)
		}
	}
	for _, name := range []string{"api-5f-ready", "debug", "api-5f-moved"} {
		if _, err := client.CoreV1().Pods("default").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
			t.Errorf("pod %s was deleted: %v", name, err)
		}
	}
}

// Issue 8: the OOM-killed container is resized, not whichever comes first.
func TestBumpMemoryResizesTheKilledContainer(t *testing.T) {
	limit := func(v string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(v)}}
	}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "default"},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "sidecar", Resources: limit("64Mi")}, {Name: "api", Resources: limit("100Mi")}}}}}}
	client := fake.NewSimpleClientset(d)
	e := NewKubernetesExecutor(client)
	a := &Action{ActionType: ActionBumpMemory, Namespace: "default", Target: "api-1", Approval: ApprovalApproved,
		Payload: []byte(`{"owner":"api","container":"api","original_mem_bytes":104857600}`)}
	if _, err := e.bumpMemory(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().Deployments("default").Get(context.Background(), "api", metav1.GetOptions{})
	side := got.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
	main := got.Spec.Template.Spec.Containers[1].Resources.Limits[corev1.ResourceMemory]
	if side.String() != "64Mi" || main.Value() != 157286400 {
		t.Fatalf("wrong container resized: sidecar=%s api=%d", side.String(), main.Value())
	}
	a.Payload = []byte(`{"owner":"api","container":"missing","original_mem_bytes":104857600}`)
	if _, err := e.bumpMemory(context.Background(), a); err == nil {
		t.Fatal("an unknown container must be refused, not guessed")
	}
}

// A repeat of a restart for a pod that is already terminating (seen against a
// real cluster) must not claim a fresh deletion.
func TestRestartOfATerminatingPodIsANoOp(t *testing.T) {
	pod := unreadyPod("api-5f-old", "api-5f")
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	client := fake.NewSimpleClientset(pod)
	got, err := (&KubernetesExecutor{Client: client}).restartPod(context.Background(),
		&Action{ActionType: ActionRestartPod, Namespace: "default", Target: "api-5f-old", Payload: []byte(`{"owner":"api"}`)})
	if err != nil || !strings.Contains(got, "already being deleted") {
		t.Fatalf("expected a no-op, got %q, %v", got, err)
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatal("a second delete was issued for a terminating pod")
		}
	}
}

// A Helm release carries the same instance label as an Argo CD Application. The
// executor refuses only the real Application; refusing a Helm release because
// Argo CD happens to be installed would silently disable healing for it.
func TestExecutorTellsAnArgoApplicationFromAHelmRelease(t *testing.T) {
	var apps graph.ApplicationSet
	apps.Replace([]string{"shop"})
	scaledTo0 := func(name, instance string) *appsv1.Deployment {
		d := scaledDeployment(name, 0)
		d.Labels = map[string]string{"app.kubernetes.io/instance": instance}
		return d
	}
	client := fake.NewSimpleClientset(scaledTo0("redis", "shop"), scaledTo0("cache", "monitoring"))
	e := &KubernetesExecutor{Client: client, Applications: apps.Has}
	action := func(target string) *Action {
		a := restoreAction(`{"old_replicas":2,"new_replicas":0}`)
		a.Target = target
		return a
	}
	if _, err := e.restoreReplicas(context.Background(), action("redis")); err == nil || !strings.Contains(err.Error(), "Argo CD") {
		t.Fatalf("a workload of a real Argo CD application must be refused, got %v", err)
	}
	if _, err := e.restoreReplicas(context.Background(), action("cache")); err != nil {
		t.Fatalf("a Helm release must not be refused because Argo CD is installed: %v", err)
	}
	// Without a set (Argo CD not in use) the label means nothing.
	plain := &KubernetesExecutor{Client: fake.NewSimpleClientset(scaledTo0("redis", "shop"))}
	if _, err := plain.restoreReplicas(context.Background(), action("redis")); err != nil {
		t.Fatalf("with Argo CD not in use the instance label is not a marker: %v", err)
	}
}
