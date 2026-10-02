package collect

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

func collectorWithOut() (*K8sCollector, chan event.Event) {
	out := make(chan event.Event, 64)
	return &K8sCollector{BaseCollector: BaseCollector{Out: out}}, out
}

func deployState(replicas, ready int32, image string, env ...corev1.EnvVar) *appsv1.Deployment {
	d := deployment(image, env...)
	d.Spec.Replicas = &replicas
	d.Status.ReadyReplicas = ready
	return d
}

func livePod(name string, ready bool, restarts int32) *corev1.Pod {
	p := pod(corev1.ContainerStatus{Name: "api", RestartCount: restarts})
	p.Name, p.Namespace = name, "default"
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
	return p
}

func types(events []event.Event) map[string]int {
	got := map[string]int{}
	for _, e := range events {
		got[e.Type]++
	}
	return got
}

func TestCheckpointSurvivesAConfigMapRoundTrip(t *testing.T) {
	store := configMapCheckpoint{client: fake.NewSimpleClientset(), namespace: "chronicle"}
	if cp, err := store.Load(context.Background()); err != nil || cp != nil {
		t.Fatalf("a first run has nothing to load: %v %v", cp, err)
	}
	want := snapshotOf([]*appsv1.Deployment{deployState(1, 1, "api:1")}, []*corev1.Pod{livePod("api-1", true, 2)}, time.Now().UTC())
	for i := 0; i < 2; i++ { // create, then update
		if err := store.Save(context.Background(), want); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Load(context.Background())
	if err != nil || got.Deployments["default/api"].Image != "api:1" || got.Pods["default/api-1"].Restarts["api"] != 2 {
		t.Fatalf("round trip lost state: %+v %v", got, err)
	}
}

// What changed while no leader was running is reported when one starts.
func TestCatchUpReportsChangesMadeWhileNobodyWasWatching(t *testing.T) {
	k, out := collectorWithOut()
	before := snapshotOf(
		[]*appsv1.Deployment{deployState(1, 1, "api:1", corev1.EnvVar{Name: "REDIS_URL", Value: "redis:6379"})},
		[]*corev1.Pod{livePod("api-1", false, 0)}, time.Now().Add(-time.Minute))

	now := deployState(0, 0, "api:2", corev1.EnvVar{Name: "REDIS_URL", Value: "typo:6379"})
	k.catchUp(before, []*appsv1.Deployment{now}, []*corev1.Pod{livePod("api-1", true, 1)})

	got := types(drain(out))
	for _, want := range []string{"deploy", "config_change", "scale", "resource_status", "became_ready", "container_restart"} {
		if got[want] == 0 {
			t.Errorf("a missed %s was not reported: %v", want, got)
		}
	}
}

func TestCatchUpIsSilentWhenNothingChanged(t *testing.T) {
	k, out := collectorWithOut()
	ds, ps := []*appsv1.Deployment{deployState(1, 1, "api:1")}, []*corev1.Pod{livePod("api-1", true, 0)}
	k.catchUp(snapshotOf(ds, ps, time.Now()), ds, ps)
	if events := drain(out); len(events) != 0 {
		t.Fatalf("an unchanged cluster must not emit: %+v", events)
	}
}

func TestCatchUpReportsDeletionsAndLateCreations(t *testing.T) {
	k, out := collectorWithOut()
	checkpointAt := time.Now().Add(-time.Minute)
	before := snapshotOf([]*appsv1.Deployment{deployState(1, 1, "api:1")}, []*corev1.Pod{livePod("gone-1", true, 0)}, checkpointAt)

	fresh := livePod("fresh-1", false, 0)
	fresh.CreationTimestamp = metav1.NewTime(checkpointAt.Add(20 * time.Second))
	old := livePod("old-1", true, 0)
	old.CreationTimestamp = metav1.NewTime(checkpointAt.Add(-time.Hour)) // long running; just not in the checkpoint

	// The collector started after the pod was created, as a new leader does.
	k.startedAt = time.Now()
	k.catchUp(before, nil, []*corev1.Pod{fresh, old})
	events := drain(out)
	got := types(events)
	if got["resource_deleted"] != 2 { // the Deployment and the Pod
		t.Errorf("missed deletions: %v", got)
	}
	if got["resource_created"] != 1 {
		t.Errorf("only the pod created after the checkpoint is new: %v", got)
	}
	for _, e := range events {
		if e.Type == "resource_created" && (e.EntityName != "fresh-1" || gjsonString(e, "missed") != "true") {
			t.Errorf("the late creation should be reported and marked: %+v", e)
		}
	}
}

// A first run has no checkpoint, so nothing is invented.
func TestAFirstRunReportsNothing(t *testing.T) {
	k, out := collectorWithOut()
	k.checkpoints = configMapCheckpoint{client: fake.NewSimpleClientset(), namespace: "chronicle"}
	if prior, _ := k.checkpoints.Load(context.Background()); prior != nil {
		t.Fatal("expected no checkpoint")
	}
	if len(drain(out)) != 0 {
		t.Fatal("no events expected")
	}
}

func TestConfigFingerprintTracksEverythingConfigChangesLooksAt(t *testing.T) {
	base := deployState(1, 1, "api:1", corev1.EnvVar{Name: "A", Value: "1"})
	if configFingerprint(base) != configFingerprint(deployState(1, 1, "api:2", corev1.EnvVar{Name: "A", Value: "1"})) {
		t.Error("an image change is not a config change")
	}
	if configFingerprint(base) == configFingerprint(deployState(1, 1, "api:1", corev1.EnvVar{Name: "A", Value: "2"})) {
		t.Error("a changed value must change the fingerprint")
	}
}

func TestConfigChangeEventsCarryFingerprintsNotValues(t *testing.T) {
	old := deployment("api:1", corev1.EnvVar{Name: "REDIS_URL", Value: "redis:6379"})
	new := deployment("api:1", corev1.EnvVar{Name: "REDIS_URL", Value: "redis-typo:6379"})
	events := emittedBy(old, new)
	if len(events) != 1 {
		t.Fatalf("expected one event, got %+v", events)
	}
	from, to := gjsonString(events[0], "from_hash"), gjsonString(events[0], "to_hash")
	if from == "" || to == "" || from == to {
		t.Fatalf("both fingerprints should be present and differ: %q %q", from, to)
	}
	if from != configFingerprint(old) || to != configFingerprint(new) {
		t.Error("fingerprints should match the deployments they describe")
	}
}

func TestCatchUpReportsChangesToRelatedObjects(t *testing.T) {
	k, out := collectorWithOut()
	svcBefore := service(map[string]string{"app": "redis"}, 6379)
	cmBefore := configMap("api-config", map[string]string{"A": "1"})
	hpaBefore := hpa(5)
	cp := &checkpoint{At: time.Now().Add(-time.Minute)}
	cp.related([]*corev1.Service{svcBefore}, []*corev1.ConfigMap{cmBefore}, []*autoscalingv2.HorizontalPodAutoscaler{hpaBefore},
		[]*corev1.Node{node(corev1.ConditionTrue)}, []*corev1.Pod{podUsing("api-config")})

	k.catchUpRelated(cp,
		[]*corev1.Service{service(map[string]string{"app": "elsewhere"}, 6379)},
		[]*corev1.ConfigMap{configMap("api-config", map[string]string{"A": "2"})},
		[]*autoscalingv2.HorizontalPodAutoscaler{hpa(1)},
		[]*corev1.Node{node(corev1.ConditionFalse)})

	got := types(drain(out))
	for _, want := range []string{"service_change", "config_change", "hpa_change", "node_not_ready"} {
		if got[want] != 1 {
			t.Errorf("a missed %s was not reported once: %v", want, got)
		}
	}
}

func TestCatchUpIsSilentForUnchangedRelatedObjects(t *testing.T) {
	k, out := collectorWithOut()
	svc, cm, h, n := service(map[string]string{"app": "redis"}, 6379), configMap("api-config", map[string]string{"A": "1"}), hpa(5), node(corev1.ConditionTrue)
	cp := &checkpoint{}
	cp.related([]*corev1.Service{svc}, []*corev1.ConfigMap{cm}, []*autoscalingv2.HorizontalPodAutoscaler{h}, []*corev1.Node{n}, []*corev1.Pod{podUsing("api-config")})
	k.catchUpRelated(cp, []*corev1.Service{svc}, []*corev1.ConfigMap{cm}, []*autoscalingv2.HorizontalPodAutoscaler{h}, []*corev1.Node{n})
	if events := drain(out); len(events) != 0 {
		t.Fatalf("nothing changed: %+v", events)
	}
}
