package collect

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	corev1 "k8s.io/api/core/v1"

	"github.com/tidwall/gjson"
)

func gjsonString(e event.Event, path string) string {
	return gjson.GetBytes(e.Payload, path).String()
}

func pod(statuses ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: statuses,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func status(name string, restarts int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, RestartCount: restarts, Ready: true}
}

func drain(out chan event.Event) []event.Event {
	var events []event.Event
	for {
		select {
		case e := <-out:
			events = append(events, e)
		default:
			return events
		}
	}
}

// Kubernetes does not promise a stable order for container statuses, so a
// reordered list must not read as a restart of whichever container moved.
func TestDiffPodsAttributesRestartsByContainerName(t *testing.T) {
	out := make(chan event.Event, 8)
	collector := NewK8sCollector(nil, out)

	old := pod(status("app", 0), status("sidecar", 3))
	reordered := pod(status("sidecar", 3), status("app", 0))
	collector.diffPods(old, reordered)
	if events := drain(out); len(events) != 0 {
		t.Fatalf("reordering container statuses is not a restart, got %d event(s): %+v", len(events), events)
	}

	restarted := pod(status("sidecar", 3), status("app", 1))
	collector.diffPods(old, restarted)
	events := drain(out)
	if len(events) != 1 {
		t.Fatalf("expected exactly one restart event, got %d", len(events))
	}
	if events[0].Type != "container_restart" {
		t.Fatalf("unexpected event type %q", events[0].Type)
	}
	if got := gjsonString(events[0], "container"); got != "app" {
		t.Fatalf("restart attributed to container %q, want \"app\"", got)
	}
}

func TestDiffPodsIgnoresContainersMissingFromThePreviousStatus(t *testing.T) {
	out := make(chan event.Event, 8)
	collector := NewK8sCollector(nil, out)
	collector.diffPods(pod(status("app", 0)), pod(status("app", 0), status("new", 2)))
	for _, e := range drain(out) {
		if e.Type == "container_restart" || e.Type == "oom_kill" {
			t.Fatalf("a newly reported container is not a restart: %+v", e)
		}
	}
}

func TestExtractSHAOnlyAcceptsCommitLikeTags(t *testing.T) {
	cases := map[string]string{
		"ghcr.io/app:9f2c1ab77d3e4b5a":        "9f2c1ab77d3e4b5a",
		"ghcr.io/app:sha256-9f2c1ab77d3e4b5a": "9f2c1ab77d3e4b5a",
		"ghcr.io/app:v1.2.3-alpine12":         "",
		"ghcr.io/app:latest":                  "",
		"ghcr.io/app:deadbeef":                "", // real hex, but too short to correlate
		"ghcr.io/app":                         "",
	}
	for image, want := range cases {
		if got := extractSHA(image); got != want {
			t.Errorf("extractSHA(%q) = %q, want %q", image, got, want)
		}
	}
}

// The informer sees a change some time after it happened. The API server
// records when each field manager last wrote, which is the real change time.
func TestSpecChangeTimeUsesTheFieldManagerNotTheStatusWriter(t *testing.T) {
	// Relative to now, because a field-manager timestamp older than the age
	// bound cannot describe the change being emitted.
	specWrite := metav1.NewTime(time.Now().Add(-20 * time.Second))
	statusWrite := metav1.NewTime(time.Now().Add(-19 * time.Second))
	older := metav1.NewTime(time.Now().Add(-90 * time.Second))
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: "worker", Namespace: "default",
		ManagedFields: []metav1.ManagedFieldsEntry{
			{Manager: "kubectl-client-side-apply", Time: &older},
			{Manager: "kubectl-set", Time: &specWrite},
			// The controller writing rollout progress is not a change to the
			// deployment, and it always lands last.
			{Manager: "kube-controller-manager", Subresource: "status", Time: &statusWrite},
		},
	}}
	at, manager := specChangeTime(deployment)
	if !at.Equal(specWrite.Time.UTC()) {
		t.Fatalf("expected the spec write at %v, got %v", specWrite.Time.UTC(), at)
	}
	if manager != "kubectl-set" {
		t.Fatalf("expected the client that made the change, got %q", manager)
	}
}

func TestSpecChangeTimeIsZeroWhenUnavailable(t *testing.T) {
	at, manager := specChangeTime(&appsv1.Deployment{})
	if !at.IsZero() || manager != "" {
		t.Fatalf("with no managed fields there is nothing to report, got %v %q", at, manager)
	}
}

// "kubectl scale" goes through the /scale subresource, which leaves no managed
// field entry of its own. The newest remaining entry is then whatever last
// applied the manifest — hours earlier — and reporting it dated every scale
// event to the original deploy.
func TestSpecChangeTimeRejectsAStaleManagerTimestamp(t *testing.T) {
	stale := metav1.NewTime(time.Now().Add(-5 * time.Hour))
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl-client-side-apply", Time: &stale}},
	}}
	if at, manager := specChangeTime(deployment); !at.IsZero() || manager != "" {
		t.Fatalf("a five-hour-old apply cannot describe the change being emitted now, got %v %q", at, manager)
	}
}

// A scale recorded against the subresource is still a real spec change.
func TestSpecChangeTimeAcceptsTheScaleSubresource(t *testing.T) {
	recent := metav1.NewTime(time.Now().Add(-10 * time.Second))
	older := metav1.NewTime(time.Now().Add(-4 * time.Hour))
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		ManagedFields: []metav1.ManagedFieldsEntry{
			{Manager: "kubectl-client-side-apply", Time: &older},
			{Manager: "kubectl-scale", Subresource: "scale", Time: &recent},
		},
	}}
	at, manager := specChangeTime(deployment)
	if !at.Equal(recent.Time.UTC()) || manager != "kubectl-scale" {
		t.Fatalf("expected the scale write, got %v %q", at, manager)
	}
}
