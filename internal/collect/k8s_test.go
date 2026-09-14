package collect

import (
	"testing"

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
