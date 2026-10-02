// Package benchdata holds the controlled incidents used to measure RCA accuracy.
//
// Every incident is a deterministic event stream with a known injected cause, on
// a synthetic topology that mirrors the victim application. It is shared by the
// RCA and healing benchmarks.
//
// What the numbers mean: accuracy on THIS controlled benchmark, written by the
// same people who wrote the analyzer. Variants of one fault class are not
// independent samples, and nothing here measures production accuracy.
package benchdata

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// TruthRef names the injected cause of an incident.
type TruthRef struct{ Type, Entity string }

// Incident is one controlled failure with its ground truth.
type Incident struct {
	Name, Group, Fault string
	Events             []event.Event
	Symptom            event.Event
	Truths             []TruthRef // empty: the cause is genuinely not observable
	AllRequired        bool       // several independent causes: every one should be found
	Ambiguous          bool       // the evidence cannot separate the truths
	// Edges overrides the default topology for incidents on another application.
	Edges []graph.Edge
}

// --- observability profiles ---------------------------------------------

// BaselineObserved is what the collectors emitted when the benchmark was
// written. The improved profile adds the event types the new collectors emit.
var BaselineObserved = map[string]bool{
	"resource_created": true, "resource_deleted": true, "resource_status": true,
	"became_ready": true, "became_unready": true, "container_restart": true,
	"oom_kill": true, "k8s_event": true, "deploy": true, "scale": true,
	"resource_change": true, "config_change": true,
	"error_spike": true, "error_spike_resolved": true,
	"latency_spike": true, "latency_spike_resolved": true,
	"log_error": true,
}

var ImprovedExtra = map[string]bool{
	"service_change": true, "node_not_ready": true, "node_pressure": true, "hpa_change": true,
}

// Observed reports whether the collectors of a profile ("baseline" or
// "improved") would have emitted the event.
func Observed(profile string, e event.Event) bool {
	if BaselineObserved[e.Type] {
		// The baseline emitted config_change for Deployments only.
		return e.Type != "config_change" || e.EntityKind == "Deployment" || profile == "improved"
	}
	return profile == "improved" && ImprovedExtra[e.Type]
}

// --- topology -------------------------------------------------------------

func node(kind, name string) graph.Node {
	ns := "default"
	if kind == "Node" {
		ns = ""
	}
	return graph.Node{Namespace: ns, Kind: kind, Name: name}
}

// benchEdges mirrors the victim application: frontend -> api -> redis / worker,
// two generations of each pod (so a rollout has somewhere to land), two nodes,
// a ConfigMap, an Ingress and the mesh's deployment-level call edges.
func Edges() []graph.Edge {
	var edges []graph.Edge
	add := func(from, to graph.Node, kind string) {
		edges = append(edges, graph.Edge{From: from, To: to, Kind: kind, Weight: 1, Source: "bench"})
	}
	pods := map[string][]string{"frontend": {"frontend-1"}, "api": {"api-1", "api-2"}, "redis": {"redis-1", "redis-2"}, "worker": {"worker-1", "worker-2"}}
	nodeOf := map[string]string{"frontend-1": "node-1", "api-1": "node-1", "api-2": "node-1", "redis-1": "node-2", "redis-2": "node-2", "worker-1": "node-2", "worker-2": "node-2"}
	for dep, ps := range pods {
		for _, p := range ps {
			add(node("Deployment", dep), node("Pod", p), "owns")
			add(node("Service", dep), node("Pod", p), "routes_to")
			add(node("Pod", p), node("Node", nodeOf[p]), "runs_on")
		}
	}
	for _, p := range pods["api"] {
		add(node("Pod", p), node("ConfigMap", "api-config"), "uses")
		add(node("Pod", p), node("Service", "redis"), "calls")
		add(node("Pod", p), node("Service", "worker"), "calls")
	}
	for _, p := range pods["frontend"] {
		add(node("Pod", p), node("Service", "api"), "calls")
	}
	add(node("Ingress", "web"), node("Service", "frontend"), "routes_to")
	add(node("Deployment", "frontend"), node("Deployment", "api"), "calls")
	add(node("Deployment", "api"), node("Deployment", "redis"), "calls")
	add(node("Deployment", "api"), node("Deployment", "worker"), "calls")
	return edges
}

// --- event construction ---------------------------------------------------

var T0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

type builder struct {
	name string
	n    int
	evs  []event.Event
}

var sourceLag = map[string]time.Duration{"k8s": time.Second, "prometheus": 5 * time.Second, "loki": 15 * time.Second}

func (b *builder) add(sec float64, source, kind, name, typ, sev, title, payload string) event.Event {
	b.n++
	at := T0.Add(time.Duration(sec * float64(time.Second)))
	ns := "default"
	if kind == "Node" {
		ns = ""
	}
	e := event.Event{
		ID: fmt.Sprintf("%s-%03d", b.name, b.n), OccurredAt: at, IngestedAt: at.Add(sourceLag[source]),
		Source: source, Namespace: ns, EntityKind: kind, EntityName: name,
		Type: typ, Severity: sev, Title: title, Payload: []byte(payload),
	}
	b.evs = append(b.evs, e)
	return e
}

func (b *builder) k8s(sec float64, kind, name, typ, sev, title, payload string) event.Event {
	return b.add(sec, "k8s", kind, name, typ, sev, title, payload)
}

func scaleP(from, to int) string {
	return fmt.Sprintf(`{"old_replicas":%d,"new_replicas":%d}`, from, to)
}

func (b *builder) logs(from, to, every float64, pod, title string) {
	for s := from; s <= to; s += every {
		b.add(s, "loki", "Pod", pod, "log_error", "warning", title, `{}`)
	}
}

// background adds events every incident shares: an unrelated deploy on a
// component outside the dependency path, an old harmless deploy, and an earlier
// outage that was fully recovered. They exist so that "the only change in the
// window" is never the answer by default.
func (b *builder) background() {
	b.k8s(-1800, "Deployment", "postgres", "deploy", "info", "postgres deployed: 16.1 -> 16.2", `{"old_image":"postgres:16.1","new_image":"postgres:16.2"}`)
	b.k8s(-7200, "Deployment", "frontend", "deploy", "info", "frontend deployed: v1 -> v2", `{"old_image":"frontend:v1","new_image":"frontend:v2"}`)
	b.k8s(-3600, "Deployment", "worker", "scale", "info", "worker scaled from 1 to 0", scaleP(1, 0))
	b.add(-3560, "prometheus", "Service", "frontend", "error_spike", "critical", "high_error_rate on frontend: 1.000", `{"value":1}`)
	b.k8s(-3300, "Deployment", "worker", "scale", "info", "worker scaled from 0 to 1", scaleP(0, 1))
	b.add(-3290, "prometheus", "Service", "frontend", "error_spike_resolved", "info", "high_error_rate resolved on frontend", `{}`)
}

// --- faults ---------------------------------------------------------------

// fault injects a cause and its direct cascade at t=0 and says how the failure
// looks downstream.
type fault struct {
	name, group string
	truths      []TruthRef
	failing     string // api | redis | worker | node: where the user-visible errors originate
	inject      func(b *builder)
	podSymptom  func(b *builder) event.Event
	allRequired bool
	ambiguous   bool
	variants    string // which symptom variants apply: A alert, B short logs, C long logs, D latency, E pod-level
	// analyseAt overrides when short variants are analysed, for faults whose
	// point is what happens after a later event (a repair).
	analyseAt float64
}

func crashLoop(b *builder, pod, owner, kind string, times ...float64) {
	for _, s := range times {
		b.k8s(s, "Pod", pod, kind, "warning", pod+" restarted (Error, exit 1)", fmt.Sprintf(`{"container":"x","reason":"Error","exit_code":1,"owner":%q}`, owner))
	}
}

func rollout(b *builder, dep, oldPod, newPod string) {
	b.k8s(1, "Deployment", dep, "resource_status", "warning", dep+" status is 0/1 replicas ready", `{"phase":"Pending"}`)
	b.k8s(1, "Pod", newPod, "resource_created", "info", newPod+" created", fmt.Sprintf(`{"owner":%q}`, dep))
	b.k8s(1, "Pod", oldPod, "resource_status", "critical", oldPod+" status is Failed", `{"phase":"Failed"}`)
	b.k8s(1, "Pod", oldPod, "became_unready", "warning", oldPod+" stopped serving traffic", fmt.Sprintf(`{"owner":%q}`, dep))
	b.k8s(2, "Pod", oldPod, "resource_deleted", "info", oldPod+" deleted", `{}`)
}

func faults() []fault {
	crashing := func(b *builder, dep, pod string) {
		rollout(b, dep, strings.Replace(pod, "-2", "-1", 1), pod)
		b.k8s(12, "Pod", pod, "k8s_event", "warning", "BackOff: Back-off restarting failed container", `{"reason":"BackOff"}`)
		crashLoop(b, pod, dep, "container_restart", 14, 35, 70, 130, 250, 550, 850, 1150, 1450)
		b.k8s(15, "Pod", pod, "became_unready", "warning", pod+" stopped serving traffic", fmt.Sprintf(`{"owner":%q}`, dep))
	}
	return []fault{
		{name: "bad_deploy", group: "known", failing: "api", variants: "ABCDE",
			truths: []TruthRef{{"deploy", "api"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "api", "deploy", "info", "api deployed: latest -> broken", `{"old_image":"api:latest","new_image":"api:broken"}`)
				crashing(b, "api", "api-2")
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(15, "Pod", "api-2", "became_unready", "warning", "api-2 stopped serving traffic", `{"owner":"api"}`)
			}},
		{name: "bad_config", group: "known", failing: "api", variants: "ABCDE",
			truths: []TruthRef{{"config_change", "api"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "api", "config_change", "info", "api configuration changed (api env REDIS_URL)", `{"changed":["api env REDIS_URL"],"from_hash":"aaa111","to_hash":"bbb222"}`)
				crashing(b, "api", "api-2")
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(15, "Pod", "api-2", "became_unready", "warning", "api-2 stopped serving traffic", `{"owner":"api"}`)
			}},
		{name: "resource_change", group: "known", failing: "worker", variants: "ABCDE",
			truths: []TruthRef{{"resource_change", "worker"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "worker", "resource_change", "info", "worker resource limits changed", `{"old_mem_limit":0,"new_mem_limit":8388608}`)
				rollout(b, "worker", "worker-1", "worker-2")
				for _, s := range []float64{20, 60, 140, 300, 620, 920, 1220, 1500} {
					b.k8s(s, "Pod", "worker-2", "oom_kill", "critical", "worker restarted (OOMKilled, exit 137)", `{"container":"worker","reason":"OOMKilled","exit_code":137,"owner":"worker"}`)
				}
				b.k8s(21, "Pod", "worker-2", "became_unready", "warning", "worker-2 stopped serving traffic", `{"owner":"worker"}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(20, "Pod", "worker-2", "oom_kill", "critical", "worker restarted (OOMKilled, exit 137)", `{"container":"worker","reason":"OOMKilled","exit_code":137,"owner":"worker"}`)
			}},
		{name: "oom_kill", group: "known", failing: "redis", variants: "ABCDE",
			truths: []TruthRef{{"oom_kill", "redis-1"}},
			inject: func(b *builder) {
				for _, s := range []float64{0, 90, 200, 380, 700, 1000, 1300, 1600} {
					b.k8s(s, "Pod", "redis-1", "oom_kill", "critical", "redis restarted (OOMKilled, exit 137)", `{"container":"redis","reason":"OOMKilled","exit_code":137,"owner":"redis"}`)
				}
				b.k8s(1, "Pod", "redis-1", "resource_status", "critical", "redis-1 status is Failed", `{"phase":"Failed"}`)
				b.k8s(1, "Pod", "redis-1", "became_unready", "warning", "redis-1 stopped serving traffic", `{"owner":"redis"}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "redis-1", "became_unready", "warning", "redis-1 stopped serving traffic", `{"owner":"redis"}`)
			}},
		{name: "scale_down", group: "known", failing: "redis", variants: "ABCDE",
			truths: []TruthRef{{"scale", "redis"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "redis", "scale", "info", "redis scaled from 1 to 0", scaleP(1, 0))
				b.k8s(1, "Deployment", "redis", "resource_status", "warning", "redis status is 0/0 replicas ready", `{"phase":"Pending"}`)
				b.k8s(1, "Pod", "redis-1", "resource_status", "critical", "redis-1 status is Failed", `{"phase":"Failed"}`)
				b.k8s(1, "Pod", "redis-1", "became_unready", "warning", "redis-1 stopped serving traffic", `{"owner":"redis"}`)
				b.k8s(2, "Pod", "redis-1", "resource_deleted", "info", "redis-1 deleted", `{}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "redis-1", "became_unready", "warning", "redis-1 stopped serving traffic", `{"owner":"redis"}`)
			}},
		{name: "container_restart", group: "known", failing: "api", variants: "ABCDE",
			truths: []TruthRef{{"container_restart", "api-1"}},
			inject: func(b *builder) {
				crashLoop(b, "api-1", "api", "container_restart", 0, 40, 85, 150, 280, 600, 900, 1200, 1500)
				b.k8s(1, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
				b.k8s(10, "Pod", "api-1", "k8s_event", "warning", "BackOff: Back-off restarting failed container", `{"reason":"BackOff"}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
			}},
		{name: "pod_unready", group: "known", failing: "worker", variants: "ABCDE",
			truths: []TruthRef{{"became_unready", "worker-1"}},
			inject: func(b *builder) {
				b.k8s(0, "Pod", "worker-1", "became_unready", "warning", "worker-1 stopped serving traffic", `{"owner":"worker"}`)
				b.k8s(0, "Pod", "worker-1", "resource_status", "warning", "worker-1 status is Pending", `{"phase":"Pending"}`)
				b.k8s(1, "Pod", "worker-1", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "worker-1", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
			}},

		// A repair arrives while the failure is still draining: the break, not
		// the repair, is the cause.
		{name: "repair_rollback", group: "repair", failing: "api", variants: "ABDE", analyseAt: 85,
			truths: []TruthRef{{"deploy", "api"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "api", "deploy", "info", "api deployed: latest -> broken", `{"old_image":"api:latest","new_image":"api:broken"}`)
				crashing(b, "api", "api-2")
				b.k8s(70, "Deployment", "api", "deploy", "info", "api deployed: broken -> latest", `{"old_image":"api:broken","new_image":"api:latest"}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(75, "Pod", "api-2", "became_unready", "warning", "api-2 stopped serving traffic", `{"owner":"api"}`)
			}},
		// A config break was restored, and a different failure begins soon after.
		{name: "restored_config_then_new_fault", group: "repair", failing: "worker", variants: "ADB",
			truths: []TruthRef{{"scale", "worker"}},
			inject: func(b *builder) {
				b.k8s(-200, "Deployment", "api", "config_change", "info", "api configuration changed (api env REDIS_URL)", `{"changed":["api env REDIS_URL"],"from_hash":"aaa111","to_hash":"bbb222"}`)
				b.k8s(-190, "Pod", "api-1", "resource_status", "critical", "api-1 status is Failed", `{"phase":"Failed"}`)
				b.k8s(-150, "Deployment", "api", "config_change", "info", "api configuration changed (api env REDIS_URL)", `{"changed":["api env REDIS_URL"],"from_hash":"bbb222","to_hash":"aaa111"}`)
				b.k8s(0, "Deployment", "worker", "scale", "info", "worker scaled from 1 to 0", scaleP(1, 0))
				b.k8s(1, "Pod", "worker-1", "became_unready", "warning", "worker-1 stopped serving traffic", `{"owner":"worker"}`)
				b.k8s(2, "Pod", "worker-1", "resource_deleted", "info", "worker-1 deleted", `{}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "worker-1", "became_unready", "warning", "worker-1 stopped serving traffic", `{"owner":"worker"}`)
			}},

		// Competing changes.
		{name: "deploy_with_benign_upstream_change", group: "competing", failing: "api", variants: "ABD",
			truths: []TruthRef{{"deploy", "api"}},
			inject: func(b *builder) {
				b.k8s(-20, "Deployment", "worker", "config_change", "info", "worker configuration changed (worker env LOG_LEVEL)", `{"changed":["worker env LOG_LEVEL"],"from_hash":"c1","to_hash":"c2"}`)
				b.k8s(0, "Deployment", "api", "deploy", "info", "api deployed: latest -> broken", `{"old_image":"api:latest","new_image":"api:broken"}`)
				crashing(b, "api", "api-2")
			}},
		{name: "deploy_config_oom_and_errors", group: "competing", failing: "api", variants: "ABD",
			truths: []TruthRef{{"deploy", "api"}},
			inject: func(b *builder) {
				b.k8s(-600, "Deployment", "api", "config_change", "info", "api configuration changed (api env CACHE_TTL)", `{"changed":["api env CACHE_TTL"],"from_hash":"d1","to_hash":"d2"}`)
				b.k8s(0, "Deployment", "api", "deploy", "info", "api deployed: latest -> leaky", `{"old_image":"api:latest","new_image":"api:leaky"}`)
				rollout(b, "api", "api-1", "api-2")
				for _, s := range []float64{25, 70, 150, 320} {
					b.k8s(s, "Pod", "api-2", "oom_kill", "critical", "api restarted (OOMKilled, exit 137)", `{"container":"api","reason":"OOMKilled","exit_code":137,"owner":"api"}`)
				}
			}},
		{name: "two_independent_faults", group: "competing", failing: "redis", variants: "ABD", allRequired: true,
			truths: []TruthRef{{"scale", "redis"}, {"scale", "worker"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "redis", "scale", "info", "redis scaled from 1 to 0", scaleP(1, 0))
				b.k8s(1, "Pod", "redis-1", "became_unready", "warning", "redis-1 stopped serving traffic", `{"owner":"redis"}`)
				b.k8s(5, "Deployment", "worker", "scale", "info", "worker scaled from 1 to 0", scaleP(1, 0))
				b.k8s(6, "Pod", "worker-1", "became_unready", "warning", "worker-1 stopped serving traffic", `{"owner":"worker"}`)
			}},
		{name: "simultaneous_deploy_and_config", group: "competing", failing: "api", variants: "AB", ambiguous: true,
			truths: []TruthRef{{"deploy", "api"}, {"config_change", "api"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "api", "config_change", "info", "api configuration changed (api env REDIS_URL)", `{"changed":["api env REDIS_URL"],"from_hash":"aaa111","to_hash":"bbb222"}`)
				b.k8s(4, "Deployment", "api", "deploy", "info", "api deployed: latest -> v2", `{"old_image":"api:latest","new_image":"api:v2"}`)
				crashing(b, "api", "api-2")
			}},

		// Two deploys land seconds apart upstream of the failure. The evidence
		// cannot say which one broke it, so both are acceptable and confidence
		// should not be high.
		{name: "two_simultaneous_deploys", group: "competing", failing: "api", variants: "AB", ambiguous: true,
			truths: []TruthRef{{"deploy", "api"}, {"deploy", "worker"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "worker", "deploy", "info", "worker deployed: v1 -> v2", `{"old_image":"worker:v1","new_image":"worker:v2"}`)
				b.k8s(2, "Deployment", "api", "deploy", "info", "api deployed: v1 -> v2", `{"old_image":"api:v1","new_image":"api:v2"}`)
				crashing(b, "api", "api-2")
			}},

		// Causes the original collectors could not see.
		{name: "service_selector_broken", group: "newly-observable", failing: "redis", variants: "ABDC",
			truths: []TruthRef{{"service_change", "redis"}},
			inject: func(b *builder) {
				b.k8s(0, "Service", "redis", "service_change", "info", "redis service changed (selector)", `{"changed":["selector"],"from_hash":"s1","to_hash":"s2"}`)
			}},
		{name: "node_failure", group: "newly-observable", failing: "node", variants: "ABDE",
			truths: []TruthRef{{"node_not_ready", "node-2"}},
			inject: func(b *builder) {
				b.k8s(0, "Node", "node-2", "node_not_ready", "critical", "node-2 is NotReady", `{"reason":"KubeletNotReady"}`)
				for _, p := range []string{"redis-1", "worker-1"} {
					b.k8s(3, "Pod", p, "became_unready", "warning", p+" stopped serving traffic", `{}`)
					b.k8s(3, "Pod", p, "resource_status", "warning", p+" status is Unknown", `{"phase":"Unknown"}`)
				}
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(3, "Pod", "redis-1", "became_unready", "warning", "redis-1 stopped serving traffic", `{}`)
			}},
		{name: "configmap_changed", group: "newly-observable", failing: "api", variants: "ABDE",
			truths: []TruthRef{{"config_change", "api-config"}},
			inject: func(b *builder) {
				b.k8s(0, "ConfigMap", "api-config", "config_change", "info", "api-config changed (keys: REDIS_ADDR)", `{"changed":["REDIS_ADDR"],"from_hash":"m1","to_hash":"m2"}`)
				crashLoop(b, "api-1", "api", "container_restart", 20, 60, 130, 260, 570, 870, 1170, 1470)
				b.k8s(21, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
			},
			podSymptom: func(b *builder) event.Event {
				return b.k8s(21, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
			}},
		{name: "hpa_limit_lowered", group: "newly-observable", failing: "worker", variants: "ABD",
			truths: []TruthRef{{"hpa_change", "worker"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "worker", "hpa_change", "info", "worker autoscaler changed (maxReplicas)", `{"changed":["maxReplicas"],"from_hash":"h1","to_hash":"h2"}`)
				b.k8s(3, "Deployment", "worker", "scale", "info", "worker scaled from 3 to 1", scaleP(3, 1))
			}},

		// Causes that stay outside what Chronicle can observe.
		{name: "external_database_outage", group: "unknown", failing: "api", variants: "ABD", inject: func(b *builder) {}},
		{name: "dns_resolution_failure", group: "unknown", failing: "api", variants: "ABD", inject: func(b *builder) {}},
		{name: "expired_credential", group: "unknown", failing: "api", variants: "ABD", inject: func(b *builder) {}},
		{name: "cloud_load_balancer_fault", group: "unknown", failing: "api", variants: "ABD", inject: func(b *builder) {}},
		// A recent change on a component the failing path does not depend on
		// must not be blamed.
		{name: "unrelated_recent_change", group: "unknown", failing: "api", variants: "ABD", inject: func(b *builder) {
			b.k8s(-45, "Deployment", "postgres", "deploy", "info", "postgres deployed: 16.2 -> 16.3", `{"old_image":"postgres:16.2","new_image":"postgres:16.3"}`)
		}},
	}
}

// --- symptoms -------------------------------------------------------------

var unknownLogLines = map[string]string{
	"external_database_outage":  "error: database connection to db.external.example:5432 timed out",
	"unrelated_recent_change":   "error: upstream connect error or disconnect/reset before headers",
	"dns_resolution_failure":    "error: lookup payments.external.example: no such host",
	"expired_credential":        "error: upstream returned HTTP 401 Unauthorized",
	"cloud_load_balancer_fault": "error: upstream connect error or disconnect/reset before headers",
}

func downstreamLog(f fault) (pod, text string) {
	if line, ok := unknownLogLines[f.name]; ok {
		return "api-1", line
	}
	switch f.failing {
	case "api":
		return "frontend-1", "error: api at http://api:8080 returned HTTP 500"
	case "worker":
		return "api-1", "error: calling worker at http://worker:8080 failed: context deadline exceeded"
	default:
		return "api-1", "error: redis INCR against redis:6379 failed: EOF"
	}
}

func buildIncident(f fault, variant byte) Incident {
	b := &builder{name: fmt.Sprintf("%s/%c", f.name, variant)}
	b.background()
	f.inject(b)

	until := map[byte]float64{'A': 60, 'B': 95, 'C': 1500, 'D': 45, 'E': 30}[variant]
	if f.analyseAt > 0 && variant != 'C' {
		until = f.analyseAt
	}
	pod, text := downstreamLog(f)
	spec := f.failing
	// Downstream effects of the failure, up to the moment of analysis.
	b.logs(8, until, map[bool]float64{true: 30, false: 15}[variant == 'C'], pod, text)
	if until >= 35 {
		svc := "api"
		if spec == "api" {
			svc = "frontend"
		}
		b.add(35, "prometheus", "Service", svc, "latency_spike", "warning", "latency_p99 on "+svc+": 3.9", `{"value":3.9}`)
	}
	if until >= 55 {
		b.add(55, "prometheus", "Service", "frontend", "error_spike", "critical", "high_error_rate on frontend: 1.000", `{"value":1}`)
		if spec != "api" {
			b.add(55, "prometheus", "Service", "api", "error_spike", "critical", "high_error_rate on api: 1.000", `{"value":1}`)
		}
	}
	if variant == 'C' {
		b.add(1250, "prometheus", "Service", "frontend", "error_spike", "critical", "high_error_rate on frontend: 1.000", `{"value":1}`)
	}

	var symptom event.Event
	switch variant {
	case 'A':
		symptom = b.add(until, "prometheus", "Service", "frontend", "error_spike", "critical", "high_error_rate on frontend: 1.000", `{"value":1}`)
	case 'B':
		symptom = b.add(until, "loki", "Pod", pod, "log_error", "warning", text, `{}`)
	case 'C':
		symptom = b.add(until, "loki", "Pod", pod, "log_error", "warning", text, `{}`)
	case 'D':
		svc := "api"
		if spec == "api" {
			svc = "frontend"
		}
		symptom = b.add(until, "prometheus", "Service", svc, "latency_spike", "warning", "latency_p99 on "+svc+": 4.1", `{"value":4.1}`)
	case 'E':
		symptom = f.podSymptom(b)
	}
	// The symptom is the newest copy of its signal, as the console picks it.
	return Incident{
		Name: b.name, Group: f.group, Fault: f.name, Events: b.evs, Symptom: symptom,
		Truths: f.truths, AllRequired: f.allRequired, Ambiguous: f.ambiguous,
	}
}

func Incidents() []Incident {
	var out []Incident
	for _, f := range faults() {
		for _, v := range []byte(f.variants) {
			if v == 'E' && f.podSymptom == nil {
				continue
			}
			out = append(out, buildIncident(f, v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Store serves events by ingestion time, like the real event store.
type Store struct{ All []event.Event }

func (s *Store) EventsBetween(_ context.Context, from, to time.Time) ([]event.Event, error) {
	var out []event.Event
	for _, e := range s.All {
		if !e.IngestedAt.Before(from) && e.IngestedAt.Before(to) {
			out = append(out, e)
		}
	}
	return out, nil
}

// Graph serves the topology's edges as the dependency graph source.
type Graph struct{ EdgeList []graph.Edge }

func (g Graph) EdgesBetween(context.Context, time.Time, time.Time) ([]graph.Edge, error) {
	return g.EdgeList, nil
}
func (g Graph) EdgesAt(context.Context, time.Time) ([]graph.Edge, error) { return g.EdgeList, nil }
func (g Graph) UpstreamAt(_ context.Context, _ time.Time, start string, depth int) (map[string]int, error) {
	gr := graph.New()
	gr.SetEdges(g.EdgeList)
	return gr.Upstream(start, depth), nil
}

// Filter keeps the events a profile's collectors would have emitted.
func Filter(profile string, events []event.Event) []event.Event {
	var out []event.Event
	for _, e := range events {
		if Observed(profile, e) {
			out = append(out, e)
		}
	}
	return out
}
