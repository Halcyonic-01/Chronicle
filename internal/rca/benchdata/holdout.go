package benchdata

import (
	"fmt"
	"sort"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// The held-out set. It was written after the ranking rules were settled and run
// once against each version, on a different application, so it was not used to
// design or tune anything. Its failures are reported, not fixed here.
//
// Topology: web -> checkout -> {cart -> inventory -> db, payments -> ledger -> db},
// web -> search -> index, plus a batch worker nothing depends on.

// HoldoutEdges is the shop application's dependency graph.
func HoldoutEdges() []graph.Edge {
	var edges []graph.Edge
	add := func(from, to graph.Node, kind string) {
		edges = append(edges, graph.Edge{From: from, To: to, Kind: kind, Weight: 1, Source: "bench"})
	}
	nodeOf := map[string]string{"web": "n1", "cart": "n1", "search": "n1", "checkout": "n2", "inventory": "n2", "payments": "n2", "ledger": "n3", "db": "n3", "index": "n3", "batch": "n1"}
	for dep, n := range nodeOf {
		for _, gen := range []string{"a", "b"} {
			pod := dep + "-" + gen
			add(node("Deployment", dep), node("Pod", pod), "owns")
			add(node("Service", dep), node("Pod", pod), "routes_to")
			add(node("Pod", pod), node("Node", n), "runs_on")
		}
	}
	calls := [][2]string{{"web", "checkout"}, {"web", "search"}, {"checkout", "cart"}, {"checkout", "payments"}, {"cart", "inventory"}, {"inventory", "db"}, {"payments", "ledger"}, {"ledger", "db"}, {"search", "index"}}
	for _, c := range calls {
		for _, gen := range []string{"a", "b"} {
			add(node("Pod", c[0]+"-"+gen), node("Service", c[1]), "calls")
		}
		add(node("Deployment", c[0]), node("Deployment", c[1]), "calls")
	}
	for _, gen := range []string{"a", "b"} {
		add(node("Pod", "checkout-"+gen), node("ConfigMap", "checkout-config"), "uses")
	}
	return edges
}

type hfault struct {
	name        string
	group       string
	truths      []TruthRef
	errPod      string // where the user-visible error is logged
	errText     string
	variants    string // W alert, L logs, K latency, P pod-level, X long logs
	inject      func(b *builder)
	pod         func(b *builder) event.Event
	ambiguous   bool
	allRequired bool
}

func (b *builder) hbackground() {
	b.k8s(-2400, "Deployment", "batch", "deploy", "info", "batch deployed: v7 -> v8", `{"old_image":"batch:v7","new_image":"batch:v8"}`)
	b.k8s(-5400, "Deployment", "cart", "scale", "info", "cart scaled from 2 to 1", scaleP(2, 1))
	b.k8s(-4800, "Deployment", "cart", "scale", "info", "cart scaled from 1 to 2", scaleP(1, 2))
	b.add(-5300, "prometheus", "Service", "web", "latency_spike", "warning", "latency_p99 on web: 2.2", `{"value":2.2}`)
	b.add(-4790, "prometheus", "Service", "web", "latency_spike_resolved", "info", "latency_p99 resolved on web", `{}`)
}

func hRollout(b *builder, dep string, at float64) {
	b.k8s(at+2, "Deployment", dep, "resource_status", "warning", dep+" status is 0/1 replicas ready", `{"phase":"Pending"}`)
	b.k8s(at+2, "Pod", dep+"-b", "resource_created", "info", dep+"-b created", fmt.Sprintf(`{"owner":%q}`, dep))
	b.k8s(at+3, "Pod", dep+"-a", "became_unready", "warning", dep+"-a stopped serving traffic", fmt.Sprintf(`{"owner":%q}`, dep))
	b.k8s(at+4, "Pod", dep+"-a", "resource_deleted", "info", dep+"-a deleted", `{}`)
}

func hCrash(b *builder, pod, owner string, at float64, steps ...float64) {
	for _, s := range steps {
		b.k8s(at+s, "Pod", pod, "container_restart", "warning", pod+" restarted (Error, exit 1)", fmt.Sprintf(`{"container":"x","reason":"Error","exit_code":1,"owner":%q}`, owner))
	}
}

func holdoutFaults() []hfault {
	steps := []float64{25, 70, 150, 300, 620, 920}
	return []hfault{
		{name: "ledger_bad_deploy", group: "holdout-known", errPod: "payments-a", errText: "error: ledger at http://ledger:9000 returned HTTP 500", variants: "WLKPX",
			truths: []TruthRef{{"deploy", "ledger"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "ledger", "deploy", "info", "ledger deployed: v4 -> v5", `{"old_image":"ledger:v4","new_image":"ledger:v5"}`)
				hRollout(b, "ledger", 0)
				hCrash(b, "ledger-b", "ledger", 0, steps...)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(26, "Pod", "ledger-b", "container_restart", "warning", "ledger-b restarted (Error, exit 1)", `{"container":"x","reason":"Error","exit_code":1,"owner":"ledger"}`)
			}},
		{name: "payments_bad_config", group: "holdout-known", errPod: "checkout-a", errText: "error: payments returned HTTP 502", variants: "WLKPX",
			truths: []TruthRef{{"config_change", "payments"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "payments", "config_change", "info", "payments configuration changed (payments env LEDGER_URL)", `{"changed":["payments env LEDGER_URL"],"from_hash":"p1","to_hash":"p2"}`)
				hRollout(b, "payments", 0)
				hCrash(b, "payments-b", "payments", 0, steps...)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(40, "Pod", "payments-b", "became_unready", "warning", "payments-b stopped serving traffic", `{"owner":"payments"}`)
			}},
		{name: "inventory_limit_oom", group: "holdout-known", errPod: "cart-a", errText: "error: inventory request timed out", variants: "WLKPX",
			truths: []TruthRef{{"resource_change", "inventory"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "inventory", "resource_change", "info", "inventory resource limits changed", `{"old_mem_limit":536870912,"new_mem_limit":16777216}`)
				hRollout(b, "inventory", 0)
				for _, s := range []float64{30, 75, 160, 330, 640, 940} {
					b.k8s(s, "Pod", "inventory-b", "oom_kill", "critical", "inventory restarted (OOMKilled, exit 137)", `{"container":"inventory","reason":"OOMKilled","exit_code":137,"owner":"inventory"}`)
				}
			},
			pod: func(b *builder) event.Event {
				return b.k8s(31, "Pod", "inventory-b", "oom_kill", "critical", "inventory restarted (OOMKilled, exit 137)", `{"container":"inventory","reason":"OOMKilled","exit_code":137,"owner":"inventory"}`)
			}},
		{name: "db_scaled_to_zero", group: "holdout-known", errPod: "inventory-a", errText: "error: db connection refused", variants: "WLKPX",
			truths: []TruthRef{{"scale", "db"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "db", "scale", "info", "db scaled from 1 to 0", scaleP(1, 0))
				b.k8s(2, "Pod", "db-a", "resource_status", "critical", "db-a status is Failed", `{"phase":"Failed"}`)
				b.k8s(2, "Pod", "db-a", "became_unready", "warning", "db-a stopped serving traffic", `{"owner":"db"}`)
				b.k8s(3, "Pod", "db-a", "resource_deleted", "info", "db-a deleted", `{}`)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(2, "Pod", "db-a", "became_unready", "warning", "db-a stopped serving traffic", `{"owner":"db"}`)
			}},
		{name: "cart_pod_not_ready", group: "holdout-known", errPod: "checkout-a", errText: "error: cart returned HTTP 503", variants: "WLKPX",
			truths: []TruthRef{{"became_unready", "cart-a"}},
			inject: func(b *builder) {
				b.k8s(0, "Pod", "cart-a", "became_unready", "warning", "cart-a stopped serving traffic", `{"owner":"cart"}`)
				b.k8s(2, "Pod", "cart-a", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(2, "Pod", "cart-a", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
			}},
		{name: "index_memory_leak", group: "holdout-known", errPod: "search-a", errText: "error: index query failed: connection reset", variants: "WLPX",
			truths: []TruthRef{{"oom_kill", "index-a"}},
			inject: func(b *builder) {
				for _, s := range []float64{0, 100, 230, 420, 760, 1050} {
					b.k8s(s, "Pod", "index-a", "oom_kill", "critical", "index restarted (OOMKilled, exit 137)", `{"container":"index","reason":"OOMKilled","exit_code":137,"owner":"index"}`)
				}
				b.k8s(1, "Pod", "index-a", "became_unready", "warning", "index-a stopped serving traffic", `{"owner":"index"}`)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "index-a", "became_unready", "warning", "index-a stopped serving traffic", `{"owner":"index"}`)
			}},
		{name: "payments_crash_loop", group: "holdout-known", errPod: "checkout-a", errText: "error: payments returned HTTP 502", variants: "WLKPX",
			truths: []TruthRef{{"container_restart", "payments-a"}},
			inject: func(b *builder) {
				hCrash(b, "payments-a", "payments", 0, 0, 45, 100, 210, 400, 700, 1000)
				b.k8s(1, "Pod", "payments-a", "became_unready", "warning", "payments-a stopped serving traffic", `{"owner":"payments"}`)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(1, "Pod", "payments-a", "became_unready", "warning", "payments-a stopped serving traffic", `{"owner":"payments"}`)
			}},

		// Causes the original collectors could not see.
		{name: "inventory_service_port_changed", group: "holdout-newly-observable", errPod: "cart-a", errText: "error: inventory dial tcp: connection refused", variants: "WLK",
			truths: []TruthRef{{"service_change", "inventory"}},
			inject: func(b *builder) {
				b.k8s(0, "Service", "inventory", "service_change", "info", "inventory service changed (ports)", `{"changed":["ports"],"from_hash":"v1","to_hash":"v2"}`)
			}},
		{name: "node_n3_lost", group: "holdout-newly-observable", errPod: "inventory-a", errText: "error: db connection refused", variants: "WLKP",
			truths: []TruthRef{{"node_not_ready", "n3"}},
			inject: func(b *builder) {
				b.k8s(0, "Node", "n3", "node_not_ready", "critical", "n3 is NotReady", `{"reason":"NodeStatusUnknown"}`)
				for _, p := range []string{"db-a", "ledger-a", "index-a"} {
					b.k8s(8, "Pod", p, "became_unready", "warning", p+" stopped serving traffic", `{}`)
					b.k8s(8, "Pod", p, "resource_status", "warning", p+" status is Unknown", `{"phase":"Unknown"}`)
				}
			},
			pod: func(b *builder) event.Event {
				return b.k8s(8, "Pod", "db-a", "became_unready", "warning", "db-a stopped serving traffic", `{}`)
			}},
		{name: "checkout_configmap_edited", group: "holdout-newly-observable", errPod: "web-a", errText: "error: checkout returned HTTP 500", variants: "WLKP",
			truths: []TruthRef{{"config_change", "checkout-config"}},
			inject: func(b *builder) {
				b.k8s(0, "ConfigMap", "checkout-config", "config_change", "info", "checkout-config changed (keys: CART_URL)", `{"changed":["CART_URL"],"from_hash":"c1","to_hash":"c2"}`)
				hCrash(b, "checkout-a", "checkout", 0, 18, 55, 130, 280, 600)
				b.k8s(19, "Pod", "checkout-a", "became_unready", "warning", "checkout-a stopped serving traffic", `{"owner":"checkout"}`)
			},
			pod: func(b *builder) event.Event {
				return b.k8s(19, "Pod", "checkout-a", "became_unready", "warning", "checkout-a stopped serving traffic", `{"owner":"checkout"}`)
			}},
		{name: "search_autoscaler_capped", group: "holdout-newly-observable", errPod: "web-a", errText: "error: search request timed out", variants: "WL",
			truths: []TruthRef{{"hpa_change", "search"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "search", "hpa_change", "info", "search autoscaler changed (maxReplicas)", `{"changed":["maxReplicas"],"from_hash":"a1","to_hash":"a2"}`)
				b.k8s(4, "Deployment", "search", "scale", "info", "search scaled from 4 to 1", scaleP(4, 1))
			}},

		// Competing and ambiguous.
		{name: "ledger_deploy_beside_search_deploy", group: "holdout-competing", errPod: "payments-a", errText: "error: ledger at http://ledger:9000 returned HTTP 500", variants: "WLK",
			truths: []TruthRef{{"deploy", "ledger"}},
			inject: func(b *builder) {
				b.k8s(-10, "Deployment", "search", "deploy", "info", "search deployed: v2 -> v3", `{"old_image":"search:v2","new_image":"search:v3"}`)
				b.k8s(0, "Deployment", "ledger", "deploy", "info", "ledger deployed: v4 -> v5", `{"old_image":"ledger:v4","new_image":"ledger:v5"}`)
				hRollout(b, "ledger", 0)
				hCrash(b, "ledger-b", "ledger", 0, steps...)
			}},
		{name: "db_down_and_search_config", group: "holdout-competing", errPod: "web-a", errText: "error: upstream request failed", variants: "WL", allRequired: true,
			truths: []TruthRef{{"scale", "db"}, {"config_change", "search"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "db", "scale", "info", "db scaled from 1 to 0", scaleP(1, 0))
				b.k8s(2, "Pod", "db-a", "became_unready", "warning", "db-a stopped serving traffic", `{"owner":"db"}`)
				b.k8s(8, "Deployment", "search", "config_change", "info", "search configuration changed (search env INDEX_URL)", `{"changed":["search env INDEX_URL"],"from_hash":"s1","to_hash":"s2"}`)
				hRollout(b, "search", 8)
				hCrash(b, "search-b", "search", 8, 20, 60, 140)
			}},
		{name: "two_ledger_deploys", group: "holdout-competing", errPod: "payments-a", errText: "error: ledger at http://ledger:9000 returned HTTP 500", variants: "WL", ambiguous: true,
			truths: []TruthRef{{"deploy", "ledger"}},
			inject: func(b *builder) {
				b.k8s(0, "Deployment", "ledger", "deploy", "info", "ledger deployed: v4 -> v5", `{"old_image":"ledger:v4","new_image":"ledger:v5"}`)
				hRollout(b, "ledger", 0)
				hCrash(b, "ledger-b", "ledger", 0, 25, 70, 150)
				b.k8s(110, "Deployment", "ledger", "deploy", "info", "ledger deployed: v5 -> v6", `{"old_image":"ledger:v5","new_image":"ledger:v6"}`)
			}},

		// Not observable, plus a recent change off the dependency path.
		{name: "payment_gateway_outage", group: "holdout-unknown", errPod: "payments-a", errText: "error: gateway https://pay.external.example timed out", variants: "WLK", inject: func(b *builder) {}},
		{name: "tls_certificate_expired", group: "holdout-unknown", errPod: "checkout-a", errText: "error: x509: certificate has expired or is not yet valid", variants: "WLK", inject: func(b *builder) {}},
		{name: "regional_network_partition", group: "holdout-unknown", errPod: "web-a", errText: "error: dial tcp: i/o timeout", variants: "WLK", inject: func(b *builder) {}},
		{name: "batch_deploy_unrelated", group: "holdout-unknown", errPod: "payments-a", errText: "error: gateway https://pay.external.example timed out", variants: "WLK", inject: func(b *builder) {
			b.k8s(-40, "Deployment", "batch", "deploy", "info", "batch deployed: v8 -> v9", `{"old_image":"batch:v8","new_image":"batch:v9"}`)
		}},
	}
}

func buildHoldout(f hfault, v byte) Incident {
	b := &builder{name: fmt.Sprintf("holdout/%s/%c", f.name, v)}
	b.hbackground()
	f.inject(b)
	until := map[byte]float64{'W': 100, 'L': 170, 'K': 75, 'P': 40, 'X': 700}[v]
	every := 20.0
	if v == 'X' {
		every = 37 // irregular relative to the polling interval
	}
	b.logs(12, until, every, f.errPod, f.errText)
	if until >= 60 {
		b.add(60, "prometheus", "Service", "checkout", "latency_spike", "warning", "latency_p99 on checkout: 3.4", `{"value":3.4}`)
	}
	if until >= 90 {
		b.add(90, "prometheus", "Service", "web", "error_spike", "critical", "high_error_rate on web: 0.64", `{"value":0.64}`)
	}
	var symptom event.Event
	switch v {
	case 'W':
		symptom = b.add(until, "prometheus", "Service", "web", "error_spike", "critical", "high_error_rate on web: 0.71", `{"value":0.71}`)
	case 'L', 'X':
		symptom = b.add(until, "loki", "Pod", f.errPod, "log_error", "warning", f.errText, `{}`)
	case 'K':
		symptom = b.add(until, "prometheus", "Service", "checkout", "latency_spike", "warning", "latency_p99 on checkout: 3.9", `{"value":3.9}`)
	case 'P':
		symptom = f.pod(b)
	}
	return Incident{
		Name: b.name, Group: f.group, Fault: f.name, Events: b.evs, Symptom: symptom, Truths: f.truths,
		AllRequired: f.allRequired, Ambiguous: f.ambiguous, Edges: HoldoutEdges(),
	}
}

// HoldoutIncidents returns the held-out incidents, sorted by name.
func HoldoutIncidents() []Incident {
	var out []Incident
	for _, f := range holdoutFaults() {
		for _, v := range []byte(f.variants) {
			if v == 'P' && f.pod == nil {
				continue
			}
			out = append(out, buildHoldout(f, v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
