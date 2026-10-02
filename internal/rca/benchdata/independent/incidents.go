package independent

import (
	"fmt"
	"sort"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// site says where users see the failure.
type site struct {
	logNS, logPod, logText string
	alertNS, alertSvc      string // error_spike
	latNS, latSvc          string // latency_spike
}

type fault struct {
	id, category, title string
	actual, plausible   []Root
	observable          bool
	ambiguous           bool
	allRequired         bool
	expected            string
	outOfScope          bool
	notes               string
	site                site
	inject              func(t *timeline)
	pod                 func(t *timeline) event.Event
	variants            string
	// custom variants build their own symptom (and any events around it).
	custom map[string]func(t *timeline) event.Event
}

func root(kind, ekind, ns, name string) Root {
	return Root{Kind: kind, EntityKind: ekind, Namespace: ns, Entity: name}
}

func scale(from, to int) string {
	return fmt.Sprintf(`{"old_replicas":%d,"new_replicas":%d}`, from, to)
}

func hashes(a, b string) string { return fmt.Sprintf(`{"from_hash":%q,"to_hash":%q}`, a, b) }

func owner(o string) string { return fmt.Sprintf(`{"owner":%q}`, o) }

// background is shared by every incident: an unrelated deploy off the dependency
// path, a harmless warning on a component nobody depends on, and an earlier
// config change on the failing path that was long since resolved.
func background(t *timeline) {
	t.put(-900, "k8s", "media", "Deployment", "report-gen", "deploy", "info", "report-gen deployed: 3.1 -> 3.2", `{"old_image":"report-gen:3.1","new_image":"report-gen:3.2"}`)
	t.put(-60, "k8s", "media", "Pod", "report-gen-r1-0", "k8s_event", "warning", "Unhealthy: Liveness probe failed", `{"reason":"Unhealthy"}`)
	t.put(-2400, "k8s", "media", "ConfigMap", "catalog-config", "config_change", "info", "catalog-config changed (keys: PAGE_SIZE)", hashes("q1", "q2"))
	t.put(-2330, "prometheus", "media", "Service", "gateway", "error_spike", "critical", "high_error_rate on gateway: 0.31", `{"value":0.31}`)
	t.put(-2290, "prometheus", "media", "Service", "gateway", "error_spike_resolved", "info", "high_error_rate resolved on gateway", `{}`)
}

func podsOf(name string, rev, n int) []string {
	var out []string
	for i := 0; i < n; i++ {
		out = append(out, podName(name, rev, i))
	}
	return out
}

// rollout replaces a deployment's pods at time at.
func rollout(t *timeline, d dep, at float64) {
	t.put(at+1, "k8s", d.ns, "Deployment", d.name, "resource_status", "warning", d.name+" status is 0/"+fmt.Sprint(d.replicas)+" replicas ready", `{"phase":"Pending"}`)
	for i := 0; i < d.replicas; i++ {
		t.put(at+1, "k8s", d.ns, "Pod", podName(d.name, 2, i), "resource_created", "info", podName(d.name, 2, i)+" created", owner(d.name))
		t.put(at+2, "k8s", d.ns, "Pod", podName(d.name, 1, i), "became_unready", "warning", podName(d.name, 1, i)+" stopped serving traffic", owner(d.name))
		t.put(at+3, "k8s", d.ns, "Pod", podName(d.name, 1, i), "resource_deleted", "info", podName(d.name, 1, i)+" deleted", `{}`)
	}
}

func restarts(t *timeline, ns, pod, ow, typ string, at ...float64) {
	for _, s := range at {
		if typ == "oom_kill" {
			t.put(s, "k8s", ns, "Pod", pod, typ, "critical", pod+" restarted (OOMKilled, exit 137)", fmt.Sprintf(`{"reason":"OOMKilled","exit_code":137,"owner":%q}`, ow))
		} else {
			t.put(s, "k8s", ns, "Pod", pod, typ, "warning", pod+" restarted (Error, exit 1)", fmt.Sprintf(`{"reason":"Error","exit_code":1,"owner":%q}`, ow))
		}
	}
}

func unready(t *timeline, at float64, ns, pod, ow string) event.Event {
	return t.put(at, "k8s", ns, "Pod", pod, "became_unready", "warning", pod+" stopped serving traffic", owner(ow))
}

var backoff = []float64{16, 52, 118, 240, 520, 840, 1140}

func crashAfter(t *timeline, d dep, rev int, at float64, steps ...float64) {
	for i := 0; i < d.replicas; i++ {
		var ts []float64
		for _, s := range steps {
			ts = append(ts, at+s+float64(i)*3)
		}
		restarts(t, d.ns, podName(d.name, rev, i), d.name, "container_restart", ts...)
	}
}

func faults() []fault {
	gw := site{"media", "gateway-r1-0", "error: catalog returned 503", "media", "gateway", "media", "gateway"}
	return []fault{
		// ---- A: known observable causes -----------------------------------
		{id: "deploy_entitlement", category: CatKnown, title: "bad release of the entitlement service",
			actual: []Root{root("deploy", "Deployment", "media", "entitlement")}, observable: true, expected: RootCause,
			site:     site{"media", "player-api-r1-0", "error: entitlement call failed: HTTP 502", "media", "gateway", "media", "gateway"},
			variants: "ABCDP",
			inject: func(t *timeline) {
				d := find("media/entitlement")
				t.put(0, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.4 -> 7.5", `{"old_image":"entitlement:7.4","new_image":"entitlement:7.5"}`)
				rollout(t, d, 0)
				restarts(t, "media", "entitlement-r2-0", "entitlement", "container_restart", backoff...)
				unready(t, 17, "media", "entitlement-r2-0", "entitlement")
			},
			pod: func(t *timeline) event.Event {
				return t.put(16, "k8s", "media", "Pod", "entitlement-r2-0", "container_restart", "warning", "entitlement-r2-0 restarted (Error, exit 1)", `{"owner":"entitlement"}`)
			}},
		{id: "configmap_transcode_profiles", category: CatKnown, title: "edited transcoding profiles ConfigMap",
			actual: []Root{root("config_change", "ConfigMap", "media", "transcode-profiles")}, observable: true, expected: RootCause,
			site:     site{"media", "transcoder-r1-0", "error: profile parse error: unknown preset", "media", "transcoder", "media", "transcoder"},
			variants: "ABCP",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "ConfigMap", "transcode-profiles", "config_change", "info", "transcode-profiles changed (keys: HD_PRESET)", hashes("t1", "t2"))
				crashAfter(t, find("media/transcoder"), 1, 0, 38, 95, 170, 330, 640, 940)
				unready(t, 39, "media", "transcoder-r1-0", "transcoder")
			},
			pod: func(t *timeline) event.Event {
				return t.put(38, "k8s", "media", "Pod", "transcoder-r1-0", "container_restart", "warning", "transcoder-r1-0 restarted (Error, exit 1)", `{"owner":"transcoder"}`)
			}},
		{id: "resource_session_cache", category: CatKnown, title: "memory limit lowered on the session cache",
			actual: []Root{root("resource_change", "Deployment", "media", "session-cache")}, observable: true, expected: RootCause,
			site:     site{"media", "player-api-r1-1", "error: session lookup failed: connection refused", "media", "gateway", "media", "gateway"},
			variants: "ABCDP",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "session-cache", "resource_change", "info", "session-cache resource limits changed", `{"old_mem_limit":1073741824,"new_mem_limit":33554432}`)
				rollout(t, find("media/session-cache"), 0)
				restarts(t, "media", "session-cache-r2-0", "session-cache", "oom_kill", 21, 66, 150, 310, 640, 950)
				unready(t, 22, "media", "session-cache-r2-0", "session-cache")
			},
			pod: func(t *timeline) event.Event {
				return t.put(21, "k8s", "media", "Pod", "session-cache-r2-0", "oom_kill", "critical", "session-cache-r2-0 restarted (OOMKilled, exit 137)", `{"owner":"session-cache"}`)
			}},
		{id: "scale_blob_proxy", category: CatKnown, title: "blob proxy scaled to zero",
			actual: []Root{root("scale", "Deployment", "media", "blob-proxy")}, observable: true, expected: RootCause,
			site:     site{"media", "transcoder-r1-1", "error: blob upload failed: connection refused", "media", "transcoder", "media", "transcoder"},
			variants: "ABCDP",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "blob-proxy", "scale", "info", "blob-proxy scaled from 2 to 0", scale(2, 0))
				for i := 0; i < 2; i++ {
					unready(t, 1, "media", podName("blob-proxy", 1, i), "blob-proxy")
					t.put(2, "k8s", "media", "Pod", podName("blob-proxy", 1, i), "resource_deleted", "info", podName("blob-proxy", 1, i)+" deleted", `{}`)
				}
			},
			pod: func(t *timeline) event.Event {
				return t.put(1, "k8s", "media", "Pod", "blob-proxy-r1-0", "k8s_event", "warning", "Killing: Stopping container", `{"reason":"Killing"}`)
			}},
		{id: "oom_leak_catalog", category: CatKnown, title: "memory leak OOM-kills one catalog replica, no change recorded",
			actual: []Root{root("oom_kill", "Pod", "media", "catalog-r1-1")}, observable: true, expected: RootCause,
			site:     gw,
			variants: "ABCP",
			inject: func(t *timeline) {
				restarts(t, "media", "catalog-r1-1", "catalog", "oom_kill", 0, 95, 210, 400, 720, 1050)
				unready(t, 1, "media", "catalog-r1-1", "catalog")
			},
			pod: func(t *timeline) event.Event {
				return t.put(1, "k8s", "media", "Pod", "catalog-r1-1", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
			}},
		{id: "readiness_auth_svc", category: CatKnown, title: "one auth replica fails its readiness probe",
			actual: []Root{root("became_unready", "Pod", "auth", "auth-svc-r1-0")}, observable: true, expected: RootCause,
			site:     site{"media", "gateway-r1-1", "error: auth lookup timed out", "media", "gateway", "media", "gateway"},
			variants: "ABDP",
			inject: func(t *timeline) {
				unready(t, 0, "auth", "auth-svc-r1-0", "auth-svc")
				t.put(0, "k8s", "auth", "Pod", "auth-svc-r1-0", "resource_status", "warning", "auth-svc-r1-0 status is Pending", `{"phase":"Pending"}`)
				t.put(1, "k8s", "auth", "Pod", "auth-svc-r1-0", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
			},
			pod: func(t *timeline) event.Event {
				return t.put(1, "k8s", "auth", "Pod", "auth-svc-r1-0", "k8s_event", "warning", "Unhealthy: Readiness probe failed (again)", `{"reason":"Unhealthy"}`)
			}},
		{id: "crash_loop_ingest", category: CatKnown, title: "ingest crash-loops on a bug, no change recorded",
			actual: []Root{root("container_restart", "Pod", "media", "ingest-r1-0")}, observable: true, expected: RootCause,
			site:     site{"media", "ingest-r1-0", "panic: assignment to entry in nil map", "media", "ingest", "media", "ingest"},
			variants: "ABDP",
			inject: func(t *timeline) {
				restarts(t, "media", "ingest-r1-0", "ingest", "container_restart", 0, 42, 97, 180, 330, 650, 960)
				unready(t, 1, "media", "ingest-r1-0", "ingest")
			},
			pod: func(t *timeline) event.Event { return unready(t, 1, "media", "ingest-r1-0", "ingest") }},

		// ---- B: previously unobserved or hard ------------------------------
		{id: "node_d_failure", category: CatDifficult, title: "node-d goes NotReady, taking every data store on it",
			actual: []Root{root("node_not_ready", "Node", "", "node-d")}, observable: true, expected: RootCause,
			site:     site{"media", "catalog-r1-0", "error: metadata-db connection refused", "media", "gateway", "media", "gateway"},
			variants: "ABDP",
			inject: func(t *timeline) {
				t.put(0, "k8s", "", "Node", "node-d", "node_not_ready", "critical", "node-d is NotReady", `{"reason":"NodeStatusUnknown"}`)
				for _, p := range [][2]string{{"media", "metadata-db-r1-0"}, {"media", "queue-r1-0"}, {"media", "ingest-r1-0"}, {"media", "transcoder-r1-1"}, {"media", "blob-proxy-r1-0"}, {"auth", "auth-db-r1-0"}} {
					t.put(8, "k8s", p[0], "Pod", p[1], "became_unready", "warning", p[1]+" stopped serving traffic", `{}`)
					t.put(8, "k8s", p[0], "Pod", p[1], "resource_status", "warning", p[1]+" status is Unknown", `{"phase":"Unknown"}`)
				}
			},
			pod: func(t *timeline) event.Event {
				return t.put(8, "k8s", "media", "Pod", "metadata-db-r1-0", "became_unready", "warning", "metadata-db-r1-0 stopped serving traffic", `{}`)
			}},
		{id: "service_selector_catalog", category: CatDifficult, title: "catalog Service selector edited; no pod changes",
			actual: []Root{root("service_change", "Service", "media", "catalog")}, observable: true, expected: RootCause,
			site:     site{"media", "gateway-r1-2", "error: catalog dial tcp: connection refused", "media", "gateway", "media", "gateway"},
			variants: "ABCD",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Service", "catalog", "service_change", "info", "catalog service changed (selector)", `{"changed":["selector"],"from_hash":"v1","to_hash":"v2"}`)
			}},
		{id: "hpa_player_api", category: CatDifficult, title: "autoscaler ceiling lowered on player-api",
			actual: []Root{root("hpa_change", "Deployment", "media", "player-api")}, observable: true, expected: RootCause,
			site:     site{"media", "gateway-r1-0", "error: player-api request timed out", "media", "gateway", "media", "gateway"},
			variants: "ABD",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "player-api", "hpa_change", "info", "player-api autoscaler changed (maxReplicas)", `{"changed":["maxReplicas"],"from_hash":"h1","to_hash":"h2"}`)
				t.put(5, "k8s", "media", "Deployment", "player-api", "scale", "info", "player-api scaled from 4 to 1", scale(4, 1))
			}},
		{id: "external_object_store", category: CatDifficult, title: "external object store outage",
			expected: NoRootCause, notes: "cause is outside the cluster", site: site{"media", "blob-proxy-r1-0", "error: s3: RequestTimeout: request timed out", "media", "transcoder", "media", "transcoder"}, variants: "ABD", inject: func(t *timeline) {}},
		{id: "external_cdn", category: CatDifficult, title: "CDN provider incident",
			expected: NoRootCause, notes: "cause is outside the cluster", site: site{"media", "gateway-r1-0", "error: cdn edge returned 503", "media", "gateway", "media", "gateway"}, variants: "AB", inject: func(t *timeline) {}},
		{id: "dns_failure", category: CatDifficult, title: "cluster DNS resolution failing",
			expected: NoRootCause, notes: "no DNS signal is collected", site: site{"media", "gateway-r1-1", "error: lookup cdn.example.com: no such host", "media", "gateway", "media", "gateway"}, variants: "AB", inject: func(t *timeline) {}},
		{id: "expired_certificate", category: CatDifficult, title: "upstream TLS certificate expired",
			expected: NoRootCause, notes: "certificates are not observed", site: site{"media", "gateway-r1-2", "error: x509: certificate has expired", "media", "gateway", "media", "gateway"}, variants: "AB", inject: func(t *timeline) {}},
		{id: "secret_rotated", category: CatDifficult, title: "database credential rotated out of band",
			expected: NoRootCause, notes: "Secrets are deliberately not observed", site: site{"auth", "auth-svc-r1-0", "error: auth-db: password authentication failed", "media", "gateway", "media", "gateway"}, variants: "ABD", inject: func(t *timeline) {}},
		{id: "az_partition", category: CatDifficult, title: "network partition between availability zones",
			expected: NoRootCause, notes: "network state is not observed", site: site{"media", "catalog-r1-0", "error: dial tcp 10.2.4.9:5432: i/o timeout", "media", "gateway", "media", "gateway"}, variants: "AB", inject: func(t *timeline) {}},

		// ---- C: rival causes -----------------------------------------------
		{id: "deploy_and_config_close", category: CatRival, title: "catalog deploy and config change six seconds apart, then crashes",
			actual: []Root{root("config_change", "Deployment", "media", "catalog")}, observable: true, ambiguous: true, expected: Ambiguous,
			plausible: []Root{root("config_change", "Deployment", "media", "catalog"), root("deploy", "Deployment", "media", "catalog")},
			site:      gw, variants: "AB",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "catalog", "config_change", "info", "catalog configuration changed (catalog env DB_POOL)", `{"changed":["catalog env DB_POOL"],"from_hash":"c1","to_hash":"c2"}`)
				t.put(6, "k8s", "media", "Deployment", "catalog", "deploy", "info", "catalog deployed: 2.0 -> 2.1", `{"old_image":"catalog:2.0","new_image":"catalog:2.1"}`)
				d := find("media/catalog")
				rollout(t, d, 6)
				crashAfter(t, d, 2, 6, backoff...)
			}},
		{id: "benign_deploy_then_oom_limit", category: CatRival, title: "a harmless entitlement deploy, then a limit change that OOM-kills it",
			actual: []Root{root("resource_change", "Deployment", "media", "entitlement")}, observable: true, expected: RootCause,
			plausible: []Root{root("resource_change", "Deployment", "media", "entitlement")},
			notes:     "a human reads the oom_kill as pointing at the limit; the analyzer has no such knowledge",
			site:      site{"media", "player-api-r1-0", "error: entitlement call failed: HTTP 502", "media", "gateway", "media", "gateway"}, variants: "AB",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.5 -> 7.6", `{"old_image":"entitlement:7.5","new_image":"entitlement:7.6"}`)
				t.put(8, "k8s", "media", "Deployment", "entitlement", "resource_change", "info", "entitlement resource limits changed", `{"old_mem_limit":536870912,"new_mem_limit":16777216}`)
				rollout(t, find("media/entitlement"), 8)
				restarts(t, "media", "entitlement-r2-0", "entitlement", "oom_kill", 29, 74, 160, 320, 640, 950)
			}},
		{id: "restart_before_config_change", category: CatRival, title: "an unrelated earlier restart, then a bad config change",
			actual: []Root{root("config_change", "Deployment", "media", "player-api")}, observable: true, expected: RootCause,
			site: site{"media", "gateway-r1-1", "error: player-api returned HTTP 500", "media", "gateway", "media", "gateway"}, variants: "AB",
			inject: func(t *timeline) {
				restarts(t, "media", "player-api-r1-0", "player-api", "container_restart", -40)
				t.put(0, "k8s", "media", "Deployment", "player-api", "config_change", "info", "player-api configuration changed (player-api env ENTITLEMENT_URL)", `{"changed":["player-api env ENTITLEMENT_URL"],"from_hash":"p1","to_hash":"p2"}`)
				d := find("media/player-api")
				rollout(t, d, 0)
				crashAfter(t, d, 2, 0, 18, 55, 120, 250)
			}},
		{id: "benign_upstream_deploy", category: CatRival, title: "a harmless session-cache deploy just before the real entitlement fault",
			actual: []Root{root("deploy", "Deployment", "media", "entitlement")}, observable: true, expected: RootCause,
			site: site{"media", "player-api-r1-0", "error: entitlement call failed: HTTP 502", "media", "gateway", "media", "gateway"}, variants: "ABD",
			inject: func(t *timeline) {
				t.put(-25, "k8s", "media", "Deployment", "session-cache", "deploy", "info", "session-cache deployed: 6.2 -> 6.3", `{"old_image":"session-cache:6.2","new_image":"session-cache:6.3"}`)
				t.put(0, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.5 -> 7.6", `{"old_image":"entitlement:7.5","new_image":"entitlement:7.6"}`)
				rollout(t, find("media/entitlement"), 0)
				restarts(t, "media", "entitlement-r2-0", "entitlement", "container_restart", backoff...)
			}},
		{id: "two_independent_faults", category: CatRival, title: "catalog crashes after a deploy while entitlement is scaled to zero",
			actual:     []Root{root("deploy", "Deployment", "media", "catalog"), root("scale", "Deployment", "media", "entitlement")},
			plausible:  []Root{root("deploy", "Deployment", "media", "catalog"), root("scale", "Deployment", "media", "entitlement")},
			observable: true, allRequired: true, expected: RootCause,
			site: site{"media", "gateway-r1-0", "error: upstream returned 5xx", "media", "gateway", "media", "gateway"}, variants: "ABD",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "catalog", "deploy", "info", "catalog deployed: 2.1 -> 2.2", `{"old_image":"catalog:2.1","new_image":"catalog:2.2"}`)
				d := find("media/catalog")
				rollout(t, d, 0)
				crashAfter(t, d, 2, 0, 20, 60, 140)
				t.put(4, "k8s", "media", "Deployment", "entitlement", "scale", "info", "entitlement scaled from 1 to 0", scale(1, 0))
				unready(t, 5, "media", "entitlement-r1-0", "entitlement")
			}},
		{id: "repair_near_new_failure", category: CatRival, title: "catalog config break was restored; a cache scale-down follows 40 seconds later",
			actual: []Root{root("scale", "Deployment", "media", "session-cache")}, observable: true, expected: RootCause,
			site: site{"media", "player-api-r1-0", "error: session lookup failed: connection refused", "media", "gateway", "media", "gateway"}, variants: "ABD",
			inject: func(t *timeline) {
				t.put(-190, "k8s", "media", "Deployment", "catalog", "config_change", "info", "catalog configuration changed (catalog env DB_POOL)", `{"changed":["catalog env DB_POOL"],"from_hash":"g1","to_hash":"g2"}`)
				t.put(-180, "k8s", "media", "Pod", "catalog-r1-0", "resource_status", "critical", "catalog-r1-0 status is Failed", `{"phase":"Failed"}`)
				t.put(-100, "k8s", "media", "Deployment", "catalog", "config_change", "info", "catalog configuration changed (catalog env DB_POOL)", `{"changed":["catalog env DB_POOL"],"from_hash":"g2","to_hash":"g1"}`)
				t.put(0, "k8s", "media", "Deployment", "session-cache", "scale", "info", "session-cache scaled from 1 to 0", scale(1, 0))
				unready(t, 1, "media", "session-cache-r1-0", "session-cache")
				t.put(2, "k8s", "media", "Pod", "session-cache-r1-0", "resource_deleted", "info", "session-cache-r1-0 deleted", `{}`)
			}},
		{id: "similar_symptoms_noise", category: CatRival, title: "catalog deploy crash among unrelated warnings on sibling services",
			actual: []Root{root("deploy", "Deployment", "media", "catalog")}, observable: true, expected: RootCause,
			site: gw, variants: "AB",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "catalog", "deploy", "info", "catalog deployed: 2.2 -> 2.3", `{"old_image":"catalog:2.2","new_image":"catalog:2.3"}`)
				d := find("media/catalog")
				rollout(t, d, 0)
				crashAfter(t, d, 2, 0, backoff...)
				t.put(5, "k8s", "media", "Pod", "player-api-r1-1", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
				t.put(25, "k8s", "media", "Pod", "player-api-r1-1", "k8s_event", "warning", "Unhealthy: Readiness probe failed", `{"reason":"Unhealthy"}`)
				t.put(10, "k8s", "auth", "Pod", "auth-svc-r1-1", "k8s_event", "warning", "BackOff: Back-off restarting failed container", `{"reason":"BackOff"}`)
			}},
		{id: "ambiguous_two_deploys_latency", category: CatRival, title: "catalog and player-api deployed four seconds apart; only latency regresses",
			actual: []Root{root("deploy", "Deployment", "media", "player-api")}, observable: true, ambiguous: true, expected: Ambiguous,
			plausible: []Root{root("deploy", "Deployment", "media", "catalog"), root("deploy", "Deployment", "media", "player-api")},
			notes:     "no pod events: nothing in the record separates the two",
			site:      site{"media", "gateway-r1-0", "warn: slow response from upstream", "media", "gateway", "media", "gateway"}, variants: "BD",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "catalog", "deploy", "info", "catalog deployed: 2.3 -> 2.4", `{"old_image":"catalog:2.3","new_image":"catalog:2.4"}`)
				t.put(4, "k8s", "media", "Deployment", "player-api", "deploy", "info", "player-api deployed: 9.1 -> 9.2", `{"old_image":"player-api:9.1","new_image":"player-api:9.2"}`)
			}},
		{id: "ambiguous_two_config_changes", category: CatRival, title: "catalog and entitlement configs changed five seconds apart; latency only",
			actual: []Root{root("config_change", "Deployment", "media", "entitlement")}, observable: true, ambiguous: true, expected: Ambiguous,
			plausible: []Root{root("config_change", "Deployment", "media", "catalog"), root("config_change", "Deployment", "media", "entitlement")},
			site:      site{"media", "gateway-r1-1", "warn: slow response from upstream", "media", "gateway", "media", "gateway"}, variants: "AD",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "catalog", "config_change", "info", "catalog configuration changed (catalog env CACHE_TTL)", `{"changed":["catalog env CACHE_TTL"],"from_hash":"k1","to_hash":"k2"}`)
				t.put(5, "k8s", "media", "Deployment", "entitlement", "config_change", "info", "entitlement configuration changed (entitlement env TIMEOUT)", `{"changed":["entitlement env TIMEOUT"],"from_hash":"e1","to_hash":"e2"}`)
			}},

		// ---- D: root vs effect (the symptom is the effect) -----------------
		{id: "effect_deploy_to_restart", category: CatRootEffect, title: "player-api deploy; the symptom is a restart",
			actual: []Root{root("deploy", "Deployment", "media", "player-api")}, observable: true, expected: RootCause,
			variants: "early late",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "player-api", "deploy", "info", "player-api deployed: 9.2 -> 9.3", `{"old_image":"player-api:9.2","new_image":"player-api:9.3"}`)
				d := find("media/player-api")
				rollout(t, d, 0)
				crashAfter(t, d, 2, 0, 20, 70, 160, 330)
			},
			custom: map[string]func(t *timeline) event.Event{
				"early": func(t *timeline) event.Event {
					return effectAt(t, "container_restart", "media", "player-api-r2-0", "player-api", 20)
				},
				"late": func(t *timeline) event.Event {
					return effectAt(t, "container_restart", "media", "player-api-r2-0", "player-api", 330)
				},
			}},
		{id: "effect_config_to_readiness", category: CatRootEffect, title: "auth config change; the symptom is a readiness failure",
			actual: []Root{root("config_change", "Deployment", "auth", "auth-svc")}, observable: true, expected: RootCause,
			variants: "early late",
			inject: func(t *timeline) {
				t.put(0, "k8s", "auth", "Deployment", "auth-svc", "config_change", "info", "auth-svc configuration changed (auth-svc env JWKS_URL)", `{"changed":["auth-svc env JWKS_URL"],"from_hash":"a1","to_hash":"a2"}`)
				d := find("auth/auth-svc")
				rollout(t, d, 0)
				for i := 0; i < 2; i++ {
					unready(t, 25+float64(i)*2, "auth", podName("auth-svc", 2, i), "auth-svc")
					unready(t, 200+float64(i)*2, "auth", podName("auth-svc", 2, i), "auth-svc")
				}
			},
			custom: map[string]func(t *timeline) event.Event{
				"early": func(t *timeline) event.Event {
					return effectAt(t, "became_unready", "auth", "auth-svc-r2-0", "auth-svc", 26)
				},
				"late": func(t *timeline) event.Event {
					return effectAt(t, "became_unready", "auth", "auth-svc-r2-0", "auth-svc", 203)
				},
			}},
		{id: "effect_resource_to_oom", category: CatRootEffect, title: "blob-proxy limit change; the symptom is an OOM kill",
			actual: []Root{root("resource_change", "Deployment", "media", "blob-proxy")}, observable: true, expected: RootCause,
			variants: "early late",
			inject: func(t *timeline) {
				t.put(0, "k8s", "media", "Deployment", "blob-proxy", "resource_change", "info", "blob-proxy resource limits changed", `{"old_mem_limit":268435456,"new_mem_limit":8388608}`)
				d := find("media/blob-proxy")
				rollout(t, d, 0)
				for i := 0; i < 2; i++ {
					restarts(t, "media", podName("blob-proxy", 2, i), "blob-proxy", "oom_kill", 30+float64(i)*4, 100+float64(i)*4, 300+float64(i)*4)
				}
			},
			custom: map[string]func(t *timeline) event.Event{
				"early": func(t *timeline) event.Event {
					return effectAt(t, "oom_kill", "media", "blob-proxy-r2-0", "blob-proxy", 30)
				},
				"late": func(t *timeline) event.Event {
					return effectAt(t, "oom_kill", "media", "blob-proxy-r2-0", "blob-proxy", 300)
				},
			}},
		{id: "effect_service_to_errors", category: CatRootEffect, title: "auth Service port edited; the symptom is application errors",
			actual: []Root{root("service_change", "Service", "auth", "auth-svc")}, observable: true, expected: RootCause,
			variants: "early late",
			inject: func(t *timeline) {
				t.put(0, "k8s", "auth", "Service", "auth-svc", "service_change", "info", "auth-svc service changed (ports)", `{"changed":["ports"],"from_hash":"u1","to_hash":"u2"}`)
			},
			custom: map[string]func(t *timeline) event.Event{
				"early": func(t *timeline) event.Event {
					return t.put(40, "loki", "media", "Pod", "gateway-r1-1", "log_error", "warning", "error: auth connection refused", `{}`)
				},
				"late": func(t *timeline) event.Event {
					for s := 40.0; s < 400; s += 25 {
						t.put(s, "loki", "media", "Pod", "gateway-r1-1", "log_error", "warning", "error: auth connection refused", `{}`)
					}
					return t.put(400, "loki", "media", "Pod", "gateway-r1-1", "log_error", "warning", "error: auth connection refused", `{}`)
				},
			}},

		// ---- E: no recorded root cause -------------------------------------
		{id: "downstream_logs_only", category: CatNoRoot, title: "only downstream error logs",
			expected: NoRootCause, site: site{"media", "gateway-r1-0", "error: upstream error", "media", "gateway", "media", "gateway"}, variants: "AB", inject: func(t *timeline) {}},
		{id: "metrics_only", category: CatNoRoot, title: "only latency and error-rate metrics",
			expected: NoRootCause, site: site{"media", "gateway-r1-0", "", "media", "gateway", "media", "gateway"}, variants: "AD", inject: func(t *timeline) {}},
		{id: "offpath_change_decoy", category: CatNoRoot, title: "a deploy on a service nothing depends on, just before the failure",
			expected: NoRootCause, site: site{"media", "gateway-r1-0", "error: upstream error", "media", "gateway", "media", "gateway"}, variants: "AB",
			inject: func(t *timeline) {
				t.put(-30, "k8s", "media", "Deployment", "report-gen", "deploy", "info", "report-gen deployed: 3.2 -> 3.3", `{"old_image":"report-gen:3.2","new_image":"report-gen:3.3"}`)
			}},
		{id: "downstream_change_decoy", category: CatNoRoot, title: "the gateway (a caller) was deployed just before a database-side failure",
			expected: NoRootCause, site: site{"media", "catalog-r1-0", "error: metadata-db query timed out", "media", "catalog", "media", "catalog"}, variants: "BD",
			inject: func(t *timeline) {
				t.put(-20, "k8s", "media", "Deployment", "gateway", "deploy", "info", "gateway deployed: 5.0 -> 5.1", `{"old_image":"gateway:5.0","new_image":"gateway:5.1"}`)
			}},
		{id: "resolved_incident_decoy", category: CatNoRoot, title: "an earlier entitlement incident fully recovered before this unexplained failure",
			expected: NoRootCause, site: site{"media", "player-api-r1-0", "error: upstream timeout", "media", "gateway", "media", "gateway"}, variants: "AB",
			inject: func(t *timeline) {
				t.put(-500, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.3 -> 7.4", `{"old_image":"entitlement:7.3","new_image":"entitlement:7.4"}`)
				restarts(t, "media", "entitlement-r2-0", "entitlement", "container_restart", -480, -440)
				t.put(-420, "k8s", "media", "Pod", "entitlement-r2-0", "became_ready", "info", "entitlement-r2-0 started serving traffic", `{}`)
				t.put(-400, "prometheus", "media", "Service", "gateway", "error_spike_resolved", "info", "high_error_rate resolved on gateway", `{}`)
			}},
		{id: "capacity_increase_decoy", category: CatNoRoot, title: "a cache scale-up (not a fault) just before unexplained errors",
			expected: NoRootCause, site: site{"media", "player-api-r1-0", "error: upstream timeout", "media", "gateway", "media", "gateway"}, variants: "AB",
			inject: func(t *timeline) {
				t.put(-15, "k8s", "media", "Deployment", "session-cache", "scale", "info", "session-cache scaled from 1 to 2", scale(1, 2))
			}},
	}
}

func effectAt(t *timeline, typ, ns, pod, ow string, at float64) event.Event {
	if typ == "oom_kill" {
		return t.put(at, "k8s", ns, "Pod", pod, typ, "critical", pod+" restarted (OOMKilled, exit 137)", owner(ow))
	}
	if typ == "became_unready" {
		return unready(t, at+0.01, ns, pod, ow)
	}
	return t.put(at+0.01, "k8s", ns, "Pod", pod, typ, "warning", pod+" restarted (Error, exit 1)", owner(ow))
}

var untilFor = map[byte]float64{'A': 45, 'B': 100, 'C': 1500, 'D': 70, 'P': 25}

func (f fault) standard(t *timeline, v byte) event.Event {
	until := untilFor[v]
	s := f.site
	every := 18.0
	if v == 'C' {
		every = 40
	}
	if s.logText != "" {
		for at := 10.0; at <= until; at += every {
			t.put(at, "loki", s.logNS, "Pod", s.logPod, "log_error", "warning", s.logText, `{}`)
		}
	}
	if until >= 35 {
		t.put(35, "prometheus", s.latNS, "Service", s.latSvc, "latency_spike", "warning", "latency_p99 on "+s.latSvc+": 3.8", `{"value":3.8}`)
	}
	if until >= 40 {
		t.put(40, "prometheus", s.alertNS, "Service", s.alertSvc, "error_spike", "critical", "high_error_rate on "+s.alertSvc+": 0.62", `{"value":0.62}`)
	}
	if v == 'C' {
		t.put(1250, "prometheus", s.alertNS, "Service", s.alertSvc, "error_spike", "critical", "high_error_rate on "+s.alertSvc+": 0.66", `{"value":0.66}`)
	}
	switch v {
	case 'A':
		return t.put(until, "prometheus", s.alertNS, "Service", s.alertSvc, "error_spike", "critical", "high_error_rate on "+s.alertSvc+": 0.71", `{"value":0.71}`)
	case 'B', 'C':
		return t.put(until, "loki", s.logNS, "Pod", s.logPod, "log_error", "warning", s.logText, `{}`)
	case 'D':
		return t.put(until, "prometheus", s.latNS, "Service", s.latSvc, "latency_spike", "warning", "latency_p99 on "+s.latSvc+": 4.2", `{"value":4.2}`)
	default:
		return f.pod(t)
	}
}

func (f fault) build(variant string) Incident {
	t := &timeline{id: "ind/" + f.id + "/" + variant}
	background(t)
	f.inject(t)
	var symptom event.Event
	if fn, ok := f.custom[variant]; ok {
		symptom = fn(t)
	} else {
		symptom = f.standard(t, variant[0])
	}
	plausible := f.plausible
	if len(plausible) == 0 {
		plausible = f.actual
	}
	return Incident{
		ID: t.id, Category: f.category, Title: f.title, ActualRoots: f.actual, Plausible: plausible,
		Observable: f.observable, Ambiguous: f.ambiguous, AllRequired: f.allRequired, Expected: f.expected,
		InScope: !f.outOfScope, Notes: f.notes, Events: t.evs, Symptom: symptom, Edges: Edges(),
	}
}

func variantsOf(f fault) []string {
	if len(f.custom) > 0 {
		var out []string
		for k := range f.custom {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	var out []string
	for _, c := range []byte(f.variants) {
		out = append(out, string(c))
	}
	return out
}
