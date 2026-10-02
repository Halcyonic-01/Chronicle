package independent

import (
	"fmt"
	"sort"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// Timing, simultaneity and delayed-observation incidents. Each has one cause (a
// bad entitlement release) and a single symptom event, so the only thing that
// varies is when things are recorded.
//
// An incident is marked out of scope when it lies outside the analyzer's
// documented contract: causes are searched within a per-symptom lookback window,
// must strictly precede the symptom, and events must reach the store within a
// 30-second allowance of the symptom. These are reported separately, never
// dropped.

func timingBase(t *timeline) {
	background(t)
	d := find("media/entitlement")
	t.put(0, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.6 -> 7.7", `{"old_image":"entitlement:7.6","new_image":"entitlement:7.7"}`)
	rollout(t, d, 0)
	restarts(t, "media", "entitlement-r2-0", "entitlement", "container_restart", 16, 52)
}

var entitlementRoot = []Root{root("deploy", "Deployment", "media", "entitlement")}

func timingIncident(id, title string, inScope bool, notes string, build func(t *timeline) event.Event) Incident {
	t := &timeline{id: "ind/" + id}
	sym := build(t)
	return Incident{
		ID: t.id, Category: CatTiming, Title: title, ActualRoots: entitlementRoot, Plausible: entitlementRoot,
		Observable: true, Expected: RootCause, InScope: inScope, Notes: notes, Events: t.evs, Symptom: sym, Edges: Edges(),
	}
}

func timingIncidents() []Incident {
	var out []Incident
	// error_spike searches 10 minutes back, log_error 15, became_unready 5.
	type probe struct {
		kind    string
		delay   float64
		inScope bool
	}
	probes := []probe{
		{"error_spike", 5, true}, {"error_spike", 30, true}, {"error_spike", 120, true}, {"error_spike", 300, true},
		{"error_spike", 600, true}, {"error_spike", 630, false},
		{"log_error", 60, true}, {"log_error", 890, true}, {"log_error", 930, false},
		{"became_unready", 20, true}, {"became_unready", 290, true}, {"became_unready", 330, false},
	}
	for _, p := range probes {
		p := p
		note := ""
		if !p.inScope {
			note = "the cause is older than the symptom's lookback window"
		}
		out = append(out, timingIncident(fmt.Sprintf("timing/%s-after-%.0fs", p.kind, p.delay),
			fmt.Sprintf("%s %.0fs after the bad release", p.kind, p.delay), p.inScope, note,
			func(t *timeline) event.Event {
				timingBase(t)
				switch p.kind {
				case "error_spike":
					return t.put(p.delay, "prometheus", "media", "Service", "gateway", "error_spike", "critical", "high_error_rate on gateway: 0.7", `{"value":0.7}`)
				case "log_error":
					return t.put(p.delay, "loki", "media", "Pod", "player-api-r1-0", "log_error", "warning", "error: entitlement call failed: HTTP 502", `{}`)
				default:
					return unready(t, p.delay, "media", "gateway-r1-2", "gateway")
				}
			}))
	}

	// Simultaneity.
	out = append(out, timingIncident("simultaneity/effect-1s-after-cause", "the effect is recorded one second after its cause", true, "",
		func(t *timeline) event.Event {
			background(t)
			t.put(0, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.6 -> 7.7", `{"old_image":"entitlement:7.6","new_image":"entitlement:7.7"}`)
			return unready(t, 1, "media", "entitlement-r1-0", "entitlement")
		}))
	out = append(out, timingIncident("simultaneity/effect-in-same-instant", "cause and effect carry the same timestamp", false, "a cause must strictly precede its symptom",
		func(t *timeline) event.Event {
			background(t)
			t.put(0, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.6 -> 7.7", `{"old_image":"entitlement:7.6","new_image":"entitlement:7.7"}`)
			return t.put(0, "k8s", "media", "Pod", "entitlement-r1-0", "became_unready", "warning", "entitlement-r1-0 stopped serving traffic", `{"owner":"entitlement"}`)
		}))
	out = append(out, timingIncident("simultaneity/burst-of-restarts", "forty restart events within three seconds", true, "",
		func(t *timeline) event.Event {
			timingBase(t)
			for i := 0; i < 40; i++ {
				t.put(20+float64(i)*0.075, "k8s", "media", "Pod", "entitlement-r2-0", "container_restart", "warning", "entitlement-r2-0 restarted (Error, exit 1)", `{"owner":"entitlement"}`)
			}
			return t.put(60, "prometheus", "media", "Service", "gateway", "error_spike", "critical", "high_error_rate on gateway: 0.7", `{"value":0.7}`)
		}))
	amb := timingIncident("simultaneity/two-changes-one-second-apart", "a deploy and a config change one second apart on the same service", true, "evidence cannot order the two",
		func(t *timeline) event.Event {
			background(t)
			t.put(0, "k8s", "media", "Deployment", "catalog", "config_change", "info", "catalog configuration changed (catalog env DB_POOL)", `{"changed":["catalog env DB_POOL"],"from_hash":"c1","to_hash":"c2"}`)
			t.put(1, "k8s", "media", "Deployment", "catalog", "deploy", "info", "catalog deployed: 2.4 -> 2.5", `{"old_image":"catalog:2.4","new_image":"catalog:2.5"}`)
			d := find("media/catalog")
			rollout(t, d, 1)
			crashAfter(t, d, 2, 1, 18, 60)
			return t.put(70, "prometheus", "media", "Service", "gateway", "error_spike", "critical", "high_error_rate on gateway: 0.7", `{"value":0.7}`)
		})
	amb.ActualRoots = []Root{root("config_change", "Deployment", "media", "catalog")}
	amb.Plausible = []Root{root("config_change", "Deployment", "media", "catalog"), root("deploy", "Deployment", "media", "catalog")}
	amb.Ambiguous, amb.Expected = true, Ambiguous
	out = append(out, amb)

	// Delayed observation, as after a collector gap.
	late := func(id, title string, inScope bool, notes string, lateBy float64, keepTime bool) Incident {
		return timingIncident("delayed/"+id, title, inScope, notes, func(t *timeline) event.Event {
			background(t)
			at := 0.0
			if !keepTime {
				at = 420 // stamped when finally observed, after the symptom
			}
			t.putLate(at, lateBy, "k8s", "media", "Deployment", "entitlement", "deploy", "info", "entitlement deployed: 7.6 -> 7.7", `{"old_image":"entitlement:7.6","new_image":"entitlement:7.7"}`)
			t.put(1, "k8s", "media", "Pod", "entitlement-r2-0", "resource_created", "info", "entitlement-r2-0 created", owner("entitlement"))
			restarts(t, "media", "entitlement-r2-0", "entitlement", "container_restart", 16, 52)
			return t.put(60, "prometheus", "media", "Service", "gateway", "error_spike", "critical", "high_error_rate on gateway: 0.7", `{"value":0.7}`)
		})
	}
	out = append(out,
		late("cause-recorded-20s-late", "the cause reaches the store 20s late, with its true time", true, "", 20, true),
		late("cause-recorded-85s-late", "the cause reaches the store 85s late, still within 30s of the symptom's own ingestion", true, "", 85, true),
		late("cause-recorded-120s-late", "the cause reaches the store 120s late, more than 30s after the symptom was ingested", false, "ingested beyond the lateness allowance, which is measured from the symptom's own ingestion", 120, true),
		late("cause-replayed-after-7min-gap", "the cause is replayed after a collector gap and stamped when observed", false, "its true time was lost; it is recorded after the symptom", 0, false),
	)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// All returns every independent incident, sorted by ID.
func All() []Incident {
	var out []Incident
	for _, f := range faults() {
		for _, v := range variantsOf(f) {
			out = append(out, f.build(v))
		}
	}
	out = append(out, timingIncidents()...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
