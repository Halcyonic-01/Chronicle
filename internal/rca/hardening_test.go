package rca

// Regression and property tests for the independent benchmark's findings F1-F5.
// Each states the behaviour the analyzer is designed to have and checks it on
// its own small topology, never on the benchmark sets:
//
//	web -> api -> {cache, db};  web -> search (a sibling nothing else depends on)

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

var th0 = time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)

func hnode(kind, name string) graph.Node {
	return graph.Node{Namespace: "prod", Kind: kind, Name: name}
}

func hardeningEdges() []graph.Edge {
	var e []graph.Edge
	add := func(f, t graph.Node, k string) {
		e = append(e, graph.Edge{From: f, To: t, Kind: k, Weight: 1, Source: "h"})
	}
	pods := map[string][]string{"web": {"web-1"}, "api": {"api-1", "api-2"}, "cache": {"cache-1", "cache-2"}, "db": {"db-1", "db-2"}, "search": {"search-1"}}
	deps := make([]string, 0, len(pods))
	for d := range pods {
		deps = append(deps, d)
	}
	sort.Strings(deps)
	for _, d := range deps {
		for _, p := range pods[d] {
			add(hnode("Deployment", d), hnode("Pod", p), "owns")
			add(hnode("Service", d), hnode("Pod", p), "routes_to")
		}
	}
	add(hnode("Pod", "web-1"), hnode("Service", "api"), "calls")
	add(hnode("Pod", "web-1"), hnode("Service", "search"), "calls")
	for _, p := range pods["api"] {
		add(hnode("Pod", p), hnode("Service", "cache"), "calls")
		add(hnode("Pod", p), hnode("Service", "db"), "calls")
	}
	return e
}

type hb struct {
	evs []event.Event
	n   int
}

func (b *hb) add(sec float64, kind, name, typ, sev, title, payload string) event.Event {
	b.n++
	at := th0.Add(time.Duration(sec * float64(time.Second)))
	e := event.Event{ID: fmt.Sprintf("h%03d", b.n), OccurredAt: at, IngestedAt: at.Add(time.Second), Namespace: "prod",
		EntityKind: kind, EntityName: name, Type: typ, Severity: sev, Title: title, Payload: []byte(payload)}
	b.evs = append(b.evs, e)
	return e
}

func (b *hb) scale(sec float64, dep string, from, to int) event.Event {
	return b.add(sec, "Deployment", dep, "scale", "info", fmt.Sprintf("%s scaled from %d to %d", dep, from, to), fmt.Sprintf(`{"old_replicas":%d,"new_replicas":%d}`, from, to))
}

func (b *hb) deploy(sec float64, dep, from, to string) event.Event {
	return b.add(sec, "Deployment", dep, "deploy", "info", dep+" deployed", fmt.Sprintf(`{"old_image":%q,"new_image":%q}`, from, to))
}

func (b *hb) config(sec float64, dep, from, to string) event.Event {
	return b.add(sec, "Deployment", dep, "config_change", "info", dep+" configuration changed", fmt.Sprintf(`{"from_hash":%q,"to_hash":%q}`, from, to))
}

// rollout: the controller creates newPod, then shuts oldPod down.
func (b *hb) rollout(sec float64, dep, oldPod, newPod string) {
	b.add(sec, "Pod", newPod, "resource_created", "info", newPod+" created", fmt.Sprintf(`{"owner":%q}`, dep))
	b.add(sec+1, "Pod", oldPod, "became_unready", "warning", oldPod+" stopped serving traffic", fmt.Sprintf(`{"owner":%q}`, dep))
	b.add(sec+2, "Pod", oldPod, "resource_deleted", "info", oldPod+" deleted", `{}`)
}

func (b *hb) fail(typ, pod, owner string, secs ...float64) {
	for _, s := range secs {
		b.add(s, "Pod", pod, typ, "warning", pod+" "+typ, fmt.Sprintf(`{"owner":%q}`, owner))
	}
}

func (b *hb) logs(pod, text string, from, to float64) {
	for s := from; s <= to; s += 15 {
		b.add(s, "Pod", pod, "log_error", "warning", text, `{}`)
	}
}

func (b *hb) alert(sec float64, svc string) event.Event {
	return b.add(sec, "Service", svc, "error_spike", "critical", "high_error_rate on "+svc+": 0.9", `{"value":0.9}`)
}

func (b *hb) analyze(t *testing.T, symptom event.Event) *Result {
	t.Helper()
	a := &Analyzer{Events: &benchdata.Store{All: b.evs}, Graph: benchdata.Graph{EdgeList: hardeningEdges()}, MaxHops: 8, GraphInterval: 30 * time.Second}
	got, err := a.Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func lead(r *Result) string {
	if len(r.Candidates) == 0 {
		return "(none)"
	}
	c := r.Candidates[0]
	return fmt.Sprintf("%s:%s verdict=%s conf=%.2f", c.Event.Type, c.Event.EntityName, r.Verdict, r.Confidence)
}

func leads(r *Result, typ, name string) bool {
	return len(r.Candidates) > 0 && r.Candidates[0].Event.Type == typ && r.Candidates[0].Event.EntityName == name
}

func hasFactor(c Candidate, label string) bool {
	for _, f := range c.Factors {
		if f.Label == label {
			return true
		}
	}
	return false
}

// --- F1: a capacity increase is not a cause ---------------------------------

// Adding replicas takes nothing away: whatever the size or timing of a scale-up,
// unexplained errors after it have no observed root cause.
func TestF1AScaleUpIsNeverTheRootCause(t *testing.T) {
	for _, s := range []struct {
		from, to int
		before   float64
	}{{1, 2, 5}, {1, 2, 15}, {2, 5, 30}, {3, 10, 120}, {4, 6, 400}} {
		b := &hb{}
		b.scale(-s.before, "cache", s.from, s.to)
		b.logs("web-1", "api returned HTTP 500", 0, 45)
		got := b.analyze(t, b.alert(60, "web"))
		if got.Verdict != VerdictNoRootCause {
			t.Errorf("scale %d->%d %.0fs before: %s, want no_root_cause", s.from, s.to, s.before, lead(got))
		}
		for _, c := range got.Candidates {
			if c.Event.Type == "scale" && (rootClass(c) || !hasFactor(c, "Capacity increase") || c.NotRoot == "") {
				t.Errorf("scale %d->%d: the scale-up was not ruled out: %+v", s.from, s.to, c.Factors)
			}
		}
		if !strings.Contains(got.Narrative, "takes nothing away") {
			t.Errorf("the narrative should say why the scale-up was ruled out: %s", got.Narrative)
		}
	}
}

// Next to a real cause, a scale-up is neither the answer nor a rival.
func TestF1AScaleUpDoesNotRivalTheRealCause(t *testing.T) {
	b := &hb{}
	b.scale(0, "db", 1, 0)
	b.add(1, "Pod", "db-1", "became_unready", "warning", "db-1 stopped serving traffic", `{"owner":"db"}`)
	b.scale(5, "cache", 2, 4)
	b.logs("web-1", "api returned HTTP 500", 8, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if !leads(got, "scale", "db") || got.Verdict == VerdictAmbiguous || got.Verdict == VerdictNoRootCause {
		t.Fatalf("the scale to zero should be the answer: %s", lead(got))
	}
	for _, a := range got.Alternatives {
		if a.Event.EntityName == "cache" {
			t.Fatal("a capacity increase was offered as an alternative")
		}
	}
}

// A capacity reduction can overload what remains, so it stays a possible cause.
func TestF1AScaleDownStillCounts(t *testing.T) {
	b := &hb{}
	b.scale(0, "cache", 3, 1)
	b.logs("web-1", "api request timed out", 20, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if !leads(got, "scale", "cache") || got.Verdict == VerdictNoRootCause {
		t.Fatalf("a scale-down is a candidate root: %s", lead(got))
	}
}

// --- F2: an unrecorded cause does not turn its effect into the root -----------

// A rollout means a change to the owner, recorded or not. Without the change,
// the new pod's failures are effects of something unobserved, at any delay that
// keeps the pod's creation inside the analysis window.
func TestF2ARolloutWithoutItsChangeHasNoRootCause(t *testing.T) {
	for _, kind := range []string{"container_restart", "oom_kill", "became_unready"} {
		for _, delay := range []float64{5, 30, 120, 450} {
			b := &hb{}
			b.rollout(0, "api", "api-1", "api-2")
			b.fail(kind, "api-2", "api", delay, delay+40, delay+110)
			b.logs("web-1", "api returned HTTP 500", delay+5, delay+80)
			got := b.analyze(t, b.alert(delay+90, "web"))
			if got.Verdict != VerdictNoRootCause {
				t.Errorf("%s %.0fs after an unrecorded rollout: %s, want no_root_cause", kind, delay, lead(got))
				continue
			}
			for _, c := range got.Candidates {
				if (c.Event.EntityName == "api-1" || c.Event.EntityName == "api-2") && rootClass(c) {
					t.Errorf("%s %.0fs: a rollout pod's %s was left as a possible root", kind, delay, c.Event.Type)
				}
			}
			if !strings.Contains(got.Narrative, "No root cause was observed") || !strings.Contains(got.Narrative, "created this pod") {
				t.Errorf("%s %.0fs: the narrative should refuse and say why: %s", kind, delay, got.Narrative)
			}
		}
	}
}

// DESIGN LIMIT, asserted so it stays visible: a pod is judged new only when its
// creation is inside the analysis window. Restarts extend the window back to
// where they began, so a crash loop is covered at any delay; an unready pod
// whose creation fell out of the window looks established and is still named.
func TestF2ThePodsCreationMustBeInsideTheWindow(t *testing.T) {
	b := &hb{}
	b.rollout(0, "api", "api-1", "api-2")
	b.fail("container_restart", "api-2", "api", 590, 630)
	b.logs("web-1", "api returned HTTP 500", 595, 670)
	if got := b.analyze(t, b.alert(680, "web")); got.Verdict != VerdictNoRootCause {
		t.Errorf("restarts reach back to the rollout: %s, want no_root_cause", lead(got))
	}
	c := &hb{}
	c.rollout(0, "api", "api-1", "api-2")
	c.fail("became_unready", "api-2", "api", 590)
	c.logs("web-1", "api returned HTTP 500", 595, 670)
	if got := c.analyze(t, c.alert(680, "web")); !leads(got, "became_unready", "api-2") {
		t.Errorf("the documented limit changed; update OBSERVABILITY.md: %s", lead(got))
	}
}

// The same rollout with its change recorded names the change.
func TestF2TheSameRolloutWithItsChangeNamesTheChange(t *testing.T) {
	b := &hb{}
	b.deploy(-1, "api", "v1", "v2")
	b.rollout(0, "api", "api-1", "api-2")
	b.fail("container_restart", "api-2", "api", 14, 54, 124)
	b.logs("web-1", "api returned HTTP 500", 20, 90)
	got := b.analyze(t, b.alert(100, "web"))
	if !leads(got, "deploy", "api") || got.Verdict != VerdictRootCause {
		t.Fatalf("the recorded deploy should be the root: %s", lead(got))
	}
}

// A crash loop on a pod that was already running, with no change behind it, is
// where the failure began: it stays a root cause.
func TestF2ACrashOnAnEstablishedPodIsStillTheRoot(t *testing.T) {
	b := &hb{}
	b.fail("container_restart", "api-1", "api", 0, 40, 110)
	b.add(1, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
	b.logs("web-1", "api returned HTTP 500", 5, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if !leads(got, "container_restart", "api-1") || got.Verdict == VerdictNoRootCause {
		t.Fatalf("an established pod's crash loop is the root: %s", lead(got))
	}
}

// A change recorded too late to be seen leaves no root cause, not its effect.
func TestF2ACauseRecordedTooLateIsNotReplacedByItsEffect(t *testing.T) {
	for _, late := range []float64{10, 120, 420} {
		b := &hb{}
		d := b.deploy(-1, "api", "v1", "v2")
		b.rollout(0, "api", "api-1", "api-2")
		b.fail("container_restart", "api-2", "api", 14, 54)
		b.logs("web-1", "api returned HTTP 500", 20, 50)
		symptom := b.alert(60, "web")
		b.evs[0].IngestedAt = symptom.IngestedAt.Add(time.Duration(late * float64(time.Second)))
		if late > allowedLateness.Seconds() && late*float64(time.Second) > float64(maxCausalSkew) {
			b.evs[0].OccurredAt = b.evs[0].IngestedAt // replayed: stamped when seen
		}
		got := b.analyze(t, symptom)
		seen := late <= allowedLateness.Seconds()
		switch {
		case seen && !leads(got, "deploy", d.EntityName):
			t.Errorf("a deploy ingested %.0fs after the symptom is inside the allowance: %s", late, lead(got))
		case !seen && got.Verdict != VerdictNoRootCause:
			t.Errorf("a deploy ingested %.0fs late is not visible, so nothing explains the failure: %s", late, lead(got))
		}
	}
}

// A pod going unready just before it is deleted is being shut down: with no
// recorded reason, there is no root cause, not a failing pod.
func TestF2APodBeingShutDownIsNotTheRoot(t *testing.T) {
	b := &hb{}
	b.add(10, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
	b.add(25, "Pod", "api-1", "resource_deleted", "info", "api-1 deleted", `{}`)
	b.logs("web-1", "api returned HTTP 503", 12, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if got.Verdict != VerdictNoRootCause {
		t.Fatalf("a pod shut down for an unrecorded reason: %s, want no_root_cause", lead(got))
	}
	// Control: the same unready pod that is not deleted is a failure.
	c := &hb{}
	c.add(10, "Pod", "api-1", "became_unready", "warning", "api-1 stopped serving traffic", `{"owner":"api"}`)
	c.logs("web-1", "api returned HTTP 503", 12, 50)
	if got := c.analyze(t, c.alert(60, "web")); !leads(got, "became_unready", "api-1") || got.Verdict == VerdictNoRootCause {
		t.Fatalf("an unready pod that stays is the failure's origin: %s", lead(got))
	}
}

// --- F3: corroboration --------------------------------------------------------

// A change that explains none of the failures observed is not a rival of the
// change whose failures were seen, wherever it sits in time.
func TestF3ABystanderChangeIsNotARival(t *testing.T) {
	for _, at := range []float64{10, 30, 50, 80} {
		b := &hb{}
		b.config(0, "db", "a", "b")
		b.fail("container_restart", "db-1", "db", 20, 60, 85)
		b.deploy(at, "search", "s1", "s2") // a sibling: it cannot reach db
		b.logs("web-1", "api returned HTTP 500", 25, 85)
		got := b.analyze(t, b.alert(90, "web"))
		if !leads(got, "config_change", "db") || got.Verdict == VerdictAmbiguous || got.Verdict == VerdictNoRootCause {
			t.Errorf("bystander at %.0fs: %s, want the db change", at, lead(got))
		}
		for _, c := range got.Candidates {
			if c.Event.EntityName == "search" && !hasFactor(c, "Uncorroborated") {
				t.Errorf("bystander at %.0fs was not held back: %+v", at, c.Factors)
			}
		}
	}
}

// Two changes that can both explain the same failure stay ambiguous: the guard
// is not weakened by corroboration.
func TestF3ChangesThatExplainTheSameFailureStayAmbiguous(t *testing.T) {
	b := &hb{}
	b.config(0, "api", "a", "b")
	b.deploy(3, "api", "v1", "v2")
	b.rollout(4, "api", "api-1", "api-2")
	b.fail("container_restart", "api-2", "api", 18, 58)
	b.logs("web-1", "api returned HTTP 500", 20, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if got.Verdict != VerdictAmbiguous || len(got.Alternatives) == 0 {
		t.Fatalf("two changes to the failing workload cannot be separated: %s alternatives=%d", lead(got), len(got.Alternatives))
	}
}

// An upstream change that could have caused the same failure still competes:
// corroboration is topological, so a silent dependency is not ruled out.
func TestF3AnUpstreamChangeThatCouldExplainTheFailureStillCompetes(t *testing.T) {
	b := &hb{}
	b.deploy(0, "cache", "c1", "c2")
	b.deploy(2, "api", "v1", "v2")
	b.rollout(3, "api", "api-1", "api-2")
	b.fail("container_restart", "api-2", "api", 16, 56)
	b.logs("web-1", "api returned HTTP 500", 20, 50)
	got := b.analyze(t, b.alert(60, "web"))
	var api, cache *Candidate
	for i := range got.Candidates {
		switch c := &got.Candidates[i]; {
		case c.Event.Type == "deploy" && c.Event.EntityName == "api":
			api = c
		case c.Event.Type == "deploy" && c.Event.EntityName == "cache":
			cache = c
		}
	}
	if api == nil || cache == nil {
		t.Fatalf("both deploys should be candidates: %+v", got.Candidates)
	}
	if hasFactor(*cache, "Uncorroborated") || !competes(*cache, evidenceOf(*api)) {
		t.Fatal("a dependency's change can explain its caller's crash and must keep competing")
	}
}

// Two faults that each explain their own failures are separate problems, not
// two readings of one.
func TestF3IndependentFaultsAreNotAlternatives(t *testing.T) {
	b := &hb{}
	b.scale(0, "cache", 2, 0)
	b.add(1, "Pod", "cache-1", "became_unready", "warning", "cache-1 stopped serving traffic", `{"owner":"cache"}`)
	b.scale(4, "db", 2, 0)
	b.add(5, "Pod", "db-1", "became_unready", "warning", "db-1 stopped serving traffic", `{"owner":"db"}`)
	b.logs("web-1", "api returned HTTP 500", 8, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if got.Verdict == VerdictAmbiguous || got.Verdict == VerdictNoRootCause {
		t.Fatalf("each fault is a cause in its own right: %s", lead(got))
	}
	if len(got.Candidates) < 2 || !got.Contested {
		t.Fatal("the second fault must still be visible and lower the confidence")
	}
}

// A failure with no change behind it says nothing about a change elsewhere: a
// quiet but real change (a broken selector) is not held back by an unrelated
// crash loop, and the two stay rivals.
func TestF3AnUnexplainedCrashDoesNotHoldBackAQuietChange(t *testing.T) {
	b := &hb{}
	b.fail("container_restart", "search-1", "search", -20, 25)
	b.add(0, "Service", "db", "service_change", "info", "db service changed (selector)", `{"changed":["selector"],"from_hash":"a","to_hash":"b"}`)
	b.logs("web-1", "api returned HTTP 500", 10, 50)
	got := b.analyze(t, b.alert(60, "web"))
	for _, c := range got.Candidates {
		if c.Event.Type == "service_change" && hasFactor(c, "Uncorroborated") {
			t.Fatalf("the selector change was held back by an unrelated crash: %+v", c.Factors)
		}
	}
	if got.Verdict == VerdictNoRootCause {
		t.Fatalf("a recorded change and a crash are both candidates: %s", lead(got))
	}
}

// With no failure facts at all there is nothing to corroborate against.
func TestF3NothingIsHeldBackWhenNoFailureWasObserved(t *testing.T) {
	b := &hb{}
	b.add(0, "Service", "db", "service_change", "info", "db service changed (selector)", `{"changed":["selector"],"from_hash":"a","to_hash":"b"}`)
	b.logs("web-1", "api returned HTTP 500", 10, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if !leads(got, "service_change", "db") || hasFactor(got.Candidates[0], "Uncorroborated") {
		t.Fatalf("a lone change with only measurements behind it keeps its weight: %s", lead(got))
	}
}

// --- F4: a recovered incident does not resurface ------------------------------

// A change whose incident recovered before this one began explained that
// incident, not this one.
func TestF4AChangeWhoseIncidentRecoveredIsNotTheCause(t *testing.T) {
	for _, recoveredAgo := range []float64{90, 180, 300} {
		b := &hb{}
		b.deploy(-480, "api", "v1", "v2")
		b.alert(-470, "web")
		b.add(-recoveredAgo, "Service", "web", "error_spike_resolved", "info", "high_error_rate resolved on web", `{}`)
		b.logs("web-1", "api returned HTTP 502", 5, 50)
		got := b.analyze(t, b.alert(60, "web"))
		if got.Verdict != VerdictNoRootCause {
			t.Errorf("recovered %.0fs before: %s, want no_root_cause", recoveredAgo, lead(got))
		}
		for _, c := range got.Candidates {
			if c.Event.Type == "deploy" && (rootClass(c) || !hasFactor(c, "Recovered")) {
				t.Errorf("recovered %.0fs before: the old deploy was not set aside", recoveredAgo)
			}
		}
	}
}

// A recovery that held for less than the flap window is a flap, not an ending.
func TestF4AFlapIsNotARecovery(t *testing.T) {
	b := &hb{}
	b.deploy(0, "api", "v1", "v2")
	b.fail("became_unready", "api-1", "api", 10)
	b.add(30, "Pod", "api-1", "became_ready", "info", "api-1 started serving traffic", `{"owner":"api"}`)
	b.logs("web-1", "api returned HTTP 500", 60, 80)
	got := b.analyze(t, b.alert(90, "web"))
	if !leads(got, "deploy", "api") {
		t.Fatalf("a 30s recovery before the errors is a flap: %s", lead(got))
	}
}

// A pod that keeps failing between ready spells is one ongoing failure, however
// long each ready spell lasts inside the kubelet's back-off.
func TestF4AFlappingPodIsNotARecovery(t *testing.T) {
	b := &hb{}
	b.config(-400, "api", "a", "b")
	for _, s := range []float64{-390, -210, -30} {
		b.fail("became_unready", "api-1", "api", s)
		b.add(s+90, "Pod", "api-1", "became_ready", "info", "api-1 started serving traffic", `{"owner":"api"}`)
	}
	b.logs("web-1", "api returned HTTP 500", 5, 50)
	got := b.analyze(t, b.alert(60, "web"))
	for _, c := range got.Candidates {
		if c.Event.Type == "config_change" && hasFactor(c, "Recovered") {
			t.Fatal("a flapping pod's ready spells were taken for a recovery")
		}
	}
}

// Silence is not recovery: a failure with no recorded recovery keeps its cause.
func TestF4AnUnresolvedEarlierFailureKeepsItsCause(t *testing.T) {
	b := &hb{}
	b.deploy(-400, "api", "v1", "v2")
	b.fail("container_restart", "api-1", "api", -390)
	b.logs("web-1", "api returned HTTP 500", 5, 50)
	got := b.analyze(t, b.alert(60, "web"))
	for _, c := range got.Candidates {
		if c.Event.Type == "deploy" && hasFactor(c, "Recovered") {
			t.Fatal("nothing recovered, so the deploy is not history")
		}
	}
}

// A healthy rollout shuts the old pod down; that is not an incident, so the
// change is not history when its failure starts later.
func TestF4AHealthyRolloutIsNotAnIncident(t *testing.T) {
	b := &hb{}
	b.deploy(-300, "api", "v1", "v2")
	b.rollout(-299, "api", "api-1", "api-2")
	b.add(-290, "Pod", "api-2", "became_ready", "info", "api-2 started serving traffic", `{"owner":"api"}`)
	b.add(5, "Pod", "api-2", "oom_kill", "critical", "api-2 OOMKilled", `{"owner":"api"}`)
	b.logs("web-1", "api returned HTTP 500", 8, 50)
	got := b.analyze(t, b.alert(60, "web"))
	for _, c := range got.Candidates {
		if c.Event.Type == "deploy" && hasFactor(c, "Recovered") {
			t.Fatal("the rollout's own pod shutdown was mistaken for a recovered incident")
		}
	}
}

// --- F5: confidence near the edge of the lookback -----------------------------

// A cause whose failure began right after it keeps its confidence however long
// before the alert it was: the gap is accounted for by evidence.
func TestF5ConfidenceDoesNotCollapseWhenEvidenceBridgesTheGap(t *testing.T) {
	confidence := map[float64]float64{}
	for _, gap := range []float64{60, 300, 540, 595} {
		b := &hb{}
		b.deploy(-gap, "api", "v1", "v2")
		b.rollout(-gap+1, "api", "api-1", "api-2")
		b.fail("container_restart", "api-2", "api", -gap+15)
		b.logs("web-1", "api returned HTTP 500", -gap+20, -gap+40)
		got := b.analyze(t, b.alert(0, "web"))
		if !leads(got, "deploy", "api") {
			t.Fatalf("gap %.0fs: %s", gap, lead(got))
		}
		if got.Verdict != VerdictRootCause {
			t.Errorf("gap %.0fs: the bridged cause should be a root cause: %s", gap, lead(got))
		}
		confidence[gap] = got.Confidence
	}
	if spread := confidence[60] - confidence[595]; spread > 0.1 {
		t.Errorf("confidence fell by %.2f across the window although the evidence is the same: %v", spread, confidence)
	}
}

// With nothing observed between a change and the alert, the gap is unexplained
// and confidence stays low near the edge: that is the evidence, not the window.
func TestF5AnUnexplainedGapStaysWeak(t *testing.T) {
	b := &hb{}
	b.deploy(-590, "api", "v1", "v2")
	got := b.analyze(t, b.alert(0, "web"))
	if !leads(got, "deploy", "api") {
		t.Fatalf("%s", lead(got))
	}
	if got.Confidence >= defaultConfig.InconclusiveBelow {
		t.Fatalf("a lone change ten minutes before, with nothing between, is weak evidence: %.2f", got.Confidence)
	}
}

// Bridging can only add evidence: it never lowers a candidate's strength.
func TestF5BridgingNeverLowersStrength(t *testing.T) {
	c := Candidate{Event: event.Event{OccurredAt: th0, IngestedAt: th0}, Gap: 30, firstEffect: th0.Add(90 * time.Second)}
	bridgeToFirstEffect([]Candidate{c}, th0.Add(30*time.Second), 200)
	cands := []Candidate{c}
	bridgeToFirstEffect(cands, th0.Add(30*time.Second), 200)
	if cands[0].bridge != 0 {
		t.Fatalf("an effect after the onset must not move strength, got bridge %.2f", cands[0].bridge)
	}
}

// --- determinism ------------------------------------------------------------

func TestHardeningIsDeterministic(t *testing.T) {
	build := func() *Result {
		b := &hb{}
		b.config(0, "db", "a", "b")
		b.fail("container_restart", "db-1", "db", 20, 60)
		b.deploy(30, "search", "s1", "s2")
		b.scale(35, "cache", 1, 3)
		b.logs("web-1", "api returned HTTP 500", 25, 85)
		return b.analyze(t, b.alert(90, "web"))
	}
	first := build()
	for i := 0; i < 20; i++ {
		got := build()
		if got.Verdict != first.Verdict || got.Confidence != first.Confidence || lead(got) != lead(first) || len(got.Alternatives) != len(first.Alternatives) {
			t.Fatalf("run %d differs: %s versus %s", i, lead(got), lead(first))
		}
	}
}

// Found by a chaos run: a pod rolled out by an earlier, recovered incident,
// which then served healthily for minutes, crashed on its own. It is
// established by then, so the crash is the root, not an effect of its creation.
func TestF2APodThatServedHealthilyIsEstablished(t *testing.T) {
	b := &hb{}
	b.rollout(-600, "api", "api-1", "api-2")
	b.add(-590, "Pod", "api-2", "became_ready", "info", "api-2 started serving traffic", `{"owner":"api"}`)
	b.fail("container_restart", "api-2", "api", 0, 40, 110)
	b.logs("web-1", "api returned HTTP 500", 5, 50)
	got := b.analyze(t, b.alert(60, "web"))
	if !leads(got, "container_restart", "api-2") || got.Verdict == VerdictNoRootCause {
		t.Fatalf("a crash after ten healthy minutes is the root: %s", lead(got))
	}
	// Control: ready for a moment, then crashing, is still the rollout's failure.
	c := &hb{}
	c.rollout(0, "api", "api-1", "api-2")
	c.add(10, "Pod", "api-2", "became_ready", "info", "api-2 started serving traffic", `{"owner":"api"}`)
	c.fail("container_restart", "api-2", "api", 30, 70, 140)
	c.logs("web-1", "api returned HTTP 500", 35, 110)
	if got := c.analyze(t, c.alert(120, "web")); got.Verdict != VerdictNoRootCause {
		t.Fatalf("a crash 20s after first readiness belongs to the unrecorded rollout: %s", lead(got))
	}
}
