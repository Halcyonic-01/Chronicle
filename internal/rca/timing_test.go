package rca

// Timing-variation properties. Each test states the behaviour the analyzer is
// DESIGNED to have, then checks it while the timing of the same incident moves:
// cause 5s to 10min before the symptom, the edge of the lookback window,
// near-simultaneous events, repeats, replicas, recoveries, repairs,
// controller-owned pods and late or replayed observation.
//
// They use their own small topology, not the benchmark sets.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

var tz = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)

func tnode(kind, name string) graph.Node {
	return graph.Node{Namespace: "shop", Kind: kind, Name: name}
}

// web -> api (deployment api owns pods api-r1-*, api-r2-*)
func timingEdges(replicas int) []graph.Edge {
	var e []graph.Edge
	add := func(f, t graph.Node, k string) {
		e = append(e, graph.Edge{From: f, To: t, Kind: k, Weight: 1, Source: "t"})
	}
	add(tnode("Pod", "web-r1-0"), tnode("Service", "api"), "calls")
	add(tnode("Service", "web"), tnode("Pod", "web-r1-0"), "routes_to")
	add(tnode("Deployment", "web"), tnode("Pod", "web-r1-0"), "owns")
	for rev := 1; rev <= 2; rev++ {
		for i := 0; i < replicas; i++ {
			p := fmt.Sprintf("api-r%d-%d", rev, i)
			add(tnode("Deployment", "api"), tnode("Pod", p), "owns")
			add(tnode("Service", "api"), tnode("Pod", p), "routes_to")
		}
	}
	return e
}

func tev(id string, at float64, kind, name, typ, sev string) event.Event {
	occurred := tz.Add(time.Duration(at * float64(time.Second)))
	return event.Event{ID: id, OccurredAt: occurred, IngestedAt: occurred.Add(time.Second), Namespace: "shop", EntityKind: kind, EntityName: name, Type: typ, Severity: sev,
		Title: typ + " " + name, Payload: []byte(`{"old_image":"a","new_image":"b","owner":"api"}`)}
}

func release() event.Event { return tev("release", 0, "Deployment", "api", "deploy", "info") }

func analyzeTiming(t *testing.T, events []event.Event, symptom event.Event, replicas int) *Result {
	t.Helper()
	a := &Analyzer{Events: &benchdata.Store{All: append(append([]event.Event{}, events...), symptom)}, Graph: benchdata.Graph{EdgeList: timingEdges(replicas)}, MaxHops: 8, GraphInterval: 30 * time.Second}
	got, err := a.Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func topIs(r *Result, typ, name string) bool {
	return len(r.Candidates) > 0 && r.Candidates[0].Event.Type == typ && r.Candidates[0].Event.EntityName == name
}

func describe(r *Result) string {
	if len(r.Candidates) == 0 {
		return "(none) " + r.Verdict
	}
	return fmt.Sprintf("%s:%s conf=%.2f %s", r.Candidates[0].Event.Type, r.Candidates[0].Event.EntityName, r.Confidence, r.Verdict)
}

// error_spike searches 10 minutes back. A cause inside is found however long
// before the symptom; one outside is not searched, by design.
func TestTimingTheCauseIsFoundAtAnyDelayInsideTheLookback(t *testing.T) {
	for _, d := range []float64{5, 30, 120, 300, 570, 600} {
		symptom := tev("s", d, "Service", "web", "error_spike", "critical")
		got := analyzeTiming(t, []event.Event{release()}, symptom, 1)
		if !topIs(got, "deploy", "api") {
			t.Errorf("cause %.0fs before the symptom: %s", d, describe(got))
		}
	}
}

func TestTimingTheCauseJustOutsideTheLookbackIsNotSearched(t *testing.T) {
	for _, d := range []float64{610, 630, 900} {
		got := analyzeTiming(t, []event.Event{release()}, tev("s", d, "Service", "web", "error_spike", "critical"), 1)
		if len(got.Candidates) != 0 || got.Verdict != VerdictNoRootCause {
			t.Errorf("cause %.0fs before an error_spike is outside its 10-minute window; got %s", d, describe(got))
		}
	}
}

// One second of separation is enough; the same instant is not, because a cause
// must strictly precede its symptom.
func TestTimingNearSimultaneousCauseAndEffect(t *testing.T) {
	if got := analyzeTiming(t, []event.Event{release()}, tev("s", 1, "Pod", "api-r1-0", "became_unready", "warning"), 1); !topIs(got, "deploy", "api") {
		t.Errorf("effect 1s after the cause: %s", describe(got))
	}
	if got := analyzeTiming(t, []event.Event{release()}, tev("s", 0, "Pod", "api-r1-0", "became_unready", "warning"), 1); len(got.Candidates) != 0 {
		t.Errorf("a cause at the same instant must not be a candidate (it cannot be shown to precede): %s", describe(got))
	}
}

// Moving every timestamp by the same amount cannot change the answer.
func TestTimingShiftingTheWholeIncidentChangesNothing(t *testing.T) {
	build := func(shift float64) *Result {
		evs := []event.Event{tev("release", shift, "Deployment", "api", "deploy", "info"), tev("r", shift+20, "Pod", "api-r2-0", "container_restart", "warning")}
		return analyzeTiming(t, evs, tev("s", shift+90, "Service", "web", "error_spike", "critical"), 1)
	}
	base := build(0)
	for _, shift := range []float64{-3600, -7, 7, 3600} {
		got := build(shift)
		if got.Candidates[0].Event.Type != base.Candidates[0].Event.Type || got.Confidence != base.Confidence {
			t.Errorf("shift %.0fs: %s versus %s", shift, describe(got), describe(base))
		}
	}
}

// A second or two of clock jitter on the effects must not flip the answer.
func TestTimingJitterOnTheEffectsDoesNotFlipTheCause(t *testing.T) {
	for _, j := range []float64{-3, -1, 1, 3} {
		evs := []event.Event{release(), tev("r1", 20+j, "Pod", "api-r2-0", "container_restart", "warning"), tev("u", 21-j, "Pod", "api-r2-0", "became_unready", "warning")}
		if got := analyzeTiming(t, evs, tev("s", 90, "Service", "web", "error_spike", "critical"), 1); !topIs(got, "deploy", "api") {
			t.Errorf("jitter %+.0fs: %s", j, describe(got))
		}
	}
}

// The same failure repeated once, five times or fifty times is the same story.
func TestTimingRepeatedEventsDoNotChangeTheRoot(t *testing.T) {
	for _, n := range []int{1, 5, 50} {
		evs := []event.Event{release()}
		for i := 0; i < n; i++ {
			evs = append(evs, tev(fmt.Sprintf("r%d", i), 15+float64(i)*3, "Pod", "api-r2-0", "container_restart", "warning"))
		}
		if got := analyzeTiming(t, evs, tev("s", 15+float64(n)*3+30, "Service", "web", "error_spike", "critical"), 1); !topIs(got, "deploy", "api") {
			t.Errorf("%d repeats: %s", n, describe(got))
		}
	}
}

// One failing replica or eight is the same cause.
func TestTimingTheNumberOfFailingReplicasDoesNotChangeTheRoot(t *testing.T) {
	for _, n := range []int{1, 3, 8} {
		evs := []event.Event{release()}
		for i := 0; i < n; i++ {
			evs = append(evs, tev(fmt.Sprintf("c%d", i), 1, "Pod", fmt.Sprintf("api-r2-%d", i), "resource_created", "info"),
				tev(fmt.Sprintf("u%d", i), 20+float64(i), "Pod", fmt.Sprintf("api-r2-%d", i), "container_restart", "warning"))
		}
		if got := analyzeTiming(t, evs, tev("s", 80, "Service", "web", "error_spike", "critical"), n); !topIs(got, "deploy", "api") {
			t.Errorf("%d replicas: %s", n, describe(got))
		}
	}
}

// A recovery that follows the break, and a symptom still inside the break, must
// not move the answer.
func TestTimingARecoveryAfterTheSymptomDoesNotMoveTheAnswer(t *testing.T) {
	evs := []event.Event{release(), tev("r", 20, "Pod", "api-r2-0", "container_restart", "warning"), tev("ok", 200, "Pod", "api-r2-0", "became_ready", "info")}
	if got := analyzeTiming(t, evs, tev("s", 90, "Service", "web", "error_spike", "critical"), 1); !topIs(got, "deploy", "api") {
		t.Errorf("%s", describe(got))
	}
}

// A repair near the symptom is not the cause. While the failure continues the
// break stays the answer however long ago the repair was made.
func TestTimingARepairNearTheSymptomIsNotTheCause(t *testing.T) {
	for _, gap := range []float64{5, 30, 120} {
		repairAt := 200.0
		evs := []event.Event{release(), tev("r", 20, "Pod", "api-r2-0", "container_restart", "warning"), tev("f", 100, "Pod", "api-r2-0", "resource_status", "critical")}
		fix := tev("fix", repairAt, "Deployment", "api", "deploy", "info")
		fix.Payload = []byte(`{"old_image":"b","new_image":"a"}`)
		evs = append(evs, fix)
		// The failure carries on after the repair, so the symptom is part of the same episode.
		for s := 40.0; s <= repairAt+gap; s += 20 {
			l := tev(fmt.Sprintf("l%.0f", s), s, "Service", "web", "error_spike", "critical")
			l.Title = "high_error_rate on web"
			evs = append(evs, l)
		}
		got := analyzeTiming(t, evs, tev("s", repairAt+gap+20, "Service", "web", "error_spike", "critical"), 1)
		if len(got.Candidates) == 0 || got.Candidates[0].Event.ID == "fix" {
			t.Errorf("repair %.0fs before the symptom, failure continuing: the repair led: %s", gap, describe(got))
		}
	}
}

// Controller-owned pod creation is the owner's doing and never a cause.
func TestTimingControllerOwnedPodCreationIsNeverTheCause(t *testing.T) {
	evs := []event.Event{release()}
	for i := 0; i < 4; i++ {
		evs = append(evs, tev(fmt.Sprintf("c%d", i), 1+float64(i), "Pod", fmt.Sprintf("api-r2-%d", i), "resource_created", "info"))
	}
	got := analyzeTiming(t, evs, tev("s", 60, "Pod", "api-r2-0", "became_unready", "warning"), 4)
	for _, c := range got.Candidates {
		if c.Event.Type == "resource_created" {
			t.Errorf("a controller's pod creation was offered as a cause: %s", describe(got))
		}
	}
	if !topIs(got, "deploy", "api") {
		t.Errorf("%s", describe(got))
	}
}

// DESIGN LIMIT, asserted so it stays visible: an event is seen only if it was
// ingested no later than 60 seconds after the symptom's own ingestion (enough
// for a node declared NotReady 50s after its last heartbeat). So a
// cause observed late is still found when the symptom is recorded soon after,
// and is lost once the gap exceeds the allowance. An event replayed after a
// collector gap and stamped when observed lands after the symptom and cannot be
// its cause; the checkpoint catch-up restores such events but not the moment
// they happened once that is more than two minutes old.
func TestTimingDelayedObservationIsOnlyToleratedWithinTheLatenessAllowance(t *testing.T) {
	symptom := tev("s", 60, "Service", "web", "error_spike", "critical") // ingested at 61s
	ingestedAfterSymptom := func(sec float64) *Result {
		r := release()
		r.IngestedAt = symptom.IngestedAt.Add(time.Duration(sec * float64(time.Second)))
		return analyzeTiming(t, []event.Event{r}, symptom, 1)
	}
	for _, sec := range []float64{-30, 0, 25, 55} {
		if got := ingestedAfterSymptom(sec); !topIs(got, "deploy", "api") {
			t.Errorf("cause ingested %+.0fs from the symptom's ingestion should be found: %s", sec, describe(got))
		}
	}
	for _, sec := range []float64{65, 120, 400} {
		if got := ingestedAfterSymptom(sec); len(got.Candidates) != 0 {
			t.Errorf("cause ingested %.0fs after the symptom is outside the allowance and was found: %s", sec, describe(got))
		}
	}
	stamped := release() // replayed after a gap: stamped when observed, i.e. after the symptom
	stamped.OccurredAt, stamped.IngestedAt = tz.Add(420*time.Second), tz.Add(421*time.Second)
	if got := analyzeTiming(t, []event.Event{stamped}, symptom, 1); len(got.Candidates) != 0 {
		t.Errorf("an event stamped after the symptom cannot be its cause: %s", describe(got))
	}
}
