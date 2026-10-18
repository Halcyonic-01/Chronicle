package rca

// A repeat outage must be diagnosed on its own evidence. Found live: the same
// redis scale-to-zero scored 0.81 the first time and 0.38 (inconclusive) five
// minutes later, because two log lines from the first outage were never closed
// and made the second fault look like a repair of the first.

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

// The real captures of a repeat redis outage. Each was analysed live with
// a defect the replay reproduces exactly (see "about" in each file).
var repeatFixtures = []string{"testdata/repeat_redis_outage.json", "testdata/repeat_redis_outage_boundary.json", "testdata/repeat_redis_outage_incident_end.json"}

type repeatFixture struct {
	FirstID  string        `json:"first_symptom_id"`
	SecondID string        `json:"second_symptom_id"`
	Events   []event.Event `json:"events"`
	Edges    []graph.Edge  `json:"edges"`
}

func loadRepeatFixture(t *testing.T, path string) repeatFixture {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f repeatFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f repeatFixture) symptom(t *testing.T, id string) event.Event {
	t.Helper()
	for _, e := range f.Events {
		if e.ID == id {
			return e
		}
	}
	t.Fatalf("symptom %s not in the fixture", id)
	return event.Event{}
}

func (f repeatFixture) analyse(t *testing.T, events []event.Event, symptom event.Event) *Result {
	t.Helper()
	res, err := (&Analyzer{Events: &benchdata.Store{All: events}, Graph: benchdata.Graph{EdgeList: f.Edges}, MaxHops: 8}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// The real incidents: the repeat's scale to zero is the cause, not a repair of
// the first outage, and it clears the restore rule's healing floor.
func TestARepeatOutageIsNotMistakenForARepairOfTheFirst(t *testing.T) {
	for _, path := range repeatFixtures {
		f := loadRepeatFixture(t, path)
		for name, id := range map[string]string{"first": f.FirstID, "repeat": f.SecondID} {
			r := f.analyse(t, f.Events, f.symptom(t, id))
			top := r.Candidates[0]
			if top.Event.Type != "scale" || top.Event.EntityName != "redis" || r.Verdict != VerdictRootCause {
				t.Fatalf("%s %s: top %s on %s, verdict %s (%.2f)", path, name, top.Event.Type, top.Event.EntityName, r.Verdict, r.Confidence)
			}
			for _, fa := range top.Factors {
				if fa.Label == "Remediation" {
					t.Errorf("%s %s: the scale to zero was demoted as a remediation (x%.2f)", path, name, fa.Multiplier)
				}
			}
			if r.Confidence < 0.65 {
				t.Errorf("%s %s: confidence %.2f is below the 0.65 restore floor", path, name, r.Confidence)
			}
		}
	}
}

// The invariant that matters: an earlier incident must not change how a fresh
// one is diagnosed. The same repeat, analysed with the first outage's events
// removed, must come out the same. (Comparing the two outages directly is not
// the test: each is judged on its own timing, and they raise different alerts first.)
func TestEarlierIncidentsDoNotChangeTheDiagnosisOfAFreshOutage(t *testing.T) {
	for _, path := range repeatFixtures {
		f := loadRepeatFixture(t, path)
		symptom := f.symptom(t, f.SecondID)
		first := f.symptom(t, f.FirstID)
		// Everything up to the first outage's recovery, and a margin: gone.
		cut := symptom.IngestedAt.Add(-150 * time.Second)
		var fresh []event.Event
		for _, e := range f.Events {
			if !e.IngestedAt.Before(cut) {
				fresh = append(fresh, e)
			}
		}
		if len(fresh) == 0 || len(fresh) >= len(f.Events) || !first.IngestedAt.Before(cut) {
			t.Fatalf("%s: setup: %d fresh of %d events", path, len(fresh), len(f.Events))
		}
		with, without := f.analyse(t, f.Events, symptom), f.analyse(t, fresh, symptom)
		if d := with.Confidence - without.Confidence; d < -0.005 || d > 0.005 {
			t.Fatalf("%s: history changed the diagnosis: %.4f with the first outage, %.4f without (strength %.3f vs %.3f, separation %.3f vs %.3f)",
				path, with.Confidence, without.Confidence, with.Strength, without.Strength, with.Separation, without.Separation)
		}
	}
}

func at(base time.Time, sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

func ev(base time.Time, sec int, kind, name, typ, severity, payload string) event.Event {
	e := event.Event{ID: typ + "-" + name + "-" + base.Add(time.Duration(sec)*time.Second).Format("150405"), Namespace: "default",
		EntityKind: kind, EntityName: name, Type: typ, Severity: severity, Payload: []byte(payload),
		OccurredAt: at(base, sec), IngestedAt: at(base, sec)}
	return e
}

func TestFailingAtClosesLogOnlyFailuresAtTheNextRecovery(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	pod := ev(base, 0, "Pod", "api-1", "log_error", "warning", `{}`)
	scope := map[string]int{key(pod): 1, "default/Service/frontend": 2}
	resolved := ev(base, 30, "Service", "frontend", "error_spike_resolved", "info", `{}`)

	if !failingAt([]event.Event{pod}, scope, at(base, 60)) {
		t.Fatal("a log error with no recovery after it is still a failure")
	}
	if failingAt([]event.Event{pod, resolved}, scope, at(base, 60)) {
		t.Fatal("a recovery in scope must end a failure that only a log line reported")
	}
	// A log line after the recovery is a new failure.
	again := ev(base, 45, "Pod", "api-1", "log_error", "warning", `{}`)
	if !failingAt([]event.Event{pod, resolved, again}, scope, at(base, 60)) {
		t.Fatal("errors logged after the recovery are a new failure")
	}
}

// What has a signal of its own is closed only by its own recovery: a recovery
// elsewhere must not hide a pod that went unready or an alert still firing.
func TestARecoveryElsewhereDoesNotCloseAFailureWithItsOwnSignal(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	unready := ev(base, 0, "Pod", "redis-1", "became_unready", "warning", `{}`)
	alert := ev(base, 5, "Service", "api", "error_spike", "critical", `{}`)
	other := ev(base, 30, "Pod", "frontend-1", "became_ready", "info", `{}`)
	scope := map[string]int{key(unready): 1, key(alert): 2, key(other): 3}
	if !failingAt([]event.Event{unready, alert, other}, scope, at(base, 60)) {
		t.Fatal("a recovery on another entity closed a failure that has its own signal")
	}
	own := ev(base, 40, "Pod", "redis-1", "became_ready", "info", `{}`)
	resolved := ev(base, 41, "Service", "api", "error_spike_resolved", "info", `{}`)
	if failingAt([]event.Event{unready, alert, own, resolved}, scope, at(base, 60)) {
		t.Fatal("each failure should close on its own recovery")
	}
	// A log line first and a real failure after it: the entity is then failing on
	// its own signal, and a recovery elsewhere must not close it.
	logFirst := ev(base, 2, "Pod", "redis-1", "log_error", "warning", `{}`)
	if !failingAt([]event.Event{logFirst, unready, other}, scope, at(base, 60)) {
		t.Fatal("a failure with its own signal was closed as if it were only a log line")
	}
	// A log error on an entity that already has a signal does not downgrade it.
	logs := ev(base, 10, "Pod", "redis-1", "log_error", "warning", `{}`)
	if !failingAt([]event.Event{unready, logs, other}, scope, at(base, 60)) {
		t.Fatal("a log line downgraded a failure that has its own signal")
	}
}

// The reason revertingChanges exists, kept intact: a change that lands while the
// service is still failing is a fix, and is demoted so it cannot outrank the
// break it answers. After a recovery it is a new change, not a fix.
func TestAFixWhileTheServiceStillFailsIsStillADemotedRemediation(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	bad := ev(base, 0, "Deployment", "api", "deploy", "info", `{"old_image":"api:v1","new_image":"api:v2"}`)
	errs := ev(base, 10, "Pod", "api-1", "log_error", "warning", `{}`)
	alert := ev(base, 15, "Service", "api", "error_spike", "critical", `{}`)
	fix := ev(base, 60, "Deployment", "api", "deploy", "info", `{"old_image":"api:v2","new_image":"api:v1"}`)
	scope := func(string) map[string]int { return map[string]int{"default/Pod/api-1": 1, "default/Service/api": 2} }

	if got := revertingChanges([]event.Event{bad, errs, alert, fix}, scope); got[fix.ID] != bad.ID {
		t.Fatalf("a fix made while the service was failing was not recognised: %v", got)
	}
	resolved := ev(base, 40, "Service", "api", "error_spike_resolved", "info", `{}`)
	if got := revertingChanges([]event.Event{bad, errs, alert, resolved, fix}, scope); len(got) != 0 {
		t.Fatalf("a change after the service had recovered was taken for a fix: %v", got)
	}
}

// ---- the two rules behind it ----------------------------------------------

func candidateOf(e event.Event) Candidate { return Candidate{Event: e, Score: 0.5} }

// Copies of one signal further apart than its episode gap are different
// episodes: the new outage keeps its own first copy.
func TestRepeatsFromAnEarlierEpisodeAreNotMergedIntoTheNewOne(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	line := func(sec int) Candidate {
		return candidateOf(ev(base, sec, "Pod", "api-1", "log_error", "warning", `{"line":"error: redis INCR failed"}`))
	}
	cfg := DefaultConfig()
	h := newHypothesisCache()
	// Two lines 10s apart, then a quiet spell of 3 minutes, then a new burst.
	got := cfg.collapseRepeats([]Candidate{line(0), line(10), line(190), line(200), line(205)}, h)
	var firsts []time.Time
	for _, c := range got {
		firsts = append(firsts, c.FirstAt)
	}
	earliestNew := false
	for _, f := range firsts {
		if f.Equal(at(base, 190)) {
			earliestNew = true
		}
	}
	if !earliestNew {
		t.Fatalf("the new episode lost its own earliest copy: first seen %v", firsts)
	}
	// Without the quiet spell it is one episode, as before.
	one := cfg.collapseRepeats([]Candidate{line(0), line(10), line(60), line(80)}, newHypothesisCache())
	if len(one) > 2 || one[0].FirstAt != at(base, 0) {
		t.Fatalf("one continuous episode should still collapse together: %d kept", len(one))
	}
}

func TestAnAlertThatResolvedBeforeTheEpisodeIsSetAsideAndAnUnresolvedOneIsNot(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	old := ev(base, 0, "Service", "frontend", "error_spike", "critical", `{}`)
	resolved := ev(base, 60, "Service", "frontend", "error_spike_resolved", "info", `{}`)
	onset := at(base, 300)
	scope := func(string) map[string]int {
		return map[string]int{"default/Service/frontend": 0, "default/Pod/api-1": 1}
	}
	cfg := DefaultConfig()
	// An unrelated symptom, so only the candidate's own recovery is in play.
	symptomOf := ev(base, 300, "Service", "checkout", "latency_spike", "warning", `{}`)
	check := func(events []event.Event, cand event.Event) bool {
		cs := []Candidate{candidateOf(cand)}
		cfg.markResolvedHistory(cs, events, scope, onset, symptomOf)
		return cs[0].inert
	}
	if !check([]event.Event{old, resolved}, old) {
		t.Fatal("an alert that resolved well before this episode is history")
	}
	if check([]event.Event{old}, old) {
		t.Fatal("an alert that never resolved is not history")
	}
	// It resolved, then failed again within the flap window: the recovery did not hold.
	relapse := ev(base, 90, "Service", "frontend", "latency_spike", "warning", `{}`)
	if check([]event.Event{old, resolved, relapse}, old) {
		t.Fatal("a recovery that did not hold must not set the incident aside")
	}
	// A failure long after a held recovery is a new incident, and the old one stays history.
	later := ev(base, 200, "Service", "frontend", "latency_spike", "warning", `{}`)
	if !check([]event.Event{old, resolved, later}, old) {
		t.Fatal("a new failure after a held recovery must not undo the old incident's recovery")
	}
	// Another alert of the same incident still firing when this one resolved: not recovered.
	sibling := ev(base, 20, "Service", "api", "error_spike", "critical", `{}`)
	siblingScope := func(string) map[string]int {
		return map[string]int{"default/Service/frontend": 0, "default/Service/api": 1}
	}
	cs := []Candidate{candidateOf(old)}
	cfg.markResolvedHistory(cs, []event.Event{old, sibling, resolved}, siblingScope, onset, symptomOf)
	if cs[0].inert {
		t.Fatal("an alert resolved while a sibling of its incident was still firing was set aside")
	}
	// Resolved only seconds before the onset: a flap, not a recovery that held.
	flap := ev(base, 290, "Service", "frontend", "error_spike_resolved", "info", `{}`)
	if check([]event.Event{old, flap}, old) {
		t.Fatal("a recovery inside the flap window is not a held recovery")
	}
	// A log line, which cannot resolve itself, ends at the next recovery in scope.
	logLine := ev(base, 10, "Pod", "api-1", "log_error", "warning", `{}`)
	if !check([]event.Event{logLine, resolved}, logLine) {
		t.Fatal("a log line before a held recovery in its scope is history")
	}
	if check([]event.Event{logLine}, logLine) {
		t.Fatal("a log line with no recovery after it is not history")
	}
}

// Found live, after the log-line fix: a repeat outage five minutes after the
// first was still demoted as a remediation. The scale to zero is stamped to the
// whole second (22:17:54) and ingested 0.66s later; the Killing warning for the
// pod it kills carries the same whole second. Judged at ingestion, that warning
// came before the scale, so the new fault looked like a repair of a failure it
// had just caused.
func TestAFaultIsNotARepairOfItsOwnConsequences(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	restore := ev(base, 0, "Deployment", "redis", "scale", "info", `{"old_replicas":0,"new_replicas":1}`)
	breakAt := 180
	scale := ev(base, breakAt, "Deployment", "redis", "scale", "info", `{"old_replicas":1,"new_replicas":0}`)
	scale.IngestedAt = scale.OccurredAt.Add(662 * time.Millisecond) // noticed a moment after
	killing := ev(base, breakAt, "Pod", "redis-1", "k8s_event", "warning", `{}`)
	killing.IngestedAt = killing.OccurredAt.Add(770 * time.Millisecond)
	unready := ev(base, breakAt+1, "Pod", "redis-1", "became_unready", "warning", `{}`)
	scope := func(string) map[string]int {
		return map[string]int{"default/Pod/redis-1": 1, "default/Service/frontend": 2}
	}

	if got := revertingChanges([]event.Event{restore, killing, scale, unready}, scope); len(got) != 0 {
		t.Fatalf("a new fault was taken for a repair of the failure it caused: %v", got)
	}
	// The same shape with the failure a few seconds BEFORE the change is a genuine fix.
	earlier := ev(base, breakAt-5, "Pod", "redis-1", "k8s_event", "warning", `{}`)
	if got := revertingChanges([]event.Event{restore, earlier, scale}, scope); got[scale.ID] != restore.ID {
		t.Fatalf("a change made while the target was already failing must still be a remediation: %v", got)
	}
}

// Whole-second stamps against a sub-second change time: a consequence stamped a
// fraction of a second before the change is still concurrent with it.
func TestSameSecondConsequencesAreConcurrentEvenWhenTheChangeIsStampedLater(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	restore := ev(base, 0, "Deployment", "redis", "scale", "info", `{"old_replicas":0,"new_replicas":1}`)
	scale := ev(base, 180, "Deployment", "redis", "scale", "info", `{"old_replicas":1,"new_replicas":0}`)
	scale.OccurredAt = scale.OccurredAt.Add(300 * time.Millisecond) // the collector's own clock
	scale.IngestedAt = scale.OccurredAt.Add(300 * time.Millisecond)
	killing := ev(base, 180, "Pod", "redis-1", "k8s_event", "warning", `{}`) // whole second, before .3
	scope := func(string) map[string]int { return map[string]int{"default/Pod/redis-1": 1} }
	if got := revertingChanges([]event.Event{restore, killing, scale}, scope); len(got) != 0 {
		t.Fatalf("a consequence stamped in the same second counted as a prior failure: %v", got)
	}
}

// What an earlier incident left behind is set aside once the symptom's own
// signal resolved and held: failure facts and statuses, not only alerts and logs.
func TestWhatAnEarlierIncidentLeftBehindIsSetAsideByTheSymptomsSignal(t *testing.T) {
	base := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	cfg := DefaultConfig()
	symptom := ev(base, 400, "Service", "frontend", "error_spike", "critical", `{}`)
	earlier := ev(base, 0, "Service", "frontend", "error_spike", "critical", `{}`)
	resolved := ev(base, 100, "Service", "frontend", "error_spike_resolved", "info", `{}`)
	onset := at(base, 395)
	// The pod's own scope is too small to hold the service-level alert; the symptom's scope has it.
	scope := func(k string) map[string]int {
		if k == key(symptom) {
			return map[string]int{key(symptom): 0, "default/Pod/redis-1": 1}
		}
		return map[string]int{k: 0}
	}
	check := func(events []event.Event, cand event.Event) bool {
		cs := []Candidate{candidateOf(cand)}
		cfg.markResolvedHistory(cs, events, scope, onset, symptom)
		return cs[0].inert
	}
	unready := ev(base, 10, "Pod", "redis-old", "became_unready", "warning", `{}`)
	status := ev(base, 20, "Pod", "redis-old", "resource_status", "warning", `{}`)
	for _, c := range []event.Event{unready, status} {
		if !check([]event.Event{earlier, c, resolved}, c) {
			t.Errorf("%s from before the symptom's held recovery should be history", c.Type)
		}
	}
	// After the recovery it is part of the new incident.
	late := ev(base, 150, "Pod", "redis-new", "became_unready", "warning", `{}`)
	if check([]event.Event{earlier, resolved, late}, late) {
		t.Error("a failure after the recovery was set aside")
	}
	// The symptom's signal never resolved: nothing is known to have ended.
	if check([]event.Event{earlier, unready}, unready) {
		t.Error("a failure was set aside although the symptom's signal never resolved")
	}
	// Another service's alert resolving says nothing about the symptom's own signal.
	otherResolved := ev(base, 100, "Service", "api", "error_spike_resolved", "info", `{}`)
	if check([]event.Event{unready, otherResolved}, unready) {
		t.Error("a different entity's resolution was taken for the symptom signal's recovery")
	}
	// The incident ends at the last recovery after which nothing fails, not at the
	// first alert to resolve: the symptom's own alert resolved while another alert
	// of the same incident still fired, then that one resolved too.
	siblingScope := func(k string) map[string]int {
		return map[string]int{key(symptom): 0, "default/Service/api": 1}
	}
	siblingAlert := ev(base, 30, "Service", "api", "error_spike", "critical", `{}`)
	siblingResolved := ev(base, 150, "Service", "api", "error_spike_resolved", "info", `{}`)
	cs := []Candidate{candidateOf(unready)}
	cfg.markResolvedHistory(cs, []event.Event{earlier, siblingAlert, unready, resolved, siblingResolved}, siblingScope, onset, symptom)
	if !cs[0].inert {
		t.Error("the incident ended when its last alert resolved; what came before is history")
	}
	cs = []Candidate{candidateOf(unready)}
	cfg.markResolvedHistory(cs, []event.Event{earlier, siblingAlert, unready, resolved}, siblingScope, onset, symptom)
	if cs[0].inert {
		t.Error("the incident had not ended: a sibling alert was still firing")
	}
	// Changes are markRecovered's: a bad deploy from before the recovery may still be the cause.
	deploy := ev(base, 5, "Deployment", "api", "deploy", "info", `{"old_image":"a","new_image":"b"}`)
	if check([]event.Event{earlier, deploy, resolved}, deploy) {
		t.Error("a change was set aside as history; that is markRecovered's decision, not this rule's")
	}
}
