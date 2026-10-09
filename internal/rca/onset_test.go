package rca

// Regression tests for the episode onset: a restart elsewhere on the path keeps
// an episode alive only while that entity is unhealthy. Found by a live chaos
// campaign, where a one-second Linkerd control-plane blip during cluster
// start-up tied with a real scale-to-zero 100 seconds later.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

// The incident exactly as the cluster recorded it.
func TestARecoveredBlipEarlierDoesNotTieWithTheRealCause(t *testing.T) {
	raw, err := os.ReadFile("testdata/postgres_after_linkerd_blip.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SymptomID string        `json:"symptom_id"`
		Events    []event.Event `json:"events"`
		Edges     []graph.Edge  `json:"edges"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	var symptom event.Event
	for _, e := range fixture.Events {
		if e.ID == fixture.SymptomID {
			symptom = e
		}
	}
	a := &Analyzer{Events: &benchdata.Store{All: fixture.Events}, Graph: benchdata.Graph{EdgeList: fixture.Edges}, MaxHops: 8}
	result, err := a.Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	top := result.Candidates[0]
	if top.Event.Type != "scale" || top.Event.EntityName != "postgres" {
		t.Fatalf("the scale to zero should lead, got %s on %s", top.Event.Type, top.Event.EntityName)
	}
	if result.Verdict != VerdictRootCause || len(result.Alternatives) != 0 {
		t.Fatalf("a blip that recovered 100s earlier must not make the verdict ambiguous: %s, %d alternatives (confidence %.2f)",
			result.Verdict, len(result.Alternatives), result.Confidence)
	}
	if result.Confidence < 0.65 {
		t.Fatalf("confidence %.2f: the cause is unrivalled and should clear the healing floor", result.Confidence)
	}
}

type onsetFixture struct {
	t0      time.Time
	events  []event.Event
	counter int
}

func (f *onsetFixture) add(sec float64, kind, name, typ, payload string) {
	f.counter++
	at := f.t0.Add(time.Duration(sec * float64(time.Second)))
	f.events = append(f.events, event.Event{
		ID: strings.Join([]string{typ, name, string(rune('a' + f.counter))}, "-"), OccurredAt: at, IngestedAt: at,
		Namespace: "default", EntityKind: kind, EntityName: name, Type: typ, Payload: []byte(payload),
	})
}

func onsetOf(f *onsetFixture, symptomAt float64) time.Time {
	symptom := event.Event{ID: "symptom", Namespace: "default", EntityKind: "Service", EntityName: "frontend", Type: "latency_spike",
		OccurredAt: f.t0.Add(time.Duration(symptomAt * float64(time.Second)))}
	symptom.IngestedAt = symptom.OccurredAt
	events := append([]event.Event(nil), f.events...)
	// the same ordering Analyze applies
	for i := 1; i < len(events); i++ {
		for j := i; j > 0 && causalTime(events[j]).Before(causalTime(events[j-1])); j-- {
			events[j], events[j-1] = events[j-1], events[j]
		}
	}
	related := func(e event.Event) bool {
		return (e.Type == "oom_kill" || e.Type == "container_restart") && e.EntityName == "dep-1"
	}
	return defaultConfig.episodeOnset(events, newHypothesisCache(), symptom, related)
}

func newOnsetFixture() *onsetFixture {
	return &onsetFixture{t0: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
}

func seconds(f *onsetFixture, at time.Time) float64 { return at.Sub(f.t0).Seconds() }

func TestAnIsolatedBlipThatRecoveredAndStayedHealthyDoesNotExtendTheEpisode(t *testing.T) {
	f := newOnsetFixture()
	f.add(10, "Pod", "dep-1", "container_restart", `{}`)
	f.add(11, "Pod", "dep-1", "became_ready", `{}`)
	if got := seconds(f, onsetOf(f, 200)); got != 200 {
		t.Fatalf("a blip that recovered 189s before the symptom pulled the onset back to %.0fs", got)
	}
}

// The recovery has to have held: a restart that only just recovered is still
// part of the failure, because alerts lag the failure that causes them.
func TestARecoveryThatHasNotHeldStillExtendsTheEpisode(t *testing.T) {
	f := newOnsetFixture()
	f.add(100, "Pod", "dep-1", "container_restart", `{}`)
	f.add(101, "Pod", "dep-1", "became_ready", `{}`)
	if got := seconds(f, onsetOf(f, 150)); got != 100 {
		t.Fatalf("recovery held only 49s, so the restart belongs to the episode; onset %.0fs", got)
	}
}

func TestARestartThatNeverRecoveredStillExtendsTheEpisode(t *testing.T) {
	f := newOnsetFixture()
	f.add(10, "Pod", "dep-1", "container_restart", `{}`)
	f.add(11, "Pod", "dep-1", "became_unready", `{}`)
	if got := seconds(f, onsetOf(f, 200)); got != 10 {
		t.Fatalf("an entity that never recovered is part of the episode; onset %.0fs", got)
	}
}

// A crash loop is healthy between restarts. Each restart resets the check, so
// the whole loop stays in the episode.
func TestACrashLoopStillExtendsTheEpisodeThroughEveryRestart(t *testing.T) {
	f := newOnsetFixture()
	for _, at := range []float64{10, 130, 250} { // restarts 120s apart, ready 100s in between
		f.add(at, "Pod", "dep-1", "container_restart", `{}`)
		f.add(at+1, "Pod", "dep-1", "became_ready", `{}`)
	}
	if got := seconds(f, onsetOf(f, 300)); got != 10 {
		t.Fatalf("a crash loop should be followed back to its first restart, onset %.0fs", got)
	}
}

// Once a loop has ended and its entity stayed healthy, it is history.
func TestACrashLoopThatEndedAndStayedHealthyIsHistory(t *testing.T) {
	f := newOnsetFixture()
	for _, at := range []float64{10, 130, 250} {
		f.add(at, "Pod", "dep-1", "container_restart", `{}`)
		f.add(at+1, "Pod", "dep-1", "became_ready", `{}`)
	}
	if got := seconds(f, onsetOf(f, 600)); got != 600 {
		t.Fatalf("a loop that ended 349s before the symptom pulled the onset back to %.0fs", got)
	}
}

// The symptom's own entity is governed, unchanged, by the existing own-recovery
// rule: a held recovery ends the episode, restarts without one extend it.
func TestTheSymptomsOwnEntityIsDecidedByItsOwnRecovery(t *testing.T) {
	onset := func(recovers bool) float64 {
		f := newOnsetFixture()
		f.add(10, "Pod", "frontend", "container_restart", `{}`)
		if recovers {
			f.add(11, "Pod", "frontend", "became_ready", `{}`)
		}
		symptom := event.Event{ID: "symptom", Namespace: "default", EntityKind: "Pod", EntityName: "frontend", Type: "container_restart",
			OccurredAt: f.t0.Add(300 * time.Second)}
		symptom.IngestedAt = symptom.OccurredAt
		related := func(e event.Event) bool { return e.Type == "container_restart" && e.EntityName == "frontend" }
		return seconds(f, defaultConfig.episodeOnset(f.events, newHypothesisCache(), symptom, related))
	}
	if got := onset(true); got != 300 {
		t.Errorf("a recovery that held ends the episode: onset %.0fs, want 300", got)
	}
	if got := onset(false); got != 10 {
		t.Errorf("a restart with no recovery extends the episode: onset %.0fs, want 10", got)
	}
}
