package rca

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

type fakeEvents []event.Event

func (f fakeEvents) EventsBetween(context.Context, time.Time, time.Time) ([]event.Event, error) {
	return f, nil
}

type fakeGraph map[string]int

func (f fakeGraph) UpstreamAt(context.Context, time.Time, string, int) (map[string]int, error) {
	return f, nil
}
func TestAnalyzeFiltersRanksAndUsesFallback(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Title: "API errors"}
	r := event.Event{ID: "r", IngestedAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "container_restart", Title: "Redis restarted"}
	n := event.Event{ID: "n", IngestedAt: now.Add(-10 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "grafana", Type: "deploy", Title: "Grafana deployed"}
	got, err := (&Analyzer{Events: fakeEvents{r, n}, Graph: fakeGraph{"default/Service/api": 0, "default/Service/redis": 1}}).Analyze(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "r" {
		t.Fatalf("unexpected candidates: %+v", got.Candidates)
	}
	if got.Narrative == "" || got.Confidence <= 0 {
		t.Fatalf("missing fallback result: %+v", got)
	}
}
func TestAnalyzeRejectsSameTimestamp(t *testing.T) {
	now := time.Now().UTC()
	s := event.Event{ID: "s", IngestedAt: now, Namespace: "n", EntityKind: "Service", EntityName: "api"}
	e := event.Event{ID: "e", IngestedAt: now, Namespace: "n", EntityKind: "Service", EntityName: "redis"}
	got, err := (&Analyzer{Events: fakeEvents{e}, Graph: fakeGraph{"n/Service/api": 0, "n/Service/redis": 1}}).Analyze(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 0 {
		t.Fatalf("same-timestamp event was causal: %+v", got.Candidates)
	}
}

// countingGraph exposes its edge set, which is the path the analyzer takes in
// production: load the graph once, then walk it in memory.
type countingGraph struct {
	edges     []graph.Edge
	edgeLoads int
	walks     int
}

func (c *countingGraph) EdgesAt(context.Context, time.Time) ([]graph.Edge, error) {
	c.edgeLoads++
	return c.edges, nil
}
func (c *countingGraph) UpstreamAt(context.Context, time.Time, string, int) (map[string]int, error) {
	c.walks++
	return nil, nil
}
func (c *countingGraph) DownstreamAt(context.Context, time.Time, string, int) (map[string]int, error) {
	c.walks++
	return nil, nil
}
func (c *countingGraph) ImpactAt(context.Context, time.Time, string, int) (map[string]int, error) {
	c.walks++
	return nil, nil
}

func TestAnalyzeLoadsTheGraphOnceRegardlessOfCandidateCount(t *testing.T) {
	now := time.Now().UTC()
	node := func(name string) graph.Node {
		return graph.Node{Kind: "Service", Name: name, Namespace: "default"}
	}
	source := &countingGraph{edges: []graph.Edge{
		{From: node("api"), To: node("redis"), Kind: "calls", Weight: 1, Source: "static"},
		{From: node("api"), To: node("worker"), Kind: "calls", Weight: 1, Source: "static"},
		{From: node("worker"), To: node("db"), Kind: "calls", Weight: 1, Source: "static"},
	}}
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	upstream := []event.Event{
		{ID: "1", IngestedAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "container_restart"},
		{ID: "2", IngestedAt: now.Add(-40 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "worker", Type: "deploy"},
		{ID: "3", IngestedAt: now.Add(-50 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "db", Type: "scale"},
	}

	got, err := (&Analyzer{Events: fakeEvents(upstream), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 3 {
		t.Fatalf("expected every upstream event to be a candidate, got %d", len(got.Candidates))
	}
	if source.edgeLoads != 1 {
		t.Fatalf("the graph must be loaded once per analysis, not per candidate: %d loads", source.edgeLoads)
	}
	if source.walks != 0 {
		t.Fatalf("per-candidate graph queries should be replaced by in-memory walks: %d queries", source.walks)
	}
	if len(got.Evidence) == 0 {
		t.Fatal("the loaded edge set should still produce evidence edges")
	}
}

func TestScoreFactorsExplainTheScoreTheyProduce(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	candidate := Candidate{
		Event:            event.Event{ID: "c", IngestedAt: now.Add(-60 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "deploy"},
		Distance:         2,
		AffectedServices: 3,
	}
	score(&candidate, symptom, time.Time{}, 300)

	if len(candidate.Factors) != 4 {
		t.Fatalf("expected a factor per scoring step, got %d: %+v", len(candidate.Factors), candidate.Factors)
	}
	if !candidate.Factors[0].Base {
		t.Fatal("the first factor is the base weight, not a multiplier")
	}
	// The derivation shown in the UI must actually reproduce the score.
	product := 1.0
	for _, f := range candidate.Factors {
		product *= f.Multiplier
	}
	if math.Abs(product-candidate.Score) > 1e-9 {
		t.Fatalf("factors multiply to %.6f but the score is %.6f", product, candidate.Score)
	}
	// Reasons stay in sync because the heal audit trail stores them.
	if len(candidate.Reasons) != len(candidate.Factors) {
		t.Fatalf("every factor should still have a sentence: %d reasons, %d factors", len(candidate.Reasons), len(candidate.Factors))
	}
}

// A factor that saturates at five affected services is a constant, and a
// constant cannot rank anything.
func TestBlastRadiusFactorKeepsDiscriminatingAtScale(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api"}
	factorFor := func(affected int) float64 {
		c := Candidate{
			Event:            event.Event{IngestedAt: now.Add(-time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "deploy"},
			AffectedServices: affected,
		}
		score(&c, symptom, time.Time{}, 300)
		for _, f := range c.Factors {
			if f.Label == "Blast radius" {
				return f.Multiplier
			}
		}
		t.Fatalf("no blast radius factor for %d services", affected)
		return 0
	}
	small, medium, large := factorFor(5), factorFor(20), factorFor(45)
	if !(small < medium && medium < large) {
		t.Fatalf("blast radius must keep separating candidates: 5=%.4f 20=%.4f 45=%.4f", small, medium, large)
	}
	if large >= 1.25 {
		t.Fatalf("the factor must stay under its ceiling, got %.4f", large)
	}
}

func deployEvent(id string, at time.Time, from, to string) event.Event {
	return event.Event{
		ID: id, IngestedAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "deploy",
		Payload: []byte(`{"old_image":"` + from + `","new_image":"` + to + `"}`),
	}
}

func failureOn(id string, at time.Time, name string) event.Event {
	return event.Event{ID: id, IngestedAt: at, Namespace: "default", EntityKind: "Pod", EntityName: name, Type: "k8s_event", Severity: "warning", Title: "ErrImageNeverPull"}
}

func recoveryOn(id string, at time.Time, name string) event.Event {
	return event.Event{ID: id, IngestedAt: at, Namespace: "default", EntityKind: "Pod", EntityName: name, Type: "became_ready", Severity: "info", Title: "started serving traffic"}
}

// Everything the worker deployment owns, for the failure-scope lookup.
func workerScope(string) map[string]int {
	return map[string]int{"default/Deployment/worker": 0, "default/Pod/worker-1": 1}
}

// A change made while its target is failing is a repair, not a cause.
func TestRemediationRequiresAFailingTarget(t *testing.T) {
	now := time.Now().UTC()
	window := []event.Event{
		deployEvent("broke", now.Add(-120*time.Second), "latest", "nonexistent"),
		failureOn("failing", now.Add(-100*time.Second), "worker-1"),
		deployEvent("fixed", now.Add(-90*time.Second), "nonexistent", "latest"),
	}
	reverts := revertingChanges(window, workerScope)
	if reverts["fixed"] != "broke" {
		t.Fatalf("a change made while the target was failing is a remediation, got %#v", reverts)
	}
	if _, marked := reverts["broke"]; marked {
		t.Error("the break undoes nothing that came before it")
	}
}

// The bug this replaced: break, fix, break, fix alternates, so every change
// reverses the one before it. Judging by "were there failures in the interval"
// still marked the second break, because the previous episode's signals had
// not finished draining. Only an explicit recovery closes an episode.
func TestRecoverySignalEndsTheEpisode(t *testing.T) {
	now := time.Now().UTC()
	window := []event.Event{
		deployEvent("broke1", now.Add(-300*time.Second), "latest", "nonexistent"),
		failureOn("failing1", now.Add(-280*time.Second), "worker-1"),
		deployEvent("fixed1", now.Add(-260*time.Second), "nonexistent", "latest"),
		// The previous episode's signals are still draining after the repair:
		// this is what made "were there failures in the interval?" mark the
		// next change as a repair too.
		failureOn("draining", now.Add(-255*time.Second), "worker-1"),
		recoveryOn("ready", now.Add(-250*time.Second), "worker-1"),
		// Twenty seconds later someone breaks it again. The target had
		// recovered, so this is a fresh cause, not part of the repair.
		deployEvent("broke2", now.Add(-230*time.Second), "latest", "nonexistent"),
		failureOn("failing2", now.Add(-210*time.Second), "worker-1"),
		deployEvent("fixed2", now.Add(-200*time.Second), "nonexistent", "latest"),
	}
	reverts := revertingChanges(window, workerScope)
	for _, fix := range []string{"fixed1", "fixed2"} {
		if _, marked := reverts[fix]; !marked {
			t.Errorf("%s repaired a failing target and should be marked", fix)
		}
	}
	for _, brk := range []string{"broke1", "broke2"} {
		if _, marked := reverts[brk]; marked {
			t.Errorf("%s was made after the target recovered; it is a cause, not a repair", brk)
		}
	}
}

// Quiet is not the same as healthy: without an explicit recovery the episode
// is still open, so a redeploy during it is still a repair.
func TestSilenceDoesNotEndAnEpisode(t *testing.T) {
	now := time.Now().UTC()
	window := []event.Event{
		deployEvent("broke", now.Add(-600*time.Second), "latest", "nonexistent"),
		failureOn("failing", now.Add(-580*time.Second), "worker-1"),
		deployEvent("fixed", now.Add(-60*time.Second), "nonexistent", "latest"),
	}
	if _, marked := revertingChanges(window, workerScope)["fixed"]; !marked {
		t.Fatal("no recovery arrived, so the episode was still open")
	}
}

// A recovery seen while nothing was failing resolves nothing.
func TestRecoveryWithoutAFailureHasNoEffect(t *testing.T) {
	now := time.Now().UTC()
	window := []event.Event{
		recoveryOn("ready", now.Add(-300*time.Second), "worker-1"),
		deployEvent("broke", now.Add(-200*time.Second), "latest", "nonexistent"),
		deployEvent("second", now.Add(-100*time.Second), "nonexistent", "latest"),
	}
	if _, marked := revertingChanges(window, workerScope)["second"]; marked {
		t.Fatal("nothing was failing, so the reversal is not a repair")
	}
}

// The regression that prompted this: restoring an image at the moment the pod
// finally reported Failed outscored the deploy that broke it a minute earlier.
func TestTheFixDoesNotOutrankTheBreak(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "worker-1", Type: "resource_status"}
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{
		{From: node("Deployment", "worker"), To: node("Pod", "worker-1"), Kind: "owns", Weight: 1, Source: "static"},
	}}
	window := []event.Event{
		deployEvent("broke", now.Add(-47*time.Second), "latest", "nonexistent"),
		failureOn("failing", now.Add(-20*time.Second), "worker-1"),
		deployEvent("fixed", now.Add(-1*time.Second), "nonexistent", "latest"),
	}

	got, err := (&Analyzer{Events: fakeEvents(window), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) < 2 {
		t.Fatalf("both deploys should be graph-reachable candidates, got %d", len(got.Candidates))
	}
	if got.Candidates[0].Event.ID != "broke" {
		t.Fatalf("the break must rank above the fix, got %q first (scores: %.3f vs %.3f)",
			got.Candidates[0].Event.ID, got.Candidates[0].Score, got.Candidates[1].Score)
	}
	if got.Candidates[1].Reverts != "broke" {
		t.Errorf("the fix should be annotated with what it undid, got %q", got.Candidates[1].Reverts)
	}
	// Demoted, not hidden: a rollback to a bad older version is a real cause.
	if got.Candidates[1].Score <= 0 {
		t.Error("a remediation stays rankable rather than being excluded")
	}
}

// Observed live: a rolling update never repairs the failing pod, it replaces
// it. The broken pod is deleted and a different pod serves traffic, so no pod
// is ever seen becoming ready and the episode would never close.
func TestDeletingTheFailingResourceEndsItsEpisode(t *testing.T) {
	now := time.Now().UTC()
	scope := func(string) map[string]int {
		return map[string]int{"default/Deployment/worker": 0, "default/Pod/worker-broken-1": 1, "default/Pod/worker-broken-2": 1}
	}
	deleted := func(id string, at time.Time, name string) event.Event {
		return event.Event{ID: id, IngestedAt: at, Namespace: "default", EntityKind: "Pod", EntityName: name, Type: "resource_deleted", Severity: "info"}
	}
	window := []event.Event{
		deployEvent("broke1", now.Add(-300*time.Second), "latest", "nonexistent"),
		failureOn("failing1", now.Add(-295*time.Second), "worker-broken-1"),
		deployEvent("fixed1", now.Add(-255*time.Second), "nonexistent", "latest"),
		deleted("gone1", now.Add(-254*time.Second), "worker-broken-1"),
		deployEvent("broke2", now.Add(-235*time.Second), "latest", "nonexistent"),
		failureOn("failing2", now.Add(-230*time.Second), "worker-broken-2"),
		deployEvent("fixed2", now.Add(-190*time.Second), "nonexistent", "latest"),
	}
	reverts := revertingChanges(window, scope)
	for _, fix := range []string{"fixed1", "fixed2"} {
		if _, marked := reverts[fix]; !marked {
			t.Errorf("%s was made while a pod was failing and is a repair", fix)
		}
	}
	if _, marked := reverts["broke2"]; marked {
		t.Error("the failing pod had been deleted; this break is a fresh cause")
	}
}

func candidateAt(score float64, hops int) Candidate {
	return Candidate{Score: score, Distance: hops}
}

// The bug: confidence multiplied the raw score, which already carries the
// graph-distance penalty, so it was structurally capped at 0.89 for a one-hop
// cause and 0.57 at three hops. rollback-bad-deploy needs 0.90 and could
// therefore never fire, however obvious the cause was.
func TestConfidenceIsNotCappedByGraphDistance(t *testing.T) {
	for _, hops := range []int{1, 2, 3, 4} {
		// A perfect candidate at this distance: nothing else competes and its
		// score is the most the scoring can produce that far out.
		best := distanceFactor(hops) * 1.25
		overall, _, _ := confidence([]Candidate{candidateAt(best, hops)})
		if overall < 0.9 {
			t.Errorf("an unambiguous cause %d hop(s) away reached only %.3f; a 0.90 gate would be unreachable", hops, overall)
		}
	}
}

// Margin confidence: how clearly the leader beats the runner-up.
func TestConfidenceFallsWhenCandidatesAreTied(t *testing.T) {
	clear, _, clearSeparation := confidence([]Candidate{candidateAt(0.64, 1), candidateAt(0.10, 1)})
	tied, _, tiedSeparation := confidence([]Candidate{candidateAt(0.64, 1), candidateAt(0.63, 1)})
	if tied >= clear {
		t.Fatalf("two near-tied candidates should be less conclusive: tied %.3f vs clear %.3f", tied, clear)
	}
	if tiedSeparation > 0.55 || clearSeparation < 0.9 {
		t.Fatalf("separation should collapse towards 0.5 when tied and approach 1 when dominant: tied %.3f clear %.3f", tiedSeparation, clearSeparation)
	}
}

// A lone candidate has nothing to compete with, but that is not the same as
// being a good explanation.
func TestASingleWeakCandidateIsNotConfident(t *testing.T) {
	overall, strength, separation := confidence([]Candidate{candidateAt(0.05, 1)})
	if separation != 1 {
		t.Errorf("with no runner-up, separation cannot argue either way: %.3f", separation)
	}
	if strength > 0.2 || overall > 0.2 {
		t.Errorf("a weak lone candidate must stay unconfident: strength %.3f overall %.3f", strength, overall)
	}
}

func TestConfidenceIsBounded(t *testing.T) {
	overall, _, _ := confidence(nil)
	if overall != 0 {
		t.Errorf("no candidates means no confidence, got %.3f", overall)
	}
	if overall, _, _ := confidence([]Candidate{candidateAt(5, 1), candidateAt(0, 1)}); overall > 1 {
		t.Errorf("confidence must stay within 0..1, got %.3f", overall)
	}
}

func logNoise(id string, at time.Time, seconds int) event.Event {
	return event.Event{
		ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Pod", EntityName: "worker-1",
		Type: "log_error", Severity: "warning",
		Title: fmt.Sprintf("[  %d.413010s] INFO ThreadId(01) inbound:server{port=8080}", seconds),
	}
}

// One misbehaving container emits the same line every 30 seconds. Ranked as
// individual events they fill every slot and push out the deploy that caused
// them, which is the only candidate anyone can act on.
func TestRepeatedNoiseCollapsesIntoOneCandidate(t *testing.T) {
	now := time.Now().UTC()
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{
		{From: node("Deployment", "worker"), To: node("Pod", "worker-1"), Kind: "owns", Weight: 1, Source: "static"},
	}}
	window := []event.Event{deployEvent("deploy", now.Add(-500*time.Second), "latest", "nonexistent")}
	for i := 1; i <= 12; i++ {
		window = append(window, logNoise(fmt.Sprintf("noise-%d", i), now.Add(-time.Duration(i*20)*time.Second), 500+i))
	}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "worker-1", Type: "k8s_event", Severity: "warning"}

	got, err := (&Analyzer{Events: fakeEvents(window), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	var sawDeploy bool
	var noiseCandidates, noiseOccurrences int
	for _, c := range got.Candidates {
		if c.Event.Type == "deploy" {
			sawDeploy = true
		}
		if c.Event.Type == "log_error" {
			noiseCandidates++
			noiseOccurrences = c.Occurrences
		}
	}
	if !sawDeploy {
		t.Fatalf("the deploy was crowded out by repeated noise: %d candidates returned", len(got.Candidates))
	}
	if noiseCandidates != 1 {
		t.Fatalf("twelve copies of one log line are one hypothesis, got %d candidates", noiseCandidates)
	}
	if noiseOccurrences != 12 {
		t.Errorf("the repeat count should be kept, got %d", noiseOccurrences)
	}
}

// An event that happened before the symptom but reached the store after it is
// exactly the deploy that caused the incident: collectors notice changes last.
func TestLateArrivingCauseIsStillConsidered(t *testing.T) {
	now := time.Now().UTC()
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{
		{From: node("Deployment", "worker"), To: node("Pod", "worker-1"), Kind: "owns", Weight: 1, Source: "static"},
	}}
	late := deployEvent("late", now.Add(-40*time.Second), "latest", "nonexistent")
	late.OccurredAt = now.Add(-40 * time.Second) // happened before the symptom
	late.IngestedAt = now.Add(5 * time.Second)   // but was seen after it
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "worker-1", Type: "k8s_event", Severity: "warning"}

	got, err := (&Analyzer{Events: fakeEvents([]event.Event{late}), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "late" {
		t.Fatalf("a cause ingested after the symptom but which happened before it must still rank: %+v", got.Candidates)
	}
	if !got.Provisional {
		t.Error("a symptom this recent may still be missing events and should say so")
	}
}

// A repeating Kubernetes Event keeps the timestamp of when its series began.
// Trusting that dates a warning still firing now to a quarter of an hour ago,
// and its causal window then excludes the change that caused it.
func TestImplausibleEventTimeFallsBackToIngestion(t *testing.T) {
	now := time.Now().UTC()
	stale := event.Event{ID: "stale", OccurredAt: now.Add(-15 * time.Minute), IngestedAt: now}
	if got := causalTime(stale); !got.Equal(now) {
		t.Fatalf("an event claiming to be 15 minutes older than its arrival should not be trusted, got %v", got)
	}
	// Ordinary lateness is still honoured.
	late := event.Event{ID: "late", OccurredAt: now.Add(-20 * time.Second), IngestedAt: now}
	if got := causalTime(late); !got.Equal(late.OccurredAt) {
		t.Fatalf("a normally late event must keep its own timestamp, got %v", got)
	}
}

// A fixed five-minute constant made the deliberately long windows useless: an
// oom_kill looks back an hour, but a change thirty minutes earlier scored e^-6.
func TestDecayScalesWithTheCausalWindow(t *testing.T) {
	now := time.Now().UTC()
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{
		{From: node("Deployment", "worker"), To: node("Pod", "worker-1"), Kind: "owns", Weight: 1, Source: "static"},
	}}
	// A memory limit change half an hour before an OOM kill: well inside the
	// hour-long oom_kill window, and exactly the cause that window exists for.
	cause := event.Event{
		ID: "limit", IngestedAt: now.Add(-30 * time.Minute), OccurredAt: now.Add(-30 * time.Minute),
		Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "resource_change",
		Title: "worker resource limits changed",
	}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "worker-1", Type: "oom_kill"}

	got, err := (&Analyzer{Events: fakeEvents([]event.Event{cause}), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 {
		t.Fatalf("the change is inside the oom_kill window and must be a candidate, got %d", len(got.Candidates))
	}
	// With the old fixed 300s constant this scored e^-6 ≈ 0.002 and was noise.
	if got.Candidates[0].Score < 0.1 {
		t.Fatalf("a cause halfway through its own window should keep real weight, got %.4f", got.Candidates[0].Score)
	}
}

// The default window must keep behaving exactly as it did.
func TestDefaultWindowKeepsItsOriginalDecay(t *testing.T) {
	now := time.Now().UTC()
	c := Candidate{Event: event.Event{IngestedAt: now.Add(-300 * time.Second), Type: "deploy"}, Distance: 0}
	symptom := event.Event{IngestedAt: now}
	// 15-minute default window / 3 = the 300s constant the code used to hardcode.
	score(&c, symptom, time.Time{}, (15*time.Minute).Seconds()/defaultDecayDivisor)
	var timeFactor float64
	for _, f := range c.Factors {
		if f.Label == "Time distance" {
			timeFactor = f.Multiplier
		}
	}
	if math.Abs(timeFactor-math.Exp(-1)) > 1e-9 {
		t.Fatalf("one time constant before the symptom should be e^-1, got %.6f", timeFactor)
	}
}

// A resource created since the last graph sync has no edges yet, so nothing it
// depends on is reachable. That makes the analysis incomplete, not wrong.
func TestAnalysisIsProvisionalWhileTheGraphIsStale(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "new-1", Type: "k8s_event", Severity: "warning"}
	analyzer := &Analyzer{Events: fakeEvents(nil), Graph: fakeGraph{"default/Pod/new-1": 0}, MaxHops: 3, GraphInterval: 30 * time.Second}
	got, err := analyzer.Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Provisional {
		t.Fatal("a symptom younger than the graph sync interval must be flagged provisional")
	}

	settled := symptom
	settled.IngestedAt = now.Add(-2 * time.Minute)
	settled.OccurredAt = settled.IngestedAt
	if got, _ := analyzer.Analyze(context.Background(), settled); got.Provisional {
		t.Error("a settled symptom should not be flagged provisional")
	}
}

// The narrative describes the cause, so it must quote the cause's own blast
// radius. The symptom's is a different number whenever the cause sits further
// from the leaves than the symptom does.
func TestNarrativeQuotesTheCauseBlastRadiusNotTheSymptoms(t *testing.T) {
	result := &Result{
		Symptom:     event.Event{ID: "s", Title: "redis stopped serving traffic", IngestedAt: time.Now()},
		Confidence:  0.63,
		BlastRadius: BlastRadius{AffectedServices: 3}, // the symptom's
		Candidates: []Candidate{{
			Event:            event.Event{Title: "redis scaled from 1 to 0", EntityName: "redis", IngestedAt: time.Now().Add(-time.Second)},
			Distance:         1,
			AffectedServices: 2, // the cause's
		}},
	}
	narrative := FallbackNarrative(result)
	if !strings.Contains(narrative, "affecting 2 downstream service(s)") {
		t.Fatalf("narrative should quote the cause's blast radius: %s", narrative)
	}
	if strings.Contains(narrative, "affecting 3 downstream service(s)") {
		t.Fatalf("narrative quoted the symptom's blast radius instead: %s", narrative)
	}
}

// Scaling a Deployment to zero is why its Pod stopped serving traffic. Ranked
// as rivals they suppress each other's separation, so Chronicle grew least
// certain exactly when it had traced the chain most completely.
func TestLinkedCandidatesDoNotCompeteForConfidence(t *testing.T) {
	now := time.Now().UTC()
	at := func(seconds int) time.Time { return now.Add(-time.Duration(seconds) * time.Second) }
	scale := Candidate{Event: event.Event{ID: "scale", OccurredAt: at(54), IngestedAt: at(54), Namespace: "default", EntityKind: "Deployment", EntityName: "redis"}, Score: 0.229, Distance: 4}
	unready := Candidate{Event: event.Event{ID: "unready", OccurredAt: at(53), IngestedAt: at(53), Namespace: "default", EntityKind: "Pod", EntityName: "redis-1"}, Score: 0.184, Distance: 3}
	unrelated := Candidate{Event: event.Event{ID: "noise", OccurredAt: at(15), IngestedAt: at(15), Namespace: "default", EntityKind: "Pod", EntityName: "api-1"}, Score: 0.103, Distance: 1}

	candidates := []Candidate{scale, unready, unrelated}
	// The Deployment reaches its Pod; nothing reaches the unrelated api pod.
	linkChains(candidates, func(from, to string) bool {
		return from == "default/Deployment/redis" && to == "default/Pod/redis-1"
	})
	if candidates[0].Chain != candidates[1].Chain {
		t.Fatalf("the pod going unready is an effect of the scale, not a rival: %q vs %q", candidates[0].Chain, candidates[1].Chain)
	}
	if candidates[2].Chain == candidates[0].Chain {
		t.Fatal("an unrelated candidate must stay a separate explanation")
	}

	_, _, separation := confidence(candidates)
	// Separation must be measured against the unrelated candidate at 0.103,
	// not against the chain link at 0.184.
	want := 0.5 + 0.5*(0.229-0.103)/0.229
	if math.Abs(separation-want) > 1e-9 {
		t.Fatalf("separation %.4f should ignore the chain link and compare with the rival (%.4f)", separation, want)
	}
}

// Topology alone is not enough: an effect cannot precede its cause.
func TestChainLinkingRequiresTimeOrderAsWellAsTopology(t *testing.T) {
	now := time.Now().UTC()
	earlier := Candidate{Event: event.Event{ID: "pod", OccurredAt: now.Add(-60 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "redis-1"}, Score: 0.3}
	later := Candidate{Event: event.Event{ID: "deploy", OccurredAt: now.Add(-10 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis"}, Score: 0.2}
	candidates := []Candidate{earlier, later}
	// The Deployment can reach the Pod, but it changed afterwards.
	linkChains(candidates, func(from, to string) bool {
		return from == "default/Deployment/redis" && to == "default/Pod/redis-1"
	})
	if candidates[0].Chain == candidates[1].Chain {
		t.Fatal("a change made after the effect cannot be its cause")
	}
}

// Over-linking would make separation meaningless: two independent changes must
// stay rivals. Redis and Postgres are both depended on by the app, but neither
// can reach the other, so neither explains the other.
func TestSiblingDependenciesStayIndependent(t *testing.T) {
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	g := graph.New()
	g.SetEdges([]graph.Edge{
		{From: node("Pod", "api-1"), To: node("Service", "redis"), Kind: "calls"},
		{From: node("Service", "redis"), To: node("Pod", "redis-1"), Kind: "routes_to"},
		{From: node("Pod", "api-1"), To: node("Service", "postgres"), Kind: "calls"},
		{From: node("Service", "postgres"), To: node("Pod", "postgres-1"), Kind: "routes_to"},
		{From: node("Deployment", "redis"), To: node("Pod", "redis-1"), Kind: "owns"},
		{From: node("Deployment", "postgres"), To: node("Pod", "postgres-1"), Kind: "owns"},
	})
	reaches := func(from, to string) bool { _, ok := g.Upstream(to, 5)[from]; return ok }

	now := time.Now().UTC()
	candidates := []Candidate{
		{Event: event.Event{ID: "redis-scale", OccurredAt: now.Add(-60 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis"}, Score: 0.30},
		{Event: event.Event{ID: "pg-scale", OccurredAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "postgres"}, Score: 0.28},
	}
	linkChains(candidates, reaches)
	if candidates[0].Chain == candidates[1].Chain {
		t.Fatal("redis and postgres are siblings; neither explains the other")
	}
	if _, _, separation := confidence(candidates); separation > 0.55 {
		t.Fatalf("two near-tied independent changes must still suppress separation, got %.3f", separation)
	}

	// But a Deployment really does explain its own Pod going down.
	linked := []Candidate{
		{Event: event.Event{ID: "scale", OccurredAt: now.Add(-60 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis"}, Score: 0.30},
		{Event: event.Event{ID: "unready", OccurredAt: now.Add(-50 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "redis-1"}, Score: 0.28},
	}
	linkChains(linked, reaches)
	if linked[0].Chain != linked[1].Chain {
		t.Fatal("the pod is downstream of the deployment that scaled it")
	}
}

// Collectors stamp simultaneous events with the same second: a Deployment and
// the Pod it owns are routinely created in one instant. An order-dependent
// pass split them into two chains depending on which the tie-break put first.
func TestSimultaneousEventsStillFormOneChain(t *testing.T) {
	sameInstant := time.Now().UTC().Add(-time.Minute)
	reaches := func(from, to string) bool {
		return from == "default/Deployment/redis" && to == "default/Pod/redis-1"
	}
	// Deliberately listed pod-first, the order that used to break it.
	candidates := []Candidate{
		{Event: event.Event{ID: "pod-created", OccurredAt: sameInstant, Namespace: "default", EntityKind: "Pod", EntityName: "redis-1"}, Score: 0.08},
		{Event: event.Event{ID: "deploy-created", OccurredAt: sameInstant, Namespace: "default", EntityKind: "Deployment", EntityName: "redis"}, Score: 0.06},
	}
	linkChains(candidates, reaches)
	if candidates[0].Chain != candidates[1].Chain {
		t.Fatalf("a deployment and the pod it owns are one chain however the tie sorts: %q vs %q", candidates[0].Chain, candidates[1].Chain)
	}
	if candidates[0].Chain != "deploy-created" {
		t.Errorf("the chain should be named for the deployment that owns the pod, got %q", candidates[0].Chain)
	}
}

// A second monitor firing on the failing component is not an explanation for
// the first. Before the co-symptom damping, latency_spike on api outscored the
// redis scale that caused both, purely because zero hops earns the maximum
// distance factor.
func TestACoSymptomDoesNotOutrankTheUpstreamChange(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Title: "api erroring"}
	coSymptom := event.Event{ID: "co", IngestedAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "latency_spike", Title: "api slow"}
	cause := event.Event{ID: "cause", IngestedAt: now.Add(-69 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}

	got, err := (&Analyzer{
		Events: fakeEvents{coSymptom, cause},
		Graph: fakeGraph{
			"default/Service/api":      0,
			"default/Deployment/redis": 4,
		},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) < 2 {
		t.Fatalf("expected both candidates, got %+v", got.Candidates)
	}
	if got.Candidates[0].Event.ID != "cause" {
		t.Fatalf("co-symptom outranked the real change: top is %q (%s), scores %v/%v",
			got.Candidates[0].Event.ID, got.Candidates[0].Event.Type,
			got.Candidates[0].Score, got.Candidates[1].Score)
	}
}

// The damping applies only on the symptom's own component. An error on an
// upstream service is genuine evidence that the failure started there.
func TestAnUpstreamObservationIsNotDamped(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	upstream := event.Event{ID: "u", IngestedAt: now.Add(-20 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "redis", Type: "log_error"}

	got, err := (&Analyzer{
		Events: fakeEvents{upstream},
		Graph:  fakeGraph{"default/Service/api": 0, "default/Service/redis": 1},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 {
		t.Fatalf("expected the upstream observation to survive: %+v", got.Candidates)
	}
	for _, f := range got.Candidates[0].Factors {
		if f.Label == "Co-symptom" {
			t.Fatal("an upstream observation was damped as a co-symptom")
		}
	}
}

// In a mesh nearly everything reaches everything, so without a propagation
// bound one early unrelated event absorbs every later candidate into its
// chain, and the separation term then skips every rival as "same chain".
func TestAnEarlyUnrelatedEventDoesNotAbsorbTheChain(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bootstrap := Candidate{Event: event.Event{ID: "boot", IngestedAt: now.Add(-10 * time.Minute), Namespace: "linkerd", EntityKind: "Pod", EntityName: "linkerd-destination"}}
	cause := Candidate{Event: event.Event{ID: "cause", IngestedAt: now.Add(-69 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis"}}
	effect := Candidate{Event: event.Event{ID: "effect", IngestedAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "api"}}

	candidates := []Candidate{bootstrap, cause, effect}
	linkChains(candidates, func(from, to string) bool { return true }) // everything reaches everything

	if candidates[0].Chain == candidates[1].Chain {
		t.Fatalf("a 10-minute-old unrelated event absorbed the incident chain: %q == %q",
			candidates[0].Chain, candidates[1].Chain)
	}
	if candidates[1].Chain != candidates[2].Chain {
		t.Fatalf("cause and effect 39s apart should share a chain: %q vs %q",
			candidates[1].Chain, candidates[2].Chain)
	}
	if candidates[1].Chain != "cause" {
		t.Fatalf("the chain should be named for its head, got %q", candidates[1].Chain)
	}
}

// chainGraph models the real victim topology: frontend calls api, api calls
// redis, and a Deployment owns each Pod. Upstream of the frontend therefore
// runs all the way back to the redis Deployment.
func chainGraph() func(from, to string) bool {
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	g := graph.New()
	g.SetEdges([]graph.Edge{
		{From: node("Pod", "frontend-1"), To: node("Service", "api"), Kind: "calls"},
		{From: node("Service", "api"), To: node("Pod", "api-1"), Kind: "routes_to"},
		{From: node("Pod", "api-1"), To: node("Service", "redis"), Kind: "calls"},
		{From: node("Service", "redis"), To: node("Pod", "redis-1"), Kind: "routes_to"},
		{From: node("Deployment", "redis"), To: node("Pod", "redis-1"), Kind: "owns"},
	})
	return func(from, to string) bool { _, ok := g.Upstream(to, 8)[from]; return ok }
}

func obs(id, typ, kind, name string, dist int, at time.Time) Candidate {
	return Candidate{
		Event:    event.Event{ID: id, OccurredAt: at, Namespace: "default", EntityKind: kind, EntityName: name, Type: typ},
		Distance: dist, Score: 0.20,
	}
}

func damped(c Candidate) bool {
	for _, f := range c.Factors {
		if f.Label == "Propagation" || f.Label == "Co-symptom" {
			return true
		}
	}
	return false
}

// The failure this was built to catch: an error_spike on api ranked as the
// cause of an error_spike on frontend. api is erroring because redis is down,
// so the api reading is the failure arriving, not starting.
func TestAMidChainObservationIsDampedAsPropagation(t *testing.T) {
	now := time.Now().UTC()
	c := []Candidate{
		obs("api-errors", "error_spike", "Service", "api", 1, now.Add(-15*time.Second)),
		obs("redis-down", "became_unready", "Pod", "redis-1", 4, now.Add(-40*time.Second)),
	}
	dampPropagation(c, chainGraph(), 10*time.Minute)
	if !damped(c[0]) {
		t.Error("an observation with an implicated dependency upstream is propagation, not origin")
	}
	if damped(c[1]) {
		t.Error("a state transition is not an observation and must not be damped")
	}
}

// The other half of the same rule: the observation at the far end of the chain
// has nothing upstream implicating it, so it survives as the origin. Without
// this, an incident with no recorded change would have every candidate damped.
func TestTheObservationAtTheEndOfTheChainSurvives(t *testing.T) {
	now := time.Now().UTC()
	c := []Candidate{
		obs("api-log", "log_error", "Pod", "api-1", 2, now.Add(-15*time.Second)),
		obs("redis-log", "log_error", "Pod", "redis-1", 4, now.Add(-20*time.Second)),
	}
	dampPropagation(c, chainGraph(), 10*time.Minute)
	if !damped(c[0]) {
		t.Error("the mid-chain reading should be damped")
	}
	if damped(c[1]) {
		t.Fatal("the end of the propagation chain is the origin and must survive")
	}
}

// A change is an intervention: it explains a failure however much monitor noise
// sits upstream of it. Damping one would hide the event RCA exists to find.
func TestAChangeIsNeverDampedAsPropagation(t *testing.T) {
	now := time.Now().UTC()
	c := []Candidate{
		{Event: event.Event{ID: "api-deploy", OccurredAt: now.Add(-15 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "deploy"}, Distance: 1, Score: 0.9},
		obs("redis-log", "log_error", "Pod", "redis-1", 4, now.Add(-20*time.Second)),
	}
	dampPropagation(c, chainGraph(), 10*time.Minute)
	if damped(c[0]) {
		t.Fatal("a deploy was damped as propagation")
	}
}

// Mutually reachable nodes give no direction, so neither can be called the
// propagation of the other. This is also the degraded path where the graph
// source cannot hand over edges and reachability collapses to "in scope".
func TestMutualReachabilityDoesNotDamp(t *testing.T) {
	now := time.Now().UTC()
	c := []Candidate{
		obs("a", "error_spike", "Service", "api", 1, now.Add(-10*time.Second)),
		obs("b", "error_spike", "Service", "redis", 2, now.Add(-12*time.Second)),
	}
	dampPropagation(c, func(from, to string) bool { return true }, 10*time.Minute)
	if damped(c[0]) || damped(c[1]) {
		t.Fatal("without a direction neither reading explains the other")
	}
}

// An anomaly from an unrelated incident an hour earlier is still upstream in
// topology. Only a reading close enough in time to be the same failure counts.
func TestAnOldUpstreamAnomalyDoesNotDamp(t *testing.T) {
	now := time.Now().UTC()
	c := []Candidate{
		obs("api-errors", "error_spike", "Service", "api", 1, now.Add(-15*time.Second)),
		obs("stale", "log_error", "Pod", "redis-1", 4, now.Add(-1*time.Hour)),
	}
	dampPropagation(c, chainGraph(), 10*time.Minute)
	if damped(c[0]) {
		t.Fatal("an anomaly an hour old is a different incident and explains nothing here")
	}
}

// The case a single global window silently refused to connect: a memory limit
// lowered half an hour before the OOM it causes. Nothing breaks when the limit
// changes -- it breaks when the workload next grows into it.
func TestASlowCauseStillLinksToItsLateSymptom(t *testing.T) {
	now := time.Now().UTC()
	reaches := func(from, to string) bool {
		return from == "default/Deployment/api" && to == "default/Pod/api-1"
	}
	candidates := []Candidate{
		{Event: event.Event{ID: "limit-change", OccurredAt: now.Add(-30 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_change"}, Score: 0.3},
		{Event: event.Event{ID: "oom", OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "oom_kill"}, Score: 0.2},
	}
	linkChains(candidates, reaches)
	if candidates[0].Chain != candidates[1].Chain {
		t.Fatalf("a resource change and the OOM it caused 30m later are one chain: %q vs %q", candidates[0].Chain, candidates[1].Chain)
	}
	if candidates[0].Chain != "limit-change" {
		t.Errorf("the chain should be headed by the change, got %q", candidates[0].Chain)
	}
}

// The other half: the horizon is per failure mode, not a single wider window.
// A scale takes effect on the next request, so a scale half an hour earlier is
// a separate hypothesis rather than the same propagation.
func TestAFastCauseDoesNotLinkAcrossTheSameGap(t *testing.T) {
	now := time.Now().UTC()
	reaches := func(from, to string) bool {
		return from == "default/Deployment/api" && to == "default/Pod/api-1"
	}
	candidates := []Candidate{
		{Event: event.Event{ID: "old-scale", OccurredAt: now.Add(-30 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "scale"}, Score: 0.3},
		{Event: event.Event{ID: "oom", OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "oom_kill"}, Score: 0.2},
	}
	linkChains(candidates, reaches)
	if candidates[0].Chain == candidates[1].Chain {
		t.Fatal("a scale propagates on the next request; 30m later is a different story")
	}
}

// A rollout lands over minutes, so a deploy is still the head of the chain for
// errors that appear well after it started -- but not for ones an hour later.
func TestADeployLinksAcrossARolloutButNotAnHour(t *testing.T) {
	now := time.Now().UTC()
	reaches := func(from, to string) bool {
		return from == "default/Deployment/api" && to == "default/Pod/api-1"
	}
	build := func(gap time.Duration) []Candidate {
		return []Candidate{
			{Event: event.Event{ID: "deploy", OccurredAt: now.Add(-gap), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "deploy"}, Score: 0.4},
			{Event: event.Event{ID: "errors", OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "became_unready"}, Score: 0.2},
		}
	}
	during := build(10 * time.Minute)
	linkChains(during, reaches)
	if during[0].Chain != during[1].Chain {
		t.Error("a deploy 10m before the failure is still the rollout that caused it")
	}
	later := build(time.Hour)
	linkChains(later, reaches)
	if later[0].Chain == later[1].Chain {
		t.Error("an hour is past any rollout; that is a separate hypothesis")
	}
}

// The horizon must stay short for events whose failure mode is unknown, or the
// bootstrap noise that absorbed an entire incident comes straight back.
func TestAnUncharacterisedEventKeepsTheShortHorizon(t *testing.T) {
	if got := propagationHorizonFor(event.Event{Type: "k8s_event"}); got != defaultPropagationHorizon {
		t.Fatalf("an unclassified event must keep the short horizon, got %s", got)
	}
	if propagationHorizonFor(event.Event{Type: "resource_change"}) <= propagationHorizonFor(event.Event{Type: "scale"}) {
		t.Fatal("resource pressure must be allowed longer to surface than a scale")
	}
}

// propagationFactor is a tuned number, so the property it has to buy is stated
// here rather than trusted: however far away the cause sits and however close
// the propagation is, an explained observation must not outrank a real cause.
// The extremes are read from typeWeight rather than named, so the property
// keeps holding if those weights are ever retuned.
//
// The worst case pits the strongest observation at zero hops -- where the
// distance factor is at its maximum of 1.0 -- against the weakest non
// observation at the edge of the hop budget. Undamped the observation wins
// that comparison, which is precisely the bug; damped it must lose at every
// distance.
func TestPropagationNeverOutranksARealCauseAtAnyDistance(t *testing.T) {
	now := time.Now().UTC()
	symptom := event.Event{ID: "s", OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "frontend", Type: "error_spike"}

	strongestObservation, weakestCause := "", ""
	for typ := range typeWeight {
		if observationTypes[typ] {
			if strongestObservation == "" || typeWeight[typ] > typeWeight[strongestObservation] {
				strongestObservation = typ
			}
			continue
		}
		if weakestCause == "" || typeWeight[typ] < typeWeight[weakestCause] {
			weakestCause = typ
		}
	}
	if strongestObservation == "" || weakestCause == "" {
		t.Fatal("typeWeight no longer contains both observations and causes")
	}
	t.Logf("worst case: %s (%.2f) damped, against %s (%.2f)",
		strongestObservation, typeWeight[strongestObservation], weakestCause, typeWeight[weakestCause])

	scored := func(typ string, distance int) float64 {
		c := Candidate{
			Event:    event.Event{OccurredAt: now.Add(-10 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "x", Type: typ},
			Distance: distance,
		}
		score(&c, symptom, time.Time{}, 200)
		return c.Score
	}

	for observationHops := 0; observationHops <= 8; observationHops++ {
		for causeHops := 0; causeHops <= 8; causeHops++ {
			propagation := scored(strongestObservation, observationHops) * propagationFactor
			cause := scored(weakestCause, causeHops)
			if propagation >= cause {
				t.Fatalf("damped propagation at %d hops (%.4f) outranked a cause at %d hops (%.4f)",
					observationHops, propagation, causeHops, cause)
			}
		}
	}

	// Confirm the damping is what buys the property, so this cannot silently
	// pass if the factor is ever neutralised.
	if scored(strongestObservation, 0) <= scored(weakestCause, 8) {
		t.Fatal("without damping the observation already lost; the property proves nothing")
	}
}

// A recurring signal arrives as a fresh event each time it fires, and a
// re-observed one arrives as a fresh event on every collector restart. Either
// way an earlier copy sits on the symptom's own resource at zero hops, where
// the distance factor is at its maximum -- so it outranked everything and
// Chronicle reported a failure as the cause of itself.
func TestARecurrenceOfTheSymptomIsNotItsCause(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	signal := func(id string, at time.Time) event.Event {
		return event.Event{
			ID: id, IngestedAt: at, OccurredAt: at,
			Namespace: "default", EntityKind: "Pod", EntityName: "probe-1",
			Type: "k8s_event", Title: `Unhealthy: Liveness probe failed: Get "http://10.244.2.33:4191/live": context deadline exceeded`,
		}
	}
	symptom := signal("now", now)
	earlier := signal("earlier", now.Add(-412*time.Second))
	realCause := event.Event{
		ID: "cause", IngestedAt: now.Add(-60 * time.Second), OccurredAt: now.Add(-60 * time.Second),
		Namespace: "default", EntityKind: "Deployment", EntityName: "probe", Type: "scale", Title: "probe scaled from 1 to 0",
	}

	got, err := (&Analyzer{
		Events: fakeEvents{earlier, realCause},
		Graph: fakeGraph{
			"default/Pod/probe-1":      0,
			"default/Deployment/probe": 1,
		},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "earlier" {
			t.Fatal("an earlier firing of the same signal was offered as its own cause")
		}
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "cause" {
		t.Fatalf("the real change should be the only candidate left: %+v", got.Candidates)
	}
}

// The rule keys on the signal, not the resource: a different kind of event on
// the same resource is still a legitimate explanation.
func TestADifferentSignalOnTheSameResourceStillCounts(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "k8s_event", Title: "Unhealthy: probe failed"}
	other := event.Event{ID: "oom", IngestedAt: now.Add(-30 * time.Second), OccurredAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "oom_kill", Title: "api-1 OOMKilled"}

	got, err := (&Analyzer{Events: fakeEvents{other}, Graph: fakeGraph{"default/Pod/api-1": 0}}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "oom" {
		t.Fatalf("an OOM kill explains a failing probe on the same pod: %+v", got.Candidates)
	}
}

// A recovery is the end of a failure, not the start of one. Left in the
// running, a resolved metric alert was ranked as the reason a service was
// erroring.
func TestARecoveryIsNeverOfferedAsACause(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "frontend", Type: "error_spike"}
	recoveries := []event.Event{
		{ID: "resolved", IngestedAt: now.Add(-20 * time.Second), OccurredAt: now.Add(-20 * time.Second), Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "latency_spike_resolved"},
		{ID: "ready", IngestedAt: now.Add(-25 * time.Second), OccurredAt: now.Add(-25 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "became_ready"},
	}
	cause := event.Event{ID: "cause", IngestedAt: now.Add(-40 * time.Second), OccurredAt: now.Add(-40 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}

	got, err := (&Analyzer{
		Events: fakeEvents(append(recoveries, cause)),
		Graph: fakeGraph{
			"default/Service/frontend": 0, "default/Service/api": 1,
			"default/Pod/api-1": 2, "default/Deployment/redis": 3,
		},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Candidates {
		if isRecovery(c.Event) {
			t.Fatalf("%q is a recovery and cannot have caused a failure", c.Event.Type)
		}
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "cause" {
		t.Fatalf("only the real change should remain: %+v", got.Candidates)
	}
}

// An outage lasts as long as it lasts. Its hundredth error line is still the
// failure arriving, even though it arrives long after the propagation delay of
// the change that caused it -- which is why damping is bounded by the incident
// window rather than by how fast that kind of cause propagates.
func TestALateErrorInASustainedOutageIsStillPropagation(t *testing.T) {
	now := time.Now().UTC()
	c := []Candidate{
		// Five minutes of continuous failure after a scale that propagates in
		// seconds; the propagation horizon for a scale is far shorter than this.
		obs("late-error", "log_error", "Pod", "api-1", 2, now.Add(-10*time.Second)),
		{Event: event.Event{ID: "scale", OccurredAt: now.Add(-5 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale"}, Distance: 5, Score: 0.2},
	}
	if 5*time.Minute <= propagationHorizonFor(c[1].Event) {
		t.Fatal("this test needs a gap wider than a scale's propagation horizon")
	}
	dampPropagation(c, chainGraph(), 10*time.Minute)
	if !damped(c[0]) {
		t.Fatal("an error still flowing during the outage is propagation, not a new origin")
	}
}

// Time decay beats a fixed discount. A restore sixteen seconds before the
// symptom outranked the scale-to-zero it reverted eight minutes earlier, so the
// recovery was named as the cause of an outage that was still draining. The
// factor has to be measured against the break, not applied in isolation.
func TestAFreshFixCannotOutrankAnOldBreak(t *testing.T) {
	candidates := []Candidate{
		{Event: event.Event{ID: "fix", Title: "redis scaled from 0 to 1"}, Reverts: "break", Score: 0.088},
		{Event: event.Event{ID: "break", Title: "redis scaled from 1 to 0"}, Score: 0.048},
	}
	capRemediations(candidates)
	if candidates[0].Score >= candidates[1].Score {
		t.Fatalf("the fix (%.4f) must stay below the break it undid (%.4f)",
			candidates[0].Score, candidates[1].Score)
	}
	if want := 0.048 * remediationFactor; math.Abs(candidates[0].Score-want) > 1e-9 {
		t.Fatalf("the fix should be capped at the break's score x the remediation factor: got %.4f want %.4f",
			candidates[0].Score, want)
	}
}

// The cap only applies when the reverted change is itself a candidate; with
// nothing to measure against, the flat factor in score() stands alone.
func TestARemediationWithNoVisibleBreakKeepsItsScore(t *testing.T) {
	candidates := []Candidate{
		{Event: event.Event{ID: "fix"}, Reverts: "break-outside-the-window", Score: 0.088},
	}
	capRemediations(candidates)
	if candidates[0].Score != 0.088 {
		t.Fatalf("nothing to compare against, so the score should be untouched: got %.4f", candidates[0].Score)
	}
}

// The console renders the derivation as "base x each factor", so every pass
// that touches a score has to do it through a factor. TestScoreFactors...
// covers score() alone, which is why a later pass adjusting the score behind
// the factors went unnoticed: this one asserts the invariant on what Analyze
// actually returns, after damping and the remediation cap have run.
func TestEveryReturnedCandidateReproducesItsScoreFromItsFactors(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "frontend-1", Type: "log_error", Title: "frontend erroring"}

	ev := func(id, typ, kind, name string, ago time.Duration, title string) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: kind, EntityName: name, Type: typ, Title: title}
	}
	events := []event.Event{
		ev("break", "scale", "Deployment", "redis", 8*time.Minute, "redis scaled from 1 to 0"),
		ev("fix", "scale", "Deployment", "redis", 10*time.Second, "redis scaled from 0 to 1"),
		ev("unready", "became_unready", "Pod", "redis-1", 8*time.Minute, "redis-1 stopped serving traffic"),
		ev("spike", "error_spike", "Service", "api", 2*time.Minute, "high_error_rate on api"),
		ev("apilog", "log_error", "Pod", "api-1", 30*time.Second, "api log line"),
	}
	got, err := (&Analyzer{
		Events: fakeEvents(events),
		Graph: fakeGraph{
			"default/Pod/frontend-1": 0, "default/Service/api": 1, "default/Pod/api-1": 2,
			"default/Pod/redis-1": 3, "default/Deployment/redis": 4,
		},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) == 0 {
		t.Fatal("expected candidates")
	}
	for _, c := range got.Candidates {
		product := 1.0
		for _, f := range c.Factors {
			product *= f.Multiplier
		}
		if math.Abs(product-c.Score) > 1e-9 {
			labels := make([]string, 0, len(c.Factors))
			for _, f := range c.Factors {
				labels = append(labels, fmt.Sprintf("%s=%.4f", f.Label, f.Multiplier))
			}
			t.Errorf("%s (%s): factors multiply to %.6f but score is %.6f [%s]",
				c.Event.ID, c.Event.Type, product, c.Score, strings.Join(labels, " "))
		}
	}
}

// A deploy on the failing component with a wide blast radius multiplies past
// 1, and the clamp used to swallow the difference: the derivation showed
// factors reaching 1.05 beside a score of 1.00.
func TestTheScoreCeilingIsShownAsAFactor(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	c := Candidate{
		Event:            event.Event{OccurredAt: now.Add(-time.Second), Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "deploy"},
		Distance:         0,
		AffectedServices: 3,
	}
	score(&c, symptom, time.Time{}, 300)

	product := 1.0
	for _, f := range c.Factors {
		product *= f.Multiplier
	}
	if math.Abs(product-c.Score) > 1e-9 {
		t.Fatalf("factors multiply to %.6f but score is %.6f", product, c.Score)
	}
	if c.Score > 1 {
		t.Fatalf("score must stay within the ceiling, got %.4f", c.Score)
	}
}

// The wire format derives the score, so even a future pass that forgets to go
// through applyFactor cannot ship a score that contradicts its derivation.
func TestMarshalledScoreIsDerivedFromFactors(t *testing.T) {
	c := Candidate{Score: 0.99} // deliberately wrong relative to the factors
	c.Factors = []Factor{
		{Label: "Event type", Multiplier: 0.75, Base: true},
		{Label: "Graph distance", Multiplier: 0.50},
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Score float64 `json:"score"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if math.Abs(out.Score-0.375) > 1e-9 {
		t.Fatalf("marshalled score should be the product of the factors (0.375), got %.6f", out.Score)
	}
}

// applyFactor is the only sanctioned way to move a score, and it keeps the
// cached value equal to the product at every step.
func TestApplyFactorKeepsTheCachedScoreInStep(t *testing.T) {
	var c Candidate
	for _, m := range []float64{0.75, 0.80, 0.50, 1.06} {
		c.applyFactor(Factor{Label: "f", Multiplier: m})
		if math.Abs(c.Score-c.derivedScore()) > 1e-12 {
			t.Fatalf("cached score %.9f drifted from the product %.9f", c.Score, c.derivedScore())
		}
	}
	c.rescaleFactor("f", 0.1, "")
	if math.Abs(c.Score-c.derivedScore()) > 1e-12 {
		t.Fatalf("rescale left the cache stale: %.9f vs %.9f", c.Score, c.derivedScore())
	}
}

// A pod's own status report does not explain a failure on that same pod: both
// are the same trouble described twice. Only failure phases get this far --
// isRecovery drops Running and Completed before ranking.
func TestAStatusReportOnTheFailingPodIsACoSymptom(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "became_unready", Title: "api-1 stopped serving traffic"}
	status := event.Event{ID: "status", IngestedAt: now, OccurredAt: now.Add(-time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "resource_status", Title: "api-1 status is Failed", Payload: []byte(`{"phase":"Failed"}`)}
	cause := event.Event{ID: "cause", IngestedAt: now, OccurredAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}

	got, err := (&Analyzer{
		Events: fakeEvents{status, cause},
		Graph:  fakeGraph{"default/Pod/api-1": 0, "default/Deployment/redis": 3},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) < 2 {
		t.Fatalf("expected both candidates, got %+v", got.Candidates)
	}
	if got.Candidates[0].Event.ID != "cause" {
		t.Fatalf("a status report outranked the change that caused it: top is %q", got.Candidates[0].Event.ID)
	}
	var damped bool
	for _, c := range got.Candidates {
		if c.Event.ID != "status" {
			continue
		}
		for _, f := range c.Factors {
			if f.Label == "Co-symptom" || f.Label == "Propagation" {
				damped = true
			}
		}
	}
	if !damped {
		t.Error("the status report on the failing pod should be damped")
	}
}

// But a status report on an upstream resource, with nothing above it, is still
// the best available explanation and must survive.
func TestAnUpstreamStatusReportSurvivesWhenNothingExplainsIt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	upstream := event.Event{ID: "up", IngestedAt: now, OccurredAt: now.Add(-20 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "redis-1", Type: "resource_status", Title: "redis-1 status is Failed", Payload: []byte(`{"phase":"Failed"}`)}

	got, err := (&Analyzer{
		Events: fakeEvents{upstream},
		Graph:  fakeGraph{"default/Service/api": 0, "default/Pod/redis-1": 2},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "up" {
		t.Fatalf("the only upstream failure report must survive as the origin: %+v", got.Candidates)
	}
	for _, f := range got.Candidates[0].Factors {
		if f.Label == "Co-symptom" || f.Label == "Propagation" {
			t.Fatal("nothing upstream explains it, so it is the origin and must not be damped")
		}
	}
}

// Creation is how a pod comes to be Pending; naming it the cause says nothing.
func TestAResourcesOwnCreationIsNotItsSymptomsCause(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "resource_status", Title: "api-1 status is Pending", Payload: []byte(`{"phase":"Pending"}`)}
	own := event.Event{ID: "own", IngestedAt: now, OccurredAt: now.Add(-3 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "resource_created", Title: "api-1 created"}
	parent := event.Event{ID: "parent", IngestedAt: now, OccurredAt: now.Add(-4 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_created", Title: "api created"}

	got, err := (&Analyzer{
		Events: fakeEvents{own, parent},
		Graph:  fakeGraph{"default/Pod/api-1": 0, "default/Deployment/api": 1},
	}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "parent" {
		t.Fatalf("only the upstream creation should remain: %+v", got.Candidates)
	}
}

// The narrative and the score's time factor must read the same clock.
func TestNarrativeGapMatchesTheScoredGap(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "became_unready", Title: "api-1 stopped serving traffic"}
	// Observed 2s late, but it happened 3s before the symptom.
	cause := event.Event{ID: "c", IngestedAt: now.Add(-2 * time.Second), OccurredAt: now.Add(-3 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "scale", Title: "api scaled from 1 to 0"}

	got, err := (&Analyzer{Events: fakeEvents{cause}, Graph: fakeGraph{"default/Pod/api-1": 0, "default/Deployment/api": 1}}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	var detail string
	for _, f := range got.Candidates[0].Factors {
		if f.Label == "Time distance" {
			detail = f.Detail
		}
	}
	if !strings.Contains(detail, "3s") || !strings.Contains(got.Narrative, "3s before the symptom") {
		t.Fatalf("score says %q, narrative says %q", detail, got.Narrative)
	}
}

// A lone candidate is uncontested, which the console must not show as a margin.
func TestContestedMeansAGenuineRivalExists(t *testing.T) {
	now := time.Now().UTC()
	mk := func(id, chain string) Candidate {
		return Candidate{Event: event.Event{ID: id, OccurredAt: now, IngestedAt: now}, Chain: chain}
	}
	if contested(nil) || contested([]Candidate{mk("a", "a")}) {
		t.Error("no candidate or a lone one has no rival")
	}
	if contested([]Candidate{mk("a", "a"), mk("b", "a")}) {
		t.Error("links of one chain are not rivals")
	}
	if !contested([]Candidate{mk("a", "a"), mk("b", "b")}) {
		t.Error("a different chain is a rival")
	}
}

// A sustained outage keeps repeating its signal; the newest copy must not make
// the cause look older than it was when the problem began.
func TestAnOngoingOutageIsJudgedFromItsOnset(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	errLine := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "log_error", Severity: "warning", Title: "redis INCR failed: EOF"}
	}
	cause := event.Event{ID: "cause", IngestedAt: now.Add(-310 * time.Second), OccurredAt: now.Add(-310 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}
	graph := fakeGraph{"default/Pod/api-1": 0, "default/Deployment/redis": 2}

	got, err := (&Analyzer{Events: fakeEvents{cause, errLine("first", 300*time.Second), errLine("a", 240*time.Second), errLine("b", 180*time.Second), errLine("c", 120*time.Second), errLine("d", 60*time.Second)}, Graph: graph}).Analyze(context.Background(), errLine("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidates[0].Gap != 10 {
		t.Fatalf("the gap should run to the onset (10s), not the newest copy: %.0fs", got.Candidates[0].Gap)
	}
	onsetOnly, _ := (&Analyzer{Events: fakeEvents{cause}, Graph: graph}).Analyze(context.Background(), errLine("now", 0))
	if got.Candidates[0].Score <= onsetOnly.Candidates[0].Score {
		t.Fatalf("a recurring signal should not lower confidence: %.3f vs %.3f", got.Candidates[0].Score, onsetOnly.Candidates[0].Score)
	}
}

// Something that happened after the problem began is an effect; it is still
// judged against the symptom, never pulled back to the onset.
func TestAnEventAfterTheOnsetIsNotMeasuredFromIt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sig := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "log_error", Severity: "warning", Title: "redis INCR failed: EOF"}
	}
	late := event.Event{ID: "late", IngestedAt: now.Add(-60 * time.Second), OccurredAt: now.Add(-60 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}

	got, err := (&Analyzer{Events: fakeEvents{sig("first", 300*time.Second), late}, Graph: fakeGraph{"default/Pod/api-1": 0, "default/Deployment/redis": 2}}).Analyze(context.Background(), sig("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidates[0].Gap != 60 {
		t.Fatalf("an event after the onset keeps its gap to the symptom (60s): %.0fs", got.Candidates[0].Gap)
	}
}

// A Service fronting the cause's own workload goes down with it but does not
// depend on it.
func TestTheCausesOwnServiceIsNotItsBlastRadius(t *testing.T) {
	node := func(kind, name string) graph.Node { return graph.Node{Namespace: "default", Kind: kind, Name: name} }
	edges := []graph.Edge{
		{From: node("Deployment", "redis"), To: node("Pod", "redis-1"), Kind: "owns"},
		{From: node("Service", "redis"), To: node("Pod", "redis-1"), Kind: "routes_to"},
		{From: node("Pod", "api-1"), To: node("Service", "redis"), Kind: "calls"},
		{From: node("Service", "api"), To: node("Pod", "api-1"), Kind: "routes_to"},
	}
	g := graph.New()
	g.SetEdges(edges)

	got := summarizeImpact(g.Impact("default/Deployment/redis", 4), frontDoors(edges, "default/Deployment/redis"))
	if len(got.Services) != 1 || got.Services[0] != "default/Service/api" {
		t.Fatalf("only the dependent service should count, got %v", got.Services)
	}
}

// rangedEvents honours the requested range, like the real event store does, and
// counts reads so a test can see how far an analysis had to reach.
type rangedEvents struct {
	all   []event.Event
	reads int
}

func (r *rangedEvents) EventsBetween(_ context.Context, from, to time.Time) ([]event.Event, error) {
	r.reads++
	var out []event.Event
	for _, e := range r.all {
		if !e.IngestedAt.Before(from) && e.IngestedAt.Before(to) {
			out = append(out, e)
		}
	}
	return out, nil
}

var outageGraph = fakeGraph{"default/Pod/api-1": 0, "default/Deployment/redis": 2}

// outage builds a cause followed by the same error repeating every interval
// until `length` has passed, and returns the newest copy as the symptom.
func outage(causeType string, length, every time.Duration, now time.Time) (*rangedEvents, event.Event, event.Event) {
	start := now.Add(-length)
	cause := event.Event{ID: "cause", IngestedAt: start.Add(-time.Second), OccurredAt: start.Add(-time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: causeType, Title: "redis scaled from 1 to 0", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	source := &rangedEvents{all: []event.Event{cause}}
	var newest event.Event
	for at, i := start, 0; !at.After(now); at, i = at.Add(every), i+1 {
		newest = event.Event{ID: fmt.Sprintf("err-%d", i), IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "log_error", Severity: "warning", Title: "redis INCR failed: EOF"}
		source.all = append(source.all, newest)
	}
	return source, newest, cause
}

// The outage screenshots: the cause is older than the lookback by the time the
// newest error is analysed, yet it is still the cause.
func TestACauseOlderThanTheLookbackIsFoundThroughTheEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, length := range []time.Duration{5 * time.Minute, 14 * time.Minute, 23 * time.Minute, 50 * time.Minute, 100 * time.Minute} {
		source, newest, _ := outage("scale", length, 15*time.Second, now)
		got, err := (&Analyzer{Events: source, Graph: outageGraph}).Analyze(context.Background(), newest)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Candidates) == 0 || got.Candidates[0].Event.ID != "cause" {
			t.Fatalf("a %v outage lost its cause: %+v", length, got.Candidates)
		}
		if got.Confidence < 0.5 {
			t.Errorf("a %v outage scored %.2f; confidence must not fall as an outage lasts", length, got.Confidence)
		}
	}
}

// Confidence must be the same however long the outage has run.
func TestConfidenceDoesNotDependOnHowLongTheOutageHasRun(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	score := func(length time.Duration) float64 {
		source, newest, _ := outage("scale", length, 15*time.Second, now)
		got, _ := (&Analyzer{Events: source, Graph: outageGraph}).Analyze(context.Background(), newest)
		return got.Confidence
	}
	short, long := score(2*time.Minute), score(90*time.Minute)
	if math.Abs(short-long) > 0.02 {
		t.Fatalf("confidence moved from %.3f to %.3f as the outage aged", short, long)
	}
}

// Alerts re-notify rather than fire continuously: two copies 21 minutes apart
// are one outage.
func TestASparselyRepeatingAlertIsOneEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	alert := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Severity: "critical", Title: "high_error_rate on api: 1.000"}
	}
	cause := event.Event{ID: "cause", IngestedAt: now.Add(-22 * time.Minute), OccurredAt: now.Add(-22 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}
	source := &rangedEvents{all: []event.Event{cause, alert("first", 21*time.Minute), alert("second", 0)}}

	got, err := (&Analyzer{Events: source, Graph: fakeGraph{"default/Service/api": 0, "default/Deployment/redis": 2}}).Analyze(context.Background(), alert("second", 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) == 0 || got.Candidates[0].Event.ID != "cause" {
		t.Fatalf("the cause behind a re-notifying alert was lost: %+v", got.Candidates)
	}
}

// A long silence means the earlier copies were a different episode.
func TestAnEarlierEpisodeIsNotFollowed(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	alert := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Severity: "critical", Title: "high_error_rate on api: 1.000"}
	}
	oldCause := event.Event{ID: "old", IngestedAt: now.Add(-92 * time.Minute), OccurredAt: now.Add(-92 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0"}
	source := &rangedEvents{all: []event.Event{oldCause, alert("earlier", 90*time.Minute), alert("now", 0)}}

	got, err := (&Analyzer{Events: source, Graph: fakeGraph{"default/Service/api": 0, "default/Deployment/redis": 2}}).Analyze(context.Background(), alert("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "old" {
			t.Fatal("a cause from an earlier, separate episode was offered for this one")
		}
	}
}

// A signal that never stops is followed only so far.
func TestAnEpisodeIsFollowedOnlyToTheCap(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	source, newest, _ := outage("scale", 6*time.Hour, time.Minute, now)
	got, err := (&Analyzer{Events: source, Graph: outageGraph}).Analyze(context.Background(), newest)
	if err != nil {
		t.Fatal(err)
	}
	if source.reads > maxEpisodeLoads+1 {
		t.Fatalf("followed the episode with %d reads, cap is %d", source.reads, maxEpisodeLoads+1)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "cause" {
			t.Fatal("a cause beyond the episode cap should not be reached")
		}
	}
}

// Thousands of repeats must not make the analysis quadratic.
func TestAManyThousandEventOutageStaysFast(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	source, newest, _ := outage("scale", 100*time.Minute, time.Second, now)
	// A second, different signal from the same pod, repeating as densely.
	for i := 0; i < 6000; i++ {
		at := now.Add(-100*time.Minute + time.Duration(i)*time.Second)
		source.all = append(source.all, event.Event{ID: fmt.Sprintf("other-%d", i), IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "log_error", Severity: "warning", Title: "worker call failed: timeout"})
	}
	begin := time.Now()
	got, err := (&Analyzer{Events: source, Graph: outageGraph}).Analyze(context.Background(), newest)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(begin); took > 3*time.Second {
		t.Fatalf("analysing %d events took %v", len(source.all), took)
	}
	if got.Candidates[0].Event.ID != "cause" {
		t.Fatalf("the cause was buried by repeats: %+v", got.Candidates[0].Event)
	}
	total := 0
	for _, c := range got.Candidates {
		if c.Event.Type == "log_error" {
			total += c.Occurrences
		}
	}
	if total < 5000 {
		t.Fatalf("repeat counts should survive collapsing, got %d", total)
	}
}

// The logs stop when a failure is fixed; identical errors after a quiet spell
// are a different outage and must not inherit the first one's causes.
func TestALogStreamThatWentQuietStartsANewEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	line := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Pod", EntityName: "web-1", Type: "log_error", Severity: "warning", Title: "api returned HTTP 500"}
	}
	old := event.Event{ID: "old", IngestedAt: now.Add(-40 * time.Minute), OccurredAt: now.Add(-40 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	fresh := event.Event{ID: "fresh", IngestedAt: now.Add(-95 * time.Second), OccurredAt: now.Add(-95 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "scale", Title: "worker scaled from 1 to 0", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	all := []event.Event{old, fresh}
	for i := 0; i < 50; i++ { // the first outage's errors, every 15s, then silence
		all = append(all, line(fmt.Sprintf("a%d", i), 39*time.Minute-time.Duration(i)*15*time.Second))
	}
	for i := 0; i < 6; i++ { // the second outage
		all = append(all, line(fmt.Sprintf("b%d", i), 90*time.Second-time.Duration(i)*15*time.Second))
	}
	graph := fakeGraph{"default/Pod/web-1": 0, "default/Deployment/redis": 2, "default/Deployment/worker": 2}

	got, err := (&Analyzer{Events: &rangedEvents{all: all}, Graph: graph}).Analyze(context.Background(), line("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidates[0].Event.ID != "fresh" {
		t.Fatalf("the current outage's cause should lead: %+v", got.Candidates[0].Event)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "old" {
			t.Fatal("the first outage's cause was offered for the second")
		}
	}
}

// An explicit resolution ends an episode even when the next alert is soon after.
func TestAResolvedAlertEndsTheEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	alert := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Severity: "critical", Title: "high_error_rate on api: 1.000"}
	}
	resolved := event.Event{ID: "resolved", IngestedAt: now.Add(-8 * time.Minute), OccurredAt: now.Add(-8 * time.Minute), Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike_resolved", Title: "high_error_rate resolved on api"}
	old := event.Event{ID: "old", IngestedAt: now.Add(-21 * time.Minute), OccurredAt: now.Add(-21 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 1 to 0", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	source := &rangedEvents{all: []event.Event{old, alert("first", 20*time.Minute), resolved, alert("again", 0)}}

	got, err := (&Analyzer{Events: source, Graph: fakeGraph{"default/Service/api": 0, "default/Deployment/redis": 2}}).Analyze(context.Background(), alert("again", 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "old" {
			t.Fatal("a cause from before the alert resolved was offered for the new alert")
		}
	}
}

// Bringing a workload up from zero is the end of an outage, never its start.
func TestScalingUpFromZeroIsNotACause(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	restore := event.Event{ID: "restore", IngestedAt: now.Add(-30 * time.Second), OccurredAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Title: "redis scaled from 0 to 1", Payload: []byte(`{"old_replicas":0,"new_replicas":1}`)}
	cut := event.Event{ID: "cut", IngestedAt: now.Add(-40 * time.Second), OccurredAt: now.Add(-40 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "scale", Title: "worker scaled from 2 to 1", Payload: []byte(`{"old_replicas":2,"new_replicas":1}`)}

	got, err := (&Analyzer{Events: fakeEvents{restore, cut}, Graph: fakeGraph{"default/Service/api": 0, "default/Deployment/redis": 2, "default/Deployment/worker": 2}}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "cut" {
		t.Fatalf("only the capacity cut should remain: %+v", got.Candidates)
	}
}

// Replacement pods appear because their owner changed; naming the pod as a
// cause splits the answer between the owner and its own side effect.
func TestAControllersPodCreationIsNotACause(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	owned := event.Event{ID: "pod", IngestedAt: now.Add(-50 * time.Second), OccurredAt: now.Add(-50 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "redis-abc", Type: "resource_created", Payload: []byte(`{"owner":"redis"}`)}
	bare := event.Event{ID: "bare", IngestedAt: now.Add(-40 * time.Second), OccurredAt: now.Add(-40 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "debug-1", Type: "resource_created", Payload: []byte(`{"owner":""}`)}

	got, err := (&Analyzer{Events: fakeEvents{owned, bare}, Graph: fakeGraph{"default/Service/api": 0, "default/Pod/redis-abc": 2, "default/Pod/debug-1": 2}}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "bare" {
		t.Fatalf("only a pod with no owner can be a cause of its own: %+v", got.Candidates)
	}
}

// A scale-down that was later undone is no longer what is breaking anything.
func TestAChangeReplacedBeforeTheEpisodeIsHistory(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	change := func(id string, ago time.Duration, name, payload string) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: name, Type: "scale", Payload: []byte(payload)}
	}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike"}
	events := fakeEvents{
		change("down", 8*time.Minute, "worker", `{"old_replicas":1,"new_replicas":0}`),
		change("up", 6*time.Minute, "worker", `{"old_replicas":0,"new_replicas":1}`),
		change("api-down", 40*time.Second, "api2", `{"old_replicas":1,"new_replicas":0}`),
	}
	graph := fakeGraph{"default/Service/api": 0, "default/Deployment/worker": 2, "default/Deployment/api2": 2}

	got, err := (&Analyzer{Events: events, Graph: graph}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "api-down" {
		t.Fatalf("a replaced change should not compete: %+v", got.Candidates)
	}
}

// A repair attempt made during the outage does not erase the break.
func TestAChangeMadeDuringTheEpisodeDoesNotReplaceTheCause(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	alert := func(id string, ago time.Duration) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "error_spike", Title: "high_error_rate"}
	}
	scale := func(id string, ago time.Duration, payload string) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Payload: []byte(payload)}
	}
	source := &rangedEvents{all: []event.Event{
		scale("break", 12*time.Minute, `{"old_replicas":1,"new_replicas":0}`),
		alert("first", 10*time.Minute),
		scale("attempt", 2*time.Minute, `{"old_replicas":0,"new_replicas":1}`),
	}}
	got, err := (&Analyzer{Events: source, Graph: fakeGraph{"default/Service/api": 0, "default/Deployment/redis": 2}}).Analyze(context.Background(), alert("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) == 0 || got.Candidates[0].Event.ID != "break" {
		t.Fatalf("the break must stay the cause: %+v", got.Candidates)
	}
}

// Warning events from the failing pod restate the symptom and must not compete
// with the release that caused it.
func TestAKubernetesWarningOnTheFailingPodIsACoSymptom(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-2", Type: "became_unready", Title: "api-2 stopped serving traffic"}
	warning := event.Event{ID: "warn", IngestedAt: now.Add(-5 * time.Second), OccurredAt: now.Add(-5 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "api-2", Type: "k8s_event", Severity: "warning", Title: "Failed: ErrImageNeverPull"}
	deploy := event.Event{ID: "deploy", IngestedAt: now.Add(-20 * time.Second), OccurredAt: now.Add(-20 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "deploy", Title: "api deployed: latest -> broken", Payload: []byte(`{"old_image":"latest","new_image":"broken"}`)}

	got, err := (&Analyzer{Events: fakeEvents{warning, deploy}, Graph: fakeGraph{"default/Pod/api-2": 0, "default/Deployment/api": 1}}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidates[0].Event.ID != "deploy" {
		t.Fatalf("the release should lead: %+v", got.Candidates[0].Event)
	}
	for _, c := range got.Candidates {
		if c.Event.ID != "warn" {
			continue
		}
		damped := false
		for _, f := range c.Factors {
			damped = damped || f.Label == "Co-symptom"
		}
		if !damped {
			t.Error("the warning on the failing pod should be damped as a co-symptom")
		}
	}
}

// A workload that reported itself healthy between two identical failures had two
// outages, not one.
func TestAHealthyReportBetweenFailuresSplitsTheEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	status := func(id string, ago time.Duration, phase, title string) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_status", Severity: "warning", Title: title, Payload: []byte(`{"phase":"` + phase + `"}`)}
	}
	change := func(id string, ago time.Duration, old, new int) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "redis", Type: "scale", Payload: []byte(fmt.Sprintf(`{"old_replicas":%d,"new_replicas":%d}`, old, new))}
	}
	source := &rangedEvents{all: []event.Event{
		change("first-break", 20*time.Minute, 1, 0),
		status("down1", 19*time.Minute, "Degraded", "api status is 0/1 replicas ready"),
		status("up", 12*time.Minute, "Running", "api status is 1/1 replicas ready"),
		change("second-break", 2*time.Minute, 1, 0),
	}}
	got, err := (&Analyzer{Events: source, Graph: fakeGraph{"default/Deployment/api": 0, "default/Deployment/redis": 2}}).Analyze(context.Background(), status("down2", 0, "Degraded", "api status is 0/1 replicas ready"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) == 0 || got.Candidates[0].Event.ID != "second-break" {
		t.Fatalf("the current outage's break should lead: %+v", got.Candidates)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "first-break" {
			t.Fatal("the earlier outage's break was offered for this one")
		}
	}
}

// A crash-looping workload reports healthy for a moment between failures. That is
// one outage, and its cause is the change made when it began.
func TestAFlappingWorkloadIsOneEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	status := func(id string, ago time.Duration, phase, title string) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_status", Severity: "warning", Title: title, Payload: []byte(`{"phase":"` + phase + `"}`)}
	}
	change := event.Event{ID: "limit", IngestedAt: now.Add(-6*time.Minute - time.Second), OccurredAt: now.Add(-6*time.Minute - time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_change", Title: "api resource limits changed"}
	all := []event.Event{change}
	for i := 0; i < 8; i++ { // fails, is briefly "ready", fails again
		down := 6*time.Minute - time.Duration(i)*45*time.Second
		all = append(all, status(fmt.Sprintf("down%d", i), down, "Degraded", "api status is 0/1 replicas ready"))
		all = append(all, status(fmt.Sprintf("up%d", i), down-3*time.Second, "Running", "api status is 1/1 replicas ready"))
	}
	got, err := (&Analyzer{Events: &rangedEvents{all: all}, Graph: fakeGraph{"default/Deployment/api": 0}}).Analyze(context.Background(), status("now", 0, "Degraded", "api status is 0/1 replicas ready"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) == 0 || got.Candidates[0].Event.ID != "limit" {
		t.Fatalf("the limit change should lead: %+v", got.Candidates)
	}
	if got.Candidates[0].Gap > 5 {
		t.Fatalf("the gap should run to when the crashing began, got %.0fs", got.Candidates[0].Gap)
	}
}

// An alert that clears and fires again half a minute later is one flapping
// outage, and another signal's resolution does not end it.
func TestAFlappingAlertAndAnotherSignalsResolutionStayOneEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration) time.Time { return now.Add(-ago) }
	mk := func(id, typ string, ago time.Duration) event.Event {
		return event.Event{ID: id, IngestedAt: at(ago), OccurredAt: at(ago), Namespace: "default", EntityKind: "Service", EntityName: "frontend", Type: typ, Severity: "critical", Title: "high_error_rate on frontend: 1.000"}
	}
	cause := event.Event{ID: "cause", IngestedAt: at(10 * time.Minute), OccurredAt: at(10 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_change", Title: "api resource limits changed"}
	source := &rangedEvents{all: []event.Event{
		cause,
		mk("e1", "error_spike", 8*time.Minute),
		mk("r1", "error_spike_resolved", 7*time.Minute+30*time.Second), // flap: back 30s later
		mk("e2", "error_spike", 7*time.Minute),
		mk("lat", "latency_spike_resolved", 4*time.Minute), // someone else's resolution
		mk("e3", "error_spike", 3*time.Minute),
	}}
	graph := fakeGraph{"default/Service/frontend": 0, "default/Deployment/api": 2}
	got, err := (&Analyzer{Events: source, Graph: graph}).Analyze(context.Background(), mk("now", "error_spike", 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) == 0 || got.Candidates[0].Event.ID != "cause" || got.Candidates[0].Gap > 125 {
		t.Fatalf("the whole flapping episode should be followed to its first alert: %+v", got.Candidates)
	}
}

// A crash loop is quiet in the caller's logs between failures but keeps
// restarting the pod behind it, so trouble on the path keeps the episode whole.
func TestTroubleOnTheCausalPathKeepsAnEpisodeAlive(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration) time.Time { return now.Add(-ago) }
	weblog := func(id string, ago time.Duration) event.Event {
		return event.Event{ID: id, IngestedAt: at(ago), OccurredAt: at(ago), Namespace: "default", EntityKind: "Pod", EntityName: "web-1", Type: "log_error", Severity: "warning", Title: "api returned HTTP 503"}
	}
	oom := func(id string, ago time.Duration) event.Event {
		return event.Event{ID: id, IngestedAt: at(ago), OccurredAt: at(ago), Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "oom_kill", Severity: "critical", Title: "api restarted (OOMKilled)"}
	}
	cause := event.Event{ID: "limit", IngestedAt: at(8 * time.Minute), OccurredAt: at(8 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "resource_change", Title: "api resource limits changed"}
	all := []event.Event{cause, weblog("w1", 7*time.Minute), weblog("w2", 3*time.Minute)} // 4 quiet minutes in the web logs
	for i := 0; i < 8; i++ {
		all = append(all, oom(fmt.Sprintf("oom%d", i), 7*time.Minute-time.Duration(i)*50*time.Second))
	}
	graph := fakeGraph{"default/Pod/web-1": 0, "default/Pod/api-1": 1, "default/Deployment/api": 2}

	got, err := (&Analyzer{Events: &rangedEvents{all: all}, Graph: graph}).Analyze(context.Background(), weblog("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	var limit *Candidate
	for i := range got.Candidates {
		if got.Candidates[i].Event.ID == "limit" {
			limit = &got.Candidates[i]
		}
	}
	if limit == nil {
		t.Fatalf("the limit change was lost: %+v", got.Candidates)
	}
	if limit.Gap > 70 {
		t.Fatalf("the gap should run to where the trouble began, got %.0fs", limit.Gap)
	}
	if limit.Chain != got.Candidates[0].Chain {
		t.Fatal("the restarts are effects of the change, so they belong to its chain")
	}
}

// Quiet on the whole path is a recovery, so the same error text afterwards is a
// different outage.
func TestQuietOnTheWholePathStartsANewEpisode(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration) time.Time { return now.Add(-ago) }
	weblog := func(id string, ago time.Duration) event.Event {
		return event.Event{ID: id, IngestedAt: at(ago), OccurredAt: at(ago), Namespace: "default", EntityKind: "Pod", EntityName: "web-1", Type: "log_error", Severity: "warning", Title: "api returned HTTP 503"}
	}
	first := event.Event{ID: "first", IngestedAt: at(20 * time.Minute), OccurredAt: at(20 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "scale", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	second := event.Event{ID: "second", IngestedAt: at(3 * time.Minute), OccurredAt: at(3 * time.Minute), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "scale", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	all := []event.Event{first, second, weblog("a", 19*time.Minute), weblog("b", 18*time.Minute+30*time.Second), weblog("c", 170*time.Second), weblog("d", 150*time.Second)}
	graph := fakeGraph{"default/Pod/web-1": 0, "default/Deployment/api": 2, "default/Deployment/worker": 2}

	got, err := (&Analyzer{Events: &rangedEvents{all: all}, Graph: graph}).Analyze(context.Background(), weblog("now", 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidates[0].Event.ID != "second" {
		t.Fatalf("the current outage's cause should lead: %+v", got.Candidates[0].Event)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "first" {
			t.Fatal("the earlier outage's cause was offered")
		}
	}
}

// edgesByWindow serves edges only inside the requested window, like the store.
type edgesByWindow struct {
	fakeGraph
	edges []graph.Edge
	at    []time.Time // when each edge was first recorded
}

func (g edgesByWindow) EdgesBetween(_ context.Context, _, to time.Time) ([]graph.Edge, error) {
	var out []graph.Edge
	for i, e := range g.edges {
		if !g.at[i].After(to) {
			out = append(out, e)
		}
	}
	return out, nil
}

// A pod created seconds before it fails is recorded in the graph on the next
// sync, a little after the symptom; its owner must still be reachable.
func TestAFreshPodStillReachesItsOwner(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	node := func(kind, name string) graph.Node { return graph.Node{Namespace: "default", Kind: kind, Name: name} }
	source := edgesByWindow{
		edges: []graph.Edge{{From: node("Deployment", "worker"), To: node("Pod", "worker-new"), Kind: "owns"}},
		at:    []time.Time{now.Add(12 * time.Second)}, // the next sync, after the symptom
	}
	deploy := event.Event{ID: "deploy", IngestedAt: now.Add(-19 * time.Second), OccurredAt: now.Add(-19 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "deploy", Title: "worker deployed: latest -> broken", Payload: []byte(`{"old_image":"latest","new_image":"broken"}`)}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "worker-new", Type: "container_restart", Severity: "warning", Title: "worker restarted (Error, exit 1)"}

	got, err := (&Analyzer{Events: fakeEvents{deploy}, Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].Event.ID != "deploy" {
		t.Fatalf("the owner's release should be reachable: %+v", got.Candidates)
	}
}

// A restored config or limit undoes the change before it, so it is a repair and
// must not compete with the break.
func TestARestoredConfigOrLimitDoesNotOutrankTheBreak(t *testing.T) {
	cases := map[string][2]string{
		"config_change":   {`{"from_hash":"aaa","to_hash":"bbb"}`, `{"from_hash":"bbb","to_hash":"aaa"}`},
		"resource_change": {`{"old_mem_limit":0,"new_mem_limit":8388608}`, `{"old_mem_limit":8388608,"new_mem_limit":0}`},
	}
	for kind, payloads := range cases {
		now := time.Now().UTC()
		symptom := event.Event{ID: "s", IngestedAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "resource_status"}
		node := func(k, n string) graph.Node { return graph.Node{Kind: k, Name: n, Namespace: "default"} }
		source := &countingGraph{edges: []graph.Edge{{From: node("Deployment", "api"), To: node("Pod", "api-1"), Kind: "owns", Weight: 1, Source: "static"}}}
		change := func(id string, ago time.Duration, payload string) event.Event {
			return event.Event{ID: id, IngestedAt: now.Add(-ago), OccurredAt: now.Add(-ago), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: kind, Payload: []byte(payload)}
		}
		window := []event.Event{
			change("broke", 50*time.Second, payloads[0]),
			failureOn("failing", now.Add(-20*time.Second), "api-1"),
			change("fixed", 1*time.Second, payloads[1]),
		}
		got, err := (&Analyzer{Events: fakeEvents(window), Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Candidates) < 2 || got.Candidates[0].Event.ID != "broke" {
			t.Fatalf("%s: the break must lead: %+v", kind, got.Candidates)
		}
		if got.Candidates[1].Reverts != "broke" {
			t.Errorf("%s: the restore should be marked as undoing the break, got %q", kind, got.Candidates[1].Reverts)
		}
	}
}

// Two different config changes are two hypotheses, not one repeated event.
func TestDifferentConfigChangesAreDifferentHypotheses(t *testing.T) {
	mk := func(payload string) event.Event {
		return event.Event{Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "config_change", Payload: []byte(payload)}
	}
	if hypothesisKey(mk(`{"from_hash":"a","to_hash":"b"}`)) == hypothesisKey(mk(`{"from_hash":"b","to_hash":"c"}`)) {
		t.Fatal("distinct transitions must not collapse into one candidate")
	}
}

// A break that was restored, with a new failure starting after the restore had
// time to take effect, is history and must not outrank the real cause.
func TestARestoredBreakDoesNotExplainALaterFailure(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	config := func(id string, ago time.Duration, from, to string) event.Event {
		at := now.Add(-ago)
		return event.Event{ID: id, IngestedAt: at, OccurredAt: at, Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "config_change", Payload: []byte(`{"from_hash":"` + from + `","to_hash":"` + to + `"}`)}
	}
	worker := event.Event{ID: "worker", IngestedAt: now.Add(-30 * time.Second), OccurredAt: now.Add(-30 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "scale", Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-1", Type: "became_unready", Severity: "warning"}
	// The break caused failures, which is what makes the restore a repair.
	events := fakeEvents{config("break", 100*time.Second, "good", "bad"), failureOn("failing", now.Add(-80*time.Second), "api-1"), config("restore", 50*time.Second, "bad", "good"), worker}
	node := func(kind, name string) graph.Node { return graph.Node{Kind: kind, Name: name, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{
		{From: node("Deployment", "api"), To: node("Pod", "api-1"), Kind: "owns", Weight: 1, Source: "static"},
		{From: node("Pod", "api-1"), To: node("Deployment", "worker"), Kind: "calls", Weight: 1, Source: "static"},
	}}

	got, err := (&Analyzer{Events: events, Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidates[0].Event.ID != "worker" {
		t.Fatalf("the live cause should lead, got %q", got.Candidates[0].Event.ID)
	}
	for _, c := range got.Candidates {
		if c.Event.ID == "break" {
			t.Fatal("a break that was already restored should not be offered")
		}
	}
}

// Undoing a change is not a rival theory of the failure it undid.
func TestARepairIsNotARivalForConfidence(t *testing.T) {
	cause := candidateAt(0.64, 1)
	cause.Event.ID = "cause"
	repair := candidateAt(0.60, 1)
	repair.Event.ID = "repair"
	repair.Reverts = "earlier-break"

	_, _, separation := confidence([]Candidate{cause, repair})
	if separation != 1 || contested([]Candidate{cause, repair}) {
		t.Fatalf("a repair must not count as a rival: separation %.2f", separation)
	}
	repair.Reverts = ""
	if _, _, separation := confidence([]Candidate{cause, repair}); separation >= 0.9 {
		t.Fatal("a real rival should still lower separation")
	}
}

func chainCand(id, typ, name, chain string, at time.Time, score float64) Candidate {
	return Candidate{
		Event: event.Event{ID: id, Type: typ, EntityKind: "Deployment", EntityName: name, Namespace: "default", OccurredAt: at, IngestedAt: at},
		Score: score, Chain: chain,
	}
}

// A restart is the effect of the deploy before it; the answer names the deploy.
func TestTheRootOfTheLeadingChainIsReportedFirst(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deploy := chainCand("deploy", "deploy", "api", "deploy", t0, 0.4)
	restart := chainCand("restart", "container_restart", "api-2", "deploy", t0.Add(15*time.Second), 0.7)
	got := promoteChainRoot([]Candidate{restart, deploy})
	if got[0].Event.ID != "deploy" || got[1].Event.ID != "restart" {
		t.Fatalf("the root should lead: %v %v", got[0].Event.ID, got[1].Event.ID)
	}
}

// Two changes on different components do not explain each other just because
// one is upstream of the other.
func TestAChangeIsNotDemotedBehindAnUnrelatedEarlierChange(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	benign := chainCand("benign", "config_change", "worker", "benign", t0, 0.3)
	benign.Chain = "benign"
	deploy := chainCand("deploy", "deploy", "api", "benign", t0.Add(20*time.Second), 0.8)
	if got := promoteChainRoot([]Candidate{deploy, benign}); got[0].Event.ID != "deploy" {
		t.Fatalf("a deploy leading its own evidence must stay first, got %s", got[0].Event.ID)
	}
}

// An autoscaler limit and the scale it caused are one target and one story.
func TestAChangeAndItsOwnScaleAreOneStory(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	hpa := chainCand("hpa", "hpa_change", "worker", "hpa", t0, 0.3)
	scale := chainCand("scale", "scale", "worker", "hpa", t0.Add(3*time.Second), 0.6)
	if got := promoteChainRoot([]Candidate{scale, hpa}); got[0].Event.ID != "hpa" {
		t.Fatalf("the autoscaler change is the root of its scale, got %s", got[0].Event.ID)
	}
}

// The representative of a repeating signal is its newest copy; which signal came
// first is decided by when each began.
func TestTheRootIsChosenByWhenASignalBeganNotByItsNewestCopy(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	restart := chainCand("restart", "container_restart", "api-1", "restart", t0.Add(85*time.Second), 0.7)
	restart.FirstAt = t0
	unready := chainCand("unready", "became_unready", "api-1", "restart", t0.Add(time.Second), 0.6)
	if got := promoteChainRoot([]Candidate{restart, unready}); got[0].Event.ID != "restart" {
		t.Fatalf("the restart began first, got %s", got[0].Event.ID)
	}
}

func TestMeasurementsAloneAreNeverARootCause(t *testing.T) {
	only := func(types ...string) *Result {
		r := &Result{Confidence: 0.9}
		for _, ty := range types {
			r.Candidates = append(r.Candidates, Candidate{Event: event.Event{Type: ty}})
		}
		return r
	}
	for _, c := range []struct {
		name string
		r    *Result
		want string
	}{
		{"nothing", only(), VerdictNoRootCause},
		{"logs and alerts", only("log_error", "error_spike", "latency_spike"), VerdictNoRootCause},
		{"a change among them", only("log_error", "deploy"), VerdictRootCause},
		{"a pod failure fact", only("became_unready"), VerdictRootCause},
	} {
		if got := verdictOf(c.r); got != c.want {
			t.Errorf("%s: verdict %q, want %q", c.name, got, c.want)
		}
	}
	weak := only("deploy")
	weak.Confidence = 0.3
	if verdictOf(weak) != VerdictInconclusive {
		t.Error("a real candidate with low confidence is inconclusive, not absent")
	}
}

func TestANoRootCauseNarrativeSaysSoAndStillPointsAtTheOrigin(t *testing.T) {
	r := &Result{Verdict: VerdictNoRootCause, Symptom: event.Event{Title: "high_error_rate on frontend"}, Scanned: 40,
		Candidates: []Candidate{{Event: event.Event{Type: "log_error", EntityName: "api-1"}}}}
	got := FallbackNarrative(r)
	if !strings.Contains(got, "No root cause was observed") || !strings.Contains(got, "api-1") {
		t.Fatalf("narrative should refuse a cause but name where it surfaced: %s", got)
	}
}

// Only a change of the same kind overwrites an earlier one; the scale an
// autoscaler change causes must not erase it.
func TestAScaleDoesNotReplaceTheLimitChangeThatCausedIt(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration) time.Time { return now.Add(-ago) }
	hpa := event.Event{ID: "hpa", IngestedAt: at(90 * time.Second), OccurredAt: at(90 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "hpa_change", Payload: []byte(`{"from_hash":"a","to_hash":"b"}`)}
	scale := event.Event{ID: "scale", IngestedAt: at(87 * time.Second), OccurredAt: at(87 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "worker", Type: "scale", Payload: []byte(`{"old_replicas":3,"new_replicas":1}`)}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Service", EntityName: "api", Type: "latency_spike"}

	got, err := (&Analyzer{Events: fakeEvents{hpa, scale}, Graph: fakeGraph{"default/Service/api": 0, "default/Deployment/worker": 2}}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, c := range got.Candidates {
		found = found || c.Event.ID == "hpa"
	}
	if !found {
		t.Fatalf("the autoscaler change was dropped as replaced: %+v", got.Candidates)
	}
}

// Two interventions that both explain one crash are rivals. Joining them through
// the shared effect made every such incident look certain.
func TestTwoCausesOfOneCrashAreRivalsAndConfidenceShowsIt(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration) time.Time { return now.Add(-ago) }
	config := event.Event{ID: "config", IngestedAt: at(64 * time.Second), OccurredAt: at(64 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "config_change", Payload: []byte(`{"from_hash":"a","to_hash":"b"}`)}
	deploy := event.Event{ID: "deploy", IngestedAt: at(60 * time.Second), OccurredAt: at(60 * time.Second), Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "deploy", Payload: []byte(`{"old_image":"v1","new_image":"v2"}`)}
	restart := event.Event{ID: "restart", IngestedAt: at(40 * time.Second), OccurredAt: at(40 * time.Second), Namespace: "default", EntityKind: "Pod", EntityName: "api-2", Type: "container_restart", Severity: "warning", Payload: []byte(`{"owner":"api"}`)}
	symptom := event.Event{ID: "s", IngestedAt: now, OccurredAt: now, Namespace: "default", EntityKind: "Pod", EntityName: "api-2", Type: "became_unready", Severity: "warning"}
	node := func(k, n string) graph.Node { return graph.Node{Kind: k, Name: n, Namespace: "default"} }
	source := &countingGraph{edges: []graph.Edge{{From: node("Deployment", "api"), To: node("Pod", "api-2"), Kind: "owns", Weight: 1, Source: "static"}}}

	got, err := (&Analyzer{Events: fakeEvents{config, deploy, restart}, Graph: source, MaxHops: 3}).Analyze(context.Background(), symptom)
	if err != nil {
		t.Fatal(err)
	}
	chains := map[string]bool{}
	for _, c := range got.Candidates {
		if changeTypes[c.Event.Type] {
			chains[c.Chain] = true
		}
	}
	if len(chains) != 2 {
		t.Fatalf("the two changes should be separate explanations, got chains %v", chains)
	}
	if !got.Contested || got.Confidence > 0.7 {
		t.Fatalf("two near-equal causes must lower confidence: contested=%v confidence=%.2f", got.Contested, got.Confidence)
	}
}

// One cause with its effects stays one story, however many effects it has.
func TestOneCauseAndItsEffectsStayOneChain(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cands := []Candidate{
		chainCand("restart", "container_restart", "api-2", "", t0.Add(15*time.Second), 0.7),
		chainCand("deploy", "deploy", "api", "", t0, 0.4),
		chainCand("unready", "became_unready", "api-2", "", t0.Add(16*time.Second), 0.3),
	}
	reaches := func(from, to string) bool { return true }
	linkChains(cands, reaches)
	if cands[0].Chain != cands[1].Chain || cands[1].Chain != cands[2].Chain {
		t.Fatalf("a cause and its effects are one chain: %q %q %q", cands[0].Chain, cands[1].Chain, cands[2].Chain)
	}
}

func rootCand(id, typ, name string, score float64, at time.Time) Candidate {
	return Candidate{Event: event.Event{ID: id, Type: typ, EntityKind: "Deployment", EntityName: name, OccurredAt: at, IngestedAt: at}, Score: score, Chain: id}
}

func TestCloseRivalRootsAreAlternativesAndTheVerdictIsAmbiguous(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lead, rival := rootCand("a", "deploy", "catalog", 0.50, t0), rootCand("b", "config_change", "catalog", 0.45, t0)
	got := defaultConfig.alternatives([]Candidate{lead, rival})
	if len(got) != 1 || got[0].Event.ID != "b" {
		t.Fatalf("a rival at 90%% of the leader is an alternative: %+v", got)
	}
	r := &Result{Candidates: []Candidate{lead, rival}, Alternatives: got, Confidence: 0.9, Symptom: event.Event{Title: "errors on gateway"}}
	if defaultConfig.verdict(r) != VerdictAmbiguous {
		t.Fatalf("verdict %q", defaultConfig.verdict(r))
	}
}

func TestAWeakOrNonRootRivalIsNotAnAlternative(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lead := rootCand("a", "deploy", "catalog", 0.50, t0)
	for name, rival := range map[string]Candidate{
		"weaker":      rootCand("b", "config_change", "worker", 0.30, t0),
		"measurement": rootCand("c", "log_error", "gateway", 0.49, t0),
		"recovery":    rootCand("d", "became_ready", "gateway", 0.49, t0),
		"repair": func() Candidate {
			c := rootCand("e", "deploy", "catalog", 0.49, t0)
			c.Reverts = "a"
			return c
		}(),
		"same chain": func() Candidate {
			c := rootCand("f", "container_restart", "catalog-1", 0.49, t0)
			c.Chain = "a"
			return c
		}(),
	} {
		if got := defaultConfig.alternatives([]Candidate{lead, rival}); len(got) != 0 {
			t.Errorf("%s should not be an alternative: %+v", name, got)
		}
	}
}

func TestTheAmbiguousNarrativeNamesEveryCauseAndRefusesToChoose(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lead, rival := rootCand("a", "deploy", "catalog", 0.50, t0), rootCand("b", "config_change", "entitlement", 0.46, t0)
	r := &Result{Candidates: []Candidate{lead, rival}, Alternatives: []Candidate{rival}, Verdict: VerdictAmbiguous, Confidence: 0.55, Symptom: event.Event{Title: "errors on gateway"}, Scanned: 12}
	got := FallbackNarrative(r)
	for _, want := range []string{"cannot separate", "catalog", "entitlement", "unresolved"} {
		if !strings.Contains(got, want) {
			t.Errorf("narrative should mention %q: %s", want, got)
		}
	}
	if strings.Contains(got, "most likely cause") {
		t.Errorf("an ambiguous answer must not assert a most likely cause: %s", got)
	}
}

func TestAWeakerRivalIsMentionedButDoesNotMakeTheAnswerAmbiguous(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lead, rival := rootCand("a", "deploy", "catalog", 0.60, t0), rootCand("b", "config_change", "worker", 0.20, t0)
	r := &Result{Candidates: []Candidate{lead, rival}, Verdict: VerdictRootCause, Confidence: 0.7, Symptom: event.Event{Title: "x"}}
	got := FallbackNarrative(r)
	if !strings.Contains(got, "next most plausible cause is config_change on worker") || strings.Contains(got, "cannot separate") {
		t.Fatalf("the rival is named as a fallback only: %s", got)
	}
}
