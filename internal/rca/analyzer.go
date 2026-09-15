// Package rca implements Chronicle's deterministic root-cause analysis
// pipeline. An LLM is used only to explain already-ranked evidence.
package rca

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/tidwall/gjson"
)

type EventSource interface {
	EventsBetween(context.Context, time.Time, time.Time) ([]event.Event, error)
}
type GraphSource interface {
	UpstreamAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error)
}
type ImpactGraphSource interface {
	GraphSource
	DownstreamAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error)
	ImpactAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error)
}
type HistoricalEdgeSource interface {
	EdgesAt(ctx context.Context, t time.Time) ([]graph.Edge, error)
}

// WindowedEdgeSource can hand over every edge that was valid anywhere in the
// causal window, which is what the analyzer wants: a resource deleted during
// the incident has no edges left by the time the symptom arrives.
type WindowedEdgeSource interface {
	EdgesBetween(ctx context.Context, from, to time.Time) ([]graph.Edge, error)
}
type Narrator interface {
	Narrate(context.Context, *Result) (string, error)
}

// Factor is one step of a candidate's score, kept as data so the derivation
// can be rendered as a table rather than read out of a sentence.
type Factor struct {
	Label      string  `json:"label"`
	Detail     string  `json:"detail"`
	Multiplier float64 `json:"multiplier"`
	Base       bool    `json:"base,omitempty"`
}

type Candidate struct {
	Event            event.Event `json:"event"`
	Distance         int         `json:"distance"`
	Score            float64     `json:"score"`
	Reasons          []string    `json:"reasons"`
	Reverts          string      `json:"reverts,omitempty"`
	Occurrences      int         `json:"occurrences"`
	Factors          []Factor    `json:"factors"`
	AffectedServices int         `json:"affected_services"`
	AffectedNodes    int         `json:"affected_nodes"`
	BlastRadiusScore float64     `json:"blast_radius_score"`
}
type BlastRadius struct {
	AffectedServices int      `json:"affected_services"`
	AffectedNodes    int      `json:"affected_nodes"`
	Score            float64  `json:"score"`
	Services         []string `json:"services"`
}
type Result struct {
	Symptom     event.Event  `json:"symptom"`
	Candidates  []Candidate  `json:"candidates"`
	Confidence  float64      `json:"confidence"`
	Strength    float64      `json:"strength"`
	Provisional bool         `json:"provisional"`
	Separation  float64      `json:"separation"`
	Scanned     int          `json:"scanned"`
	Narrative   string       `json:"narrative"`
	BlastRadius BlastRadius  `json:"blast_radius"`
	Evidence    []graph.Edge `json:"evidence"`
}
type Analyzer struct {
	Events   EventSource
	Graph    GraphSource
	Narrator Narrator
	MaxHops  int
}

var lookback = map[string]time.Duration{
	"error_spike": 10 * time.Minute, "latency_spike": 15 * time.Minute,
	"oom_kill": 60 * time.Minute, "crash_loop": 20 * time.Minute, "became_unready": 5 * time.Minute,
}
var typeWeight = map[string]float64{
	"deploy": 1.00, "config_change": 0.95, "resource_change": 0.90, "oom_kill": 0.85,
	"scale": 0.75, "container_restart": 0.70, "node_pressure": 0.65,
	"became_unready": 0.50, "error_spike": 0.30, "latency_spike": 0.25, "log_error": 0.15,
}

// allowedLateness is how long Chronicle waits for stragglers. Collectors see a
// change some time after it happens, so an event that occurred before the
// symptom can be ingested after it. Bounding the ingestion query by symptom
// time alone silently drops exactly those events — usually the deploy that
// caused the incident, because it is noticed last.
const allowedLateness = 30 * time.Second

// maxCausalSkew bounds how far an event's own timestamp may sit before the
// moment Chronicle received it. Sources disagree about time: clocks drift, and
// a repeating Kubernetes Event can carry the timestamp of when its series
// began. Past this bound the source's clock is not trustworthy for ordering,
// so ingestion time — which Chronicle controls — is used instead.
const maxCausalSkew = 5 * time.Minute

// causalTime is when an event actually happened. Ingestion time is how the
// event store is indexed; it is not when the world changed, and using it for
// ordering makes causality mean "the order we noticed things".
func causalTime(e event.Event) time.Time {
	if e.OccurredAt.IsZero() || e.IngestedAt.Sub(e.OccurredAt) > maxCausalSkew {
		return e.IngestedAt
	}
	return e.OccurredAt
}

func key(e event.Event) string {
	return fmt.Sprintf("%s/%s/%s", e.Namespace, e.EntityKind, e.EntityName)
}

func (a *Analyzer) Analyze(ctx context.Context, symptom event.Event) (*Result, error) {
	if a.Events == nil || a.Graph == nil {
		return nil, fmt.Errorf("rca analyzer requires an event source and graph")
	}
	back := lookback[symptom.Type]
	if back == 0 {
		back = 15 * time.Minute
	}
	// The ingestion scan reaches past the symptom by the lateness allowance so
	// straggling events are seen; causality is then judged on event time below.
	raw, err := a.Events.EventsBetween(ctx, symptom.IngestedAt.Add(-back-allowedLateness), symptom.IngestedAt.Add(allowedLateness))
	if err != nil {
		return nil, fmt.Errorf("load causal window: %w", err)
	}
	sort.SliceStable(raw, func(i, j int) bool { return causalTime(raw[i]).Before(causalTime(raw[j])) })
	symptomAt := causalTime(symptom)
	windowStart := symptomAt.Add(-back)
	inWindow := make([]event.Event, 0, len(raw))
	for _, e := range raw {
		if at := causalTime(e); !at.Before(windowStart) && at.Before(symptomAt) {
			inWindow = append(inWindow, e)
		}
	}
	raw = inWindow
	hops := a.MaxHops
	if hops <= 0 {
		hops = 4
	}

	// The graph at the symptom instant is loaded once and walked in memory.
	// Querying it per candidate turns one analysis into N+2 full graph loads.
	var (
		local *graph.Graph
		edges []graph.Edge
	)
	if windowed, ok := a.Graph.(WindowedEdgeSource); ok {
		if loaded, edgeErr := windowed.EdgesBetween(ctx, symptom.IngestedAt.Add(-back), symptom.IngestedAt); edgeErr == nil {
			edges = loaded
		}
	}
	if edges == nil {
		if edgeSource, ok := a.Graph.(HistoricalEdgeSource); ok {
			if loaded, edgeErr := edgeSource.EdgesAt(ctx, symptom.IngestedAt); edgeErr == nil {
				edges = loaded
			}
		}
	}
	if edges != nil {
		local = graph.New()
		local.SetEdges(edges)
	}

	var upstream map[string]int
	if local != nil {
		upstream = local.Upstream(key(symptom), hops)
	} else if upstream, err = a.Graph.UpstreamAt(ctx, symptom.IngestedAt, key(symptom), hops); err != nil {
		return nil, fmt.Errorf("load graph at symptom time: %w", err)
	}

	impactOf := func(start string) map[string]int {
		if local != nil {
			return local.Impact(start, hops)
		}
		return a.remoteImpact(ctx, symptom.IngestedAt, start, hops)
	}

	result := &Result{Symptom: symptom, BlastRadius: summarizeImpact(impactOf(key(symptom))), Scanned: len(raw)}
	if edges != nil {
		result.Evidence = evidenceEdges(edges, upstream, key(symptom))
	}
	reverts := revertingChanges(raw, impactOf)
	candidates := make([]Candidate, 0, len(raw))
	for _, e := range raw {
		if e.ID == symptom.ID {
			continue
		}
		distance, reachable := upstream[key(e)]
		if !reachable {
			continue
		}
		c := Candidate{Event: e, Distance: distance, Reverts: reverts[e.ID]}
		impact := summarizeImpact(impactOf(key(e)))
		c.AffectedServices, c.AffectedNodes, c.BlastRadiusScore = impact.AffectedServices, impact.AffectedNodes, impact.Score
		score(&c, symptom)
		candidates = append(candidates, c)
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	result.Candidates = topDistinct(candidates, 5)

	// A symptom younger than the lateness allowance may still be missing the
	// events that explain it, so the answer is provisional rather than wrong.
	result.Provisional = time.Since(symptomAt) < allowedLateness
	result.Confidence, result.Strength, result.Separation = confidence(candidates)
	if a.Narrator != nil {
		result.Narrative, err = a.Narrator.Narrate(ctx, result)
		if err != nil {
			result.Narrative = FallbackNarrative(result)
		}
	} else {
		result.Narrative = FallbackNarrative(result)
	}
	return result, nil
}

// remoteImpact is the fallback for graph sources that cannot hand over their
// edge set. It is one query per call, so it is only reached when the analyzer
// could not load the graph once up front.
func (a *Analyzer) remoteImpact(ctx context.Context, at time.Time, start string, hops int) map[string]int {
	if source, ok := a.Graph.(ImpactGraphSource); ok {
		if nodes, err := source.ImpactAt(ctx, at, start, hops); err == nil {
			return nodes
		}
	}
	return map[string]int{start: 0}
}

func summarizeImpact(nodes map[string]int) BlastRadius {
	result := BlastRadius{Services: []string{}}
	for node, distance := range nodes {
		if distance == 0 {
			continue
		}
		result.AffectedNodes++
		parsed, err := graph.ParseKey(node)
		if err != nil {
			continue
		}
		if parsed.Kind == "Service" || parsed.Kind == "Deployment" || parsed.Kind == "StatefulSet" {
			result.AffectedServices++
			result.Services = append(result.Services, node)
		}
	}
	result.Score = math.Min(1, float64(result.AffectedServices)*0.15+float64(result.AffectedNodes)*0.03)
	sort.Strings(result.Services)
	return result
}

func evidenceEdges(edges []graph.Edge, reachable map[string]int, symptom string) []graph.Edge {
	reachable[symptom] = 0
	result := make([]graph.Edge, 0)
	seen := map[string]bool{}
	for _, edge := range edges {
		if _, from := reachable[edge.From.Key()]; !from {
			continue
		}
		if _, to := reachable[edge.To.Key()]; !to {
			continue
		}
		id := edge.From.Key() + "|" + edge.To.Key() + "|" + edge.Kind
		if !seen[id] {
			seen[id] = true
			result = append(result, edge)
		}
	}
	return result
}

// score multiplies a base weight for the event type by three dampening or
// amplifying factors. Each step is recorded twice: Reasons keeps the sentence
// form the heal audit trail stores, and Factors keeps the same step as data so
// a caller can lay the derivation out as a table instead of parsing prose.
func score(c *Candidate, symptom event.Event) {
	s := typeWeight[c.Event.Type]
	if s == 0 {
		s = 0.20
	}
	c.Reasons = append(c.Reasons, fmt.Sprintf("event type %q (base %.2f)", c.Event.Type, s))
	c.Factors = append(c.Factors, Factor{Label: "Event type", Detail: c.Event.Type, Multiplier: s, Base: true})

	gap := causalTime(symptom).Sub(causalTime(c.Event)).Seconds()
	tf := math.Exp(-gap / 300.0)
	s *= tf
	c.Reasons = append(c.Reasons, fmt.Sprintf("%.0fs before symptom (×%.2f)", gap, tf))
	c.Factors = append(c.Factors, Factor{Label: "Time distance", Detail: fmt.Sprintf("%.0fs before the symptom", gap), Multiplier: tf})

	df := distanceFactor(c.Distance)
	s *= df
	c.Reasons = append(c.Reasons, fmt.Sprintf("%d hops away (×%.2f)", c.Distance, df))
	c.Factors = append(c.Factors, Factor{Label: "Graph distance", Detail: fmt.Sprintf("%d hop(s) upstream", c.Distance), Multiplier: df})

	if c.AffectedServices > 0 {
		affected := float64(c.AffectedServices)
		impactFactor := 1 + 0.25*affected/(affected+10)
		s *= impactFactor
		c.Reasons = append(c.Reasons, fmt.Sprintf("%d affected service(s) (×%.2f)", c.AffectedServices, impactFactor))
		c.Factors = append(c.Factors, Factor{Label: "Blast radius", Detail: fmt.Sprintf("%d service(s) depend on this cause", c.AffectedServices), Multiplier: impactFactor})
	}
	if c.Reverts != "" {
		s *= remediationFactor
		c.Reasons = append(c.Reasons, fmt.Sprintf("undoes an earlier change (×%.2f)", remediationFactor))
		c.Factors = append(c.Factors, Factor{Label: "Remediation", Detail: "undoes an earlier change in this window", Multiplier: remediationFactor})
	}
	c.Score = math.Min(s, 1.0)
}

// distanceFactor is the structural penalty for how far a candidate sits from
// the symptom in the dependency graph.
func distanceFactor(hops int) float64 { return 1.0 / (1.0 + float64(hops)*0.4) }

// confidence answers two independent questions and multiplies the answers:
// how strong the leading candidate's evidence is, and how clearly it beats the
// runner-up. Uncertainty sampling calls the second one margin confidence — the
// separation between the top two candidates rather than the top score alone.
//
// They are kept apart because the previous formula multiplied the raw score,
// which already carries the graph-distance penalty. That made confidence
// structurally capped: at one hop it could never exceed 0.89 and at three hops
// 0.57, so a cause could be the only possible explanation and still never
// clear a 0.90 gate. Dividing the score by its own distance penalty measures
// the evidence on its merits, and lets a distant but obvious cause be
// identified as confidently as a close one.
func confidence(c []Candidate) (overall, strength, separation float64) {
	if len(c) == 0 {
		return 0, 0, 0
	}
	// How good is the leading candidate, setting aside how far away it is.
	strength = math.Min(1, c[0].Score/distanceFactor(c[0].Distance))
	// How clearly does it beat the runner-up? With nothing to compare against,
	// separation cannot argue either way, so it does not reduce the result.
	separation = 1
	if len(c) > 1 && c[0].Score > 0 {
		margin := (c[0].Score - c[1].Score) / c[0].Score
		separation = 0.5 + 0.5*math.Max(0, math.Min(1, margin))
	}
	return strength * separation, strength, separation
}

func FallbackNarrative(r *Result) string {
	if len(r.Candidates) == 0 {
		return fmt.Sprintf("No upstream cause was found for %s at %s; the analysis is inconclusive.", r.Symptom.Title, r.Symptom.IngestedAt.Format(time.RFC3339))
	}
	c := r.Candidates[0]
	prefix := "The analysis is inconclusive. "
	if r.Confidence >= 0.5 {
		prefix = "The most likely cause is "
	}
	impact := ""
	if r.BlastRadius.AffectedServices > 0 {
		impact = fmt.Sprintf(", affecting %d downstream service(s)", r.BlastRadius.AffectedServices)
	}
	return fmt.Sprintf("%s%s on %s at %s (%d hop(s) upstream, %.0fs before the symptom%s), with confidence %.2f. The analysis scanned %d events and retained %d graph-reachable candidate(s).", prefix, c.Event.Title, c.Event.EntityName, c.Event.IngestedAt.Format(time.RFC3339), c.Distance, r.Symptom.IngestedAt.Sub(c.Event.IngestedAt).Seconds(), impact, r.Confidence, r.Scanned, len(r.Candidates))
}

// remediationFactor damps a change that undoes an earlier one. It is a
// demotion rather than an exclusion: rolling back to a bad older version is a
// real way to cause an incident, so the candidate stays rankable.
const remediationFactor = 0.35

// revertingChanges finds changes that undo the previous change to the same
// target *after that change had already started failing*, returning the
// reverting event's ID mapped to the event it undoes.
//
// Fixing an outage puts a change into the causal window like any other, and
// recency alone ranks the fix above the break it was fixing. But a there-and-
// back pair is not enough on its own: break, fix, break, fix alternates, so
// every change reverses the one before it and marking them all demotes the
// whole window equally — which changes no ranking at all.
//
// What separates the two is that nobody fixes something before it breaks. A
// change only counts as a remediation when the thing it targets was already
// emitting failures when it was made.
func revertingChanges(events []event.Event, affected func(target string) map[string]int) map[string]string {
	type change struct{ id, from, to string }
	previous := make(map[string]change)
	reverts := make(map[string]string)
	for _, e := range events {
		from, to, ok := changeSignature(e)
		if !ok {
			continue
		}
		target := key(e)
		last, seen := previous[target]
		previous[target] = change{id: e.ID, from: from, to: to}
		if !seen || last.from != to || last.to != from {
			continue // not a there-and-back pair with the change right before it
		}
		if failingAt(events, affected(target), e.IngestedAt) {
			reverts[e.ID] = last.id
		}
	}
	return reverts
}

// isRecovery reports whether an event says a resource became healthy again.
// Chronicle already emits these explicitly, which is what makes the state
// machine below possible without inventing a heuristic.
func isRecovery(e event.Event) bool {
	switch e.Type {
	case "became_ready", "application_healthy":
		return true
	case "resource_status":
		phase := gjson.GetBytes(e.Payload, "phase").String()
		return phase == "Running" || phase == "Completed"
	}
	return strings.HasSuffix(e.Type, "_resolved")
}

// failingAt reports whether anything the target affects was in a failing state
// at that instant, using hysteresis: the two directions are not symmetric.
//
// A failure opens an episode immediately. Closing it requires an explicit
// signal — never merely the absence of further failures, because quiet and
// healthy are not the same thing, and a redeploy that lands while the previous
// failures have simply stopped arriving would otherwise read as a repair. This
// mirrors how monitors avoid flapping with separate alert and recovery
// thresholds; a recovery seen while nothing was failing has no effect, exactly
// as a metric crossing a recovery threshold it never breached resolves nothing.
//
// Episodes are tracked per entity, not per target, because a rolling update
// replaces the failing pod rather than repairing it: the broken pod is deleted
// and a different one serves traffic, so no pod is ever observed becoming
// ready again. A deleted resource therefore closes its own episode — whatever
// was unhealthy no longer exists.
//
// Events must be in ascending time order, which EventsBetween guarantees.
func failingAt(events []event.Event, scope map[string]int, at time.Time) bool {
	failing := make(map[string]bool)
	for _, e := range events {
		if !causalTime(e).Before(at) {
			break
		}
		entity := key(e)
		if _, within := scope[entity]; !within {
			continue
		}
		switch {
		case e.Type == "resource_deleted", isRecovery(e):
			delete(failing, entity)
		case e.Severity == "warning" || e.Severity == "critical":
			failing[entity] = true
		}
	}
	return len(failing) > 0
}

// topDistinct keeps the best candidate per distinct hypothesis instead of the
// best n events. One misbehaving container emits the same log line twenty
// times; ranked individually they fill every slot and push out the deploy that
// actually caused them. This is the deduplicating half of result
// diversification: a list of distinct explanations is more useful than a list
// of the highest-scoring rows, which is the same insight behind maximal
// marginal relevance in search ranking.
func topDistinct(candidates []Candidate, limit int) []Candidate {
	kept := make([]Candidate, 0, limit)
	seen := make(map[string]int, limit) // hypothesis -> index in kept
	for _, c := range candidates {
		hypothesis := hypothesisKey(c.Event)
		if at, exists := seen[hypothesis]; exists {
			kept[at].Occurrences++
			continue
		}
		if len(kept) == limit {
			continue // still counting repeats of what we kept
		}
		c.Occurrences = 1
		seen[hypothesis] = len(kept)
		kept = append(kept, c)
	}
	return kept
}

// changeSignature returns the before and after state a change event describes,
// which is the only reliable way to tell two changes to the same resource
// apart: titles are cosmetic, the transition is the change.
func changeSignature(e event.Event) (from, to string, ok bool) {
	switch e.Type {
	case "deploy":
		from = gjson.GetBytes(e.Payload, "old_image").String()
		to = gjson.GetBytes(e.Payload, "new_image").String()
	case "scale":
		from = gjson.GetBytes(e.Payload, "old_replicas").String()
		to = gjson.GetBytes(e.Payload, "new_replicas").String()
	default:
		return "", "", false
	}
	return from, to, from != "" && to != "" && from != to
}

var (
	hexRun      = regexp.MustCompile(`\b[0-9a-f]{8,}\b`)
	digits      = regexp.MustCompile(`\d+`)
	whitespaceR = regexp.MustCompile(`\s+`)
)

// hypothesisKey identifies the explanation an event represents rather than the
// event itself. Two deploys of different images on one Deployment are two
// hypotheses; twenty copies of the same log line, differing only in a
// timestamp or a pod hash, are one. Normalising the volatile parts of the
// title is what separates those cases.
func hypothesisKey(e event.Event) string {
	if from, to, ok := changeSignature(e); ok {
		return key(e) + "|" + e.Type + "|" + from + ">" + to
	}
	shape := hexRun.ReplaceAllString(strings.ToLower(e.Title), "<hash>")
	shape = digits.ReplaceAllString(shape, "<n>")
	shape = strings.TrimSpace(whitespaceR.ReplaceAllString(shape, " "))
	if len(shape) > 160 {
		shape = shape[:160]
	}
	return key(e) + "|" + e.Type + "|" + shape
}
