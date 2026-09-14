// Package rca implements Chronicle's deterministic root-cause analysis
// pipeline. An LLM is used only to explain already-ranked evidence.
package rca

import (
	"context"
	"fmt"
	"math"
	"sort"
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
	// The upper bound is exclusive; an event at the symptom timestamp cannot cause it.
	raw, err := a.Events.EventsBetween(ctx, symptom.IngestedAt.Add(-back), symptom.IngestedAt)
	if err != nil {
		return nil, fmt.Errorf("load causal window: %w", err)
	}
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
	reverts := revertingChanges(raw)
	candidates := make([]Candidate, 0, len(raw))
	for _, e := range raw {
		if !e.IngestedAt.Before(symptom.IngestedAt) || e.ID == symptom.ID {
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
	if len(candidates) > 5 {
		candidates = candidates[:5]
	}
	result.Candidates = candidates
	result.Confidence = confidence(candidates)
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

	gap := symptom.IngestedAt.Sub(c.Event.IngestedAt).Seconds()
	tf := math.Exp(-gap / 300.0)
	s *= tf
	c.Reasons = append(c.Reasons, fmt.Sprintf("%.0fs before symptom (×%.2f)", gap, tf))
	c.Factors = append(c.Factors, Factor{Label: "Time distance", Detail: fmt.Sprintf("%.0fs before the symptom", gap), Multiplier: tf})

	df := 1.0 / (1.0 + float64(c.Distance)*0.4)
	s *= df
	c.Reasons = append(c.Reasons, fmt.Sprintf("%d hops away (×%.2f)", c.Distance, df))
	c.Factors = append(c.Factors, Factor{Label: "Graph distance", Detail: fmt.Sprintf("%d hop(s) upstream", c.Distance), Multiplier: df})

	if c.AffectedServices > 0 {
		affected := float64(c.AffectedServices)
		impactFactor := 1 + 0.25*affected/(affected+10)
		s *= impactFactor
		c.Reasons = append(c.Reasons, fmt.Sprintf("%d affected service(s) (×%.2f)", c.AffectedServices, impactFactor))
		c.Factors = append(c.Factors, Factor{Label: "Blast radius", Detail: fmt.Sprintf("%d service(s) affected", c.AffectedServices), Multiplier: impactFactor})
	}
	if c.Reverts != "" {
		s *= remediationFactor
		c.Reasons = append(c.Reasons, fmt.Sprintf("undoes an earlier change (×%.2f)", remediationFactor))
		c.Factors = append(c.Factors, Factor{Label: "Remediation", Detail: "undoes an earlier change in this window", Multiplier: remediationFactor})
	}
	c.Score = math.Min(s, 1.0)
}
func confidence(c []Candidate) float64 {
	if len(c) == 0 {
		return 0
	}
	if len(c) == 1 {
		return c[0].Score
	}
	gap := c[0].Score - c[1].Score
	return c[0].Score * (0.5 + math.Min(gap*2, 0.5))
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

// revertingChanges finds events that undo an earlier change to the same target
// inside the causal window, returning the reverting event's ID mapped to the
// event it undoes.
//
// Fixing an outage puts a change into the causal window like any other, and
// recency alone ranks it above the break it was fixing: restoring an image at
// the moment the pod finally reports Failed scores higher than the deploy that
// broke it a minute earlier. An exact there-and-back pair is the one signal
// that separates the two without guessing.
func revertingChanges(events []event.Event) map[string]string {
	type change struct{ id, from, to string }
	history := make(map[string][]change)
	reverts := make(map[string]string)
	for _, e := range events {
		var from, to string
		switch e.Type {
		case "deploy":
			from = gjson.GetBytes(e.Payload, "old_image").String()
			to = gjson.GetBytes(e.Payload, "new_image").String()
		case "scale":
			from = gjson.GetBytes(e.Payload, "old_replicas").String()
			to = gjson.GetBytes(e.Payload, "new_replicas").String()
		default:
			continue
		}
		if from == "" || to == "" || from == to {
			continue
		}
		target := key(e)
		for _, earlier := range history[target] {
			if earlier.from == to && earlier.to == from {
				reverts[e.ID] = earlier.id
				break
			}
		}
		history[target] = append(history[target], change{id: e.ID, from: from, to: to})
	}
	return reverts
}
