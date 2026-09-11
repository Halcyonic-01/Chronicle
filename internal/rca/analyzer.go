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
type Narrator interface {
	Narrate(context.Context, *Result) (string, error)
}

type Candidate struct {
	Event            event.Event `json:"event"`
	Distance         int         `json:"distance"`
	Score            float64     `json:"score"`
	Reasons          []string    `json:"reasons"`
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
	upstream, err := a.Graph.UpstreamAt(ctx, symptom.IngestedAt, key(symptom), hops)
	if err != nil {
		return nil, fmt.Errorf("load graph at symptom time: %w", err)
	}

	impact, _ := a.downstream(ctx, symptom.IngestedAt, key(symptom), hops)
	result := &Result{Symptom: symptom, BlastRadius: summarizeImpact(impact), Scanned: len(raw)}
	if edgeSource, ok := a.Graph.(HistoricalEdgeSource); ok {
		if edges, edgeErr := edgeSource.EdgesAt(ctx, symptom.IngestedAt); edgeErr == nil {
			result.Evidence = evidenceEdges(edges, upstream, key(symptom))
		}
	}
	candidates := make([]Candidate, 0, len(raw))
	for _, e := range raw {
		if !e.IngestedAt.Before(symptom.IngestedAt) || e.ID == symptom.ID {
			continue
		}
		distance, reachable := upstream[key(e)]
		if !reachable {
			continue
		}
		c := Candidate{Event: e, Distance: distance}
		candidateImpact, _ := a.downstream(ctx, symptom.IngestedAt, key(e), hops)
		impact := summarizeImpact(candidateImpact)
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

func (a *Analyzer) downstream(ctx context.Context, at time.Time, start string, hops int) (map[string]int, error) {
	if source, ok := a.Graph.(ImpactGraphSource); ok {
		return source.ImpactAt(ctx, at, start, hops)
	}
	return map[string]int{start: 0}, nil
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

func score(c *Candidate, symptom event.Event) {
	s := typeWeight[c.Event.Type]
	if s == 0 {
		s = 0.20
	}
	c.Reasons = append(c.Reasons, fmt.Sprintf("event type %q (base %.2f)", c.Event.Type, s))
	gap := symptom.IngestedAt.Sub(c.Event.IngestedAt).Seconds()
	tf := math.Exp(-gap / 300.0)
	s *= tf
	c.Reasons = append(c.Reasons, fmt.Sprintf("%.0fs before symptom (×%.2f)", gap, tf))
	df := 1.0 / (1.0 + float64(c.Distance)*0.4)
	s *= df
	c.Reasons = append(c.Reasons, fmt.Sprintf("%d hops away (×%.2f)", c.Distance, df))
	if c.AffectedServices > 0 {
		impactFactor := 1 + math.Min(0.25, float64(c.AffectedServices)*0.05)
		s *= impactFactor
		c.Reasons = append(c.Reasons, fmt.Sprintf("%d affected service(s) (×%.2f)", c.AffectedServices, impactFactor))
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
