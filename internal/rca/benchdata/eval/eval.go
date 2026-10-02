// Package eval runs benchmark cases through the real RCA analyzer and the real
// (dry-run) heal engine and scores the results. It is shared by the development,
// held-out and independent benchmarks and by the sensitivity analysis, so they
// all measure the same way.
//
// Nothing here executes a remedy: the heal engine runs in dry-run and only
// decides what it WOULD do. Outcomes that follow are never simulated here.
package eval

import (
	"context"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata/independent"
)

// Expected results of a case.
const (
	RootCause   = independent.RootCause
	Ambiguous   = independent.Ambiguous
	NoRootCause = independent.NoRootCause
)

// ConfidentAt is the confidence at or above which a root-cause verdict counts as
// a confident diagnosis. Fixed here so "false confident" means the same
// everywhere; it is a reporting threshold, not an RCA input.
const ConfidentAt = 0.7

// Root matches a candidate by event type and entity name.
type Root struct{ Type, Entity string }

// Case is one benchmark incident in a common shape.
type Case struct {
	Name, Group string
	Events      []event.Event
	Edges       []graph.Edge
	Symptom     event.Event
	Actual      []Root // the real cause(s); empty when the cause is not recorded
	Plausible   []Root // every cause the evidence cannot rule out
	Ambiguous   bool
	AllRequired bool
	Expected    string
	InScope     bool
}

// FromIndependent adapts an independent incident.
func FromIndependent(inc independent.Incident) Case {
	c := Case{Name: inc.ID, Group: inc.Category, Events: inc.Events, Edges: inc.Edges, Symptom: inc.Symptom,
		Ambiguous: inc.Ambiguous, AllRequired: inc.AllRequired, Expected: inc.Expected, InScope: inc.InScope}
	for _, r := range inc.ActualRoots {
		c.Actual = append(c.Actual, Root{r.Kind, r.Entity})
	}
	for _, r := range inc.Plausible {
		c.Plausible = append(c.Plausible, Root{r.Kind, r.Entity})
	}
	return c
}

// FromDevelopment adapts a development or held-out incident. Its truths are the
// acceptable causes; when it is flagged ambiguous they are all plausible.
func FromDevelopment(inc benchdata.Incident, profile string) Case {
	edges := inc.Edges
	if edges == nil {
		edges = benchdata.Edges()
	}
	c := Case{Name: inc.Name, Group: inc.Group, Events: benchdata.Filter(profile, inc.Events), Edges: edges, Symptom: inc.Symptom,
		Ambiguous: inc.Ambiguous, AllRequired: inc.AllRequired, InScope: true, Expected: RootCause}
	for _, t := range inc.Truths {
		c.Actual = append(c.Actual, Root{t.Type, t.Entity})
		c.Plausible = append(c.Plausible, Root{t.Type, t.Entity})
	}
	switch {
	case len(inc.Truths) == 0:
		c.Expected = NoRootCause
	case inc.Ambiguous:
		c.Expected = Ambiguous
	}
	return c
}

// Options varies how a case is run.
type Options struct {
	Config *rca.Config
	Rules  []heal.Rule // overrides the healing rules (and their floors) when set
}

// Row is everything recorded about one incident.
type Row struct {
	Name, Group, Expected string
	InScope               bool
	ActualRoots           []string
	Verdict               string
	Top                   []string // type:entity, best first
	Alternatives          []string
	Confidence            float64
	Contested             bool

	Top1, Top3, AllPlausibleFound bool
	Outcome                       string // see classify
	FalseConfident                bool
	NarrativeCommunicatesDoubt    bool
	Narrative                     string

	// Healing, dry-run.
	Rule, ProposedAction   string
	WouldRun               bool
	HasRemedy              bool // some actual root has an action among the engine's
	ActionMatchesRoot      bool // the proposed action addresses an actual root
	ActionMatchesPlausible bool
	ActionSafe             bool // would-run and addresses an actual root of an unambiguous, observable incident
	ActionInappropriate    bool // would-run and addresses no plausible root
	SkippedInsufficient    bool // nothing would run
	ShouldSkip             bool // no single safe action exists: ambiguous, unobserved, or no remedy
}

// remedyFor is the action that actually addresses each kind of cause. A kind not
// listed has no remedy among the engine's actions. It is a judgement, written
// down here so the evaluation can be argued with.
var remedyFor = map[string]string{
	"scale":           heal.ActionRestoreReplicas,
	"deploy":          heal.ActionRollbackDeployment,
	"oom_kill":        heal.ActionBumpMemory,
	"resource_change": heal.ActionBumpMemory,
	"became_unready":  heal.ActionRestartPod,
}

type audit struct{}

func (audit) RecordAction(context.Context, *heal.Action) error               { return nil }
func (audit) CountRuleSince(context.Context, string, time.Time) (int, error) { return 0, nil }
func (audit) HasActionForIncident(context.Context, string) (bool, error)     { return false, nil }

func has(set []Root, c rca.Candidate) bool {
	for _, r := range set {
		if c.Event.Type == r.Type && c.Event.EntityName == r.Entity {
			return true
		}
	}
	return false
}

// Run analyses one case and scores it.
func Run(c Case, opt Options) (Row, error) {
	analyzer := &rca.Analyzer{
		Events:        &benchdata.Store{All: c.Events},
		Graph:         benchdata.Graph{EdgeList: c.Edges},
		MaxHops:       8, // production setting
		GraphInterval: 30 * time.Second,
		Config:        opt.Config,
	}
	res, err := analyzer.Analyze(context.Background(), c.Symptom)
	if err != nil {
		return Row{}, err
	}
	row := Row{Name: c.Name, Group: c.Group, Expected: c.Expected, InScope: c.InScope, Verdict: res.Verdict,
		Confidence: res.Confidence, Contested: res.Contested, Narrative: res.Narrative}
	for _, r := range c.Actual {
		row.ActualRoots = append(row.ActualRoots, r.Type+":"+r.Entity)
	}
	for _, cand := range res.Candidates {
		row.Top = append(row.Top, cand.Event.Type+":"+cand.Event.EntityName)
	}
	for _, a := range res.Alternatives {
		row.Alternatives = append(row.Alternatives, a.Event.Type+":"+a.Event.EntityName)
	}

	// Ranking.
	for i, cand := range res.Candidates {
		if has(c.Plausible, cand) {
			if i == 0 {
				row.Top1 = true
			}
			if i < 3 {
				row.Top3 = true
			}
		}
	}
	row.AllPlausibleFound = len(c.Plausible) > 0
	for _, p := range c.Plausible {
		found := false
		for i, cand := range res.Candidates {
			if i < 3 && cand.Event.Type == p.Type && cand.Event.EntityName == p.Entity {
				found = true
			}
		}
		row.AllPlausibleFound = row.AllPlausibleFound && found
	}
	row.Outcome, row.FalseConfident = classify(c, row)
	row.NarrativeCommunicatesDoubt = communicatesDoubt(c, row)

	// Healing, dry-run only.
	rules := opt.Rules
	engine := heal.NewEngine(audit{})
	if rules != nil {
		engine.Rules = rules
	}
	if action, err := engine.Evaluate(context.Background(), res); err == nil && action != nil {
		row.Rule, row.ProposedAction = action.Rule, action.ActionType
		row.WouldRun = action.Status == heal.StatusWouldRun
	}
	// Which rule the leader matches with the floors lifted, to show what a floor
	// is withholding.
	open := heal.NewEngine(audit{})
	for i := range open.Rules {
		open.Rules[i].MinConfidence, open.Rules[i].MaxPerHour = 0, 1000
	}
	if probe, err := open.Evaluate(context.Background(), res); err == nil && probe != nil && row.Rule == "" {
		row.Rule = probe.Rule
	}
	for _, r := range c.Actual {
		if remedyFor[r.Type] != "" {
			row.HasRemedy = true
		}
	}
	row.ShouldSkip = c.Expected != RootCause || !row.HasRemedy
	if row.WouldRun {
		for _, r := range c.Actual {
			if remedyFor[r.Type] == row.ProposedAction {
				row.ActionMatchesRoot = true
			}
		}
		for _, r := range c.Plausible {
			if remedyFor[r.Type] == row.ProposedAction {
				row.ActionMatchesPlausible = true
			}
		}
		row.ActionInappropriate = !row.ActionMatchesPlausible
		row.ActionSafe = row.ActionMatchesRoot && !row.ShouldSkip
	} else {
		row.SkippedInsufficient = true
	}
	return row, nil
}

// classify names what happened and whether it was a false confident diagnosis.
func classify(c Case, r Row) (outcome string, falseConfident bool) {
	confident := r.Verdict == rca.VerdictRootCause && r.Confidence >= ConfidentAt
	switch c.Expected {
	case NoRootCause:
		if r.Verdict == rca.VerdictNoRootCause {
			return "declared-no-root-cause", false
		}
		return "invented-a-cause", confident
	case Ambiguous:
		switch {
		case r.Verdict == rca.VerdictAmbiguous && r.AllPlausibleFound:
			return "ambiguity-recognised", false
		case r.Verdict == rca.VerdictAmbiguous:
			return "ambiguous-but-rival-missing", false
		case r.Verdict == rca.VerdictNoRootCause:
			return "missed", false
		default:
			return "asserted-one-cause", confident
		}
	default:
		switch {
		case r.Verdict == rca.VerdictNoRootCause:
			return "missed", false
		case r.Verdict == rca.VerdictAmbiguous && r.Top3:
			return "found-but-called-ambiguous", false
		case r.Top1:
			return "correct", false
		default:
			return "wrong-root", confident
		}
	}
}

// communicatesDoubt: for an ambiguous incident, does the explanation say the
// evidence cannot separate the causes AND name every plausible cause?
func communicatesDoubt(c Case, r Row) bool {
	if c.Expected != Ambiguous {
		return false
	}
	text := strings.ToLower(r.Narrative)
	if !strings.Contains(text, "cannot separate") && !strings.Contains(text, "ambiguous") {
		return false
	}
	for _, p := range c.Plausible {
		if !strings.Contains(text, strings.ToLower(p.Type)) || !strings.Contains(text, strings.ToLower(p.Entity)) {
			return false
		}
	}
	return true
}

// RunAll runs every case.
func RunAll(cases []Case, opt Options) ([]Row, error) {
	rows := make([]Row, 0, len(cases))
	for _, c := range cases {
		r, err := Run(c, opt)
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return rows, nil
}

// Development, held-out and independent case lists.
func Development(profile string) []Case {
	var out []Case
	for _, inc := range benchdata.Incidents() {
		out = append(out, FromDevelopment(inc, profile))
	}
	return out
}

func HeldOut(profile string) []Case {
	var out []Case
	for _, inc := range benchdata.HoldoutIncidents() {
		out = append(out, FromDevelopment(inc, profile))
	}
	return out
}

func Independent() []Case {
	var out []Case
	for _, inc := range independent.All() {
		out = append(out, FromIndependent(inc))
	}
	return out
}
