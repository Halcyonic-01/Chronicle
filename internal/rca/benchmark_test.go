package rca

// RCA accuracy benchmark. See internal/rca/benchdata for the incidents and for
// what the numbers do and do not mean.
//
// Run:   go test ./internal/rca -run TestRCABenchmark -v -count=1
// Env:   RCA_BENCH_PROFILE=baseline|improved  which events the collectors emit
//        RCA_BENCH_OUT=path.json              write the per-incident results

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

// declaredNoCause is how an analyzer says "no root cause". The original
// analyzer can only say it by returning nothing; a later one may say it with an
// explicit verdict (see benchmark_verdict_test.go).
var declaredNoCause = func(r *Result) bool { return len(r.Candidates) == 0 }

type benchResult struct {
	Name, Group, Fault string
	Truths             []string
	Top                []string // type:entity, best first
	Confidence         float64
	Candidates         int
	Top1, Top3         bool
	AllFound           bool
	Unknown            bool
	NoCandidates       bool // zero candidates: the console's "NO REACHABLE CAUSE"
	DeclaredNoCause    bool // the analyzer's own declaration that no root cause was found
	Abstained          bool // no candidate, or confidence below 50%
	Ambiguous          bool
	AllRequired        bool
}

func matches(c Candidate, t benchdata.TruthRef) bool {
	return c.Event.Type == t.Type && c.Event.EntityName == t.Entity
}

func incidentEdges(inc benchdata.Incident) []graph.Edge {
	if inc.Edges != nil {
		return inc.Edges
	}
	return benchdata.Edges()
}

func benchAnalyzer(stream []event.Event, edges []graph.Edge) *Analyzer {
	return &Analyzer{
		Events:        &benchdata.Store{All: stream},
		Graph:         benchdata.Graph{EdgeList: edges},
		MaxHops:       8, // production setting
		GraphInterval: 30 * time.Second,
	}
}

func runBenchmark(t *testing.T, profile string) []benchResult {
	return runIncidents(t, profile, benchdata.Incidents(), nil)
}

// runIncidents analyses each incident; tune, when set, adjusts the analyzer
// first (the sensitivity analysis uses it to vary the tunables).
func runIncidents(t *testing.T, profile string, incidents []benchdata.Incident, tune func(*Analyzer)) []benchResult {
	t.Helper()
	var results []benchResult
	for _, inc := range incidents {
		analyzer := benchAnalyzer(benchdata.Filter(profile, inc.Events), incidentEdges(inc))
		if tune != nil {
			tune(analyzer)
		}
		got, err := analyzer.Analyze(context.Background(), inc.Symptom)
		if err != nil {
			t.Fatalf("%s: %v", inc.Name, err)
		}
		r := benchResult{
			Name: inc.Name, Group: inc.Group, Fault: inc.Fault, Confidence: got.Confidence,
			Candidates: len(got.Candidates), Unknown: len(inc.Truths) == 0,
			Ambiguous: inc.Ambiguous, AllRequired: inc.AllRequired,
			NoCandidates:    len(got.Candidates) == 0,
			DeclaredNoCause: declaredNoCause(got),
			Abstained:       len(got.Candidates) == 0 || got.Confidence < 0.5,
		}
		for _, tr := range inc.Truths {
			r.Truths = append(r.Truths, tr.Type+":"+tr.Entity)
		}
		found := make([]bool, len(inc.Truths))
		for i, c := range got.Candidates {
			r.Top = append(r.Top, c.Event.Type+":"+c.Event.EntityName)
			for j, tr := range inc.Truths {
				if matches(c, tr) {
					if i == 0 {
						r.Top1 = true
					}
					if i < 3 {
						r.Top3 = true
						found[j] = true
					}
				}
			}
		}
		r.AllFound = len(inc.Truths) > 0
		for _, f := range found {
			r.AllFound = r.AllFound && f
		}
		results = append(results, r)
	}
	return results
}

type groupStats struct{ N, Top1, Top3, AllFound int }

type benchSummary struct {
	Profile                   string
	Incidents, Known, Unknown int
	Top1N, Top3N              int
	Top1Pct, Top3Pct          float64
	UnknownStrictN            int // zero candidates
	UnknownDeclaredN          int // the analyzer declared no root cause
	UnknownAbstainN           int
	UnknownStrictPct          float64
	UnknownDeclaredPct        float64
	UnknownAbstainPct         float64
	NCorrect, NWrong          int
	ConfCorrect, ConfWrong    float64
	ConfUnknownGuess          float64
	UnknownWithCandidates     int
	MultiCause, MultiCauseAll int
	ByGroup                   map[string]groupStats
	Failed                    []string
}

func pct(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return 100 * float64(n) / float64(d)
}

func firstN(v []string, n int) []string {
	if len(v) > n {
		return v[:n]
	}
	return v
}

func summarize(profile string, results []benchResult) benchSummary {
	s := benchSummary{Profile: profile, Incidents: len(results), ByGroup: map[string]groupStats{}}
	var sumC, sumW, sumU float64
	for _, r := range results {
		if r.Unknown {
			s.Unknown++
			if r.NoCandidates {
				s.UnknownStrictN++
			} else {
				s.UnknownWithCandidates++
				sumU += r.Confidence
			}
			if r.DeclaredNoCause {
				s.UnknownDeclaredN++
			}
			if r.Abstained {
				s.UnknownAbstainN++
			}
			continue
		}
		s.Known++
		g := s.ByGroup[r.Group]
		g.N++
		if r.Top1 {
			s.Top1N++
			g.Top1++
			s.NCorrect++
			sumC += r.Confidence
		} else {
			s.NWrong++
			sumW += r.Confidence
			s.Failed = append(s.Failed, fmt.Sprintf("%-44s truth=%v got=%v conf=%.2f", r.Name, r.Truths, firstN(r.Top, 3), r.Confidence))
		}
		if r.Top3 {
			s.Top3N++
			g.Top3++
		}
		if r.AllRequired {
			s.MultiCause++
			if r.AllFound {
				s.MultiCauseAll++
			}
		}
		if r.AllFound {
			g.AllFound++
		}
		s.ByGroup[r.Group] = g
	}
	s.Top1Pct, s.Top3Pct = pct(s.Top1N, s.Known), pct(s.Top3N, s.Known)
	s.UnknownStrictPct = pct(s.UnknownStrictN, s.Unknown)
	s.UnknownDeclaredPct = pct(s.UnknownDeclaredN, s.Unknown)
	s.UnknownAbstainPct = pct(s.UnknownAbstainN, s.Unknown)
	if s.NCorrect > 0 {
		s.ConfCorrect = sumC / float64(s.NCorrect)
	}
	if s.NWrong > 0 {
		s.ConfWrong = sumW / float64(s.NWrong)
	}
	if s.UnknownWithCandidates > 0 {
		s.ConfUnknownGuess = sumU / float64(s.UnknownWithCandidates)
	}
	return s
}

func printSummary(t *testing.T, s benchSummary) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\nRCA BENCHMARK  profile=%s\n", s.Profile)
	fmt.Fprintf(&sb, "  incidents: %d  (known cause %d, unknown cause %d)\n", s.Incidents, s.Known, s.Unknown)
	fmt.Fprintf(&sb, "  Top-1 accuracy : %d/%d = %.1f%%\n", s.Top1N, s.Known, s.Top1Pct)
	fmt.Fprintf(&sb, "  Top-3 accuracy : %d/%d = %.1f%%\n", s.Top3N, s.Known, s.Top3Pct)
	fmt.Fprintf(&sb, "  Unknown cause, strict (zero candidates)            : %d/%d = %.1f%%\n", s.UnknownStrictN, s.Unknown, s.UnknownStrictPct)
	fmt.Fprintf(&sb, "  Unknown cause, declared (analyzer says no cause)   : %d/%d = %.1f%%\n", s.UnknownDeclaredN, s.Unknown, s.UnknownDeclaredPct)
	fmt.Fprintf(&sb, "  Unknown cause, abstained (none or confidence<50%%)  : %d/%d = %.1f%%\n", s.UnknownAbstainN, s.Unknown, s.UnknownAbstainPct)
	fmt.Fprintf(&sb, "  Avg confidence, top-1 correct   (n=%d): %.3f\n", s.NCorrect, s.ConfCorrect)
	fmt.Fprintf(&sb, "  Avg confidence, top-1 incorrect (n=%d): %.3f\n", s.NWrong, s.ConfWrong)
	fmt.Fprintf(&sb, "  Avg confidence on unknown incidents that still produced a candidate (n=%d): %.3f\n", s.UnknownWithCandidates, s.ConfUnknownGuess)
	if s.MultiCause > 0 {
		fmt.Fprintf(&sb, "  Multi-cause incidents with every cause in top-3: %d/%d\n", s.MultiCauseAll, s.MultiCause)
	}
	groups := make([]string, 0, len(s.ByGroup))
	for g := range s.ByGroup {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	fmt.Fprintf(&sb, "  by group:\n")
	for _, g := range groups {
		gs := s.ByGroup[g]
		fmt.Fprintf(&sb, "    %-17s n=%-3d top1=%-3d top3=%-3d\n", g, gs.N, gs.Top1, gs.Top3)
	}
	if len(s.Failed) > 0 {
		fmt.Fprintf(&sb, "  top-1 misses:\n")
		for _, f := range s.Failed {
			fmt.Fprintf(&sb, "    %s\n", f)
		}
	}
	t.Log(sb.String())
}

func benchProfile() string {
	if p := os.Getenv("RCA_BENCH_PROFILE"); p != "" {
		return p
	}
	return "improved"
}

func TestRCABenchmark(t *testing.T) {
	profile := benchProfile()
	results := runBenchmark(t, profile)
	summary := summarize(profile, results)
	printSummary(t, summary)
	if out := os.Getenv("RCA_BENCH_OUT"); out != "" {
		raw, _ := json.MarshalIndent(map[string]any{"summary": summary, "incidents": results}, "", "  ")
		if err := os.WriteFile(out, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A benchmark miss must be the analyzer's fault, never the fixture's: every
// injected truth has to exist, precede the symptom and be reachable from it.
func TestBenchmarkFixturesAreSound(t *testing.T) {
	ids := map[string]bool{}
	for _, inc := range append(benchdata.Incidents(), benchdata.HoldoutIncidents()...) {
		gr := graph.New()
		gr.SetEdges(incidentEdges(inc))
		upstream := gr.Upstream(key(inc.Symptom), 8)
		for _, e := range inc.Events {
			if ids[e.ID] {
				t.Fatalf("%s: duplicate event id %s", inc.Name, e.ID)
			}
			ids[e.ID] = true
		}
		for _, tr := range inc.Truths {
			var truth *event.Event // the first occurrence is when the cause began
			for i, e := range inc.Events {
				if e.Type == tr.Type && e.EntityName == tr.Entity && (truth == nil || e.OccurredAt.Before(truth.OccurredAt)) {
					truth = &inc.Events[i]
				}
			}
			if truth == nil {
				t.Errorf("%s: truth %s:%s is not in the event stream", inc.Name, tr.Type, tr.Entity)
				continue
			}
			if !truth.OccurredAt.Before(inc.Symptom.OccurredAt) {
				t.Errorf("%s: truth %s does not precede the symptom", inc.Name, tr.Type)
			}
			if _, ok := upstream[key(*truth)]; !ok {
				t.Errorf("%s: truth entity %s is not upstream of the symptom %s", inc.Name, key(*truth), key(inc.Symptom))
			}
		}
		if len(inc.Truths) == 0 {
			for _, e := range inc.Events {
				if _, onPath := upstream[key(e)]; onPath && changeTypes[e.Type] && e.OccurredAt.After(benchdata.T0.Add(-300*time.Second)) {
					t.Errorf("%s: an unknown-cause incident carries a recent upstream change: %s:%s", inc.Name, e.Type, e.EntityName)
				}
			}
		}
		if inc.Symptom.ID == "" {
			t.Errorf("%s: no symptom", inc.Name)
		}
	}
}

func TestBenchmarkIsDeterministic(t *testing.T) {
	a, b := runBenchmark(t, "improved"), runBenchmark(t, "improved")
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatal("two runs of the benchmark disagree")
	}
}

// TestRCAHoldout runs the held-out incidents, which were not used to design or
// tune any rule. Run it with RCA_BENCH_PROFILE like the main benchmark.
func TestRCAHoldout(t *testing.T) {
	profile := benchProfile()
	results := runIncidents(t, profile, benchdata.HoldoutIncidents(), nil)
	summary := summarize(profile, results)
	summary.Profile = profile + " (held-out set)"
	printSummary(t, summary)
	if out := os.Getenv("RCA_HOLDOUT_OUT"); out != "" {
		raw, _ := json.MarshalIndent(map[string]any{"summary": summary, "incidents": results}, "", "  ")
		if err := os.WriteFile(out, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A regression floor, not a target. The floors sit just under what the
// benchmark measured when they were set (main 100% top-1/top-3 and 15/15
// unknown-cause verdicts; held-out 98.1% top-1, 100% top-3 and 12/12), so a
// change that makes the analyzer worse on either set fails the suite.
func TestRCABenchmarkRegressionFloor(t *testing.T) {
	main := summarize("improved", runBenchmark(t, "improved"))
	hold := summarize("improved", runIncidents(t, "improved", benchdata.HoldoutIncidents(), nil))
	check := func(label string, got, floor float64) {
		if got < floor {
			t.Errorf("%s fell to %.1f%%, below the %.1f%% floor", label, got, floor)
		}
	}
	check("main top-1", main.Top1Pct, 95)
	check("main top-3", main.Top3Pct, 97)
	check("main unknown-cause verdicts", main.UnknownDeclaredPct, 90)
	check("held-out top-1", hold.Top1Pct, 90)
	check("held-out top-3", hold.Top3Pct, 97)
	check("held-out unknown-cause verdicts", hold.UnknownDeclaredPct, 90)
}
