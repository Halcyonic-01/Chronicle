package heal

// Healing benchmark, in DRY-RUN only: it evaluates what the engine would
// propose, never executes anything.
//
// For every controlled incident (see internal/rca/benchdata) it runs the real
// RCA analyzer, then the real heal engine, and compares the proposed action with
// the action that would actually be appropriate for the injected cause. That
// mapping is a judgement and is written down below.
//
// What it does NOT show: the "simulated outcome" is built from the same ground
// truth the incident was, so it exercises the outcome labeller; it is not
// evidence about how real remediations turn out. Confidence values here are
// ranking scores, not probabilities, and nothing in this file calibrates them
// against real outcomes.
//
// Run: go test ./internal/heal -run TestHealingBenchmark -v -count=1
// Env: RCA_BENCH_PROFILE=baseline|improved

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
	"github.com/tidwall/gjson"
)

// appropriateAction is the remedy that actually addresses each injected cause.
// A cause not listed has no remedy among the engine's actions: proposing any is
// a wrong action, whatever its confidence. Notably a node failure, a changed
// ConfigMap or Service and a crash loop are not fixed by restarting a pod.
var appropriateAction = map[string]string{
	"scale":           ActionRestoreReplicas,
	"deploy":          ActionRollbackDeployment,
	"oom_kill":        ActionBumpMemory,
	"resource_change": ActionBumpMemory,
	"became_unready":  ActionRestartPod,
}

type healRow struct {
	Name        string
	Truths      []string
	Unknown     bool
	Top1        bool
	Confidence  float64
	Rule        string // the rule the top candidate's type matches, ignoring floors
	Action      string
	Appropriate bool // the proposed action addresses a true cause
	WouldRun    bool // at the shipped floor
	Outcome     string
}

func benchProfile() string {
	if p := os.Getenv("RCA_BENCH_PROFILE"); p != "" {
		return p
	}
	return "improved"
}

func analyze(t *testing.T, profile string, inc benchdata.Incident) *rca.Result {
	t.Helper()
	edges := inc.Edges
	if edges == nil {
		edges = benchdata.Edges()
	}
	analyzer := &rca.Analyzer{
		Events:        &benchdata.Store{All: benchdata.Filter(profile, inc.Events)},
		Graph:         benchdata.Graph{EdgeList: edges},
		MaxHops:       8,
		GraphInterval: 30 * time.Second,
	}
	got, err := analyzer.Analyze(context.Background(), inc.Symptom)
	if err != nil {
		t.Fatalf("%s: %v", inc.Name, err)
	}
	return got
}

// simulatedFollowUps is what the cluster would show after a decision: if the
// proposed action is the right one it is carried out and the symptom recovers;
// if it is not, the symptom recovers only after somebody makes the real fix.
func simulatedFollowUps(a *Action, symptom string, appropriate bool) []event.Event {
	fix := func() event.Event {
		switch a.ActionType {
		case ActionRestoreReplicas:
			return ev("scale", a.Target, a.Target+" scaled from 0 to 1", `{"old_replicas":0,"new_replicas":1}`)
		case ActionRollbackDeployment:
			return ev("deploy", a.Target, a.Target+" rolled back", fmt.Sprintf(`{"new_image":%q}`, gjson.GetBytes(a.Payload, "old_image").String()))
		case ActionBumpMemory:
			return ev("resource_change", gjson.GetBytes(a.Payload, "owner").String(), a.Target+" memory raised", `{"old_mem_limit":1,"new_mem_limit":2}`)
		default:
			e := ev("resource_deleted", a.Target, a.Target+" deleted", `{}`)
			e.EntityKind = "Pod"
			return e
		}
	}
	recovery := ev("error_spike_resolved", symptom, "high_error_rate resolved", `{}`)
	if appropriate {
		return []event.Event{fix(), recovery}
	}
	return []event.Event{ev("config_change", symptom, "the real fix", `{}`), recovery}
}

func runHealing(t *testing.T, profile string, incidents []benchdata.Incident) []healRow {
	t.Helper()
	var rows []healRow
	for _, inc := range incidents {
		result := analyze(t, profile, inc)
		row := healRow{Name: inc.Name, Unknown: len(inc.Truths) == 0, Confidence: result.Confidence}
		for _, tr := range inc.Truths {
			row.Truths = append(row.Truths, tr.Type+":"+tr.Entity)
			if len(result.Candidates) > 0 && result.Candidates[0].Event.Type == tr.Type && result.Candidates[0].Event.EntityName == tr.Entity {
				row.Top1 = true
			}
		}

		// Which rule the leading candidate matches, with the floors lifted.
		open := NewEngine(&memoryAudit{})
		for i := range open.Rules {
			open.Rules[i].MinConfidence, open.Rules[i].MaxPerHour = 0, 1000
		}
		if probe, err := open.Evaluate(context.Background(), result); err == nil && probe != nil {
			row.Rule, row.Action = probe.Rule, probe.ActionType
		}
		// What the engine actually decides with the shipped floors.
		shipped := NewEngine(&memoryAudit{})
		var decided *Action
		if action, err := shipped.Evaluate(context.Background(), result); err == nil && action != nil {
			row.WouldRun = action.Status == StatusWouldRun
			decided = action
		}
		for _, tr := range inc.Truths {
			if row.Action != "" && appropriateAction[tr.Type] == row.Action {
				row.Appropriate = true
			}
		}
		if row.WouldRun {
			a := decided
			symptom := SymptomRef{Namespace: "default", Name: inc.Symptom.EntityName}
			row.Outcome, _ = classifyOutcome(a, symptom, simulatedFollowUps(a, symptom.Name, row.Appropriate))
		}
		rows = append(rows, row)
	}
	return rows
}

func report(t *testing.T, label string, rows []healRow) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\nHEALING BENCHMARK  %s  (dry-run; nothing executed)\n", label)

	var runs, runsRight, runsWrong, remediable, remediableRun int
	outcomes := map[string]int{}
	for _, r := range rows {
		if !r.Unknown && appropriateAction[strings.SplitN(r.Truths[0], ":", 2)[0]] != "" {
			remediable++
		}
		if r.WouldRun {
			runs++
			outcomes[r.Outcome]++
			if r.Appropriate {
				runsRight++
				remediableRun++
			} else {
				runsWrong++
			}
		}
	}
	fmt.Fprintf(&sb, "  incidents: %d   would-run decisions at the shipped floors: %d\n", len(rows), runs)
	fmt.Fprintf(&sb, "    appropriate action: %d   wrong action: %d   precision: %s\n", runsRight, runsWrong, ratio(runsRight, runs))
	fmt.Fprintf(&sb, "    incidents with a real remedy: %d   of those the engine would run the right one: %d (%s)\n", remediable, remediableRun, ratio(remediableRun, remediable))
	fmt.Fprintf(&sb, "    simulated outcome labels of the would-run decisions: %v\n", outcomes)
	if os.Getenv("RCA_BENCH_LIST") != "" {
		for _, r := range rows {
			if r.WouldRun {
				fmt.Fprintf(&sb, "    RUN %-46s %s conf=%.2f\n", r.Name, r.Action, r.Confidence)
			}
		}
	}
	if runsWrong > 0 {
		fmt.Fprintf(&sb, "    wrong actions:\n")
		for _, r := range rows {
			if r.WouldRun && !r.Appropriate {
				fmt.Fprintf(&sb, "      %-46s truth=%v proposed=%s conf=%.2f\n", r.Name, r.Truths, r.Action, r.Confidence)
			}
		}
	}

	// Per-rule precision as the floor moves. Every incident whose leading
	// candidate matches the rule counts, whatever its confidence.
	byRule := map[string][]healRow{}
	for _, r := range rows {
		if r.Rule != "" {
			byRule[r.Rule] = append(byRule[r.Rule], r)
		}
	}
	names := make([]string, 0, len(byRule))
	for n := range byRule {
		names = append(names, n)
	}
	sort.Strings(names)
	floors := []float64{0.5, 0.65, 0.8, 0.85, 0.9, 0.95}
	fmt.Fprintf(&sb, "  precision of each rule's proposals as its confidence floor moves (proposals n / precision):\n")
	fmt.Fprintf(&sb, "    %-30s shipped", "rule")
	for _, f := range floors {
		fmt.Fprintf(&sb, "  >=%.2f   ", f)
	}
	fmt.Fprintln(&sb)
	shipped := map[string]float64{}
	for _, r := range defaultRules {
		shipped[r.Name] = r.MinConfidence
	}
	for _, n := range names {
		fmt.Fprintf(&sb, "    %-30s %.2f   ", n, shipped[n])
		for _, f := range floors {
			var n1, ok int
			for _, r := range byRule[n] {
				if r.Confidence >= f {
					n1++
					if r.Appropriate {
						ok++
					}
				}
			}
			fmt.Fprintf(&sb, " %3d/%-6s", n1, ratio(ok, n1))
		}
		fmt.Fprintln(&sb)
	}

	// How often a confidence is right, across every incident that has a cause.
	buckets := []struct{ lo, hi float64 }{{0, .2}, {.2, .4}, {.4, .6}, {.6, .8}, {.8, 1.01}}
	fmt.Fprintf(&sb, "  calibration of confidence (top-1 correct among known-cause incidents):\n")
	for _, b := range buckets {
		var n, ok int
		var sum float64
		for _, r := range rows {
			if r.Unknown || r.Confidence < b.lo || r.Confidence >= b.hi {
				continue
			}
			n++
			sum += r.Confidence
			if r.Top1 {
				ok++
			}
		}
		if n > 0 {
			fmt.Fprintf(&sb, "    confidence %.1f-%.1f  n=%-3d  mean confidence %.2f  actually correct %s\n", b.lo, b.hi, n, sum/float64(n), ratio(ok, n))
		}
	}
	t.Log(sb.String())
}

func ratio(n, d int) string {
	if d == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.0f%%", 100*float64(n)/float64(d))
}

func TestHealingBenchmark(t *testing.T) {
	profile := benchProfile()
	incidents := append(benchdata.Incidents(), benchdata.HoldoutIncidents()...)
	rows := runHealing(t, profile, incidents)
	report(t, "profile="+profile+" (main + held-out incidents)", rows)

	// Dry-run is a property, not a hope.
	for _, r := range NewEngine(&memoryAudit{}).Rules {
		_ = r
	}
	if !NewEngine(&memoryAudit{}).DryRun {
		t.Fatal("the engine must default to dry-run")
	}
}
