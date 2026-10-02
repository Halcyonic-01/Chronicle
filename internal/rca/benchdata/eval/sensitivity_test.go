package eval

// Sensitivity of the benchmark to each tunable.
//
// The goal is robustness, not a better score: nothing here picks the value that
// scores highest. Each parameter is varied around its shipped value and the
// spread of every metric is classified by a rule fixed BEFORE the first run:
//
//	STABLE            every metric moves by at most 1 incident across the range
//	SENSITIVE         the largest movement is 2 to 4 incidents
//	HIGHLY SENSITIVE  the largest movement is 5 or more incidents
//	NON-MONOTONIC     (flag) a count rises and then falls across the ordered
//	                  range, by 2 or more, so no direction is "safe"
//
// Only the development and held-out sets are used. The independent set is never
// touched here.
//
// Run: RCA_SENSITIVITY=1 go test ./internal/rca/benchdata/eval -run TestSensitivity -v -count=1

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

type point struct {
	Label string
	M     Metrics
}

type param struct {
	Name   string
	Group  string
	Shown  string // the tested range, for the table
	Values []string
	Apply  func(i int, c *rca.Config, rules []heal.Rule)
}

func durs(vs ...time.Duration) []string {
	var out []string
	for _, v := range vs {
		out = append(out, v.String())
	}
	return out
}

func floats(vs ...float64) []string {
	var out []string
	for _, v := range vs {
		out = append(out, fmt.Sprintf("%g", v))
	}
	return out
}

func params() []param {
	f := func(name, group string, vals []float64, set func(c *rca.Config, v float64)) param {
		return param{Name: name, Group: group, Values: floats(vals...), Apply: func(i int, c *rca.Config, _ []heal.Rule) { set(c, vals[i]) }}
	}
	d := func(name, group string, vals []time.Duration, set func(c *rca.Config, v time.Duration)) param {
		return param{Name: name, Group: group, Values: durs(vals...), Apply: func(i int, c *rca.Config, _ []heal.Rule) { set(c, vals[i]) }}
	}
	var ps []param
	ps = append(ps,
		f("DecayDivisor", "scoring", []float64{1.5, 2, 3, 4, 6}, func(c *rca.Config, v float64) { c.DecayDivisor = v }),
		f("DistanceSlope", "scoring", []float64{0.2, 0.3, 0.4, 0.6, 0.8}, func(c *rca.Config, v float64) { c.DistanceSlope = v }),
		f("PropagationFactor", "scoring", []float64{0.1, 0.25, 0.5, 1}, func(c *rca.Config, v float64) { c.PropagationFactor = v }),
		f("RemediationFactor", "scoring", []float64{0.15, 0.35, 0.6, 1}, func(c *rca.Config, v float64) { c.RemediationFactor = v }),
		f("BlastMax", "scoring", []float64{0, 0.25, 0.5}, func(c *rca.Config, v float64) { c.BlastMax = v }),
		f("BlastSaturation", "scoring", []float64{5, 10, 20}, func(c *rca.Config, v float64) { c.BlastSaturation = v }),
		f("DefaultWeight", "scoring", []float64{0.1, 0.2, 0.3}, func(c *rca.Config, v float64) { c.DefaultWeight = v }),
		f("InconclusiveBelow (confidence)", "thresholds", []float64{0.3, 0.4, 0.5, 0.6, 0.7}, func(c *rca.Config, v float64) { c.InconclusiveBelow = v }),
		f("AmbiguityRatio (separation)", "thresholds", []float64{0.6, 0.7, 0.8, 0.9, 0.95}, func(c *rca.Config, v float64) { c.AmbiguityRatio = v }),
		d("LogEpisodeGap", "timing", []time.Duration{45 * time.Second, 90 * time.Second, 3 * time.Minute, 5 * time.Minute}, func(c *rca.Config, v time.Duration) { c.LogEpisodeGap = v }),
		d("EpisodeGap", "timing", []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour}, func(c *rca.Config, v time.Duration) { c.EpisodeGap = v }),
		d("RestartGap", "timing", []time.Duration{3 * time.Minute, 6 * time.Minute, 12 * time.Minute}, func(c *rca.Config, v time.Duration) { c.RestartGap = v }),
		d("FlapWindow", "timing", []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute}, func(c *rca.Config, v time.Duration) { c.FlapWindow = v }),
		d("SettleAfterChange", "timing", []time.Duration{5 * time.Second, 20 * time.Second, time.Minute, 2 * time.Minute}, func(c *rca.Config, v time.Duration) { c.SettleAfterChange = v }),
		d("MaxEpisode", "timing", []time.Duration{time.Hour, 2 * time.Hour, 4 * time.Hour}, func(c *rca.Config, v time.Duration) { c.MaxEpisode = v }),
	)
	ps = append(ps,
		f("Lookback scale (all windows)", "timing", []float64{0.5, 0.75, 1, 1.5, 2}, func(c *rca.Config, v float64) {
			for k, w := range c.Lookback {
				c.Lookback[k] = time.Duration(float64(w) * v)
			}
			c.DefaultLookback = time.Duration(float64(c.DefaultLookback) * v)
		}),
		f("Propagation-horizon scale", "timing", []float64{0.5, 1, 2, 4}, func(c *rca.Config, v float64) {
			for k, w := range c.PropagationHorizon {
				c.PropagationHorizon[k] = time.Duration(float64(w) * v)
			}
			c.DefaultHorizon = time.Duration(float64(c.DefaultHorizon) * v)
		}),
	)
	for _, w := range []struct {
		typ  string
		vals []float64
	}{
		{"deploy", []float64{0.6, 0.8, 1.0}}, {"config_change", []float64{0.55, 0.75, 0.95}}, {"resource_change", []float64{0.5, 0.7, 0.9}},
		{"scale", []float64{0.55, 0.75, 0.95}}, {"oom_kill", []float64{0.45, 0.65, 0.85}}, {"container_restart", []float64{0.5, 0.7, 0.9}},
		{"became_unready", []float64{0.3, 0.5, 0.7}}, {"service_change", []float64{0.5, 0.7, 0.9}}, {"node_not_ready", []float64{0.45, 0.65, 0.85}},
		{"hpa_change", []float64{0.5, 0.7, 0.9}},
	} {
		w := w
		ps = append(ps, f("weight["+w.typ+"]", "weights", w.vals, func(c *rca.Config, v float64) { c.TypeWeights[w.typ] = v }))
	}
	rule := func(name string, vals []float64) param {
		return param{Name: "heal floor: " + name, Group: "healing floors", Values: floats(vals...), Apply: func(i int, _ *rca.Config, rules []heal.Rule) {
			for j := range rules {
				if rules[j].Name == name {
					rules[j].MinConfidence = vals[i]
				}
			}
		}}
	}
	ps = append(ps,
		rule("rollback-bad-deploy", []float64{0.70, 0.75, 0.80, 0.85, 0.90, 0.95}),
		rule("bump-memory-on-oom", []float64{0.70, 0.75, 0.80, 0.85, 0.90, 0.95}),
		rule("restart-deadlocked-pod", []float64{0.60, 0.70, 0.80, 0.90, 0.95}),
		rule("restore-scaled-down-workload", []float64{0.50, 0.55, 0.65, 0.75, 0.85}),
	)
	return ps
}

// vector is what the sensitivity tracks, as counts so spreads are in incidents.
type vector struct {
	Top1, Top3, Declared, FalseConfident, Coverage, WrongHeal, Unsafe, Ambiguous, OverAmbiguous int
	ConfCorrect                                                                                 float64
}

func vectorOf(m Metrics) vector {
	v := vector{m.Top1, m.Top3, m.DeclaredNoRoot, m.FalseConfident, m.Correct, m.Wrong, m.Unsafe, m.AmbiguityRecognised, m.FoundButAmbiguous, 0}
	if m.NCorrect > 0 {
		v.ConfCorrect = m.ConfCorrect / float64(m.NCorrect)
	}
	return v
}

func spread(vs []int) (lo, hi int) {
	lo, hi = vs[0], vs[0]
	for _, v := range vs {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	return
}

// nonMonotonic: the series rises and falls, each by at least 2.
func nonMonotonic(vs []int) bool {
	up, down := 0, 0
	for i := 1; i < len(vs); i++ {
		switch d := vs[i] - vs[i-1]; {
		case d >= 2:
			up++
		case d <= -2:
			down++
		}
	}
	return up > 0 && down > 0
}

// stabilityOf returns the verdict for a set of count series, the metric with the
// largest movement, and whether any series is non-monotonic.
func stabilityOf(series map[string][]int) (verdict, driver string, flat bool) {
	worst := 0
	names := make([]string, 0, len(series))
	for n := range series {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		lo, hi := spread(series[n])
		if hi-lo > worst {
			worst, driver = hi-lo, n
		}
		if nonMonotonic(series[n]) {
			flat = true
		}
	}
	switch {
	case worst <= 1:
		return "STABLE", "", flat
	case worst <= 4:
		return "SENSITIVE", driver, flat
	default:
		return "HIGHLY SENSITIVE", driver, flat
	}
}

// classifyStability applies the pre-declared rule to all metrics (overall), and
// also reports the ranking/verdict metrics and the healing metrics separately.
// Healing coverage is excluded from a healing-floor parameter's verdict, because
// moving a floor is meant to move coverage.
func classifyStability(group string, vs []vector) (overall, overallDriver, ranking, healing string, flat bool) {
	col := func(f func(vector) int) []int {
		out := make([]int, len(vs))
		for i, v := range vs {
			out[i] = f(v)
		}
		return out
	}
	rank := map[string][]int{
		"Top-1": col(func(v vector) int { return v.Top1 }), "Top-3": col(func(v vector) int { return v.Top3 }),
		"no-root declared": col(func(v vector) int { return v.Declared }), "false-confident": col(func(v vector) int { return v.FalseConfident }),
		"ambiguity recognised": col(func(v vector) int { return v.Ambiguous }), "over-ambiguous": col(func(v vector) int { return v.OverAmbiguous }),
	}
	heals := map[string][]int{"wrong heal actions": col(func(v vector) int { return v.WrongHeal }), "unsafe actions": col(func(v vector) int { return v.Unsafe })}
	if group != "healing floors" {
		heals["heal coverage"] = col(func(v vector) int { return v.Coverage })
	}
	all := map[string][]int{}
	for k, v := range rank {
		all[k] = v
	}
	for k, v := range heals {
		all[k] = v
	}
	overall, overallDriver, flat = stabilityOf(all)
	ranking, _, _ = stabilityOf(rank)
	healing, _, _ = stabilityOf(heals)
	return
}

func TestSensitivity(t *testing.T) {
	if os.Getenv("RCA_SENSITIVITY") == "" {
		t.Skip("set RCA_SENSITIVITY=1 to run the sensitivity analysis")
	}
	cases := append(Development("improved"), HeldOut("improved")...)
	defaultRules := heal.NewEngine(audit{}).Rules
	run := func(c *rca.Config, rules []heal.Rule) Metrics {
		rows, err := RunAll(cases, Options{Config: c, Rules: rules})
		if err != nil {
			t.Fatal(err)
		}
		return Summarize(rows, false)
	}
	copyRules := func() []heal.Rule { return append([]heal.Rule(nil), defaultRules...) }
	base := run(rca.DefaultConfig(), copyRules())
	bv := vectorOf(base)

	var sb strings.Builder
	fmt.Fprintf(&sb, "\nSENSITIVITY on the development + held-out sets (%d incidents), shipped values marked *.\n", base.N)
	fmt.Fprintf(&sb, "shipped: Top-1 %d  Top-3 %d  no-root declared %d  false-confident %d  heal coverage %d wrong %d unsafe %d  ambiguity recognised %d  over-ambiguous %d\n\n",
		bv.Top1, bv.Top3, bv.Declared, bv.FalseConfident, bv.Coverage, bv.WrongHeal, bv.Unsafe, bv.Ambiguous, bv.OverAmbiguous)
	fmt.Fprintf(&sb, "%-34s %-26s %-9s %-9s %-9s %-9s %-10s %-9s %-9s %s\n", "parameter", "tested", "Top-1", "Top-3", "no-root", "falseConf", "heal cov", "healWrong", "ambiguity", "verdict")
	counts := map[string]int{}
	out := map[string]any{}
	var rows []string
	for _, p := range params() {
		var vs []vector
		labels := make([]string, len(p.Values))
		for i := range p.Values {
			cfg := rca.DefaultConfig()
			rules := copyRules()
			p.Apply(i, cfg, rules)
			vs = append(vs, vectorOf(run(cfg, rules)))
			labels[i] = p.Values[i]
		}
		class, driver, rankClass, healClass, flat := classifyStability(p.Group, vs)
		rng := func(f func(vector) int) string {
			s := make([]int, len(vs))
			for i, v := range vs {
				s[i] = f(v)
			}
			lo, hi := spread(s)
			if lo == hi {
				return fmt.Sprintf("%d", lo)
			}
			return fmt.Sprintf("%d..%d", lo, hi)
		}
		mark := fmt.Sprintf("%-16s ranking:%-16s healing:%-16s", class, rankClass, healClass)
		if driver != "" {
			mark += " driver:" + driver
		}
		if flat {
			mark += " NON-MONOTONIC"
		}
		counts[class]++
		rows = append(rows, fmt.Sprintf("%-34s %-26s %-9s %-9s %-9s %-9s %-10s %-9s %-9s %s", p.Name, strings.Join(labels, ","),
			rng(func(v vector) int { return v.Top1 }), rng(func(v vector) int { return v.Top3 }), rng(func(v vector) int { return v.Declared }),
			rng(func(v vector) int { return v.FalseConfident }), rng(func(v vector) int { return v.Coverage }), rng(func(v vector) int { return v.WrongHeal }),
			rng(func(v vector) int { return v.Ambiguous }), mark))
		out[p.Name] = map[string]any{"values": labels, "vectors": vs, "overall": class, "ranking": rankClass, "healing": healClass, "driver": driver, "non_monotonic": flat}
	}
	sort.SliceStable(rows, func(i, j int) bool { return false })
	sb.WriteString(strings.Join(rows, "\n"))
	fmt.Fprintf(&sb, "\n\nclassification counts: %v\n", counts)
	t.Log(sb.String())
	if path := os.Getenv("RCA_SENSITIVITY_OUT"); path != "" {
		raw, _ := json.MarshalIndent(map[string]any{"shipped": bv, "parameters": out}, "", "  ")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
