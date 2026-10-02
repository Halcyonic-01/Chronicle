package rca

// Sensitivity of the benchmark to each tunable. It exists to answer one
// question honestly: does the benchmark justify changing a value, or is the
// value merely untested? A parameter whose neighbours score the same is not
// validated by this benchmark, only unchallenged by it.
//
// Run: RCA_SENSITIVITY=1 go test ./internal/rca -run TestRCASensitivity -v -count=1

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata"
)

type variation struct {
	name  string
	apply func(c *Config)
}

func sensitivityVariations() []variation {
	var v []variation
	add := func(name string, f func(c *Config)) { v = append(v, variation{name, f}) }
	for _, d := range []float64{1.5, 2, 4, 6} {
		d := d
		add(fmt.Sprintf("DecayDivisor=%.1f", d), func(c *Config) { c.DecayDivisor = d })
	}
	for _, d := range []float64{0.2, 0.3, 0.6, 0.8} {
		d := d
		add(fmt.Sprintf("DistanceSlope=%.1f", d), func(c *Config) { c.DistanceSlope = d })
	}
	for _, d := range []float64{0.1, 0.5, 1.0} {
		d := d
		add(fmt.Sprintf("PropagationFactor=%.2f", d), func(c *Config) { c.PropagationFactor = d })
	}
	for _, d := range []float64{0.15, 0.6, 1.0} {
		d := d
		add(fmt.Sprintf("RemediationFactor=%.2f", d), func(c *Config) { c.RemediationFactor = d })
	}
	for _, d := range []float64{0, 0.5} {
		d := d
		add(fmt.Sprintf("BlastMax=%.2f", d), func(c *Config) { c.BlastMax = d })
	}
	for _, d := range []float64{0.1, 0.3} {
		d := d
		add(fmt.Sprintf("DefaultWeight=%.1f", d), func(c *Config) { c.DefaultWeight = d })
	}
	for _, d := range []time.Duration{45 * time.Second, 180 * time.Second, 300 * time.Second} {
		d := d
		add("LogEpisodeGap="+d.String(), func(c *Config) { c.LogEpisodeGap = d })
	}
	for _, d := range []time.Duration{10 * time.Minute, 60 * time.Minute} {
		d := d
		add("EpisodeGap="+d.String(), func(c *Config) { c.EpisodeGap = d })
	}
	for _, d := range []time.Duration{30 * time.Second, 120 * time.Second} {
		d := d
		add("FlapWindow="+d.String(), func(c *Config) { c.FlapWindow = d })
	}
	for _, d := range []time.Duration{5 * time.Second, 60 * time.Second, 120 * time.Second} {
		d := d
		add("SettleAfterChange="+d.String(), func(c *Config) { c.SettleAfterChange = d })
	}
	for _, w := range []struct {
		typ string
		val float64
	}{
		{"deploy", 0.8}, {"config_change", 0.75}, {"resource_change", 0.6}, {"scale", 0.5}, {"scale", 0.9},
		{"oom_kill", 0.6}, {"container_restart", 0.4}, {"container_restart", 0.9}, {"became_unready", 0.3}, {"became_unready", 0.7},
		{"service_change", 0.5}, {"node_not_ready", 0.5}, {"hpa_change", 0.4}, {"hpa_change", 0.9},
	} {
		w := w
		add(fmt.Sprintf("weight[%s]=%.2f", w.typ, w.val), func(c *Config) { c.TypeWeights[w.typ] = w.val })
	}
	return v
}

func TestRCASensitivity(t *testing.T) {
	if os.Getenv("RCA_SENSITIVITY") == "" {
		t.Skip("set RCA_SENSITIVITY=1 to run the sensitivity analysis")
	}
	run := func(c *Config) (benchSummary, benchSummary) {
		tune := func(a *Analyzer) { a.Config = c }
		main := summarize("improved", runIncidents(t, "improved", benchdata.Incidents(), tune))
		hold := summarize("improved", runIncidents(t, "improved", benchdata.HoldoutIncidents(), tune))
		return main, hold
	}
	baseMain, baseHold := run(DefaultConfig())
	var sb strings.Builder
	fmt.Fprintf(&sb, "\nSENSITIVITY (improved events)   main: top1 top3 decl | held-out: top1 top3 decl\n")
	row := func(name string, m, h benchSummary) {
		flag := ""
		if m.Top1N != baseMain.Top1N || m.Top3N != baseMain.Top3N || h.Top1N != baseHold.Top1N || h.Top3N != baseHold.Top3N ||
			m.UnknownDeclaredN != baseMain.UnknownDeclaredN || h.UnknownDeclaredN != baseHold.UnknownDeclaredN {
			flag = "  <-- differs"
		}
		fmt.Fprintf(&sb, "  %-34s %2d/%d %2d/%d %2d/%d | %2d/%d %2d/%d %2d/%d%s\n", name,
			m.Top1N, m.Known, m.Top3N, m.Known, m.UnknownDeclaredN, m.Unknown,
			h.Top1N, h.Known, h.Top3N, h.Known, h.UnknownDeclaredN, h.Unknown, flag)
	}
	row("(shipped values)", baseMain, baseHold)
	for _, v := range sensitivityVariations() {
		c := DefaultConfig()
		v.apply(c)
		m, h := run(c)
		row(v.name, m, h)
	}
	t.Log(sb.String())
}
