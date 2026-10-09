package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata/eval"
)

// Config tunes a run. The defaults suit the kind setup: metric alerts fire
// within one or two Prometheus polls, and an analysis settles once the 60s lateness allowance has passed.
type Config struct {
	DetectTimeout time.Duration // how long Chronicle has to raise a signal
	Settle        time.Duration // wait after the symptom so late events and the graph settle
	SteadyTimeout time.Duration // longest the system may take to recover
	// Cooldown is how long the steady state must have held before any fault
	// goes in, the first included. Chronicle treats a recovery shorter than its
	// FlapWindow (60s) as a flap, so a fault sooner after the last one merges the
	// two incidents and is blamed on the first one's cause.
	Cooldown      time.Duration
	Poll          time.Duration // how often to look
	ExtraSymptoms int           // other signals analysed per experiment, for consistency
}

func DefaultConfig() Config {
	return Config{DetectTimeout: 4 * time.Minute, Settle: 65 * time.Second, SteadyTimeout: 6 * time.Minute,
		Cooldown: 2 * time.Minute, Poll: 5 * time.Second, ExtraSymptoms: 4}
}

// Statuses of an experiment.
const (
	StatusScored      = "scored"       // detected and analysed
	StatusNotDetected = "not-detected" // it broke the system and Chronicle raised nothing
	StatusNoEffect    = "no-effect"    // the steady state held: nothing to diagnose
	StatusSkipped     = "skipped"      // a precondition refused it
	StatusError       = "error"        // the harness could not carry it out
)

// Planned is one run of one experiment.
type Planned struct {
	Experiment Experiment
	Trial      int
}

// Plan orders trials at random, seeded so a run can be repeated: nobody picks
// the order, so nobody picks which incident follows which.
func Plan(exps []Experiment, trials int, seed int64) []Planned {
	var out []Planned
	for t := 1; t <= trials; t++ {
		for _, e := range exps {
			out = append(out, Planned{Experiment: e, Trial: t})
		}
	}
	rand.New(rand.NewSource(seed)).Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// RunInfo heads a run's log.
type RunInfo struct {
	Seed          int64             `json:"seed"`
	Order         []string          `json:"order"`
	Config        map[string]string `json:"config"`
	Context       string            `json:"kube_context"`
	Namespace     string            `json:"namespace"`
	API           string            `json:"chronicle_api"`
	Prometheus    string            `json:"prometheus,omitempty"`
	Commit        string            `json:"commit,omitempty"`
	RCASourceHash string            `json:"rca_source_hash,omitempty"`
}

// Record is one line of a run's log. A "start" record is written before its
// fault is injected, so the expectation is fixed before the answer exists.
type Record struct {
	Kind string    `json:"kind"` // run | start | result
	At   time.Time `json:"at"`

	Run *RunInfo `json:"run,omitempty"`

	Experiment string   `json:"experiment,omitempty"`
	Category   string   `json:"category,omitempty"`
	Trial      int      `json:"trial,omitempty"`
	Hypothesis string   `json:"hypothesis,omitempty"`
	Truth      []Root   `json:"truth,omitempty"`
	Expected   string   `json:"expected,omitempty"`
	Actions    []string `json:"actions,omitempty"`

	Result *Outcome `json:"result,omitempty"`
}

// Symptom is the signal an analysis was asked about.
type Symptom struct {
	ID     string    `json:"id"`
	Type   string    `json:"type"`
	Entity string    `json:"entity"`
	Title  string    `json:"title"`
	At     time.Time `json:"at"`
}

// Analysis is Chronicle's answer for one symptom, scored.
type Analysis struct {
	Symptom      Symptom  `json:"symptom"`
	Verdict      string   `json:"verdict"`
	Confidence   float64  `json:"confidence"`
	Top          []string `json:"top"`
	Alternatives []string `json:"alternatives,omitempty"`
	Provisional  bool     `json:"provisional"`
	Outcome      string   `json:"outcome"`
}

// Outcome is everything measured about one experiment.
type Outcome struct {
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	InjectedAt time.Time `json:"injected_at"`

	Signals       int     `json:"signals"`
	DetectedAfter float64 `json:"detected_after_seconds,omitempty"` // first signal
	AlertAfter    float64 `json:"alert_after_seconds,omitempty"`    // the symptom analysed, by its own timestamp
	// AlertSeenAfter is when that symptom could first be read through the API,
	// which is what an operator sees; ingestion lag is the difference.
	AlertSeenAfter float64 `json:"alert_seen_after_seconds,omitempty"`
	AnalysedAfter  float64 `json:"analysed_after_seconds,omitempty"`

	Headline     *Analysis  `json:"headline,omitempty"`
	Narrative    string     `json:"narrative,omitempty"`
	Row          *eval.Row  `json:"row,omitempty"`
	ServerAction string     `json:"server_action,omitempty"`
	Others       []Analysis `json:"other_symptoms,omitempty"`
	Consistency  float64    `json:"consistency,omitempty"` // share of analysed symptoms answered well

	Restored       []string `json:"restored,omitempty"`
	RestoreError   string   `json:"restore_error,omitempty"`
	Recovered      bool     `json:"recovered"`
	RecoveredAfter float64  `json:"recovered_after_seconds,omitempty"`

	PreviousExperiment string  `json:"previous_experiment,omitempty"`
	PreviousEndedAgo   float64 `json:"previous_ended_seconds_ago,omitempty"`
}

// Runner carries out a plan against a live cluster and a live Chronicle.
type Runner struct {
	Cluster   *Cluster
	Chronicle *Chronicle
	Steady    Checker
	Config    Config
	Out       io.Writer // the JSONL log
	Log       io.Writer // progress for a person
	Now       func() time.Time
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Runner) sleep(ctx context.Context, d time.Duration) error { return r.Cluster.Sleep(ctx, d) }

func (r *Runner) logf(format string, args ...any) {
	if r.Log != nil {
		fmt.Fprintf(r.Log, "%s  %s\n", r.now().UTC().Format("15:04:05"), fmt.Sprintf(format, args...))
	}
}

func (r *Runner) write(rec Record) error {
	rec.At = r.now().UTC()
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := r.Out.Write(append(line, '\n')); err != nil {
		return err
	}
	if s, ok := r.Out.(interface{ Sync() error }); ok {
		return s.Sync()
	}
	return nil
}

// Run carries out the plan. It stops early, after restoring, when the context
// ends or when the system fails to return to its steady state.
func (r *Runner) Run(ctx context.Context, plan []Planned, info RunInfo) error {
	if err := r.write(Record{Kind: "run", Run: &info}); err != nil {
		return err
	}
	var prev string
	var prevEnd time.Time
	for i, p := range plan {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.logf("[%d/%d] %s (trial %d): %s", i+1, len(plan), p.Experiment.Name, p.Trial, p.Experiment.Hypothesis)
		out, abort := r.runOne(ctx, p)
		if prev != "" && !out.InjectedAt.IsZero() {
			out.PreviousExperiment, out.PreviousEndedAgo = prev, out.InjectedAt.Sub(prevEnd).Seconds()
		}
		if err := r.write(Record{Kind: "result", Experiment: p.Experiment.Name, Category: p.Experiment.Category, Trial: p.Trial, Result: &out}); err != nil {
			return err
		}
		r.logf("    %s", describeOutcome(out))
		if abort != nil {
			return abort
		}
		prev, prevEnd = p.Experiment.Name, r.now()
	}
	return nil
}

func describeOutcome(o Outcome) string {
	switch {
	case o.Status == StatusScored && o.Row != nil:
		return fmt.Sprintf("%s: %s, verdict %s at %.2f (alert after %.0fs, readable after %.0fs), recovered=%v", o.Status, o.Row.Outcome, o.Row.Verdict, o.Row.Confidence, o.AlertAfter, o.AlertSeenAfter, o.Recovered)
	default:
		return fmt.Sprintf("%s: %s (recovered=%v)", o.Status, o.Reason, o.Recovered)
	}
}

// runOne runs one experiment. Whatever happens after the snapshot, the cluster
// is restored; abort is set when the run must not continue.
func (r *Runner) runOne(ctx context.Context, p Planned) (out Outcome, abort error) {
	exp, c := p.Experiment, r.Cluster
	if exp.Requires != nil {
		if err := exp.Requires(ctx, c); err != nil {
			return Outcome{Status: StatusSkipped, Reason: err.Error(), Recovered: true}, nil
		}
	}
	quiet := r.Config.Cooldown
	if quiet < 15*time.Second {
		quiet = 15 * time.Second
	}
	r.logf("    waiting for %s of steady state", quiet)
	if ok, why := waitSteady(ctx, r.Steady, r.Config.SteadyTimeout+quiet, r.Config.Poll, quiet, r.now, r.sleep); !ok {
		return Outcome{Status: StatusError, Reason: "not in its steady state before injecting: " + why},
			fmt.Errorf("the system is not in its steady state (%s); not injecting anything", why)
	}
	snap, err := c.Snapshot(ctx)
	if err != nil {
		return Outcome{Status: StatusError, Reason: "snapshot: " + err.Error(), Recovered: true}, err
	}
	truth, err := exp.Truth(ctx, c)
	if err != nil {
		return Outcome{Status: StatusError, Reason: "resolve the truth: " + err.Error(), Recovered: true}, nil
	}
	if err := r.write(Record{Kind: "start", Experiment: exp.Name, Category: exp.Category, Trial: p.Trial, Hypothesis: exp.Hypothesis,
		Truth: truth, Expected: Expected(truth), Actions: describeActions(exp)}); err != nil {
		return Outcome{Status: StatusError, Reason: err.Error(), Recovered: true}, err
	}

	defer func() {
		if err := r.restore(&out, exp, snap); err != nil {
			abort = err
		}
	}()
	for _, a := range exp.Setup {
		if err := a.Do(ctx, c); err != nil {
			out.Status, out.Reason = StatusError, "setup ("+a.Describe()+"): "+err.Error()
			return out, nil
		}
	}
	out.InjectedAt = r.now()
	r.logf("    injecting at %s", out.InjectedAt.UTC().Format(time.RFC3339))
	for _, a := range exp.Inject {
		if err := a.Do(ctx, c); err != nil {
			out.Status, out.Reason = StatusError, "inject ("+a.Describe()+"): "+err.Error()
			return out, nil
		}
	}
	r.observe(ctx, &out, exp, truth)
	return out, nil
}

func describeActions(exp Experiment) []string {
	var out []string
	for _, group := range []struct {
		name string
		as   []Action
	}{{"setup", exp.Setup}, {"inject", exp.Inject}, {"restore", exp.Restore}} {
		for _, a := range group.as {
			out = append(out, group.name+": "+a.Describe())
		}
	}
	return append(out, "restore: every Deployment and Service spec from the snapshot; remove chaos NetworkPolicies; unpause nodes")
}

// restore undoes the experiment on a fresh context, so an interrupted run
// still puts everything back, then waits for the steady state to return.
func (r *Runner) restore(out *Outcome, exp Experiment, snap *Snapshot) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.Config.SteadyTimeout+3*time.Minute)
	defer cancel()
	var problems []string
	for i := len(exp.Restore) - 1; i >= 0; i-- {
		if err := exp.Restore[i].Do(ctx, r.Cluster); err != nil {
			problems = append(problems, exp.Restore[i].Describe()+": "+err.Error())
		}
	}
	restored, err := r.Cluster.Restore(ctx, snap)
	out.Restored = restored
	if err != nil {
		problems = append(problems, err.Error())
	}
	start := r.now()
	ok, why := waitSteady(ctx, r.Steady, r.Config.SteadyTimeout, r.Config.Poll, 15*time.Second, r.now, r.sleep)
	out.Recovered, out.RecoveredAfter = ok, r.now().Sub(start).Seconds()
	if !ok {
		problems = append(problems, "did not return to its steady state: "+why)
	}
	if len(problems) > 0 {
		out.RestoreError = fmt.Sprint(problems)
	}
	if !ok {
		return fmt.Errorf("stopping: %s did not recover (%s); inspect the cluster before running again", exp.Name, why)
	}
	return nil
}

func isAlert(s rca.Signal) bool { return s.Type == "error_spike" || s.Type == "latency_spike" }

// observe waits for Chronicle to raise a signal, picks the one an on-call
// engineer would be paged by (the first alert, else the first signal), lets the
// analysis settle, then asks for and scores it.
func (r *Runner) observe(ctx context.Context, out *Outcome, exp Experiment, truth []Root) {
	cfg := r.Config
	since := out.InjectedAt
	seen := map[string]rca.Signal{} // accumulated: the API caps rows per series
	readable := map[string]time.Time{}
	var headline *rca.Signal
	for {
		if all, err := r.Chronicle.Signals(ctx, since.Add(-5*time.Second), r.now().Add(time.Minute)); err == nil {
			for _, s := range all {
				if (s.Namespace == r.Cluster.Namespace || s.Namespace == "") && !s.IngestedAt.Before(since.Add(-2*time.Second)) {
					seen[s.ID] = s
					if _, ok := readable[s.ID]; !ok {
						readable[s.ID] = r.now()
					}
				}
			}
		} else {
			r.logf("    signals: %v", err)
		}
		signals := sortedSignals(seen)
		if a := earliest(signals, isAlert); a != nil {
			headline = a
			break
		}
		if !r.now().Before(since.Add(cfg.DetectTimeout)) {
			headline = earliest(signals, func(rca.Signal) bool { return true })
			break
		}
		if err := r.sleep(ctx, cfg.Poll); err != nil {
			out.Status, out.Reason = StatusError, "interrupted while waiting for a signal: "+err.Error()
			return
		}
	}
	signals := sortedSignals(seen)
	out.Signals = len(signals)
	if len(signals) > 0 {
		out.DetectedAfter = signals[0].IngestedAt.Sub(since).Seconds()
	}
	if headline == nil {
		// Nothing raised: did the fault disturb the system at all?
		if ok, _ := r.Steady.Check(ctx); ok {
			out.Status, out.Reason = StatusNoEffect, "the steady state held: the fault did not disturb the system"
		} else {
			_, why := r.Steady.Check(ctx)
			out.Status, out.Reason = StatusNotDetected, "the system left its steady state ("+why+") but Chronicle raised no signal"
		}
		return
	}
	out.AlertAfter = headline.IngestedAt.Sub(since).Seconds()
	out.AlertSeenAfter = readable[headline.ID].Sub(since).Seconds()
	if wait := headline.IngestedAt.Add(cfg.Settle).Sub(r.now()); wait > 0 {
		if err := r.sleep(ctx, wait); err != nil {
			out.Status, out.Reason = StatusError, "interrupted while the analysis settled: "+err.Error()
			return
		}
	}
	c := caseFor(exp, truth)
	analysis, res, action, err := r.analyse(ctx, c, *headline)
	if err != nil {
		out.Status, out.Reason = StatusError, "analyse: "+err.Error()
		return
	}
	row := eval.Score(c, res, eval.Options{})
	out.Status, out.Headline, out.Row, out.Narrative = StatusScored, &analysis, &row, res.Narrative
	out.AnalysedAfter = r.now().Sub(since).Seconds()
	if action != nil {
		out.ServerAction = action.Status + ": " + action.Result
	}
	good := 0
	if goodOutcome(row.Outcome) {
		good++
	}
	// The same incident seen through other signals, one per series, settled ones only.
	settled := r.now().Add(-cfg.Settle)
	for _, s := range otherSeries(signals, *headline, settled, cfg.ExtraSymptoms) {
		a, _, _, err := r.analyse(ctx, c, s)
		if err != nil {
			continue
		}
		out.Others = append(out.Others, a)
		if goodOutcome(a.Outcome) {
			good++
		}
	}
	out.Consistency = float64(good) / float64(1+len(out.Others))
}

func (r *Runner) analyse(ctx context.Context, c eval.Case, s rca.Signal) (Analysis, *rca.Result, *heal.Action, error) {
	res, action, err := r.Chronicle.Analyze(ctx, s.ID)
	if err != nil {
		return Analysis{}, nil, nil, err
	}
	row := eval.Score(c, res, eval.Options{})
	a := Analysis{Symptom: symptomOf(s), Verdict: res.Verdict, Confidence: res.Confidence, Provisional: res.Provisional, Outcome: row.Outcome, Top: row.Top, Alternatives: row.Alternatives}
	if len(a.Top) > 3 {
		a.Top = a.Top[:3]
	}
	return a, res, action, nil
}

func symptomOf(s rca.Signal) Symptom {
	return Symptom{ID: s.ID, Type: s.Type, Entity: s.EntityName, Title: s.Title, At: s.IngestedAt}
}

// caseFor is the experiment's truth in the shape the benchmarks score.
func caseFor(exp Experiment, truth []Root) eval.Case {
	c := eval.Case{Name: exp.Name, Group: exp.Category, Expected: Expected(truth), InScope: true}
	for _, t := range truth {
		c.Actual = append(c.Actual, eval.Root{Type: t.Type, Entity: t.Entity})
		c.Plausible = append(c.Plausible, eval.Root{Type: t.Type, Entity: t.Entity})
	}
	return c
}

func goodOutcome(o string) bool {
	return o == "correct" || o == "declared-no-root-cause" || o == "ambiguity-recognised"
}

func sortedSignals(m map[string]rca.Signal) []rca.Signal {
	out := make([]rca.Signal, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].IngestedAt.Equal(out[j].IngestedAt) {
			return out[i].IngestedAt.Before(out[j].IngestedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func earliest(sorted []rca.Signal, keep func(rca.Signal) bool) *rca.Signal {
	for i := range sorted {
		if keep(sorted[i]) {
			s := sorted[i]
			return &s
		}
	}
	return nil
}

// otherSeries picks the first signal of each other series that has settled.
func otherSeries(sorted []rca.Signal, headline rca.Signal, settled time.Time, limit int) []rca.Signal {
	series := func(s rca.Signal) string { return s.Type + "|" + s.Namespace + "|" + s.EntityKind + "|" + s.EntityName }
	taken := map[string]bool{series(headline): true}
	var out []rca.Signal
	for _, s := range sorted {
		if len(out) >= limit {
			break
		}
		if taken[series(s)] || s.IngestedAt.After(settled) {
			continue
		}
		taken[series(s)] = true
		out = append(out, s)
	}
	return out
}
