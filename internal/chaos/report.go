package chaos

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata/eval"
)

// ReadRecords parses a run's JSONL log.
func ReadRecords(r io.Reader) ([]Record, error) {
	var out []Record
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var rec Record
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		out = append(out, rec)
	}
	return out, scanner.Err()
}

// Report summarises a run: the benchmarks' metrics over the incidents Chronicle
// detected, and what only a live run shows (detection, latency, consistency).
// Every experiment is listed, including the ones that were not scored.
func Report(records []Record) string {
	var sb strings.Builder
	var starts = map[string]Record{}
	var results []Record
	for _, rec := range records {
		switch rec.Kind {
		case "run":
			if rec.Run != nil {
				fmt.Fprintf(&sb, "CHAOS RUN  seed %d  context %s  namespace %s  commit %s  rca source hash %s\n",
					rec.Run.Seed, rec.Run.Context, rec.Run.Namespace, orDash(rec.Run.Commit), orDash(rec.Run.RCASourceHash))
				fmt.Fprintf(&sb, "  started %s, %d experiment runs planned\n", rec.At.Format("2006-01-02 15:04 MST"), len(rec.Run.Order))
			}
		case "start":
			starts[fmt.Sprintf("%s#%d", rec.Experiment, rec.Trial)] = rec
		case "result":
			results = append(results, rec)
		}
	}
	if len(results) == 0 {
		return sb.String() + "  no results yet\n"
	}

	status := map[string]int{}
	var rows []eval.Row
	var detect, alert, readable []float64
	var consistency float64
	byCategory := map[string][]eval.Row{}
	for _, rec := range results {
		o := rec.Result
		status[o.Status]++
		if o.Status != StatusScored || o.Row == nil {
			continue
		}
		rows = append(rows, *o.Row)
		byCategory[rec.Category] = append(byCategory[rec.Category], *o.Row)
		detect = append(detect, o.DetectedAfter)
		alert = append(alert, o.AlertAfter)
		readable = append(readable, o.AlertSeenAfter)
		consistency += o.Consistency
	}
	fmt.Fprintf(&sb, "\n  experiments %d: scored %d, not detected %d, no effect %d, skipped %d, error %d\n",
		len(results), status[StatusScored], status[StatusNotDetected], status[StatusNoEffect], status[StatusSkipped], status[StatusError])
	if len(rows) > 0 {
		fmt.Fprintf(&sb, "  first signal after the fault: median %.0fs (max %.0fs); paging symptom raised after: median %.0fs (max %.0fs), readable through the API after: median %.0fs (max %.0fs)\n",
			median(detect), maxOf(detect), median(alert), maxOf(alert), median(readable), maxOf(readable))
		fmt.Fprintf(&sb, "  mean consistency (other symptoms of the same incident answered well): %.0f%%\n", 100*consistency/float64(len(rows)))
		sb.WriteString("\n" + eval.Summarize(rows, false).Format("LIVE incidents Chronicle detected (scored like the benchmarks)"))
		cats := make([]string, 0, len(byCategory))
		for c := range byCategory {
			cats = append(cats, c)
		}
		sort.Strings(cats)
		for _, c := range cats {
			sb.WriteString("\n" + eval.Summarize(byCategory[c], false).Format("category "+c))
		}
	}

	sb.WriteString("\n  EVERY EXPERIMENT\n")
	fmt.Fprintf(&sb, "  %-26s %-3s %-13s %-14s %-24s %-44s %5s %6s %s\n", "experiment", "#", "expected", "status", "outcome", "verdict / top", "conf", "alert", "truth")
	for _, rec := range results {
		o := rec.Result
		start := starts[fmt.Sprintf("%s#%d", rec.Experiment, rec.Trial)]
		truth := "(none recorded)"
		if len(start.Truth) > 0 {
			var parts []string
			for _, t := range start.Truth {
				parts = append(parts, t.Type+":"+t.Entity)
			}
			truth = strings.Join(parts, ",")
		}
		outcome, verdict, conf, alertAt := "-", "-", "", ""
		if o.Row != nil {
			outcome = o.Row.Outcome
			verdict = o.Row.Verdict
			if len(o.Row.Top) > 0 {
				verdict += " / " + o.Row.Top[0]
			}
			conf = fmt.Sprintf("%.2f", o.Row.Confidence)
			alertAt = fmt.Sprintf("%.0fs", o.AlertAfter)
		} else if o.Reason != "" {
			outcome = trim(o.Reason, 24)
		}
		fmt.Fprintf(&sb, "  %-26s %-3d %-13s %-14s %-24s %-44s %5s %6s %s\n", rec.Experiment, rec.Trial, start.Expected, o.Status, trim(outcome, 24), trim(verdict, 44), conf, alertAt, truth)
		if !o.Recovered {
			fmt.Fprintf(&sb, "      NOT RECOVERED: %s\n", o.RestoreError)
		}
	}
	return sb.String()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func maxOf(v []float64) float64 {
	m := 0.0
	for _, x := range v {
		if x > m {
			m = x
		}
	}
	return m
}
