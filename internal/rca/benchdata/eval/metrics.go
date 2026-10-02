package eval

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

// Metrics aggregates rows. Every ratio keeps its numerator and denominator.
type Metrics struct {
	N, NRoot, NAmbiguous, NNone int

	// Ranking, on single-cause incidents.
	Top1, Top3 int

	// Unknown cause.
	DeclaredNoRoot int // of NNone
	InventedCause  int // of NNone

	// Ambiguity.
	AmbiguityRecognised int // of NAmbiguous
	RivalMissing        int
	AssertedOneCause    int
	NarrativeDoubt      int // of NAmbiguous
	FoundButAmbiguous   int // single-cause incidents called ambiguous

	// Calibration of confidence.
	FalseConfident int // confident (>=ConfidentAt) root-cause verdicts that were wrong
	Confident      int // all confident root-cause verdicts
	ConfCorrect    float64
	NCorrect       int
	ConfWrong      float64
	NWrong         int
	ConfAmbiguous  float64
	NAmbiguousConf int

	// Healing, dry-run.
	WouldRun, Correct, Wrong, Unsafe   int
	CorrectlySkipped, MissedActionable int
	Remediable                         int

	Outcomes map[string]int
}

func Summarize(rows []Row, inScopeOnly bool) Metrics {
	m := Metrics{Outcomes: map[string]int{}}
	for _, r := range rows {
		if inScopeOnly && !r.InScope {
			continue
		}
		m.N++
		m.Outcomes[r.Outcome]++
		switch r.Expected {
		case NoRootCause:
			m.NNone++
			if r.Verdict == rca.VerdictNoRootCause {
				m.DeclaredNoRoot++
			} else {
				m.InventedCause++
			}
		case Ambiguous:
			m.NAmbiguous++
			switch r.Outcome {
			case "ambiguity-recognised":
				m.AmbiguityRecognised++
			case "ambiguous-but-rival-missing":
				m.RivalMissing++
			case "asserted-one-cause":
				m.AssertedOneCause++
			}
			if r.NarrativeCommunicatesDoubt {
				m.NarrativeDoubt++
			}
			m.ConfAmbiguous += r.Confidence
			m.NAmbiguousConf++
		default:
			m.NRoot++
			if r.Top1 {
				m.Top1++
				m.ConfCorrect += r.Confidence
				m.NCorrect++
			} else {
				m.ConfWrong += r.Confidence
				m.NWrong++
			}
			if r.Top3 {
				m.Top3++
			}
			if r.Outcome == "found-but-called-ambiguous" {
				m.FoundButAmbiguous++
			}
		}
		if r.Verdict == rca.VerdictRootCause && r.Confidence >= ConfidentAt {
			m.Confident++
		}
		if r.FalseConfident {
			m.FalseConfident++
		}
		// Healing.
		if r.WouldRun {
			m.WouldRun++
			if r.ActionSafe {
				m.Correct++
			}
			if r.ActionInappropriate {
				m.Wrong++
			}
			if r.ShouldSkip || r.ActionInappropriate {
				m.Unsafe++
			}
		}
		if !r.ShouldSkip {
			m.Remediable++
			if !r.WouldRun {
				m.MissedActionable++
			}
		}
		if !r.WouldRun && r.ShouldSkip {
			m.CorrectlySkipped++
		}
	}
	return m
}

func ratio(n, d int) string {
	if d == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d/%d = %.1f%%", n, d, 100*float64(n)/float64(d))
}

func mean(sum float64, n int) string {
	if n == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.3f", sum/float64(n))
}

// Format renders the metrics for a report.
func (m Metrics) Format(label string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s\n  incidents %d: single-cause %d, ambiguous %d, no-root-cause %d\n", label, m.N, m.NRoot, m.NAmbiguous, m.NNone)
	fmt.Fprintf(&sb, "  Top-1 (single-cause)            : %s\n", ratio(m.Top1, m.NRoot))
	fmt.Fprintf(&sb, "  Top-3 (single-cause)            : %s\n", ratio(m.Top3, m.NRoot))
	fmt.Fprintf(&sb, "  single-cause called ambiguous   : %d (cause still in the top 3 and named)\n", m.FoundButAmbiguous)
	fmt.Fprintf(&sb, "  no-root-cause declared          : %s   (invented a cause: %d)\n", ratio(m.DeclaredNoRoot, m.NNone), m.InventedCause)
	fmt.Fprintf(&sb, "  ambiguity recognised            : %s   (rival missing %d, asserted one cause %d)\n", ratio(m.AmbiguityRecognised, m.NAmbiguous), m.RivalMissing, m.AssertedOneCause)
	fmt.Fprintf(&sb, "  explanation communicates doubt  : %s\n", ratio(m.NarrativeDoubt, m.NAmbiguous))
	fmt.Fprintf(&sb, "  false confident diagnoses (>=%.1f): %d of %d confident root-cause verdicts\n", ConfidentAt, m.FalseConfident, m.Confident)
	fmt.Fprintf(&sb, "  avg confidence: correct %s (n=%d) | wrong %s (n=%d) | ambiguous %s (n=%d)\n",
		mean(m.ConfCorrect, m.NCorrect), m.NCorrect, mean(m.ConfWrong, m.NWrong), m.NWrong, mean(m.ConfAmbiguous, m.NAmbiguousConf), m.NAmbiguousConf)
	fmt.Fprintf(&sb, "  healing (dry-run): would-run %d | correct actionable %d | wrong %d | unsafe %d (unsafe rate %s)\n",
		m.WouldRun, m.Correct, m.Wrong, m.Unsafe, ratio(m.Unsafe, m.WouldRun))
	fmt.Fprintf(&sb, "                      coverage %s | missed actionable %d | correctly skipped uncertain %d\n",
		ratio(m.Correct, m.Remediable), m.MissedActionable, m.CorrectlySkipped)
	keys := make([]string, 0, len(m.Outcomes))
	for k := range m.Outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m.Outcomes[k]))
	}
	fmt.Fprintf(&sb, "  outcomes: %s\n", strings.Join(parts, ", "))
	return sb.String()
}
