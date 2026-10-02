package eval

// The independent benchmark: the final-evaluation set.
//
// This test asserts nothing about accuracy, on purpose: a pass threshold would
// become a target. It reports. Every run records a hash of the RCA sources it ran
// against, so a result is always tied to exactly one version of the code.
//
// Do not tune RCA against these results. A bug found here is documented, and any
// rerun after a fix is a "post-fix independent benchmark", not an untouched one.
//
// Run: RCA_INDEPENDENT=1 go test ./internal/rca/benchdata/eval -run TestIndependentBenchmark -v -count=1
// Env: RCA_INDEPENDENT_OUT=path.json writes the per-incident rows.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// sourceHash identifies the RCA implementation under test: the non-test files of
// internal/rca and the healing rules and outcome logic.
func sourceHash(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..", "..", "..", "..")
	files, _ := filepath.Glob(filepath.Join(root, "internal", "rca", "*.go"))
	var names []string
	for _, f := range files {
		if !strings.HasSuffix(f, "_test.go") {
			names = append(names, f)
		}
	}
	names = append(names, filepath.Join(root, "internal", "heal", "action.go"), filepath.Join(root, "internal", "heal", "outcome.go"))
	sort.Strings(names)
	var manifest strings.Builder
	for _, f := range names {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		rel, _ := filepath.Rel(root, f)
		fmt.Fprintf(&manifest, "%s  %s\n", hex.EncodeToString(sum[:]), rel)
	}
	total := sha256.Sum256([]byte(manifest.String()))
	return hex.EncodeToString(total[:])[:16]
}

func TestIndependentBenchmark(t *testing.T) {
	if os.Getenv("RCA_INDEPENDENT") == "" {
		t.Skip("set RCA_INDEPENDENT=1 to run the final-evaluation set; do not tune RCA against it")
	}
	rows, err := RunAll(Independent(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	hash := sourceHash(t)
	var sb strings.Builder
	fmt.Fprintf(&sb, "\nINDEPENDENT BENCHMARK   rca source hash %s\n", hash)

	cats := map[string][]Row{}
	for _, r := range rows {
		cats[r.Group] = append(cats[r.Group], r)
	}
	names := make([]string, 0, len(cats))
	for c := range cats {
		names = append(names, c)
	}
	sort.Strings(names)

	sb.WriteString("\n" + Summarize(rows, true).Format("HEADLINE: incidents inside the analyzer's documented contract"))
	sb.WriteString("\n" + Summarize(rows, false).Format("ALL incidents, including those outside the documented contract"))
	var outside []Row
	for _, r := range rows {
		if !r.InScope {
			outside = append(outside, r)
		}
	}
	sb.WriteString("\n" + Summarize(outside, false).Format(fmt.Sprintf("OUTSIDE the documented contract only (%d incidents: cause older than the lookback, same-instant cause, event ingested too late)", len(outside))))

	for _, c := range names {
		sb.WriteString("\n" + Summarize(cats[c], false).Format("category "+c))
	}

	// Ambiguous incidents, one by one.
	var ambAvg float64
	var ambN int
	var correctAvg float64
	var correctN int
	for _, r := range rows {
		if r.Expected == Ambiguous {
			ambAvg += r.Confidence
			ambN++
		}
		if r.Expected == RootCause && r.Top1 && r.Verdict == "root_cause" {
			correctAvg += r.Confidence
			correctN++
		}
	}
	sb.WriteString("\nAMBIGUOUS incidents (plausible roots; recognised; rivals named; confidence; explanation):\n")
	for _, r := range rows {
		if r.Expected != Ambiguous {
			continue
		}
		fmt.Fprintf(&sb, "  %-52s verdict=%-10s outcome=%-26s conf=%.2f alternatives=%v doubtInText=%v\n", r.Name, r.Verdict, r.Outcome, r.Confidence, r.Alternatives, r.NarrativeCommunicatesDoubt)
	}
	if ambN > 0 && correctN > 0 {
		fmt.Fprintf(&sb, "  mean confidence: ambiguous %.3f versus confident single-cause answers %.3f\n", ambAvg/float64(ambN), correctAvg/float64(correctN))
	}

	// Misses, listed and not explained away.
	sb.WriteString("\nEVERYTHING THAT DID NOT GO AS EXPECTED (documented, not fixed):\n")
	n := 0
	for _, r := range rows {
		ok := r.Outcome == "correct" || r.Outcome == "declared-no-root-cause" || r.Outcome == "ambiguity-recognised"
		if ok {
			continue
		}
		n++
		scope := ""
		if !r.InScope {
			scope = " [outside contract]"
		}
		fmt.Fprintf(&sb, "  %-54s expected=%-13s got verdict=%-12s outcome=%-26s conf=%.2f actual=%v top=%v%s\n", r.Name, r.Expected, r.Verdict, r.Outcome, r.Confidence, r.ActualRoots, firstN(r.Top, 3), scope)
	}
	fmt.Fprintf(&sb, "  (%d of %d incidents)\n", n, len(rows))
	t.Log(sb.String())

	if path := os.Getenv("RCA_INDEPENDENT_OUT"); path != "" {
		raw, _ := json.MarshalIndent(map[string]any{"rca_source_hash": hash, "rows": rows}, "", "  ")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func firstN(v []string, n int) []string {
	if len(v) > n {
		return v[:n]
	}
	return v
}
