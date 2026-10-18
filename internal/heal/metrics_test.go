package heal

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/Halcyonic-01/Chronicle/internal/metrics"
)

func decisionSamples(t *testing.T) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.DecisionToExecution.(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestExecutionsAreCountedByOutcomeAndTimedFromTheApproval(t *testing.T) {
	succeeded := metrics.HealExecutions.WithLabelValues(ActionRestartPod, StatusSucceeded)
	blocked := metrics.HealExecutions.WithLabelValues(ActionRestartPod, StatusBlocked)
	s0, b0, n0 := testutil.ToFloat64(succeeded), testutil.ToFloat64(blocked), decisionSamples(t)

	decided := time.Now().Add(-45 * time.Second)
	a := approvedRestart("metrics-ok")
	a.DecidedAt = &decided
	if _, err := liveController(&executionMemory{}, &recordingExecutor{}).ExecuteApproved(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(succeeded) - s0; got != 1 {
		t.Fatalf("succeeded counted %v times", got)
	}
	if decisionSamples(t)-n0 != 1 {
		t.Fatal("the time from approval to execution was not recorded")
	}

	// A decision the gates refuse is counted as blocked and has no execution time.
	refused := approvedRestart("metrics-blocked")
	refused.DecidedAt = &decided
	refused.Confidence = 0.1
	if _, err := liveController(&executionMemory{}, &recordingExecutor{}).ExecuteApproved(context.Background(), refused); err == nil {
		t.Fatal("expected a refusal")
	}
	if got := testutil.ToFloat64(blocked) - b0; got != 1 {
		t.Fatalf("blocked counted %v times", got)
	}
	if decisionSamples(t)-n0 != 1 {
		t.Fatal("a blocked decision was timed as an execution")
	}
}
