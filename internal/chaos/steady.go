package chaos

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/Halcyonic-01/Chronicle/internal/collect"
)

// Checker answers whether the system is in its steady state.
type Checker interface {
	Check(ctx context.Context) (ok bool, why string)
}

// Steady is the hypothesis every experiment starts from and must return to:
// every workload fully rolled out and ready, and no service over the
// thresholds Chronicle itself alerts on.
type Steady struct {
	Cluster *Cluster
	// Prom is optional; without it only the workloads are checked.
	Prom v1.API
}

// alertTypes Chronicle raises from metrics that say a service is unhealthy.
var steadyRules = map[string]bool{"error_spike": true, "latency_spike": true}

// checkTimeout bounds one health check: a connection broken by a laptop sleep
// otherwise hangs the run forever (seen live).
const checkTimeout = 15 * time.Second

func (s *Steady) Check(ctx context.Context) (bool, string) {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	ok, why, err := s.Cluster.Rolled(ctx)
	if err != nil {
		return false, err.Error()
	}
	if !ok {
		return false, why
	}
	if s.Prom == nil {
		return true, ""
	}
	for _, rule := range collect.AlertRules() {
		if !steadyRules[rule.EventType] {
			continue
		}
		value, _, err := s.Prom.Query(ctx, rule.Query, time.Now())
		if err != nil {
			return false, fmt.Sprintf("prometheus: %v", err)
		}
		vec, _ := value.(model.Vector)
		var over []string
		for _, sample := range vec {
			v := float64(sample.Value)
			if !math.IsNaN(v) && v > rule.Threshold {
				over = append(over, fmt.Sprintf("%s=%.3f", sample.Metric["service"], v))
			}
		}
		if len(over) > 0 {
			sort.Strings(over)
			return false, fmt.Sprintf("%s above %.2f: %v", rule.Name, rule.Threshold, over)
		}
	}
	return true, ""
}

// waitSteady polls until the check holds for `hold` in a row, or timeout.
func waitSteady(ctx context.Context, check Checker, timeout, every, hold time.Duration, now func() time.Time, pause func(context.Context, time.Duration) error) (bool, string) {
	deadline := now().Add(timeout)
	var since time.Time
	why := ""
	for {
		ok, reason := check.Check(ctx)
		switch {
		case ok && since.IsZero():
			since = now()
		case !ok:
			since, why = time.Time{}, reason
		}
		if ok && now().Sub(since) >= hold {
			return true, ""
		}
		if !now().Before(deadline) {
			if why == "" {
				why = "not stable for long enough"
			}
			return false, why
		}
		if err := pause(ctx, every); err != nil {
			return false, err.Error()
		}
	}
}
