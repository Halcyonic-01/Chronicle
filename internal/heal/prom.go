package heal

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/Halcyonic-01/Chronicle/internal/collect"
)

// PromProbe re-reads the metric behind a symptom with the same expression and
// threshold the collector alerts on (collect.AlertRules), so "recovered" means
// exactly "no longer firing".
type PromProbe struct{ api v1.API }

func NewPromProbe(client api.Client) *PromProbe { return &PromProbe{api: v1.NewAPI(client)} }

func (p *PromProbe) Read(ctx context.Context, symptomType, namespace, entity string) (Reading, error) {
	var rule *collect.Rule
	for _, r := range collect.AlertRules() {
		if r.EventType == symptomType {
			r := r
			rule = &r
			break
		}
	}
	if rule == nil {
		return Reading{}, ErrNotMetric
	}
	result, _, err := p.api.Query(ctx, rule.Query, time.Now())
	if err != nil {
		return Reading{}, err
	}
	vec, ok := result.(model.Vector)
	if !ok {
		return Reading{}, fmt.Errorf("unexpected result type %s for %s", result.Type(), rule.Name)
	}
	for _, sample := range vec {
		// The collector names a series by its service label, else its pod label.
		name := string(sample.Metric["service"])
		if name == "" {
			name = string(sample.Metric["pod"])
		}
		ns := string(sample.Metric["namespace"])
		if ns == "" {
			ns = "default"
		}
		if name == entity && (namespace == "" || ns == namespace) {
			return Reading{Value: float64(sample.Value), Threshold: rule.Threshold}, nil
		}
	}
	return Reading{}, ErrNoSeries
}
