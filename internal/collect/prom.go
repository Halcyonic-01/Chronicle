package collect

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type Rule struct {
	Name      string
	Query     string
	Threshold float64
	Severity  string
	EventType string
}

var rules = []Rule{
	{
		Name: "high_error_rate",
		Query: `sum by (service) (rate(http_requests_total{status=~"5.."}[1m])) 
			  / sum by (service) (rate(http_requests_total[1m]))`,
		Threshold: 0.05,
		Severity:  "critical",
		EventType: "error_spike",
	},
	{
		Name: "latency_p99",
		Query: `histogram_quantile(0.99, 
			  sum by (service, le) (rate(http_duration_seconds_bucket[1m])))`,
		Threshold: 1.0,
		Severity:  "warning",
		EventType: "latency_spike",
	},
	{
		Name: "memory_pressure",
		Query: `container_memory_working_set_bytes 
			  / container_spec_memory_limit_bytes`,
		Threshold: 0.90,
		Severity:  "warning",
		EventType: "memory_pressure",
	},
}

type PromCollector struct {
	BaseCollector
	api v1.API
}

func NewPromCollector(client api.Client, out chan<- event.Event) *PromCollector {
	return &PromCollector{
		BaseCollector: BaseCollector{Out: out},
		api:           v1.NewAPI(client),
	}
}

func (p *PromCollector) Run(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	firing := map[string]bool{} // dedupe state

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, rule := range rules {
				result, _, err := p.api.Query(ctx, rule.Query, time.Now())
				if err != nil {
					continue
				}

				vec, ok := result.(model.Vector)
				if !ok {
					continue
				}

				for _, sample := range vec {
					svc := string(sample.Metric["service"])
					if svc == "" {
						svc = string(sample.Metric["pod"])
					}
					if svc == "" {
						continue
					}

					key := rule.Name + "/" + svc
					over := float64(sample.Value) > rule.Threshold

					switch {
					case over && !firing[key]:
						firing[key] = true
						p.Emit(event.Event{
							Source:     "prometheus",
							EntityKind: "Service",
							EntityName: svc,
							Namespace:  "default",
							Type:       rule.EventType,
							Severity:   rule.Severity,
							Title: fmt.Sprintf("%s on %s: %.3f (limit %.3f)",
								rule.Name, svc, float64(sample.Value), rule.Threshold),
							Payload: mustJSON(map[string]any{
								"value": float64(sample.Value),
								"query": rule.Query,
							}),
						})
					case !over && firing[key]:
						firing[key] = false
						p.Emit(event.Event{
							Source:     "prometheus",
							EntityKind: "Service",
							EntityName: svc,
							Namespace:  "default",
							Type:       rule.EventType + "_resolved",
							Severity:   "info",
							Title:      fmt.Sprintf("%s resolved on %s", rule.Name, svc),
						})
					}
				}
			}
		}
	}
}
