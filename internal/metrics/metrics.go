// Package metrics is Chronicle's own telemetry: what its service-level
// objectives are measured from. Go runtime and process metrics come from the
// default registry beside these.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventLag is how long an event spent in the pipeline: from a collector
	// emitting it to it being stored. It is not the event's age: a collector
	// reports old facts (existing pods at start-up, commits), and that age is not
	// something the pipeline can be held to.
	EventLag = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "chronicle_event_lag_seconds", Help: "Seconds from a collector emitting an event to it being stored.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"source"})
	EventsStored = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chronicle_events_stored_total", Help: "Events stored, by source."}, []string{"source"})
	EventsDuplicate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chronicle_events_duplicate_total", Help: "Events the store already had (redelivery or a second observer)."})
	EventWriteErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chronicle_event_write_errors_total", Help: "Event batches that failed to write."})

	AnalyzeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "chronicle_analyze_duration_seconds", Help: "Time to analyse one incident and evaluate healing for it.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}})
	AnalyzeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chronicle_analyze_total", Help: "Analyses, by result."}, []string{"result"})

	HealExecutions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chronicle_heal_executions_total", Help: "Approved healing decisions that reached an end state."},
		[]string{"action_type", "status"})
	// DecisionToExecution is the approval-to-end time of an executed decision.
	DecisionToExecution = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "chronicle_heal_decision_to_execution_seconds", Help: "Seconds from an approval to the execution ending.",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800}})
	SymptomChecks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chronicle_symptom_checks_total", Help: "Post-action symptom checks, by verdict."}, []string{"verdict"})

	GraphSyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "chronicle_graph_sync_duration_seconds", Help: "Time to build and persist the dependency graph.",
		Buckets: []float64{0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60}})
	GraphEdges = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chronicle_graph_edges", Help: "Edges in the last synced dependency graph."})

	Leader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chronicle_leader", Help: "1 while this replica holds the leader lease."})
)

// Since observes a duration on a histogram.
func Since(h prometheus.Observer, start time.Time) { h.Observe(time.Since(start).Seconds()) }

// Serve exposes the default registry on addr until ctx ends. It is separate from
// the API port: the API sits behind a token, a scraper should not need one.
func Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	slog.Info("metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
