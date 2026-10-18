package heal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/api"
)

// fakeProm answers /api/v1/query with the given series. A series is
// "service|namespace|value".
func fakeProm(t *testing.T, status int, series ...string) *PromProbe {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			http.Error(w, "boom", status)
			return
		}
		var result []string
		for _, s := range series {
			parts := strings.Split(s, "|")
			result = append(result, fmt.Sprintf(`{"metric":{"service":%q,"namespace":%q},"value":[1700000000,%q]}`, parts[0], parts[1], parts[2]))
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, strings.Join(result, ","))
	}))
	t.Cleanup(srv.Close)
	client, err := api.NewClient(api.Config{Address: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return NewPromProbe(client)
}

// The probe uses the collector's own rule: same query, same 0.05 threshold.
func TestTheProbeReadsTheMetricTheAlertFiredOn(t *testing.T) {
	probe := fakeProm(t, http.StatusOK, "api|default|0.001", "frontend|default|0.40", "frontend|other|0.00")
	got, err := probe.Read(context.Background(), "error_spike", "default", "frontend")
	if err != nil || got.Value != 0.40 || got.Threshold != 0.05 {
		t.Fatalf("got %+v, %v", got, err)
	}
	// The same name in another namespace is a different series.
	got, err = probe.Read(context.Background(), "error_spike", "other", "frontend")
	if err != nil || got.Value != 0 {
		t.Fatalf("namespace not respected: %+v, %v", got, err)
	}
}

func TestTheProbeSeparatesAbsentFromUnreachableFromNotAMetric(t *testing.T) {
	ctx := context.Background()
	probe := fakeProm(t, http.StatusOK, "api|default|0.001")
	if _, err := probe.Read(ctx, "error_spike", "default", "frontend"); !errors.Is(err, ErrNoSeries) {
		t.Fatalf("a missing series: %v", err)
	}
	if _, err := probe.Read(ctx, "log_error", "default", "frontend"); !errors.Is(err, ErrNotMetric) {
		t.Fatalf("a non-metric symptom: %v", err)
	}
	down := fakeProm(t, http.StatusInternalServerError)
	_, err := down.Read(ctx, "error_spike", "default", "frontend")
	if err == nil || errors.Is(err, ErrNoSeries) || errors.Is(err, ErrNotMetric) {
		t.Fatalf("an unreachable Prometheus must be its own error, not 'no series': %v", err)
	}
}

// 0/0 is what the error-rate query returns when nothing is being served.
func TestTheProbeSurfacesNaNSoItIsNeverTakenForHealth(t *testing.T) {
	probe := fakeProm(t, http.StatusOK, "frontend|default|NaN")
	got, err := probe.Read(context.Background(), "error_spike", "default", "frontend")
	if err != nil {
		t.Fatal(err)
	}
	if j := judgeSymptom(task(), got, nil, task().DueAt, t0policy); j.verdict != SymptomUnknown {
		t.Fatalf("NaN from Prometheus was judged %+v", j)
	}
}
