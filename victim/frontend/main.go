package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
		},
		[]string{"service", "status"},
	)
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_duration_seconds",
			Help:    "Duration of HTTP requests.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"service"},
	)
)

func main() {
	apiURL := os.Getenv("API_URL")
	if apiURL == "" {
		apiURL = "http://api:8080"
	}

	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		status := "200"
		defer func() {
			requestsTotal.WithLabelValues("frontend", status).Inc()
			requestDuration.WithLabelValues("frontend").Observe(time.Since(start).Seconds())
		}()

		client := http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(apiURL)
		if err != nil {
			status = "500"
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			status = fmt.Sprintf("%d", resp.StatusCode)
			http.Error(w, "api returned non-200", resp.StatusCode)
			return
		}

		body, _ := io.ReadAll(resp.Body)
		w.Write(body)
	})

	log.Println("Frontend started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
