package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
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
	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = "postgres://postgres:postgres@postgres:5432/postgres"
	}

	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		status := "200"
		defer func() {
			requestsTotal.WithLabelValues("worker", status).Inc()
			requestDuration.WithLabelValues("worker").Observe(time.Since(start).Seconds())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, pgURL)
		if err != nil {
			status = "500"
			http.Error(w, fmt.Sprintf("postgres connection error: %v", err), http.StatusInternalServerError)
			return
		}
		defer conn.Close(context.Background())

		var now time.Time
		err = conn.QueryRow(ctx, "SELECT NOW()").Scan(&now)
		if err != nil {
			status = "500"
			http.Error(w, fmt.Sprintf("postgres query error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Write([]byte(fmt.Sprintf("worker ok, db time: %s\n", now.Format(time.RFC3339))))
	})

	log.Println("Worker started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
