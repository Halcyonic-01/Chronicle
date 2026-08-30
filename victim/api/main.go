package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
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
	workerURL := os.Getenv("WORKER_URL")
	if workerURL == "" {
		workerURL = "http://worker:8080"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis:6379"
	}

	// Deliberate weakness: tiny connection pool, no dial timeouts, no read/write timeouts
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisURL,
		PoolSize:     5,
		ReadTimeout:  0, // no timeout
		WriteTimeout: 0, // no timeout
		DialTimeout:  0, // no timeout
	})

	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		status := "200"
		defer func() {
			requestsTotal.WithLabelValues("api", status).Inc()
			requestDuration.WithLabelValues("api").Observe(time.Since(start).Seconds())
		}()

		ctx := context.Background() // no timeout context either

		// 1. Talk to Redis (this blocks forever if Redis hangs or connections pile up)
		val, err := rdb.Incr(ctx, "api_hits").Result()
		if err != nil {
			status = "500"
			http.Error(w, fmt.Sprintf("redis error: %v", err), http.StatusInternalServerError)
			return
		}

		// 2. Talk to worker
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(workerURL)
		if err != nil {
			status = "500"
			http.Error(w, fmt.Sprintf("worker error: %v", err), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			status = fmt.Sprintf("%d", resp.StatusCode)
			http.Error(w, "worker returned non-200", resp.StatusCode)
			return
		}

		w.Write([]byte(fmt.Sprintf("api ok, redis hits: %d\n", val)))
	})

	log.Println("API started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
