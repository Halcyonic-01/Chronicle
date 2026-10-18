package api

import (
	"context"
	"net/http"
	"time"
)

// Pinger is what readiness needs from the database.
type Pinger interface{ Ping(context.Context) error }

// Healthz says the process is serving. It asks nothing of any dependency, so a
// database outage can never get a healthy replica restarted.
func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// Readyz says whether the replica can do its job: the database must answer. It
// is for monitoring and for an operator; the kubelet probes stay on the console,
// because taking every replica out of service during a database outage would
// also take away the console that shows it.
func Readyz(db Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			http.Error(w, "database unavailable\n", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	}
}
