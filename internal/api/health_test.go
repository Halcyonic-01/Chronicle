package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type pinger struct{ err error }

func (p pinger) Ping(context.Context) error { return p.err }

func TestReadinessFollowsTheDatabaseAndLivenessDoesNot(t *testing.T) {
	get := func(h http.Handler) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code
	}
	if got := get(Readyz(pinger{})); got != http.StatusOK {
		t.Errorf("a reachable database: %d", got)
	}
	if got := get(Readyz(pinger{errors.New("connection refused")})); got != http.StatusServiceUnavailable {
		t.Errorf("an unreachable database: %d", got)
	}
	if got := get(http.HandlerFunc(Healthz)); got != http.StatusOK {
		t.Errorf("liveness depends on nothing: %d", got)
	}
}
