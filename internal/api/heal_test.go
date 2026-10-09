package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/heal"
)

type decisionStore struct{ err error }

func (decisionStore) ListActions(context.Context, int) ([]heal.Action, error) { return nil, nil }
func (s decisionStore) DecideAction(_ context.Context, d heal.Decision) (*heal.Action, error) {
	if s.err != nil {
		return nil, s.err
	}
	// What the real store returns for an approval: an approved, unexpired decision.
	expires := time.Now().Add(10 * time.Minute)
	return &heal.Action{ID: d.ID, Rule: "restore-scaled-down-workload", ActionType: heal.ActionRestoreReplicas, Namespace: "default",
		Target: "cache", Workload: "cache", Confidence: 0.9, Status: heal.StatusApproved, Approval: heal.ApprovalApproved,
		ExpiresAt: &expires, Payload: []byte(`{"old_replicas":3,"new_replicas":0}`)}, nil
}

func decide(t *testing.T, store heal.ActionStore, token string) *httptest.ResponseRecorder {
	t.Helper()
	t.Setenv("HEAL_APPROVAL_TOKEN", "test-token")
	h := NewHandler(nil, nil, nil, nil, nil, store, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/heal/actions/abc/approve", strings.NewReader(`{"by":"reviewer"}`))
	if token != "" {
		req.Header.Set("X-Chronicle-Heal-Token", token)
	}
	rec := httptest.NewRecorder()
	h.DecideHealingAction(rec, req)
	return rec
}

// Each way a decision cannot be recorded has its own status, and a storage
// failure is neither a conflict nor allowed to leak its text to the caller.
func TestDecidingAHealingActionReportsEachFailureHonestly(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		token      string
		wantStatus int
		wantBody   string
		notInBody  string
	}{
		{"approved", nil, "test-token", http.StatusOK, "abc", ""},
		{"no token", nil, "", http.StatusUnauthorized, "authentication required", ""},
		{"wrong token", nil, "nope", http.StatusUnauthorized, "authentication required", ""},
		{"unknown action", heal.ErrActionNotFound, "test-token", http.StatusNotFound, "not found", ""},
		{"expired", heal.ErrExpired, "test-token", http.StatusConflict, "expired", ""},
		{"already decided", fmt.Errorf("%w (it is approved)", heal.ErrNotPending), "test-token", http.StatusConflict, "it is approved", ""},
		{"storage failure", errors.New(`pq: connection to 10.1.2.3:5432 refused (user=postgres)`), "test-token", http.StatusInternalServerError, "could not record", "10.1.2.3"},
	}
	for _, c := range cases {
		rec := decide(t, decisionStore{err: c.err}, c.token)
		if rec.Code != c.wantStatus || !strings.Contains(rec.Body.String(), c.wantBody) {
			t.Errorf("%s: got %d %q, want %d containing %q", c.name, rec.Code, rec.Body.String(), c.wantStatus, c.wantBody)
		}
		if c.notInBody != "" && strings.Contains(rec.Body.String(), c.notInBody) {
			t.Errorf("%s: internal detail leaked to the caller: %q", c.name, rec.Body.String())
		}
	}
}

type recordingExecution struct{ claims, completions, runs int }

func (r *recordingExecution) ClaimExecution(context.Context, heal.ClaimRequest) (bool, string, error) {
	r.claims++
	return true, "", nil
}
func (r *recordingExecution) CompleteExecution(context.Context, string, string, string, string, string) error {
	r.completions++
	return nil
}
func (r *recordingExecution) Execute(context.Context, *heal.Action) (string, error) {
	r.runs++
	return "ran", nil
}

// Approval records the decision and nothing else. Even with live healing on, the
// request never reaches the store's claim or the executor: that is the worker's
// job, after it re-checks every gate.
func TestApprovingNeverExecutes(t *testing.T) {
	t.Setenv("HEAL_APPROVAL_TOKEN", "test-token")
	exec := &recordingExecution{}
	controller := heal.NewController(exec, exec)
	controller.Policy.LiveEnabled = true
	controller.Rules = []heal.Rule{{Name: "restore-scaled-down-workload", CauseType: "scale", ActionType: heal.ActionRestoreReplicas, MinConfidence: 0.65, MaxPerHour: 2}}
	h := NewHandler(nil, nil, nil, nil, nil, decisionStore{}, nil, controller)
	req := httptest.NewRequest(http.MethodPost, "/api/heal/actions/abc/approve", strings.NewReader(`{}`))
	req.Header.Set("X-Chronicle-Heal-Token", "test-token")
	rec := httptest.NewRecorder()
	h.DecideHealingAction(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"approved"`) {
		t.Fatalf("unexpected response %d %s", rec.Code, rec.Body.String())
	}
	if exec.claims+exec.completions+exec.runs != 0 {
		t.Fatalf("approving reached the executor: %d claims, %d completions, %d runs", exec.claims, exec.completions, exec.runs)
	}
}
