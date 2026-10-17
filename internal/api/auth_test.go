package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Halcyonic-01/Chronicle/internal/auth"
	"github.com/Halcyonic-01/Chronicle/internal/auth/authtest"
	"github.com/Halcyonic-01/Chronicle/internal/heal"
)

// capturingStore records the decision it is given.
type capturingStore struct {
	decisionStore
	got   []heal.Decision
	reads int
}

func (s *capturingStore) DecideAction(ctx context.Context, d heal.Decision) (*heal.Action, error) {
	s.got = append(s.got, d)
	return s.decisionStore.DecideAction(ctx, d)
}
func (s *capturingStore) ListActions(context.Context, int) ([]heal.Action, error) {
	s.reads++
	return nil, nil
}

func oidcHandler(t *testing.T, idp *authtest.IDP, store heal.ActionStore) *Handler {
	t.Helper()
	verifier, err := auth.NewOIDC(auth.Config{Issuer: idp.URL(), ClientID: "chronicle", AllowedGroups: []string{"sre"}})
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(nil, nil, nil, nil, nil, store, nil).WithOIDC(verifier)
}

func approve(h *Handler, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/heal/actions/abc/approve", strings.NewReader(body))
	if token != "" {
		req.Header.Set("X-Chronicle-Heal-Token", token)
	}
	rec := httptest.NewRecorder()
	h.DecideHealingAction(rec, req)
	return rec
}

// The audit trail records who the provider vouched for, whatever the request
// body claims.
func TestTheApproverIsTheVerifiedIdentityNotTheBody(t *testing.T) {
	idp := authtest.New(t)
	store := &capturingStore{}
	h := oidcHandler(t, idp, store)

	rec := approve(h, idp.Token(t, idp.Key, nil), `{"by":"the-ceo","reason":"looks right"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	if len(store.got) != 1 {
		t.Fatalf("recorded %d decisions", len(store.got))
	}
	d := store.got[0]
	if d.By != "ana@example.com" || d.Subject != "user-1" || d.Issuer != idp.URL() || d.Reason != "looks right" {
		t.Fatalf("recorded %+v", d)
	}
	if strings.Contains(d.By, "ceo") {
		t.Fatal("the body's claim reached the audit trail")
	}
}

func TestOIDCRefusalsNameNoDetailsAndTouchNothing(t *testing.T) {
	idp := authtest.New(t)
	store := &capturingStore{}
	h := oidcHandler(t, idp, store)
	cases := []struct {
		name  string
		token string
		want  int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"forged", idp.Token(t, idp.Other, nil), http.StatusUnauthorized},
		{"wrong audience", idp.Token(t, idp.Key, authtest.Claims{"aud": "other"}), http.StatusUnauthorized},
		{"valid but not an approver", idp.Token(t, idp.Key, authtest.Claims{"groups": []string{"interns"}}), http.StatusForbidden},
	}
	for _, c := range cases {
		rec := approve(h, c.token, `{"by":"x"}`)
		if rec.Code != c.want {
			t.Errorf("%s: got %d, want %d", c.name, rec.Code, c.want)
		}
		for _, leak := range []string{"signature", "audience", "oidc:", "interns"} {
			if strings.Contains(rec.Body.String(), leak) {
				t.Errorf("%s: response leaks %q: %s", c.name, leak, rec.Body.String())
			}
		}
	}
	if len(store.got) != 0 {
		t.Fatalf("a refused request recorded %d decisions", len(store.got))
	}
}

// With OIDC configured the shared token is not a second door.
func TestTheSharedTokenStopsWorkingOnceOIDCIsConfigured(t *testing.T) {
	t.Setenv("HEAL_APPROVAL_TOKEN", "shared-secret")
	idp := authtest.New(t)
	store := &capturingStore{}
	h := oidcHandler(t, idp, store)
	if rec := approve(h, "shared-secret", `{"by":"anyone"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("the shared token was accepted: %d", rec.Code)
	}
	if len(store.got) != 0 {
		t.Fatal("a decision was recorded")
	}
}

// A caller who cannot approve learns nothing about the store or the action.
func TestAuthenticationPrecedesEveryOtherCheck(t *testing.T) {
	idp := authtest.New(t)
	h := oidcHandler(t, idp, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/heal/actions/not/a/real/path", nil)
	rec := httptest.NewRecorder()
	h.DecideHealingAction(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401 before the path or store is examined", rec.Code)
	}
}

func TestAnUnreachableProviderRefusesApprovalWith503(t *testing.T) {
	idp := authtest.New(t)
	idp.Down.Store(true)
	store := &capturingStore{}
	h := oidcHandler(t, idp, store)
	if rec := approve(h, idp.Token(t, idp.Key, nil), `{}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", rec.Code)
	}
	if len(store.got) != 0 {
		t.Fatal("a decision was recorded while the provider was down")
	}
}

// Without OIDC the old behaviour holds, and the recorded name says it is only
// what the caller claimed.
func TestSharedTokenDecisionsAreMarkedAsSelfDeclared(t *testing.T) {
	t.Setenv("HEAL_APPROVAL_TOKEN", "shared-secret")
	store := &capturingStore{}
	h := NewHandler(nil, nil, nil, nil, nil, store, nil)
	if rec := approve(h, "shared-secret", `{"by":"ana"}`); rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	d := store.got[0]
	if d.By != "shared-token:ana" || d.Subject != "" || d.Issuer != "" {
		t.Fatalf("recorded %+v", d)
	}
}

func TestAuthConfigAndMe(t *testing.T) {
	idp := authtest.New(t)
	h := oidcHandler(t, idp, &capturingStore{})

	rec := httptest.NewRecorder()
	h.AuthConfig(rec, httptest.NewRequest(http.MethodGet, "/api/auth/config", nil))
	var cfg map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &cfg)
	if cfg["mode"] != "oidc" || cfg["client_id"] != "chronicle" || cfg["authorization_endpoint"] != idp.URL()+"/authorize" || cfg["token_endpoint"] != idp.URL()+"/token" {
		t.Fatalf("config %v", cfg)
	}

	me := func(token string) (int, map[string]any) {
		req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
		if token != "" {
			req.Header.Set("X-Chronicle-Heal-Token", token)
		}
		rec := httptest.NewRecorder()
		h.AuthMe(rec, req)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}
	if code, body := me(idp.Token(t, idp.Key, nil)); code != 200 || body["name"] != "ana@example.com" || body["can_approve"] != true {
		t.Fatalf("me: %d %v", code, body)
	}
	if code, body := me(idp.Token(t, idp.Key, authtest.Claims{"groups": []string{"interns"}})); code != 403 || body["can_approve"] != false {
		t.Fatalf("not an approver: %d %v", code, body)
	}
	if code, _ := me(""); code != 401 {
		t.Fatalf("no token: %d", code)
	}

	plain := NewHandler(nil, nil, nil, nil, nil, &capturingStore{}, nil)
	rec = httptest.NewRecorder()
	plain.AuthConfig(rec, httptest.NewRequest(http.MethodGet, "/api/auth/config", nil))
	if !strings.Contains(rec.Body.String(), `"shared_token"`) {
		t.Fatalf("plain config %s", rec.Body.String())
	}
}
