package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Halcyonic-01/Chronicle/internal/auth"
	"github.com/Halcyonic-01/Chronicle/internal/heal"
)

// WithOIDC makes OIDC the only way to approve. The shared HEAL_APPROVAL_TOKEN
// stops working once it is set: leaving it as a second door would make the
// weaker one the real one.
func (h *Handler) WithOIDC(o *auth.OIDC) *Handler {
	h.oidc = o
	return h
}

// approver is who is deciding, and the HTTP status to refuse with when nobody is.
type approver struct {
	display, issuer, subject string
}

func (h *Handler) authenticateApprover(r *http.Request, claimedBy string) (approver, int, string) {
	if h.oidc != nil {
		id, err := h.oidc.Authenticate(r)
		switch {
		case errors.Is(err, auth.ErrForbidden):
			return approver{}, http.StatusForbidden, "not permitted to approve healing actions"
		case errors.Is(err, auth.ErrProviderUnavailable):
			return approver{}, http.StatusServiceUnavailable, "identity provider unavailable; approvals are refused until it is back"
		case err != nil:
			return approver{}, http.StatusUnauthorized, "approval authentication required"
		}
		return approver{display: id.Display(), issuer: id.Issuer, subject: id.Subject}, 0, ""
	}
	if !heal.ApprovalAuthorized(r) {
		return approver{}, http.StatusUnauthorized, "approval authentication required"
	}
	// The shared token proves nothing about who is holding it, so the name is
	// only what the caller claims, and is recorded as such.
	if strings.TrimSpace(claimedBy) == "" {
		claimedBy = "local-reviewer"
	}
	return approver{display: "shared-token:" + strings.TrimSpace(claimedBy)}, 0, ""
}

// GET /api/auth/config tells the console how approvers sign in.
func (h *Handler) AuthConfig(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"use GET"}`, http.StatusMethodNotAllowed)
		return
	}
	if h.oidc == nil {
		json.NewEncoder(w).Encode(map[string]any{"mode": "shared_token"})
		return
	}
	cfg := h.oidc.Config()
	body := map[string]any{"mode": "oidc", "issuer": cfg.Issuer, "client_id": cfg.ClientID, "scopes": cfg.Scopes}
	if authorize, token, ok := h.oidc.Endpoints(r.Context()); ok {
		body["authorization_endpoint"], body["token_endpoint"] = authorize, token
	} else {
		body["unavailable"] = true
	}
	json.NewEncoder(w).Encode(body)
}

// GET /api/auth/me reports who the presented token belongs to, and whether
// they may approve, without deciding anything.
func (h *Handler) AuthMe(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"use GET"}`, http.StatusMethodNotAllowed)
		return
	}
	if h.oidc == nil {
		writeJSONError(w, http.StatusNotFound, "OIDC is not configured")
		return
	}
	id, err := h.oidc.Authenticate(r)
	switch {
	case errors.Is(err, auth.ErrForbidden):
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]any{"error": "not permitted to approve healing actions", "name": id.Display(), "can_approve": false})
	case errors.Is(err, auth.ErrProviderUnavailable):
		writeJSONError(w, http.StatusServiceUnavailable, "identity provider unavailable")
	case err != nil:
		writeJSONError(w, http.StatusUnauthorized, "approval authentication required")
	default:
		json.NewEncoder(w).Encode(map[string]any{"name": id.Display(), "subject": id.Subject, "issuer": id.Issuer, "can_approve": true})
	}
}
