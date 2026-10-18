package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/heal"
)

// Heal mode: instead of the harness undoing the fault, Chronicle's own
// proposal is decided by a real OIDC identity and carried out by Chronicle's
// worker. The harness only watches, and its snapshot restore still runs
// afterwards as a safety net.

// HealExpectation is written to the log before the fault, like the truth.
type HealExpectation struct {
	Decide string `json:"decide"` // approve | deny
	// Expect is the final state: succeeded, failed, blocked, denied, or none
	// (no proposal should exist, e.g. a cause no rule acts on).
	Expect string `json:"expect"`
	// Symptom, when set, is the verdict of the post-action symptom check
	// (succeeded, failed or unknown) the execution must end with.
	Symptom string `json:"symptom,omitempty"`
	// Reason, when set, must appear in the result or error: "blocked" by the
	// confidence floor is not "blocked" by the cooldown.
	Reason string `json:"reason,omitempty"`
	Why    string `json:"why"`
}

// HealOutcome is what happened to the proposal.
type HealOutcome struct {
	Expected   HealExpectation `json:"expected"`
	ActionID   string          `json:"action_id,omitempty"`
	Rule       string          `json:"rule,omitempty"`
	ActionType string          `json:"action_type,omitempty"`
	Target     string          `json:"target,omitempty"`
	Confidence float64         `json:"confidence,omitempty"`
	FoundState string          `json:"found_state,omitempty"`

	// Authentication and authorization, probed before the real decision.
	// For an action that needs several approvers: the first approval must leave it
	// pending, the same person again must be refused (409), and only a different
	// person completes it.
	ApprovalsRequired    int    `json:"approvals_required,omitempty"`
	FirstApprovalState   string `json:"first_approval_state,omitempty"`
	RepeatApprovalStatus int    `json:"repeat_approval_status,omitempty"`

	NoTokenStatus     int  `json:"no_token_status,omitempty"`     // want 401
	NonApproverStatus int  `json:"non_approver_status,omitempty"` // want 403
	PendingAfterProbe bool `json:"pending_after_probes,omitempty"`

	DecisionStatus int    `json:"decision_status,omitempty"`
	DecidedBy      string `json:"decided_by,omitempty"`
	DecidedSubject string `json:"decided_subject,omitempty"`

	Final        string  `json:"final"`
	Result       string  `json:"result,omitempty"`
	Error        string  `json:"error,omitempty"`
	Verification string  `json:"verification,omitempty"`
	TerminalIn   float64 `json:"terminal_after_decision_seconds,omitempty"`

	// HealedWithoutRestore: the steady state returned before the harness
	// restored anything, so the remediation alone fixed the outage.
	HealedWithoutRestore bool    `json:"healed_without_restore"`
	HealedAfter          float64 `json:"healed_after_seconds,omitempty"`

	// SymptomCheck is what the check concluded after the action: did the alert
	// clear and stay clear. Empty when no check ran (nothing succeeded).
	SymptomCheck  string `json:"symptom_check,omitempty"`
	SymptomDetail string `json:"symptom_detail,omitempty"`

	// Decisions is every decision Chronicle recorded in the namespace since the
	// fault, not only the one the harness acted on: the others are the evidence
	// of what else it would have done (or declined to), and of duplicates.
	Decisions []HealDecision `json:"decisions,omitempty"`

	Matches bool   `json:"matches_expectation"`
	Problem string `json:"problem,omitempty"`
}

// HealDecision is one recorded decision, in brief.
type HealDecision struct {
	ID         string  `json:"id"`
	Rule       string  `json:"rule,omitempty"`
	ActionType string  `json:"action_type,omitempty"`
	Target     string  `json:"target,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
	Proposed   bool    `json:"proposed"`
	State      string  `json:"state"`
	Status     string  `json:"status"`
	Result     string  `json:"result,omitempty"`
}

// TokenSource yields an ID token for one identity.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// PasswordGrant gets an ID token with the resource-owner password grant. Only
// for the disposable test realm, where the client allows it.
type PasswordGrant struct {
	TokenURL, ClientID, Username, Password string
	HTTP                                   *http.Client
}

func (p PasswordGrant) Token(ctx context.Context) (string, error) {
	form := url.Values{"grant_type": {"password"}, "client_id": {p.ClientID}, "username": {p.Username},
		"password": {p.Password}, "scope": {"openid email profile"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := p.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var body struct {
		IDToken string `json:"id_token"`
		Error   string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body)
	if resp.StatusCode != http.StatusOK || body.IDToken == "" {
		return "", fmt.Errorf("token for %s: HTTP %d %s", p.Username, resp.StatusCode, body.Error)
	}
	return body.IDToken, nil
}

// Actions lists recent healing decisions.
func (a *Chronicle) Actions(ctx context.Context) ([]heal.Action, error) {
	var out struct {
		Actions []heal.Action `json:"actions"`
	}
	if err := a.do(ctx, http.MethodGet, "/api/heal/actions", url.Values{}, &out); err != nil {
		return nil, err
	}
	return out.Actions, nil
}

// Decide approves or denies with the given ID token ("" sends none) and
// returns the HTTP status and, on success, the recorded decision.
func (a *Chronicle) Decide(ctx context.Context, id string, approve bool, idToken string) (int, *heal.Action, error) {
	verb := "deny"
	if approve {
		verb = "approve"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/api/heal/actions/"+url.PathEscape(id)+"/"+verb,
		strings.NewReader(`{"by":"chaos-harness-claims-to-be-someone-else","reason":"chaos campaign"}`))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	}
	if idToken != "" {
		req.Header.Set("X-Chronicle-Heal-Token", idToken)
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil, nil
	}
	var action heal.Action
	if err := json.NewDecoder(resp.Body).Decode(&action); err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, &action, nil
}

// Healer drives the decision as two people would: someone not allowed to
// approve tries first, then the approver decides.
type Healer struct {
	Approver, NonApprover TokenSource
	// SecondApprover is a different person from Approver, who completes an
	// action that needs two.
	SecondApprover TokenSource
	// Terminal bounds the wait for the worker to finish (execution plus verify).
	Terminal time.Duration
	// SymptomWait bounds the wait for the symptom check after a success (zero:
	// do not wait). It must outlast the checker's improve window plus its hold.
	SymptomWait time.Duration
}

var terminalStates = map[string]bool{
	heal.StatusSucceeded: true, heal.StatusFailed: true, heal.StatusBlocked: true,
	heal.StatusDenied: true, heal.StatusExpired: true, heal.StatusSkipped: true,
}

// decisionsSince lists everything recorded for the namespace since t, oldest
// first. A failure to list is not hidden by an empty list, but it cannot fail
// the experiment either: the main decision was already tracked on its own.
func (r *Runner) decisionsSince(ctx context.Context, t time.Time) []HealDecision {
	actions, err := r.Chronicle.Actions(ctx)
	if err != nil {
		return []HealDecision{{ID: "unavailable", Result: "could not list decisions: " + err.Error()}}
	}
	var out []HealDecision
	for i := len(actions) - 1; i >= 0; i-- {
		a := actions[i]
		if a.Namespace != r.Cluster.Namespace || a.CreatedAt.Before(t) {
			continue
		}
		out = append(out, HealDecision{ID: a.ID, Rule: a.Rule, ActionType: a.ActionType, Target: a.Target, Confidence: a.Confidence,
			Proposed: a.Proposed, State: a.State, Status: a.Status, Result: a.Result})
	}
	return out
}

func (r *Runner) findProposal(ctx context.Context, out *Outcome) (*heal.Action, error) {
	actions, err := r.Chronicle.Actions(ctx)
	if err != nil {
		return nil, err
	}
	var found *heal.Action
	for i := range actions {
		a := &actions[i]
		if out.ProposalID != "" && a.ID == out.ProposalID {
			return a, nil
		}
		if a.Proposed && a.Namespace == r.Cluster.Namespace && !a.CreatedAt.Before(out.InjectedAt) &&
			(found == nil || a.CreatedAt.After(found.CreatedAt)) {
			found = a
		}
	}
	return found, nil
}

// heal runs after the analysis and before the restore.
func (r *Runner) heal(ctx context.Context, out *Outcome, want HealExpectation) *HealOutcome {
	h := &HealOutcome{Expected: want}
	defer func() {
		h.Decisions = r.decisionsSince(ctx, out.InjectedAt)
		h.Matches = h.Problem == "" && h.Final == want.Expect &&
			(want.Reason == "" || strings.Contains(h.Result+" "+h.Error, want.Reason)) &&
			(want.Symptom == "" || h.SymptomCheck == want.Symptom)
	}()
	action, err := r.findProposal(ctx, out)
	if err != nil {
		h.Final, h.Problem = "error", "list actions: "+err.Error()
		return h
	}
	if action == nil {
		h.Final = "none"
		return h
	}
	h.ActionID, h.Rule, h.ActionType, h.Target, h.Confidence = action.ID, action.Rule, action.ActionType, action.Target, action.Confidence
	h.FoundState, h.Result = action.State, action.Result
	// A skipped decision is the record that nothing was proposed (no rule).
	if action.Status == heal.StatusSkipped {
		h.Final = "none"
		return h
	}
	if action.State != "pending_approval" {
		h.Final = action.Status
		return h
	}

	// Nobody signed in, then a signed-in non-approver: both refused, nothing changed.
	h.NoTokenStatus, _, _ = r.Chronicle.Decide(ctx, action.ID, true, "")
	if tok, err := r.Healer.NonApprover.Token(ctx); err != nil {
		h.Problem = "non-approver token: " + err.Error()
	} else {
		h.NonApproverStatus, _, _ = r.Chronicle.Decide(ctx, action.ID, true, tok)
	}
	if current, err := r.findProposal(ctx, &Outcome{ProposalID: action.ID}); err == nil && current != nil {
		h.PendingAfterProbe = current.State == "pending_approval"
	}
	if h.NoTokenStatus != http.StatusUnauthorized || h.NonApproverStatus != http.StatusForbidden || !h.PendingAfterProbe {
		h.Problem = fmt.Sprintf("refusals: no token %d (want 401), non-approver %d (want 403), still pending %v",
			h.NoTokenStatus, h.NonApproverStatus, h.PendingAfterProbe)
	}

	tok, err := r.Healer.Approver.Token(ctx)
	if err != nil {
		h.Final, h.Problem = "error", "approver token: "+err.Error()
		return h
	}
	if action.ApprovalsRequired > 1 && want.Decide == "approve" {
		h.ApprovalsRequired = action.ApprovalsRequired
		if r.Healer.SecondApprover == nil {
			h.Final, h.Problem = "error", "the action needs several approvers and the harness has only one"
			return h
		}
		first, firstAction, _ := r.Chronicle.Decide(ctx, action.ID, true, tok)
		if firstAction != nil {
			h.FirstApprovalState = firstAction.State
		}
		if first != http.StatusOK || firstAction == nil || firstAction.State != "pending_approval" || len(firstAction.Approvals) != 1 {
			h.Final, h.Problem = "error", fmt.Sprintf("the first of %d approvals: HTTP %d, state %q (want 200, pending_approval)", action.ApprovalsRequired, first, h.FirstApprovalState)
			return h
		}
		h.RepeatApprovalStatus, _, _ = r.Chronicle.Decide(ctx, action.ID, true, tok)
		if h.RepeatApprovalStatus != http.StatusConflict {
			h.Final, h.Problem = "error", fmt.Sprintf("the same person approving twice got HTTP %d, want 409", h.RepeatApprovalStatus)
			return h
		}
		second, err := r.Healer.SecondApprover.Token(ctx)
		if err != nil {
			h.Final, h.Problem = "error", "second approver token: "+err.Error()
			return h
		}
		tok = second
	}
	decided := r.now()
	status, recorded, err := r.Chronicle.Decide(ctx, action.ID, want.Decide == "approve", tok)
	h.DecisionStatus = status
	if err != nil || status != http.StatusOK || recorded == nil {
		h.Final, h.Problem = "error", fmt.Sprintf("decision: HTTP %d %v", status, err)
		return h
	}
	h.DecidedBy, h.DecidedSubject = recorded.DecisionBy, recorded.DecisionSubject

	// The worker polls every few seconds; wait for a final state.
	deadline := decided.Add(r.Healer.Terminal)
	for {
		current, err := r.findProposal(ctx, &Outcome{ProposalID: action.ID})
		if err == nil && current != nil && terminalStates[current.Status] {
			h.Final, h.Result, h.Error, h.Verification = current.Status, current.Result, current.Error, current.Verification
			h.TerminalIn = r.now().Sub(decided).Seconds()
			break
		}
		if !r.now().Before(deadline) {
			h.Final = "timeout"
			if current != nil {
				h.Result = "last state " + current.State
			}
			break
		}
		if err := r.sleep(ctx, r.Config.Poll); err != nil {
			h.Final = "interrupted"
			return h
		}
	}
	if h.Final == heal.StatusSucceeded {
		// How long recovery took, measured before the symptom check's hold. With a
		// check to follow, a symptom that never clears must not stall the run for
		// the full steady-state timeout.
		wait := r.Config.SteadyTimeout
		if r.Healer.SymptomWait > 0 && wait > 3*time.Minute {
			wait = 3 * time.Minute
		}
		start := r.now()
		ok, why := waitSteady(ctx, r.Steady, wait, r.Config.Poll, 15*time.Second, r.now, r.sleep)
		h.HealedWithoutRestore, h.HealedAfter = ok, r.now().Sub(start).Seconds()
		if r.Healer.SymptomWait > 0 {
			// The harness must not restore while the check is still measuring, or
			// it would be judging the harness's own repair.
			h.SymptomCheck, h.SymptomDetail = r.awaitSymptomCheck(ctx, action.ID)
		}
		if !ok && h.SymptomCheck != heal.SymptomFailed {
			h.Problem = "the remediation succeeded but the steady state did not return: " + why
		}
	}
	return h
}

// awaitSymptomCheck polls until the check leaves "pending", or the wait ends.
func (r *Runner) awaitSymptomCheck(ctx context.Context, id string) (verdict, detail string) {
	deadline := r.now().Add(r.Healer.SymptomWait)
	for {
		if current, err := r.findProposal(ctx, &Outcome{ProposalID: id}); err == nil && current != nil &&
			current.SymptomCheck != "" && current.SymptomCheck != heal.SymptomPending {
			return current.SymptomCheck, current.SymptomDetail
		}
		if !r.now().Before(deadline) {
			return heal.SymptomPending, "the check did not conclude in time"
		}
		if err := r.sleep(ctx, r.Config.Poll); err != nil {
			return heal.SymptomPending, "interrupted"
		}
	}
}
