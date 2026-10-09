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
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

// Chronicle is a client for the running Chronicle API: the same endpoints the
// console uses, so the harness sees exactly what an operator would.
type Chronicle struct {
	BaseURL string
	Token   string // CHRONICLE_API_TOKEN, when the API requires one
	HTTP    *http.Client
}

// NewChronicle returns a client with a request timeout.
func NewChronicle(baseURL, token string) *Chronicle {
	return &Chronicle{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

func (a *Chronicle) do(ctx context.Context, method, path string, query url.Values, into any) error {
	req, err := http.NewRequestWithContext(ctx, method, a.BaseURL+path+"?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, into)
}

// Signals returns the warning and critical events Chronicle raised in [from, to].
func (a *Chronicle) Signals(ctx context.Context, from, to time.Time) ([]rca.Signal, error) {
	var out struct {
		Events []rca.Signal `json:"events"`
	}
	q := url.Values{"signals": {"true"}, "limit": {"500"}, "from": {from.UTC().Format(time.RFC3339)}, "to": {to.UTC().Format(time.RFC3339)}}
	if err := a.do(ctx, http.MethodGet, "/api/events", q, &out); err != nil {
		return nil, err
	}
	return out.Events, nil
}

// Analyze asks Chronicle for its analysis of one event, and the healing
// decision it recorded (dry-run; nil when one was already recorded).
func (a *Chronicle) Analyze(ctx context.Context, eventID string) (*rca.Result, *heal.Action, error) {
	var out struct {
		rca.Result
		Action *heal.Action `json:"action"`
	}
	if err := a.do(ctx, http.MethodPost, "/api/analyze", url.Values{"event_id": {eventID}}, &out); err != nil {
		return nil, nil, err
	}
	return &out.Result, out.Action, nil
}

// Reachable checks the API answers before any fault is injected.
func (a *Chronicle) Reachable(ctx context.Context) error {
	now := time.Now()
	_, err := a.Signals(ctx, now.Add(-time.Minute), now)
	return err
}
