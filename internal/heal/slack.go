package heal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"
)

type SlackNotifier struct {
	webhook string
	client  *http.Client
}

func NewSlackNotifierFromEnv() *SlackNotifier {
	if webhook := os.Getenv("SLACK_WEBHOOK_URL"); webhook != "" {
		return &SlackNotifier{webhook: webhook, client: &http.Client{Timeout: 10 * time.Second}}
	}
	return nil
}

func (n *SlackNotifier) Notify(ctx context.Context, action *Action) error {
	body, _ := json.Marshal(map[string]string{"text": "Chronicle healing action " + action.Status + ": " + action.ActionType + " " + action.Namespace + "/" + action.Target + " — " + action.Result})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.webhook, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return &slackStatusError{code: resp.StatusCode}
	}
	return nil
}

type slackStatusError struct{ code int }

func (e *slackStatusError) Error() string { return "slack webhook returned non-success status" }
