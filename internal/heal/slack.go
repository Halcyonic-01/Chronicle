package heal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type SlackNotifier struct {
	webhook string
	client  *http.Client
	// consoleURL is where the console is reachable by the people reading Slack
	// (CHRONICLE_PUBLIC_URL). Empty: the message carries no link.
	consoleURL string
}

func NewSlackNotifierFromEnv() *SlackNotifier {
	if webhook := os.Getenv("SLACK_WEBHOOK_URL"); webhook != "" {
		return &SlackNotifier{webhook: webhook, client: &http.Client{Timeout: 10 * time.Second},
			consoleURL: strings.TrimRight(strings.TrimSpace(os.Getenv("CHRONICLE_PUBLIC_URL")), "/")}
	}
	return nil
}

// message is what Slack shows for a decision: what, on what, how many people it
// needs, and a link to where they decide.
func (n *SlackNotifier) message(a *Action) string {
	text := "Chronicle healing action " + a.Status + ": " + a.ActionType + " " + a.Namespace + "/" + a.Target + " — " + a.Result
	if a.Approval == ApprovalPending && a.ApprovalsRequired > 1 {
		text += fmt.Sprintf(" (needs %d different approvers)", a.ApprovalsRequired)
	}
	if n.consoleURL != "" && a.Approval == ApprovalPending {
		text += "\nReview and decide: " + n.consoleURL + "/#healing"
	}
	return text
}

func (n *SlackNotifier) Notify(ctx context.Context, action *Action) error {
	// No webhook configured. This is reached rather than skipped because a nil
	// *SlackNotifier stored in a Notifier interface is not a nil interface.
	if n == nil || n.webhook == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]string{"text": n.message(action)})
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
