package heal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/metrics"
)

// The symptom check answers a different question from the executor's Verify and
// from the outcome labeller:
//
//   - Verify: is the workload whole again (replicas ready, rollout complete)?
//   - Symptom check: did the alert that started this go away, and stay away?
//   - Outcome labeller: was the proposal what fixed it? It never credits
//     Chronicle's own actions as evidence (finding 20), and this check does not
//     change that: the result is recorded beside the outcome, never in it.
//
// The verdict is three-valued, as in progressive-delivery analysis: succeeded,
// failed, or unknown. "No data" is never recovery.
const (
	SymptomPending   = "pending"
	SymptomSucceeded = "succeeded"
	SymptomFailed    = "failed"
	SymptomUnknown   = "unknown"
)

// Defaults: the symptom must start improving within two minutes of the action,
// and then stay recovered for five.
const (
	DefaultImproveWithin = 2 * time.Minute
	DefaultSymptomHold   = 5 * time.Minute
)

// SymptomPolicy is how long to wait for improvement and how long recovery must hold.
type SymptomPolicy struct {
	ImproveWithin time.Duration
	Hold          time.Duration
}

func SymptomPolicyFromEnv() SymptomPolicy {
	return SymptomPolicy{
		ImproveWithin: durationEnv("SYMPTOM_IMPROVE_WITHIN", DefaultImproveWithin),
		Hold:          durationEnv("SYMPTOM_HOLD", DefaultSymptomHold),
	}
}

// Due is when the check gives up waiting for data.
func (p SymptomPolicy) Due(start time.Time) time.Time { return start.Add(p.ImproveWithin + p.Hold) }

// Reading is one sample of the metric behind a symptom.
type Reading struct{ Value, Threshold float64 }

var (
	// ErrNotMetric: the symptom is not a metric alert (a log line, a pod state),
	// so there is no query to ask again.
	ErrNotMetric = errors.New("the symptom is not a metric alert")
	// ErrNoSeries: the query returned nothing for this entity. No traffic, or no
	// series, is not health.
	ErrNoSeries = errors.New("the metric has no series for this entity")
)

// SymptomProbe reads the metric behind a symptom type for one entity.
type SymptomProbe interface {
	Read(ctx context.Context, symptomType, namespace, entity string) (Reading, error)
}

// SymptomTask is a check in progress.
type SymptomTask struct {
	ID               string
	ActionType       string
	Target           string
	SymptomType      string
	SymptomNamespace string
	SymptomEntity    string
	Severity         string
	StartedAt        time.Time
	ImprovedAt       *time.Time
	DueAt            time.Time
}

// SymptomStore persists checks, so a leader change resumes them.
type SymptomStore interface {
	BeginSymptomCheck(ctx context.Context, id string, startedAt, dueAt time.Time) error
	PendingSymptomChecks(ctx context.Context, limit int) ([]SymptomTask, error)
	RecordSymptomImprovement(ctx context.Context, id string, at time.Time) error
	// FinishSymptomCheck ends a pending check and returns the action, or nil if
	// it was already finished (by another pass): the caller notifies only on a
	// non-nil result, so a failure is announced once.
	FinishSymptomCheck(ctx context.Context, id, verdict, detail string, at time.Time) (*Action, error)
}

type symptomJudgement struct {
	done       bool
	verdict    string
	detail     string
	improvedAt *time.Time // set when this reading is the first below the threshold
}

// judgeSymptom is one decision on one reading, and nothing else: pure, so every
// outcome can be tested without a clock or a metrics server.
func judgeSymptom(t SymptomTask, r Reading, readErr error, now time.Time, p SymptomPolicy) symptomJudgement {
	if errors.Is(readErr, ErrNotMetric) {
		return symptomJudgement{done: true, verdict: SymptomUnknown, detail: fmt.Sprintf(
			"%s is not a metric alert, so there is nothing to measure again; the outcome labeller judges it later", t.SymptomType)}
	}
	if readErr == nil && (math.IsNaN(r.Value) || math.IsInf(r.Value, 0)) {
		// 0/0 when nothing is served: an absent signal, not a healthy one.
		readErr = ErrNoSeries
	}
	if readErr != nil {
		if now.Before(t.DueAt) {
			return symptomJudgement{}
		}
		if errors.Is(readErr, ErrNoSeries) {
			return symptomJudgement{done: true, verdict: SymptomUnknown, detail: fmt.Sprintf(
				"no data for %s on %s by the deadline: no traffic is not recovery", t.SymptomType, t.SymptomEntity)}
		}
		return symptomJudgement{done: true, verdict: SymptomUnknown, detail: "the metrics source was unavailable by the deadline: " + readErr.Error()}
	}
	if r.Value > r.Threshold { // the collector's own rule for "firing"
		if t.ImprovedAt != nil {
			return symptomJudgement{done: true, verdict: SymptomFailed, detail: fmt.Sprintf(
				"the symptom came back (%.3f, limit %.3f) after improving at %s", r.Value, r.Threshold, t.ImprovedAt.UTC().Format(time.RFC3339))}
		}
		if now.Sub(t.StartedAt) >= p.ImproveWithin {
			return symptomJudgement{done: true, verdict: SymptomFailed, detail: fmt.Sprintf(
				"no improvement within %s of the action: %s on %s is %.3f, limit %.3f", p.ImproveWithin, t.SymptomType, t.SymptomEntity, r.Value, r.Threshold)}
		}
		return symptomJudgement{}
	}
	improved, first := t.ImprovedAt, false
	if improved == nil {
		at := now
		improved, first = &at, true
	}
	j := symptomJudgement{}
	if first {
		j.improvedAt = improved
	}
	if now.Sub(*improved) >= p.Hold {
		j.done, j.verdict = true, SymptomSucceeded
		j.detail = fmt.Sprintf("%s on %s recovered at %s and stayed below %.3f for %s", t.SymptomType, t.SymptomEntity, improved.UTC().Format(time.RFC3339), r.Threshold, p.Hold)
	}
	return j
}

// SymptomNotifier says a remediation did not clear its symptom.
type SymptomNotifier interface {
	NotifySymptom(context.Context, *Action) error
}

// Pager wakes someone. It is used only for a critical symptom in production.
type Pager interface {
	Page(context.Context, *Action) error
}

// SymptomChecker runs the checks. Run it on the leader, from the worker's pass.
type SymptomChecker struct {
	Store    SymptomStore
	Probe    SymptomProbe
	Policy   SymptomPolicy
	Notifier SymptomNotifier
	Pager    Pager
	// Environment is CHRONICLE_ENVIRONMENT. Only "production" pages.
	Environment string
	Now         func() time.Time
}

// Begin starts the check for an action that has just succeeded.
func (c *SymptomChecker) Begin(ctx context.Context, id string, now time.Time) error {
	return c.Store.BeginSymptomCheck(ctx, id, now, c.Policy.Due(now))
}

// Pass advances every pending check by one reading.
func (c *SymptomChecker) Pass(ctx context.Context, now time.Time) error {
	tasks, err := c.Store.PendingSymptomChecks(ctx, 50)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		var reading Reading
		readErr := ErrNotMetric
		if c.Probe != nil {
			reading, readErr = c.Probe.Read(ctx, t.SymptomType, t.SymptomNamespace, t.SymptomEntity)
		}
		j := judgeSymptom(t, reading, readErr, now, c.Policy)
		if j.improvedAt != nil {
			if err := c.Store.RecordSymptomImprovement(ctx, t.ID, *j.improvedAt); err != nil {
				return err
			}
			t.ImprovedAt = j.improvedAt
		}
		if !j.done {
			continue
		}
		action, err := c.Store.FinishSymptomCheck(ctx, t.ID, j.verdict, j.detail, now)
		if err != nil {
			return err
		}
		if action != nil {
			metrics.SymptomChecks.WithLabelValues(j.verdict).Inc()
		}
		if action == nil || j.verdict != SymptomFailed {
			continue
		}
		slog.Warn("a healing action did not clear its symptom", "action", action.ID, "target", action.Target, "detail", j.detail)
		if c.Notifier != nil {
			if err := c.Notifier.NotifySymptom(ctx, action); err != nil {
				slog.Warn("symptom notification failed", "action", action.ID, "err", err)
			}
		}
		if c.Pager != nil && t.Severity == "critical" && c.Environment == "production" {
			if err := c.Pager.Page(ctx, action); err != nil {
				slog.Warn("page failed", "action", action.ID, "err", err)
			}
		}
	}
	return nil
}

// NewSymptomCheckerFromEnv builds a checker; the probe is supplied by the caller.
func NewSymptomCheckerFromEnv(store SymptomStore, probe SymptomProbe) *SymptomChecker {
	return &SymptomChecker{Store: store, Probe: probe, Policy: SymptomPolicyFromEnv(), Notifier: NewSlackNotifierFromEnv(),
		Pager: NewPagerFromEnv(), Environment: os.Getenv("CHRONICLE_ENVIRONMENT")}
}

// NotifySymptom posts the failure to Slack.
func (n *SlackNotifier) NotifySymptom(ctx context.Context, a *Action) error {
	if n == nil || n.webhook == "" {
		return nil
	}
	return post(ctx, n.client, n.webhook, map[string]string{"text": fmt.Sprintf(
		"Chronicle: %s on %s/%s succeeded, but the symptom did not clear. %s", a.ActionType, a.Namespace, a.Target, a.SymptomDetail)})
}

// WebhookPager posts to PAGER_WEBHOOK_URL. Nothing is sent unless it is set,
// and the checker pages only for a critical symptom in production.
type WebhookPager struct {
	url    string
	client *http.Client
}

func NewPagerFromEnv() *WebhookPager {
	if url := os.Getenv("PAGER_WEBHOOK_URL"); url != "" {
		return &WebhookPager{url: url, client: &http.Client{Timeout: 10 * time.Second}}
	}
	return nil
}

func (p *WebhookPager) Page(ctx context.Context, a *Action) error {
	if p == nil || p.url == "" {
		return nil
	}
	return post(ctx, p.client, p.url, map[string]string{
		"summary":  fmt.Sprintf("Chronicle remediation did not clear the symptom: %s on %s/%s", a.ActionType, a.Namespace, a.Target),
		"detail":   a.SymptomDetail,
		"severity": "critical",
		"action":   a.ID,
	})
}

func post(ctx context.Context, client *http.Client, url string, body map[string]string) error {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return &slackStatusError{code: resp.StatusCode}
	}
	return nil
}
