package heal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type executionMemory struct {
	claimed bool
	status  string
}

func (m *executionMemory) ClaimExecution(context.Context, string, time.Time) (bool, error) {
	if m.claimed {
		return false, nil
	}
	m.claimed = true
	return true, nil
}
func (m *executionMemory) CompleteExecution(_ context.Context, _ string, status, _, _, _ string) error {
	m.status = status
	return nil
}

type recordingExecutor struct{ calls int }

func (e *recordingExecutor) Execute(context.Context, *Action) (string, error) {
	e.calls++
	return "deleted", nil
}
func (e *recordingExecutor) Verify(context.Context, *Action) (string, error) { return "verified", nil }

func livePolicy() Policy {
	return Policy{
		LiveEnabled:       true,
		AllowedActions:    map[string]bool{ActionRestartPod: true},
		AllowedNamespaces: map[string]bool{"default": true},
		AllowedTargets:    map[string]bool{"default/redis": true},
		ObservationSince:  time.Now().Add(-31 * 24 * time.Hour),
		Timeout:           time.Second,
	}
}

func TestPolicyAllowsOnlyAllowlistedPodRestart(t *testing.T) {
	p := livePolicy()
	if err := p.Allows(&Action{ActionType: ActionRollbackDeployment, Namespace: "default", Target: "redis"}); err == nil {
		t.Fatal("rollback should remain disabled")
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "monitoring", Target: "redis"}); err == nil {
		t.Fatal("unexpected namespace was allowed")
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis"}); err != nil {
		t.Fatal(err)
	}
}

func TestControllerExecutesAndVerifiesApprovedRestart(t *testing.T) {
	store := &executionMemory{}
	executor := &recordingExecutor{}
	controller := &Controller{Store: store, Executor: executor, Policy: livePolicy()}
	action := &Action{ID: "action-1", ActionType: ActionRestartPod, Namespace: "default", Target: "redis", Approval: ApprovalApproved, DryRun: true}
	got, err := controller.ExecuteApproved(context.Background(), action)
	if err != nil {
		t.Fatal(err)
	}
	if executor.calls != 1 || store.status != StatusSucceeded || got.Verification != "verified" || got.DryRun {
		t.Fatalf("unexpected execution: %+v", got)
	}
}

func TestControllerKillSwitchBlocksWithoutExecutorCall(t *testing.T) {
	store := &executionMemory{}
	executor := &recordingExecutor{}
	policy := livePolicy()
	policy.KillSwitch = true
	controller := &Controller{Store: store, Executor: executor, Policy: policy}
	action := &Action{ID: "action-2", ActionType: ActionRestartPod, Namespace: "default", Target: "redis", Approval: ApprovalApproved}
	if _, err := controller.ExecuteApproved(context.Background(), action); err == nil {
		t.Fatal("kill switch did not block")
	}
	if executor.calls != 0 || store.status != StatusBlocked {
		t.Fatalf("unexpected blocked execution: calls=%d status=%s", executor.calls, store.status)
	}
}

// NewSlackNotifierFromEnv returns a nil *SlackNotifier when no webhook is set,
// and a typed nil in an interface is not a nil interface -- so the guard has to
// live on the receiver or the first notification panics.
func TestNotifyIsSafeWithoutAWebhook(t *testing.T) {
	t.Setenv("SLACK_WEBHOOK_URL", "")
	var n Notifier = NewSlackNotifierFromEnv()
	if n == nil {
		t.Skip("interface is genuinely nil here; the guard is not needed")
	}
	if err := n.Notify(context.Background(), &Action{Status: "would_run", ActionType: "restart_pod"}); err != nil {
		t.Fatalf("an unconfigured notifier should do nothing, got %v", err)
	}
}

type stubStore struct {
	earliest time.Time
	claimed  bool
}

func (s *stubStore) ClaimExecution(context.Context, string, time.Time) (bool, error) {
	return s.claimed, nil
}
func (s *stubStore) CompleteExecution(context.Context, string, string, string, string, string) error {
	return nil
}
func (s *stubStore) EarliestActionAt(context.Context) (time.Time, error) { return s.earliest, nil }

// An unset HEAL_OBSERVATION_STARTED_AT used to mean "never", so the gate looked
// like a countdown while being permanently shut. It now starts when Chronicle
// first recorded a decision.
func TestObservationPeriodStartsFromTheFirstRecordedDecision(t *testing.T) {
	first := time.Now().Add(-10 * 24 * time.Hour)
	c := &Controller{Store: &stubStore{earliest: first}}

	if got := c.ObservationStart(context.Background()); !got.Equal(first) {
		t.Fatalf("expected the first decision %v, got %v", first, got)
	}
	remaining, started := c.ObservationRemaining(context.Background())
	if !started {
		t.Fatal("observation has started; the countdown should be real")
	}
	if days := int(remaining.Hours() / 24); days < 19 || days > 20 {
		t.Fatalf("expected about 20 days remaining, got %d", days)
	}
}

func TestObservationPeriodReportsNotStartedWhenNothingRecorded(t *testing.T) {
	c := &Controller{Store: &stubStore{}}
	if _, started := c.ObservationRemaining(context.Background()); started {
		t.Fatal("nothing observed yet, so the period cannot have started")
	}
	if err := (Policy{}).Allows(&Action{ActionType: ActionRestartPod}); err == nil {
		t.Fatal("an unstarted observation period must still block execution")
	}
}

// An explicit override wins, so an operator can vouch for earlier observation.
func TestObservationPeriodHonoursAnExplicitOverride(t *testing.T) {
	override := time.Now().Add(-60 * 24 * time.Hour)
	c := &Controller{Store: &stubStore{earliest: time.Now()}, Policy: Policy{ObservationSince: override}}
	if got := c.ObservationStart(context.Background()); !got.Equal(override) {
		t.Fatalf("the configured start should win, got %v", got)
	}
}

func TestRetryClassificationOnlyRepeatsTransientFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"conflict", apierrors.NewConflict(schema.GroupResource{Resource: "deployments"}, "redis", errors.New("changed")), true},
		{"server timeout", apierrors.NewServerTimeout(schema.GroupResource{Resource: "pods"}, "get", 1), true},
		{"too many requests", apierrors.NewTooManyRequests("slow down", 1), true},
		{"not found", apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "redis"), false},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "redis", errors.New("no")), false},
		{"our own refusal", errors.New("refusing to restore: an autoscaler owns the replica count"), false},
	}
	for _, c := range cases {
		if got := retryableExecution(c.err); got != c.want {
			t.Errorf("%s: retryable = %v, want %v", c.name, got, c.want)
		}
	}
}

// HEAL_RULES lets a threshold be retuned without a rebuild, but a bad value
// must not disarm the engine.
func TestRulesFromEnv(t *testing.T) {
	t.Setenv("HEAL_RULES", "")
	if got := RulesFromEnv(); len(got) != len(defaultRules) {
		t.Fatalf("unset should keep the defaults, got %d rules", len(got))
	}
	t.Setenv("HEAL_RULES", `[{"name":"custom","cause_type":"scale","action_type":"restore_replicas","min_confidence":0.5,"max_per_hour":9}]`)
	got := RulesFromEnv()
	if len(got) != 1 || got[0].Name != "custom" || got[0].MinConfidence != 0.5 {
		t.Fatalf("a valid override should replace the defaults, got %+v", got)
	}
	for _, bad := range []string{`not json`, `[]`, `[{"name":"x","cause_type":"scale","action_type":"launch_missiles"}]`} {
		t.Setenv("HEAL_RULES", bad)
		if got := RulesFromEnv(); len(got) != len(defaultRules) {
			t.Fatalf("%q should fall back to the defaults, got %d", bad, len(got))
		}
	}
}

func TestPolicyFromEnvIsDefaultDeny(t *testing.T) {
	for _, k := range []string{"HEAL_LIVE_ENABLED", "HEAL_KILL_SWITCH", "HEAL_ALLOWED_ACTIONS", "HEAL_ALLOWED_NAMESPACES", "HEAL_ALLOWED_TARGETS", "HEAL_ACTION_TIMEOUT", "HEAL_OBSERVATION_STARTED_AT"} {
		t.Setenv(k, "")
	}
	p := PolicyFromEnv()
	if p.LiveEnabled {
		t.Error("live execution must be off unless explicitly enabled")
	}
	if !p.KillSwitch {
		t.Error("the kill switch must be on unless explicitly set to false")
	}
	if p.Timeout != 30*time.Second {
		t.Errorf("expected the default timeout, got %v", p.Timeout)
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis"}); err == nil {
		t.Error("an empty configuration must permit nothing")
	}

	t.Setenv("HEAL_KILL_SWITCH", "false")
	t.Setenv("HEAL_ACTION_TIMEOUT", "45s")
	t.Setenv("HEAL_OBSERVATION_STARTED_AT", "2020-01-01T00:00:00Z")
	p = PolicyFromEnv()
	if p.KillSwitch || p.Timeout != 45*time.Second || p.ObservationSince.IsZero() {
		t.Errorf("explicit configuration was not applied: %+v", p)
	}
}

func TestApprovalAuthorizedRequiresAConfiguredToken(t *testing.T) {
	req := func(header, value string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/api/heal/actions/x/approve", nil)
		if header != "" {
			r.Header.Set(header, value)
		}
		return r
	}
	t.Setenv("HEAL_APPROVAL_TOKEN", "")
	if ApprovalAuthorized(req("X-Chronicle-Heal-Token", "anything")) {
		t.Fatal("with no token configured nothing may be approved")
	}

	t.Setenv("HEAL_APPROVAL_TOKEN", "s3cret")
	if ApprovalAuthorized(req("", "")) {
		t.Error("a missing token must be rejected")
	}
	if ApprovalAuthorized(req("X-Chronicle-Heal-Token", "wrong")) {
		t.Error("a wrong token must be rejected")
	}
	if !ApprovalAuthorized(req("X-Chronicle-Heal-Token", "s3cret")) {
		t.Error("the configured token should be accepted")
	}
	if !ApprovalAuthorized(req("Authorization", "Bearer s3cret")) {
		t.Error("a bearer token should still be accepted")
	}
}

func TestExecutionBackoffGrows(t *testing.T) {
	if executionBackoff(1) >= executionBackoff(2) || executionBackoff(2) >= executionBackoff(3) {
		t.Fatal("each retry should wait longer than the last")
	}
}

// The gate the observation period was missing: elapsed time is a calendar, not
// a standard. A month could pass with every decision unjudged.
func TestAutonomyNeedsEvidenceNotJustElapsedTime(t *testing.T) {
	open := func(e EvidenceSummary) Policy {
		return Policy{
			LiveEnabled: true, ObservationSince: time.Now().Add(-40 * 24 * time.Hour),
			AllowedActions:    csvSet("restart_pod"),
			AllowedNamespaces: map[string]bool{"default": true},
			AllowedTargets:    map[string]bool{"default/redis": true},
			MinDecisive:       20, MinPrecision: 0.8, Evidence: e,
		}
	}
	a := &Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis"}

	// A month of nothing but unjudged decisions must not open the gate.
	err := open(EvidenceSummary{Unknown: 500}).Allows(a)
	if err == nil {
		t.Fatal("500 unjudged decisions are not evidence")
	}
	if !strings.Contains(err.Error(), "unjudged") {
		t.Errorf("the refusal should say the decisions were unjudged, got %v", err)
	}

	// Enough decisions, but too often wrong.
	if err := open(EvidenceSummary{Confirmed: 12, Contradicted: 12}).Allows(a); err == nil {
		t.Fatal("50% precision must not clear an 80% bar")
	}

	// Enough decisive decisions, right often enough.
	if err := open(EvidenceSummary{Confirmed: 22, Contradicted: 3, Unknown: 900}).Allows(a); err != nil {
		t.Fatalf("22/25 correct should clear the bar even alongside many unknowns: %v", err)
	}
}

func TestEvidenceSummaryArithmetic(t *testing.T) {
	e := EvidenceSummary{Confirmed: 3, Contradicted: 1, Unknown: 96}
	if e.Decisive() != 4 {
		t.Errorf("unknowns must not count as decisive, got %d", e.Decisive())
	}
	if got := e.Precision(); got != 0.75 {
		t.Errorf("precision should be 3/4, got %v", got)
	}
	if got := (EvidenceSummary{Unknown: 10}).Precision(); got != 0 {
		t.Errorf("no decisive decisions means no precision, got %v", got)
	}
}

func TestEvidenceThresholdsAreConfigurable(t *testing.T) {
	t.Setenv("HEAL_MIN_DECISIVE_DECISIONS", "5")
	t.Setenv("HEAL_MIN_PRECISION", "0.6")
	p := PolicyFromEnv()
	if p.MinDecisive != 5 || p.MinPrecision != 0.6 {
		t.Fatalf("configured thresholds not applied: %+v", p)
	}
	t.Setenv("HEAL_MIN_PRECISION", "nonsense")
	if got := PolicyFromEnv().MinPrecision; got != 0.80 {
		t.Fatalf("an invalid value should fall back to the default, got %v", got)
	}
}

// One outage analysed many times is one piece of evidence. Before this, forty
// decisions about a single redis failure cleared a twenty-decision bar.
func TestEvidenceIsCountedPerOutageNotPerDecision(t *testing.T) {
	e := EvidenceSummary{Confirmed: 1, Contradicted: 0, Unknown: 0}
	if e.Decisive() != 1 {
		t.Fatalf("one outage is one piece of evidence, got %d", e.Decisive())
	}
	p := Policy{
		LiveEnabled: true, ObservationSince: time.Now().Add(-40 * 24 * time.Hour),
		AllowedActions:    csvSet("restart_pod"),
		AllowedNamespaces: map[string]bool{"default": true},
		AllowedTargets:    map[string]bool{"default/redis": true},
		MinDecisive:       20, MinPrecision: 0.8, Evidence: e,
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis"}); err == nil {
		t.Fatal("a single outage must not clear a twenty-outage bar")
	}
}

// Being right but declining to act measures the floor, not the engine, so it
// must not count toward the evidence that acting is safe.
func TestBlockedButCorrectIsReportedNotCredited(t *testing.T) {
	e := EvidenceSummary{Confirmed: 0, Contradicted: 0, MissedByFloor: 38}
	if e.Decisive() != 0 {
		t.Fatalf("decisions the floor blocked never tested autonomy, got decisive=%d", e.Decisive())
	}
	if e.Precision() != 0 {
		t.Fatalf("no acted-on decisions means no precision, got %v", e.Precision())
	}
	p := Policy{
		LiveEnabled: true, ObservationSince: time.Now().Add(-40 * 24 * time.Hour),
		AllowedActions:    csvSet("restart_pod"),
		AllowedNamespaces: map[string]bool{"default": true},
		AllowedTargets:    map[string]bool{"default/redis": true},
		MinDecisive:       20, MinPrecision: 0.8, Evidence: e,
	}
	if err := p.Allows(&Action{ActionType: ActionRestartPod, Namespace: "default", Target: "redis"}); err == nil {
		t.Fatal("38 correctly-declined decisions are not evidence that acting is safe")
	}
}
