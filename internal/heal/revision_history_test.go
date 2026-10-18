package heal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

var histBase = time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)

func atMin(m int) time.Time { return histBase.Add(time.Duration(m) * time.Minute) }

func TestPriorRevisionsRebuildsTheChainNewestFirst(t *testing.T) {
	steps := []RevisionStep{
		{From: "A", To: "B", At: atMin(0)},
		{From: "B", To: "C", At: atMin(60)},
		{From: "B", To: "C", At: atMin(60)}, // the same edit, recorded as a deploy and a config change
		{From: "C", To: "D", At: atMin(120)},
	}
	got := priorRevisions(steps, "D", 5)
	if len(got) != 3 || got[0].Hash != "C" || got[1].Hash != "B" || got[2].Hash != "A" {
		t.Fatalf("%+v", got)
	}
	if !got[0].Since.Equal(atMin(60)) || !got[0].Until.Equal(atMin(120)) || !got[1].Since.Equal(atMin(0)) || !got[2].Since.IsZero() {
		t.Fatalf("periods wrong: %+v", got)
	}
	if got := priorRevisions(steps, "D", 2); len(got) != 2 {
		t.Fatalf("the limit was ignored: %+v", got)
	}
	if got := priorRevisions(steps, "Z", 5); len(got) != 0 {
		t.Fatalf("a revision with no recorded step has no history: %+v", got)
	}
	// A change that put the template back where it was is not a step to follow.
	if got := priorRevisions([]RevisionStep{{From: "A", To: "A", At: atMin(1)}}, "A", 5); len(got) != 0 {
		t.Fatalf("looped: %+v", got)
	}
}

func TestAnEditRevertedAndRepeatedDoesNotLoopTheChain(t *testing.T) {
	steps := []RevisionStep{{From: "A", To: "B", At: atMin(0)}, {From: "B", To: "A", At: atMin(10)}, {From: "A", To: "B", At: atMin(20)}}
	got := priorRevisions(steps, "B", 5)
	if len(got) > 5 || len(got) == 0 || got[0].Hash != "A" {
		t.Fatalf("%+v", got)
	}
}

func TestAnUnobservedRevisionIsNotGoodEnough(t *testing.T) {
	healthy := RevisionPeriod{Hash: "B", Since: atMin(0), Until: atMin(60)}
	cases := []struct {
		name     string
		p        RevisionPeriod
		observed time.Time
		failures []time.Time
		want     bool
		contains string
	}{
		{"ran long enough, no failure", healthy, time.Time{}, nil, true, "without a recorded failure"},
		{"ran too briefly", RevisionPeriod{Hash: "B", Since: atMin(0), Until: atMin(10)}, time.Time{}, nil, false, "less than the 30m0s required"},
		{"a failure while it ran", healthy, time.Time{}, []time.Time{atMin(30)}, false, "a failure was recorded"},
		{"a failure only in the rollout that brought it in", healthy, time.Time{}, []time.Time{atMin(1)}, true, ""},
		{"a failure after it was replaced does not count", healthy, time.Time{}, []time.Time{atMin(61)}, true, ""},
		{"beginning unseen and no record at all", RevisionPeriod{Hash: "A", Until: atMin(60)}, time.Time{}, nil, false, "no record of this workload"},
		{"beginning unseen, observed long enough", RevisionPeriod{Hash: "A", Until: atMin(60)}, atMin(5), nil, true, ""},
		{"beginning unseen, observed too briefly", RevisionPeriod{Hash: "A", Until: atMin(60)}, atMin(50), nil, false, "already running when Chronicle began watching"},
	}
	for _, c := range cases {
		got, why := judgeRevision(c.p, c.observed, c.failures, 30*time.Minute)
		if got != c.want || !strings.Contains(why, c.contains) {
			t.Errorf("%s: got %v %q, want %v containing %q", c.name, got, why, c.want, c.contains)
		}
	}
}

// ---- the engine's choice ---------------------------------------------------

type historyAudit struct {
	memoryAudit
	prior   []RevisionPeriod
	healthy map[string]bool
	err     error
}

func (h *historyAudit) PriorRevisions(context.Context, string, string, string, int) ([]RevisionPeriod, error) {
	return h.prior, h.err
}
func (h *historyAudit) RanHealthily(_ context.Context, _, _ string, p RevisionPeriod, _ time.Duration) (bool, string, error) {
	if h.healthy[p.Hash] {
		return true, "ran for 47m without a recorded failure", nil
	}
	return false, "it was seen running for 4m0s, less than the 30m0s required", nil
}

func rollbackResult() *rca.Result {
	cause := event.Event{ID: "c1", Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "deploy",
		Payload: []byte(`{"old_image":"api:v1","new_image":"api:v2","from_template_hash":"PREV","to_template_hash":"BAD"}`)}
	return &rca.Result{Symptom: event.Event{ID: "s1"}, Confidence: 0.95, Verdict: rca.VerdictRootCause, Candidates: []rca.Candidate{{Event: cause}}}
}

func TestTheRollbackTargetIsThePreviousRevisionWhenItRanHealthily(t *testing.T) {
	store := &historyAudit{prior: []RevisionPeriod{{Hash: "PREV"}, {Hash: "OLDER"}}, healthy: map[string]bool{"PREV": true, "OLDER": true}}
	a, err := NewEngine(store).Evaluate(context.Background(), rollbackResult())
	if err != nil || !a.Proposed {
		t.Fatalf("%+v, %v", a, err)
	}
	if got := gjson.GetBytes(a.Payload, "target_template_hash").String(); got != "PREV" {
		t.Fatalf("target %q", got)
	}
}

// Decision 3: when the previous revision is not known to be good, propose a
// verified older one, and say that a revision was skipped.
func TestTheRollbackSkipsToAnOlderRevisionThatIsKnownGood(t *testing.T) {
	store := &historyAudit{prior: []RevisionPeriod{{Hash: "PREV"}, {Hash: "OLDER"}}, healthy: map[string]bool{"OLDER": true}}
	a, err := NewEngine(store).Evaluate(context.Background(), rollbackResult())
	if err != nil || !a.Proposed {
		t.Fatalf("%+v, %v", a, err)
	}
	if got := gjson.GetBytes(a.Payload, "target_template_hash").String(); got != "OLDER" {
		t.Fatalf("target %q", got)
	}
	if !strings.Contains(strings.Join(a.Reasoning, " "), "skips 1 revision") {
		t.Fatalf("the skip is not explained: %v", a.Reasoning)
	}
}

func TestNoRollbackIsProposedWhenNoRevisionIsKnownGood(t *testing.T) {
	store := &historyAudit{prior: []RevisionPeriod{{Hash: "PREV"}, {Hash: "OLDER"}}}
	a, err := NewEngine(store).Evaluate(context.Background(), rollbackResult())
	if err != nil {
		t.Fatal(err)
	}
	if a.Proposed || a.Approval == ApprovalPending || a.Status != StatusBlocked || !strings.Contains(a.Result, "no earlier revision of api is known to have run healthily") {
		t.Fatalf("%+v", a)
	}
}

func TestNoRollbackIsProposedWithoutAHistoryToCheck(t *testing.T) {
	for name, store := range map[string]AuditStore{
		"a store that cannot answer": &memoryAudit{},
		"no earlier revision":        &historyAudit{},
	} {
		a, err := NewEngine(store).Evaluate(context.Background(), rollbackResult())
		if err != nil || a.Proposed || a.Status != StatusBlocked {
			t.Errorf("%s: %+v, %v", name, a, err)
		}
	}
	if _, err := NewEngine(&historyAudit{err: errors.New("db down")}).Evaluate(context.Background(), rollbackResult()); err == nil {
		t.Fatal("a failed history read must not become a proposal")
	}
}

// Events recorded before template hashes existed keep the old behaviour.
func TestAnOlderEventWithOnlyImagesIsStillProposed(t *testing.T) {
	r := rollbackResult()
	r.Candidates[0].Event.Payload = []byte(`{"old_image":"api:v1","new_image":"api:v2"}`)
	a, err := NewEngine(&memoryAudit{}).Evaluate(context.Background(), r)
	if err != nil || !a.Proposed {
		t.Fatalf("%+v, %v", a, err)
	}
}

// A rule floor that is not met says so in the floor's own words, before any
// history is consulted.
func TestALowConfidenceRollbackIsRefusedByTheFloorFirst(t *testing.T) {
	r := rollbackResult()
	r.Confidence = 0.5
	a, _ := NewEngine(&historyAudit{}).Evaluate(context.Background(), r)
	if a.Status != StatusBlocked || !strings.Contains(a.Result, "is below") {
		t.Fatalf("%+v", a)
	}
}

// ---- the configuration rule ------------------------------------------------

func configResult(payload string) *rca.Result {
	cause := event.Event{ID: "cfg1", Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "config_change",
		Payload: []byte(payload)}
	return &rca.Result{Symptom: event.Event{ID: "s9"}, Confidence: 0.93, Verdict: rca.VerdictRootCause, Candidates: []rca.Candidate{{Event: cause}}}
}

func TestABadConfigChangeIsRevertedThroughTheExactRevisionRollback(t *testing.T) {
	store := &historyAudit{prior: []RevisionPeriod{{Hash: "PREV"}}, healthy: map[string]bool{"PREV": true}}
	a, err := NewEngine(store).Evaluate(context.Background(), configResult(
		`{"changed":["api env REDIS_URL"],"from_hash":"c1","to_hash":"c2","from_template_hash":"PREV","to_template_hash":"BAD"}`))
	if err != nil || !a.Proposed || a.Rule != "revert-bad-config" || a.ActionType != ActionRollbackDeployment {
		t.Fatalf("%+v, %v", a, err)
	}
	if a.Approval != ApprovalPending || a.Workload != "api" || gjson.GetBytes(a.Payload, "target_template_hash").String() != "PREV" {
		t.Fatalf("unexpected decision %+v payload %s", a, a.Payload)
	}
}

// A config change recorded before revisions were captured has nothing exact to
// return to: it is not proposed, and the reason says why.
func TestAConfigChangeWithoutRevisionEvidenceIsNotProposed(t *testing.T) {
	a, err := NewEngine(&historyAudit{}).Evaluate(context.Background(), configResult(`{"changed":["api env X"],"from_hash":"c1","to_hash":"c2"}`))
	if err != nil || a.Proposed || !strings.Contains(a.Result, "revisions before and after") {
		t.Fatalf("%+v, %v", a, err)
	}
}

func TestAConfigChangeOnAGitOpsWorkloadIsProposedAsAGitRevertOnly(t *testing.T) {
	a, err := NewEngine(&historyAudit{}).Evaluate(context.Background(), configResult(
		`{"gitops":"argocd:shop","from_template_hash":"PREV","to_template_hash":"BAD"}`))
	if err != nil || a.Proposed || !strings.Contains(a.Result, "revert the change in Git") {
		t.Fatalf("%+v, %v", a, err)
	}
	a, _ = NewEngine(&historyAudit{}).Evaluate(context.Background(), &rca.Result{Symptom: event.Event{ID: "s10"}, Confidence: 0.95, Verdict: rca.VerdictRootCause,
		Candidates: []rca.Candidate{{Event: event.Event{ID: "cfg2", Namespace: "default", EntityKind: "Deployment", EntityName: "api", Type: "config_change",
			Payload: []byte(`{"gitops":"flagger:api","from_template_hash":"PREV","to_template_hash":"BAD"}`)}}}})
	if a.Proposed || !strings.Contains(a.Result, "will not compete") {
		t.Fatalf("a Flagger-managed workload was proposed a competing rollback: %+v", a)
	}
}

// Decision 6: ConfigMap and Secret contents are permanently out of scope for
// remediation. A config_change on one is never turned into a Deployment rollback.
func TestAConfigMapChangeIsNeverReverted(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		r := configResult(`{"changed":["REDIS_ADDR"],"from_hash":"m1","to_hash":"m2","from_template_hash":"PREV","to_template_hash":"BAD"}`)
		r.Candidates[0].Event.EntityKind, r.Candidates[0].Event.EntityName = kind, "api-config"
		a, err := NewEngine(&historyAudit{prior: []RevisionPeriod{{Hash: "PREV"}}, healthy: map[string]bool{"PREV": true}}).Evaluate(context.Background(), r)
		if err != nil || a.Proposed || a.Approval == ApprovalPending || !strings.Contains(a.Result, "never reverted") {
			t.Errorf("%s: %+v, %v", kind, a, err)
		}
	}
}
