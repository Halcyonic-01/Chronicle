package heal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const idp = "https://idp.example.com"

func approve(id, who string, now time.Time) Decision {
	return Decision{ID: id, Approved: true, By: who + "@example.com", Issuer: idp, Subject: who, Email: who + "@example.com",
		Groups: []string{"approvers"}, Result: "APPROVED; waiting for the worker", Now: now}
}

func needsTwo(a *Action) *Action {
	a.ApprovalsRequired = 2
	return a
}

// Two approvers means two different people, counted once each.
func TestPostgresAnActionNeedingTwoApproversWaitsForTheSecondPerson(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	a := needsTwo(proposal("two-"+sx, "c-two-"+sx, "redis", now))
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}

	first, err := s.DecideAction(ctx, approve(a.ID, "ana", now))
	if err != nil {
		t.Fatal(err)
	}
	if first.State != "pending_approval" || first.Approval != ApprovalPending || len(first.Approvals) != 1 || first.ApprovalsRequired != 2 {
		t.Fatalf("after one approval of two: %+v", first)
	}
	// The same person again is not a second person, whatever the display name.
	again := approve(a.ID, "ana", now)
	again.By = "Someone Else"
	if _, err := s.DecideAction(ctx, again); !errors.Is(err, ErrAlreadyApproved) {
		t.Fatalf("a second approval by the same subject: %v", err)
	}
	if mid, _ := s.GetAction(ctx, a.ID); mid.State != "pending_approval" || len(mid.Approvals) != 1 {
		t.Fatalf("a repeated approval changed the decision: %+v", mid)
	}
	// Another person completes it.
	done, err := s.DecideAction(ctx, approve(a.ID, "bea", now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if done.State != "approved" || done.Approval != ApprovalApproved || DistinctApprovers(done.Approvals) != 2 || done.DecisionSubject != "bea" {
		t.Fatalf("after two: %+v", done)
	}
	// The worker's queue returns it with both approvers, for the re-check.
	queue, err := s.ApprovedActions(ctx, now, 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range queue {
		if q.ID == a.ID && DistinctApprovers(q.Approvals) == 2 {
			return
		}
	}
	t.Fatal("the approved action did not reach the work queue with its two approvers")
}

func TestPostgresOneDenialEndsAnActionEvenAfterAnApproval(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	a := needsTwo(proposal("deny-"+sx, "c-deny-"+sx, "redis", now))
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideAction(ctx, approve(a.ID, "ana", now)); err != nil {
		t.Fatal(err)
	}
	denied, err := s.DecideAction(ctx, Decision{ID: a.ID, Approved: false, By: "bea", Issuer: idp, Subject: "bea", Now: now})
	if err != nil || denied.Status != StatusDenied {
		t.Fatalf("a denial after an approval: %+v %v", denied, err)
	}
	if _, err := s.DecideAction(ctx, approve(a.ID, "cat", now)); !errors.Is(err, ErrNotPending) {
		t.Fatalf("a denied action took another approval: %v", err)
	}
}

func TestPostgresTheSharedTokenCannotApproveWhatNeedsTwoPeople(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	two := needsTwo(proposal("sh2-"+sx, "c-sh2-"+sx, "redis", now))
	one := proposal("sh1-"+sx, "c-sh1-"+sx, "cache", now)
	for _, a := range []*Action{two, one} {
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.DecideAction(ctx, Decision{ID: two.ID, Approved: true, By: "shared-token:ana", Now: now}); !errors.Is(err, ErrNeedsIdentity) {
		t.Fatalf("the shared token approved an action that needs two: %v", err)
	}
	if got, _ := s.GetAction(ctx, two.ID); got.State != "pending_approval" || len(got.Approvals) != 0 {
		t.Fatalf("a refused approval left a mark: %+v", got)
	}
	if got, err := s.DecideAction(ctx, Decision{ID: one.ID, Approved: true, By: "shared-token:ana", Now: now}); err != nil || got.State != "approved" {
		t.Fatalf("one approver, shared token: %+v %v", got, err)
	}
}

// Two people approving in the same instant must both be counted: neither may read
// "one so far" and leave the action stuck, and it must become approved once.
func TestPostgresSimultaneousApprovalsAreBothCounted(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	for trial := 0; trial < 15; trial++ {
		a := needsTwo(proposal(fmt.Sprintf("race%d-%s", trial, sx), fmt.Sprintf("c-race%d-%s", trial, sx), "redis", now))
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, who := range []string{"ana", "bea"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = s.DecideAction(ctx, approve(a.ID, who, now))
			}()
		}
		wg.Wait()
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("trial %d: %v %v", trial, errs[0], errs[1])
		}
		if got, _ := s.GetAction(ctx, a.ID); got.State != "approved" || DistinctApprovers(got.Approvals) != 2 {
			t.Fatalf("trial %d: two simultaneous approvals left %+v", trial, got)
		}
	}
}

func TestPostgresAnExpiredActionTakesNoApproval(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	a := needsTwo(proposal("exp-"+sx, "c-exp-"+sx, "redis", now.Add(-time.Hour)))
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideAction(ctx, approve(a.ID, "ana", now)); !errors.Is(err, ErrExpired) {
		t.Fatalf("got %v", err)
	}
	if got, _ := s.GetAction(ctx, a.ID); got.Status != StatusExpired || len(got.Approvals) != 0 {
		t.Fatalf("%+v", got)
	}
}

// ---- the check at execution ------------------------------------------------

func approvedBy(a *Action, who ...string) *Action {
	for _, w := range who {
		a.Approvals = append(a.Approvals, Approval{Issuer: idp, Subject: w, Display: w, Groups: []string{"approvers"}, At: time.Now()})
	}
	return a
}

func TestTheWorkerRefusesAnActionWithTooFewDistinctApprovers(t *testing.T) {
	c := liveController(&executionMemory{}, &recordingExecutor{})
	c.Policy.TwoApprover = map[string]bool{ActionRestartPod: true}
	rule := &c.Rules[0]

	one := approvedBy(approvedRestart("one"), "ana")
	one.ApprovalsRequired = 2
	if reason := c.approvalsShort(one, rule); reason == "" {
		t.Fatal("one approver of two was enough")
	}
	// Two rows for one person are one person.
	dup := approvedBy(approvedRestart("dup"), "ana", "ana")
	dup.ApprovalsRequired = 2
	if reason := c.approvalsShort(dup, rule); reason == "" {
		t.Fatal("the same person twice was enough")
	}
	// A policy tightened after the proposal reaches it: proposed under one approver, now two are needed.
	old := approvedBy(approvedRestart("old"), "ana")
	old.ApprovalsRequired = 1
	if reason := c.approvalsShort(old, rule); reason == "" {
		t.Fatal("a tightened policy did not reach a decision already approved")
	}
	two := approvedBy(approvedRestart("two"), "ana", "bea")
	two.ApprovalsRequired = 2
	if reason := c.approvalsShort(two, rule); reason != "" {
		t.Fatalf("two distinct approvers were refused: %s", reason)
	}
}

func TestTheWorkerRefusesAnApproverWhoIsNoLongerPermitted(t *testing.T) {
	c := liveController(&executionMemory{}, &recordingExecutor{})
	c.Policy.TwoApprover = map[string]bool{ActionRestartPod: true}
	c.ApproverStillPermitted = func(a Approval) bool { return a.Subject != "bea" }
	a := approvedBy(approvedRestart("revoked"), "ana", "bea")
	a.ApprovalsRequired = 2
	if reason := c.approvalsShort(a, &c.Rules[0]); reason == "" {
		t.Fatal("an approver removed from the allowlist was still honoured")
	}
	c.ApproverStillPermitted = func(Approval) bool { return true }
	if reason := c.approvalsShort(a, &c.Rules[0]); reason != "" {
		t.Fatalf("%s", reason)
	}
}

// Decisions approved before this existed carry one approver in decided_by and no
// approval rows; they stay executable while policy asks for one.
func TestAnActionApprovedBeforeApprovalRowsExistedStillRunsWhenOneIsEnough(t *testing.T) {
	c := liveController(&executionMemory{}, &recordingExecutor{})
	c.Policy.TwoApprover = map[string]bool{}
	legacy := approvedRestart("legacy") // ApprovalsRequired 0, no approvals
	if reason := c.approvalsShort(legacy, &c.Rules[0]); reason != "" {
		t.Fatalf("%s", reason)
	}
}

func TestExecuteApprovedBlocksWithoutEnoughApprovers(t *testing.T) {
	store, executor := &executionMemory{}, &recordingExecutor{}
	c := liveController(store, executor)
	c.Policy.TwoApprover = map[string]bool{ActionRestartPod: true}
	a := approvedBy(approvedRestart("gate"), "ana")
	a.ApprovalsRequired = 2
	if _, err := c.ExecuteApproved(context.Background(), a); err == nil || store.status != StatusBlocked || executor.calls != 0 {
		t.Fatalf("err %v status %q calls %d", err, store.status, executor.calls)
	}
}

// ---- the engine ------------------------------------------------------------

func TestTheEngineAsksForTwoApproversOnlyWhereConfigured(t *testing.T) {
	two := map[string]bool{ActionRollbackDeployment: true}
	rule := func(action string, n int) *Rule { return &Rule{ActionType: action, Approvers: n} }
	cases := []struct {
		rule   *Rule
		action string
		want   int
	}{
		{rule(ActionRollbackDeployment, 0), ActionRollbackDeployment, 2},
		{rule(ActionRestartPod, 0), ActionRestartPod, 1},
		{rule(ActionRestartPod, 2), ActionRestartPod, 2},                 // a rule can ask for more
		{rule(ActionRollbackDeployment, 1), ActionRollbackDeployment, 2}, // and never for less
		{rule(ActionRollbackDeployment, 3), ActionRollbackDeployment, 3},
	}
	for _, c := range cases {
		if got := approversFor(c.rule, c.action, two); got != c.want {
			t.Errorf("%+v for %s: %d, want %d", c.rule, c.action, got, c.want)
		}
	}
}

func TestTwoApproverActionsFollowTheEnvironment(t *testing.T) {
	cases := map[string][]string{
		"":                                 {ActionRollbackDeployment, ActionBumpMemory},
		"none":                             {},
		"restart_pod":                      {ActionRestartPod},
		"restart_pod, rollback_deployment": {ActionRestartPod, ActionRollbackDeployment},
	}
	for value, want := range cases {
		t.Setenv("HEAL_TWO_APPROVER_ACTIONS", value)
		got := TwoApproverActionsFromEnv()
		if len(got) != len(want) {
			t.Errorf("%q: %v, want %v", value, got, want)
		}
		for _, a := range want {
			if !got[a] {
				t.Errorf("%q: %s is missing from %v", value, a, got)
			}
		}
	}
}

// A proposal records how many approvers it needs, and says so in its reasoning.
func TestARollbackProposalCarriesTheTwoApproverRequirement(t *testing.T) {
	store := &memoryAudit{}
	engine := NewEngine(store)
	engine.TwoApprover = map[string]bool{ActionRollbackDeployment: true}
	action, err := engine.Evaluate(context.Background(), restartResult(`{"x":1}`))
	if err != nil || action.ActionType != ActionRestartPod || action.ApprovalsRequired != 1 {
		t.Fatalf("a restart: %+v %v", action, err)
	}

	engine = NewEngine(&memoryAudit{})
	engine.TwoApprover = map[string]bool{ActionRestartPod: true}
	action, err = engine.Evaluate(context.Background(), restartResult(`{"x":1}`))
	if err != nil || action.ApprovalsRequired != 2 {
		t.Fatalf("a restart configured to need two: %+v %v", action, err)
	}
	found := false
	for _, r := range action.Reasoning {
		found = found || r == "needs 2 distinct approvers"
	}
	if !found {
		t.Fatalf("the reviewer is not told: %v", action.Reasoning)
	}
}
