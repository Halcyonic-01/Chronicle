package heal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/gitops"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

var gitNow = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

// gitopsStore is a decision store that can answer what a GitOps application
// deployed and whether a revision ran healthily.
type gitopsStore struct {
	memoryAudit
	revisions map[string][]GitOpsRevision // "argocd/api" -> newest first
	unhealthy string                      // non-empty: the reason RanHealthily says no
	asked     RevisionPeriod
}

func (s *gitopsStore) GitOpsRevisions(_ context.Context, controller, app string, at time.Time) ([]GitOpsRevision, error) {
	var out []GitOpsRevision
	for _, r := range s.revisions[controller+"/"+app] {
		if !r.At.After(at) {
			out = append(out, r)
		}
	}
	return out, nil
}
func (s *gitopsStore) PriorRevisions(context.Context, string, string, string, int) ([]RevisionPeriod, error) {
	return nil, nil
}
func (s *gitopsStore) RanHealthily(_ context.Context, _, _ string, p RevisionPeriod, _ time.Duration) (bool, string, error) {
	s.asked = p
	if s.unhealthy != "" {
		return false, s.unhealthy, nil
	}
	return true, "ran for 2h0m without a recorded failure", nil
}

func appHistory(extra ...func(*GitOpsRevision)) []GitOpsRevision {
	bad := GitOpsRevision{Revision: "bad0000000000000000000000000000000000000", At: gitNow.Add(-time.Minute),
		RepoURL: "https://git.example.com/acme/gitops.git", Branch: "main", Path: "apps/api", Namespace: "default", AutoSync: true}
	for _, f := range extra {
		f(&bad)
	}
	good := bad
	good.Revision, good.At = "good000000000000000000000000000000000000", gitNow.Add(-2*time.Hour)
	return []GitOpsRevision{bad, good}
}

func gitopsCause(manager string, confidence float64) *rca.Result {
	return &rca.Result{Symptom: event.Event{ID: "incident-git-" + manager}, Confidence: confidence, Verdict: rca.VerdictRootCause,
		Candidates: []rca.Candidate{{Event: event.Event{ID: "cause-" + manager, Source: "k8s", Namespace: "default", EntityKind: "Deployment",
			EntityName: "api", Type: "deploy", OccurredAt: gitNow,
			Payload: []byte(`{"gitops":"` + manager + `","old_image":"api:v1","new_image":"api:v2","from_template_hash":"a","to_template_hash":"b"}`)},
			Reasons: []string{"one hop upstream"}}}}
}

func gitopsEngine(store *gitopsStore) *Engine {
	e := NewEngine(store)
	e.Now = func() time.Time { return gitNow }
	return e
}

// A bad release on a workload Argo CD manages becomes a pull request, proposed
// for approval like any other action, with everything the reviewer needs.
func TestABadReleaseOnAnArgoWorkloadIsProposedAsAGitRevert(t *testing.T) {
	store := &gitopsStore{revisions: map[string][]GitOpsRevision{"argocd/api": appHistory()}}
	action, err := gitopsEngine(store).Evaluate(context.Background(), gitopsCause("argocd:api", 0.95))
	if err != nil {
		t.Fatal(err)
	}
	if action.ActionType != ActionGitRevert || action.State != "pending_approval" || !action.Proposed {
		t.Fatalf("got %s %s: %s", action.ActionType, action.State, action.Result)
	}
	if action.Target != "argocd:api" || action.Workload != "api" || action.Namespace != "default" || action.ApprovalsRequired != 1 {
		t.Fatalf("target %q workload %q namespace %q approvers %d", action.Target, action.Workload, action.Namespace, action.ApprovalsRequired)
	}
	for field, want := range map[string]string{"repo_url": "https://git.example.com/acme/gitops.git", "branch": "main", "path": "apps/api",
		"bad_revision": "bad0000000000000000000000000000000000000", "good_revision": "good000000000000000000000000000000000000", "gitops_app": "api"} {
		if got := gjson.GetBytes(action.Payload, field).String(); got != want {
			t.Errorf("payload %s = %q, want %q", field, got, want)
		}
	}
	// The healthy check is over the good revision's own run, up to the bad deploy.
	if !store.asked.Since.Equal(gitNow.Add(-2*time.Hour)) || !store.asked.Until.Equal(gitNow.Add(-time.Minute)) {
		t.Errorf("judged the wrong period: %+v", store.asked)
	}
	if !strings.Contains(strings.Join(action.Reasoning, "\n"), "managed by Argo CD from https://git.example.com/acme/gitops.git (apps/api)") {
		t.Errorf("the reasoning does not say where the change will be made: %v", action.Reasoning)
	}
}

func TestTheGitRevertIsHeldToTheSameGatesAsAnyAction(t *testing.T) {
	t.Run("below the floor", func(t *testing.T) {
		store := &gitopsStore{revisions: map[string][]GitOpsRevision{"argocd/api": appHistory()}}
		action, _ := gitopsEngine(store).Evaluate(context.Background(), gitopsCause("argocd:api", 0.70))
		if action.ActionType != ActionGitRevert || action.Status != StatusBlocked || !strings.Contains(action.Result, "below") {
			t.Fatalf("%s %s: %s", action.ActionType, action.Status, action.Result)
		}
	})
	t.Run("the revision to return to did not run healthily", func(t *testing.T) {
		store := &gitopsStore{revisions: map[string][]GitOpsRevision{"argocd/api": appHistory()}, unhealthy: "a failure was recorded"}
		action, _ := gitopsEngine(store).Evaluate(context.Background(), gitopsCause("argocd:api", 0.95))
		if action.Status != StatusBlocked || action.Proposed || !strings.Contains(action.Result, "not known to have run healthily") {
			t.Fatalf("%s: %s", action.Status, action.Result)
		}
		// The record names the remedy that was considered: never a direct rollback.
		if action.ActionType != ActionGitRevert || action.Target != "argocd:api" {
			t.Fatalf("a blocked Git revert was recorded as %s on %s", action.ActionType, action.Target)
		}
	})
}

// Where a Git revert cannot be planned exactly, the decision says what to do by
// hand and is not approvable; nothing is ever written to the workload.
func TestWhenAGitRevertCannotBePlannedTheAdviceStaysManual(t *testing.T) {
	cases := map[string]struct {
		history []GitOpsRevision
		want    string
	}{
		"no earlier revision":   {appHistory()[:1], "no record of an earlier revision"},
		"a Helm chart repo":     {appHistory(func(r *GitOpsRevision) { r.Chart = "redis" }), "chart repository"},
		"several sources":       {appHistory(func(r *GitOpsRevision) { r.MultiSource = true }), "several sources"},
		"repository unknown":    {appHistory(func(r *GitOpsRevision) { r.RepoURL = "" }), "repository it deploys from is unknown"},
		"no history at all":     {nil, "no record of an earlier revision"},
		"an OCI source in Flux": {appHistory(func(r *GitOpsRevision) { r.SourceKind = "OCIRepository" }), "not a Git repository"},
	}
	for name, c := range cases {
		store := &gitopsStore{revisions: map[string][]GitOpsRevision{"argocd/api": c.history}}
		action, err := gitopsEngine(store).Evaluate(context.Background(), gitopsCause("argocd:api", 0.95))
		if err != nil {
			t.Fatal(err)
		}
		if action.Proposed || action.ActionType == ActionGitRevert || !strings.Contains(action.Result, c.want) || !strings.Contains(action.Result, "revert the change in Git") {
			t.Errorf("%s: %s %v: %s", name, action.ActionType, action.Proposed, action.Result)
		}
	}
}

func TestOnlyGitOpsWorkloadsAreRemediatedInGit(t *testing.T) {
	// No GitOps controller: the direct rollback, exactly as before.
	plain := gitopsCause("", 0.95)
	plain.Candidates[0].Event.Payload = []byte(`{"old_image":"api:v1","new_image":"api:v2"}`)
	action, _ := gitopsEngine(&gitopsStore{}).Evaluate(context.Background(), plain)
	if action.ActionType != ActionRollbackDeployment {
		t.Fatalf("a workload nobody manages from Git got %s", action.ActionType)
	}
	// A release controller owns rollback itself: neither a write nor a pull request.
	// Even with a revision history on record, it is never reverted in Git.
	withHistory := &gitopsStore{revisions: map[string][]GitOpsRevision{"argo-rollouts/api": appHistory(), "argocd/api": appHistory()}}
	rollouts, _ := gitopsEngine(withHistory).Evaluate(context.Background(), gitopsCause("argo-rollouts:api", 0.95))
	if rollouts.Proposed || rollouts.ActionType == ActionGitRevert || !strings.Contains(rollouts.Result, "rollout controller") {
		t.Fatalf("argo-rollouts: %s %s", rollouts.ActionType, rollouts.Result)
	}
	// Flux, from the Kustomization's own sync event.
	flux := gitopsCause("flux:apps", 0.95)
	flux.Candidates[0].Event = event.Event{ID: "flux-sync", Source: "flux", EntityKind: "Kustomization", EntityName: "apps", Namespace: "flux-system",
		Type: "deploy", OccurredAt: gitNow, Payload: []byte(`{"revision":"bad"}`)}
	store := &gitopsStore{revisions: map[string][]GitOpsRevision{"flux/apps": appHistory()}}
	got, _ := gitopsEngine(store).Evaluate(context.Background(), flux)
	if got.ActionType != ActionGitRevert || got.Target != "flux:apps" || got.Namespace != "default" {
		t.Fatalf("flux: %s %s %s: %s", got.ActionType, got.Target, got.Namespace, got.Result)
	}
}

func TestCurrentAndPreviousRevisionsIgnoreRepeats(t *testing.T) {
	at := func(m int) time.Time { return gitNow.Add(-time.Duration(m) * time.Minute) }
	rows := []GitOpsRevision{{Revision: "c", At: at(1)}, {Revision: "c", At: at(5)}, {Revision: "b", At: at(30)}, {Revision: "b", At: at(60)}, {Revision: "a", At: at(90)}}
	cur, prev, ok := currentAndPrevious(rows)
	if !ok || cur.Revision != "c" || !cur.At.Equal(at(5)) || prev.Revision != "b" || !prev.At.Equal(at(60)) {
		t.Fatalf("cur %+v prev %+v ok %v", cur, prev, ok)
	}
	if _, _, ok := currentAndPrevious(rows[:2]); ok {
		t.Fatal("one revision has no previous")
	}
}

// ---- policy and execution ----------------------------------------------------

func gitRevertAction() *Action {
	a := approvedRestart("git-1")
	a.ActionType, a.Rule, a.Target, a.Workload = ActionGitRevert, "rollback-bad-deploy", "argocd:api", "api"
	a.Payload = []byte(`{"gitops":"argocd:api","gitops_app":"api","repo_url":"https://git.example.com/acme/gitops.git","branch":"main","path":"apps/api",` +
		`"bad_revision":"bad0000000000000000000000000000000000000","good_revision":"good000000000000000000000000000000000000"}`)
	a.Approvals = []Approval{{Issuer: "i", Subject: "ana", Display: "ana@example.com"}}
	a.DryRun = false // as the worker leaves it once it has claimed the action
	return a
}

func gitPolicy() Policy {
	p := livePolicy()
	p.AllowedActions = map[string]bool{ActionGitRevert: true}
	p.AllowedTargets = map[string]bool{"default/api": true}
	p.GitRemediationEnabled = true
	return p
}

func TestAGitRevertNeedsItsOwnSwitchAndTheUsualAllowlists(t *testing.T) {
	p := gitPolicy()
	if err := p.Allows(gitRevertAction()); err != nil {
		t.Fatalf("an allowlisted, enabled Git revert was refused: %v", err)
	}
	p.GitRemediationEnabled = false
	if err := p.Allows(gitRevertAction()); err == nil || !strings.Contains(err.Error(), "HEAL_GIT_REMEDIATION_ENABLED") {
		t.Fatalf("without the switch: %v", err)
	}
	p = gitPolicy()
	p.AllowedTargets = map[string]bool{"default/other": true}
	if err := p.Allows(gitRevertAction()); err == nil {
		t.Fatal("an application not on the allowlist was allowed")
	}
	p = gitPolicy()
	p.LiveEnabled = false
	if err := p.Allows(gitRevertAction()); err == nil {
		t.Fatal("a Git revert ran with live healing off")
	}
}

type fakeReverter struct {
	change   gitops.Change
	err      error
	requests []gitops.Request
	status   gitops.Status
}

func (f *fakeReverter) Revert(_ context.Context, r gitops.Request) (gitops.Change, error) {
	f.requests = append(f.requests, r)
	return f.change, f.err
}
func (f *fakeReverter) PullStatus(context.Context, string, int) (gitops.Status, error) {
	return f.status, nil
}

func TestTheExecutorOpensThePullRequestAndNeverTouchesTheCluster(t *testing.T) {
	git := &fakeReverter{change: gitops.Change{URL: "https://git.example.com/acme/gitops/pulls/7", Number: 7, Branch: "chronicle/revert-x", Commit: "c0ffee",
		Files: []string{"apps/api/deploy.yaml"}}}
	e := &KubernetesExecutor{Git: git} // no Kubernetes client at all: nothing can be written
	a := gitRevertAction()
	a.DryRun = false
	out, err := e.Execute(context.Background(), a)
	if err != nil || !strings.Contains(out, "opened pull request #7") {
		t.Fatalf("%q %v", out, err)
	}
	req := git.requests[0]
	if req.DecisionID != "git-1" || req.App != "argocd:api" || req.Path != "apps/api" || req.GoodRevision == "" || req.Approvers[0] != "ana@example.com" {
		t.Fatalf("request %+v", req)
	}
	if a.GitChange == nil || a.GitChange.Number != 7 || a.GitChange.State != GitOpen {
		t.Fatalf("git change %+v", a.GitChange)
	}
	if v, err := e.Verify(context.Background(), a); err != nil || !strings.Contains(v, "after it is merged") {
		t.Fatalf("verify %q %v", v, err)
	}
	// Git disagreeing with the evidence is a refusal: blocked, not failed.
	git.err = &gitops.Refusal{Reason: "apps/api/deploy.yaml changed on main after revision bad00000"}
	_, err = e.Execute(context.Background(), gitRevertAction())
	var refused *Refusal
	if !errors.As(err, &refused) || !strings.Contains(refused.Reason, "changed on main") {
		t.Fatalf("got %v", err)
	}
	// Not configured: refused.
	if _, err := (&KubernetesExecutor{}).Execute(context.Background(), gitRevertAction()); !errors.As(err, &refused) {
		t.Fatalf("an unconfigured Git revert was not refused: %v", err)
	}
}

// ---- following the pull request ----------------------------------------------

type gitWorkStore struct {
	executionMemory
	recorded  []GitChange
	pending   []Action
	updates   []GitChange
	revisions []GitOpsRevision
}

func (s *gitWorkStore) RecordGitChange(_ context.Context, _ string, g GitChange) error {
	s.recorded = append(s.recorded, g)
	return nil
}
func (s *gitWorkStore) PendingGitChanges(context.Context, int) ([]Action, error) {
	return s.pending, nil
}
func (s *gitWorkStore) UpdateGitChange(_ context.Context, id string, g GitChange) error {
	s.updates = append(s.updates, g)
	for i := range s.pending {
		if s.pending[i].ID == id {
			c := g
			s.pending[i].GitChange = &c
		}
	}
	return nil
}
func (s *gitWorkStore) GitOpsRevisions(context.Context, string, string, time.Time) ([]GitOpsRevision, error) {
	return s.revisions, nil
}

type beginRecorder struct {
	fakeSymptomStore
	begun []string
}

func (b *beginRecorder) BeginSymptomCheck(_ context.Context, id string, _, _ time.Time) error {
	b.begun = append(b.begun, id)
	return nil
}

func TestAnExecutedGitRevertRecordsThePullRequestAndWaitsBeforeTheSymptomCheck(t *testing.T) {
	store := &gitWorkStore{}
	symptoms := &beginRecorder{}
	git := &fakeReverter{change: gitops.Change{URL: "u", Number: 3, Branch: "b"}}
	c := &Controller{Store: store, Executor: &KubernetesExecutor{Git: git}, Policy: gitPolicy(), Rules: append([]Rule(nil), defaultRules...),
		Symptoms: &SymptomChecker{Store: symptoms, Policy: SymptomPolicy{ImproveWithin: time.Minute, Hold: time.Minute}}}
	a := gitRevertAction()
	a.DryRun = false
	got, err := c.ExecuteApproved(context.Background(), a)
	if err != nil || got.Status != StatusSucceeded {
		t.Fatalf("%v %+v", err, got)
	}
	if len(store.recorded) != 1 || store.recorded[0].Number != 3 {
		t.Fatalf("the pull request was not recorded on its decision: %+v", store.recorded)
	}
	if len(symptoms.begun) != 0 {
		t.Fatal("the symptom check started before anything changed in the cluster")
	}
}

func TestThePullRequestIsFollowedToTheSyncAndThenTheSymptomIsChecked(t *testing.T) {
	merged := gitNow.Add(-2 * time.Minute)
	a := *gitRevertAction()
	a.Status, a.GitChange = StatusSucceeded, &GitChange{URL: "u", Number: 3, State: GitOpen}
	store := &gitWorkStore{pending: []Action{a}}
	symptoms := &beginRecorder{}
	git := &fakeReverter{status: gitops.Status{State: "open"}}
	c := &Controller{Store: store, Git: git, Symptoms: &SymptomChecker{Store: symptoms, Policy: SymptomPolicy{ImproveWithin: time.Minute, Hold: time.Minute}}}

	c.trackGitChanges(context.Background(), gitNow)
	if len(store.updates) != 0 {
		t.Fatal("an open pull request was recorded as progress")
	}
	git.status = gitops.Status{State: "merged", MergeCommit: "f00dfeed00", MergedAt: merged}
	c.trackGitChanges(context.Background(), gitNow)
	if g := store.pending[0].GitChange; g.State != GitMerged || g.MergedAt == nil || !strings.Contains(g.Detail, "waiting for argocd:api to sync") {
		t.Fatalf("after the merge: %+v", g)
	}
	// Still on the bad revision: not synced yet.
	store.revisions = []GitOpsRevision{{Revision: "bad0000000000000000000000000000000000000", At: gitNow.Add(-time.Hour)}}
	c.trackGitChanges(context.Background(), gitNow)
	if store.pending[0].GitChange.State != GitMerged || len(symptoms.begun) != 0 {
		t.Fatalf("counted as synced while the controller still runs the bad revision: %+v", store.pending[0].GitChange)
	}
	// The controller has synced the revert.
	store.revisions = []GitOpsRevision{{Revision: "f00dfeed00", At: gitNow.Add(-30 * time.Second)}}
	c.trackGitChanges(context.Background(), gitNow)
	if g := store.pending[0].GitChange; g.State != GitSynced || g.SyncedAt == nil {
		t.Fatalf("after the sync: %+v", g)
	}
	if len(symptoms.begun) != 1 || symptoms.begun[0] != "git-1" {
		t.Fatalf("the symptom check did not start after the sync: %v", symptoms.begun)
	}
}

func TestAPullRequestClosedWithoutMergingIsRecordedAndNothingElseHappens(t *testing.T) {
	a := *gitRevertAction()
	a.Status, a.GitChange = StatusSucceeded, &GitChange{URL: "u", Number: 3, State: GitOpen}
	store := &gitWorkStore{pending: []Action{a}}
	symptoms := &beginRecorder{}
	c := &Controller{Store: store, Git: &fakeReverter{status: gitops.Status{State: "closed"}},
		Symptoms: &SymptomChecker{Store: symptoms}}
	c.trackGitChanges(context.Background(), gitNow)
	if g := store.pending[0].GitChange; g.State != GitClosed || !strings.Contains(g.Detail, "nothing changed in the cluster") {
		t.Fatalf("%+v", g)
	}
	if len(symptoms.begun) != 0 {
		t.Fatal("a symptom check started for a change that was never merged")
	}
}

// A git_revert is the rollback rule carried out in Git: the worker accepts it.
func TestTheWorkerAcceptsAGitRevertForTheRollbackRule(t *testing.T) {
	c := &Controller{Policy: gitPolicy(), Rules: append([]Rule(nil), defaultRules...)}
	a := gitRevertAction()
	if reason := c.Revalidate(context.Background(), a); reason != "" {
		t.Fatalf("refused: %s", reason)
	}
	a.Payload = []byte(`{"gitops":"argocd:api","gitops_app":"api"}`)
	if reason := c.Revalidate(context.Background(), a); !strings.Contains(reason, "lacks repo_url") {
		t.Fatalf("a Git revert without its evidence: %q", reason)
	}
}

func TestADryRunGitRevertIsNeverExecuted(t *testing.T) {
	git := &fakeReverter{}
	a := gitRevertAction()
	a.DryRun = true
	if _, err := (&KubernetesExecutor{Git: git}).Execute(context.Background(), a); err == nil || len(git.requests) != 0 {
		t.Fatalf("a dry-run Git revert reached Git: %v, %d requests", err, len(git.requests))
	}
}

func TestPostgresGitOpsHistoryAndThePullRequestsJourney(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC().Truncate(time.Second)
	app := "shop-" + sx
	insert := func(id string, at time.Time, payload string) {
		if _, err := pool.Exec(ctx, `INSERT INTO events (id, occurred_at, ingested_at, source, namespace, entity_kind, entity_name, type, severity, title, payload, trace_id, correlation_key)
			VALUES ($1,$2,$2,'argocd','argocd','Application',$3,'deploy','info',$4,$5,'','')`, id+sx, at, app, "synced "+id, payload); err != nil {
			t.Fatal(err)
		}
	}
	src := `"repo_url":"https://git.example.com/acme/gitops.git","branch":"main","path":"apps/shop","destination_namespace":"default","auto_sync":true`
	insert("g1", now.Add(-3*time.Hour), `{"revision":"aaa",`+src+`}`)
	insert("g2", now.Add(-time.Hour), `{"revision":"bbb",`+src+`}`)
	insert("g3", now.Add(-time.Minute), `{"revision":"ccc",`+src+`,"multi_source":false}`)
	insert("g4", now.Add(time.Hour), `{"revision":"ddd",`+src+`}`) // after the moment asked about
	revs, err := s.GitOpsRevisions(ctx, "argocd", app, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 3 || revs[0].Revision != "ccc" || revs[1].Revision != "bbb" || revs[0].RepoURL != "https://git.example.com/acme/gitops.git" ||
		revs[0].Path != "apps/shop" || revs[0].Namespace != "default" || !revs[0].AutoSync || revs[0].MultiSource {
		t.Fatalf("%+v", revs)
	}

	a := proposal("git-"+sx, "c-git-"+sx, app, now)
	a.ActionType = ActionGitRevert
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET status='succeeded', started_at=now() WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordGitChange(ctx, a.ID, GitChange{URL: "https://git.example.com/acme/gitops/pulls/9", Number: 9, Branch: "chronicle/revert-x", Commit: "c0ffee", State: GitOpen}); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingGitChanges(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	var got *Action
	for i := range pending {
		if pending[i].ID == a.ID {
			got = &pending[i]
		}
	}
	if got == nil || got.GitChange == nil || got.GitChange.Number != 9 || got.GitChange.State != GitOpen || got.GitChange.Branch != "chronicle/revert-x" {
		t.Fatalf("pending %+v", got)
	}
	merged := now.Add(-time.Minute)
	g := *got.GitChange
	g.State, g.MergedAt, g.Detail = GitSynced, &merged, "shop synced revision eee"
	if err := s.UpdateGitChange(ctx, a.ID, g); err != nil {
		t.Fatal(err)
	}
	reread, _ := s.GetAction(ctx, a.ID)
	if reread.GitChange == nil || reread.GitChange.State != GitSynced || reread.GitChange.MergedAt == nil || reread.GitChange.Detail != "shop synced revision eee" {
		t.Fatalf("reread %+v", reread.GitChange)
	}
	pending, _ = s.PendingGitChanges(ctx, 100)
	for _, p := range pending {
		if p.ID == a.ID {
			t.Fatal("a synced change is still pending")
		}
	}
}

// The controller's record of a sync can come later than the change it made (it
// is polled). The good revision's time ends at the cluster's own change, or the
// bad release's first failures would be blamed on the revision before it.
func TestTheGoodRevisionsTimeEndsWhenTheBadOneReachedTheCluster(t *testing.T) {
	late := appHistory(func(r *GitOpsRevision) { r.At = gitNow.Add(30 * time.Second) }) // recorded 30 s after the change
	store := &gitopsStore{revisions: map[string][]GitOpsRevision{"argocd/api": late}}
	if _, err := gitopsEngine(store).Evaluate(context.Background(), gitopsCause("argocd:api", 0.95)); err != nil {
		t.Fatal(err)
	}
	if !store.asked.Until.Equal(gitNow) {
		t.Fatalf("judged the good revision up to %v, not the change at %v", store.asked.Until, gitNow)
	}
}
