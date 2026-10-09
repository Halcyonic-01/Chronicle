package heal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run with CHRONICLE_TEST_POSTGRES_URL against a scratch database containing
// the Chronicle migrations. Skipped by normal unit-test runs. It writes and
// deletes rows, so never point it at a database that matters.
func healTestStore(t *testing.T) (*PostgresAuditStore, *pgxpool.Pool, string) {
	t.Helper()
	url := os.Getenv("CHRONICLE_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set CHRONICLE_TEST_POSTGRES_URL to run the PostgreSQL integration tests")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	suffix := fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM heal_actions WHERE id LIKE $1`, "%"+suffix)
		_, _ = pool.Exec(context.Background(), `DELETE FROM events WHERE id LIKE $1`, "%"+suffix)
	})
	return NewPostgresAuditStore(pool), pool, suffix
}

func proposal(id, cause, workload string, created time.Time) *Action {
	expires := created.Add(DefaultApprovalTTL)
	return &Action{ID: id, IncidentID: "inc-" + id, Rule: "restore-scaled-down-workload", CauseType: "scale",
		CauseEventID: cause, ActionType: ActionRestoreReplicas, Namespace: "default", Target: workload, Workload: workload,
		Confidence: 0.9, Status: StatusWouldRun, Approval: ApprovalPending, Proposed: true, DryRun: true,
		CreatedAt: created, ExpiresAt: &expires, Result: "WOULD HAVE RUN (approval required)",
		Payload: []byte(`{"old_replicas":1,"new_replicas":0}`)}
}

// Issues 1 and 2 in SQL: only a pending, unexpired proposal can be approved.
func TestPostgresApprovalOnlyForPendingUnexpiredProposals(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()

	blocked := proposal("blocked-"+sx, "c-blocked-"+sx, "redis", now)
	blocked.Status, blocked.Approval, blocked.Proposed, blocked.ExpiresAt = StatusBlocked, ApprovalNotRequired, false, nil
	if err := s.RecordAction(ctx, blocked); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideAction(ctx, Decision{ID: blocked.ID, Approved: true, Now: now}); !errors.Is(err, ErrNotPending) {
		t.Fatalf("a blocked decision was approvable: %v", err)
	}

	old := proposal("old-"+sx, "c-old-"+sx, "redis", now.Add(-time.Hour))
	if err := s.RecordAction(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideAction(ctx, Decision{ID: old.ID, Approved: true, Now: now}); !errors.Is(err, ErrExpired) {
		t.Fatalf("an expired decision was approvable: %v", err)
	}
	if got, _ := s.GetAction(ctx, old.ID); got.Status != StatusExpired {
		t.Fatalf("the expired decision was not marked expired: %s", got.Status)
	}

	fresh := proposal("fresh-"+sx, "c-fresh-"+sx, "redis", now)
	if err := s.RecordAction(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	got, err := s.DecideAction(ctx, Decision{ID: fresh.ID, Approved: true, By: "alice", Result: "APPROVED", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusApproved || got.State != "approved" {
		t.Fatalf("approval should only queue the decision: %+v", got)
	}
	if _, err := s.DecideAction(ctx, Decision{ID: fresh.ID, Approved: false, Now: now}); !errors.Is(err, ErrNotPending) {
		t.Fatalf("a decided action was decided again: %v", err)
	}
}

// Issue 3 in SQL: one proposal per outage, and the rule limit counts outages.
func TestPostgresProposalsAreCountedPerOutage(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	cause := "c-outage-" + sx
	if err := s.RecordAction(ctx, proposal("p1-"+sx, cause, "redis", now)); err != nil {
		t.Fatal(err)
	}
	id, state, found, err := s.ProposalForCause(ctx, cause)
	if err != nil || !found || id != "p1-"+sx || state != "pending_approval" {
		t.Fatalf("the earlier proposal was not found: %s %s %v %v", id, state, found, err)
	}
	dup := proposal("p2-"+sx, cause, "redis", now)
	if err := s.RecordAction(ctx, dup); err != nil {
		t.Fatal(err)
	}
	before, _ := s.CountRuleSince(ctx, "restore-scaled-down-workload", now.Add(-time.Minute))
	if err := s.RecordAction(ctx, proposal("p3-"+sx, "c-other-"+sx, "worker", now)); err != nil {
		t.Fatal(err)
	}
	after, _ := s.CountRuleSince(ctx, "restore-scaled-down-workload", now.Add(-time.Minute))
	if after-before != 1 {
		t.Fatalf("a second outage should add one to the limit count, added %d", after-before)
	}
}

// Issue 4 in SQL: limits, cooldown, one execution per outage, and a claim that
// only one caller can win.
func TestPostgresClaimEnforcesLimitsAtomically(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	// Isolate from rows other tests left executing.
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET started_at = started_at - interval '2 hours' WHERE started_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	approve := func(a *Action) {
		t.Helper()
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DecideAction(ctx, Decision{ID: a.ID, Approved: true, Now: now}); err != nil {
			t.Fatal(err)
		}
	}
	req := func(a *Action) ClaimRequest {
		return ClaimRequest{ID: a.ID, Rule: a.Rule, Namespace: a.Namespace, Workload: a.Workload, Now: now,
			RuleMaxPerHour: 5, MaxPerHour: 5, Cooldown: 30 * time.Minute}
	}

	first := proposal("x1-"+sx, "c-x1-"+sx, "redis-"+sx, now)
	approve(first)
	if ok, refusal, err := s.ClaimExecution(ctx, req(first)); !ok || refusal != "" || err != nil {
		t.Fatalf("the first claim should win: %v %q %v", ok, refusal, err)
	}
	if ok, _, _ := s.ClaimExecution(ctx, req(first)); ok {
		t.Fatal("the same decision was claimed twice")
	}

	sameTarget := proposal("x2-"+sx, "c-x2-"+sx, "redis-"+sx, now)
	approve(sameTarget)
	if _, refusal, _ := s.ClaimExecution(ctx, req(sameTarget)); !strings.Contains(refusal, "cooldown") {
		t.Fatalf("the per-workload cooldown was not enforced: %q", refusal)
	}

	sameOutage := proposal("x3-"+sx, "c-x1-"+sx, "other-"+sx, now)
	approve(sameOutage)
	if _, refusal, _ := s.ClaimExecution(ctx, req(sameOutage)); !strings.Contains(refusal, "already acted on") {
		t.Fatalf("one outage was acted on twice: %q", refusal)
	}

	limited := proposal("x4-"+sx, "c-x4-"+sx, "third-"+sx, now)
	approve(limited)
	r := req(limited)
	r.MaxPerHour = 1
	if _, refusal, _ := s.ClaimExecution(ctx, r); !strings.Contains(refusal, "global limit") {
		t.Fatalf("the global hourly limit was not enforced: %q", refusal)
	}
	r = req(limited)
	r.RuleMaxPerHour = 1
	if _, refusal, _ := s.ClaimExecution(ctx, r); !strings.Contains(refusal, "rule") {
		t.Fatalf("the rule's hourly limit was not enforced: %q", refusal)
	}
}

// Issue 12 in SQL: stale decisions expire, interrupted executions fail and are
// never retried, and only approved, unexpired work is queued.
func TestPostgresWorkQueueStates(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	stale := proposal("q1-"+sx, "c-q1-"+sx, "redis", now.Add(-time.Hour))
	if err := s.RecordAction(ctx, stale); err != nil {
		t.Fatal(err)
	}
	queued := proposal("q2-"+sx, "c-q2-"+sx, "redis", now)
	if err := s.RecordAction(ctx, queued); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideAction(ctx, Decision{ID: queued.ID, Approved: true, Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireStale(ctx, now); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetAction(ctx, stale.ID); got.Status != StatusExpired || got.Approval != ApprovalExpired {
		t.Fatalf("a stale proposal was not expired: %s/%s", got.Status, got.Approval)
	}
	work, err := s.ApprovedActions(ctx, now, 500)
	if err != nil {
		t.Fatal(err)
	}
	var sawQueued, sawStale bool
	for _, a := range work {
		sawQueued = sawQueued || a.ID == queued.ID
		sawStale = sawStale || a.ID == stale.ID
	}
	if !sawQueued || sawStale {
		t.Fatalf("queue contents wrong: queued=%v stale=%v", sawQueued, sawStale)
	}

	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET status='executing', started_at=$2 WHERE id=$1`, queued.ID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FailInterrupted(ctx, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetAction(ctx, queued.ID); got.Status != StatusFailed || !strings.Contains(got.Result, "not retried") {
		t.Fatalf("an interrupted execution was not closed: %s %q", got.Status, got.Result)
	}
}

// Issue 13 in SQL: pruning old rows does not move the observation start.
func TestPostgresObservationStartSurvivesPruning(t *testing.T) {
	s, _, sx := healTestStore(t)
	ctx := context.Background()
	early := time.Now().UTC().Add(-90 * 24 * time.Hour)
	skipped := &Action{ID: "obs-" + sx, IncidentID: "inc-obs-" + sx, Rule: "", Status: StatusSkipped,
		Approval: ApprovalNotRequired, CreatedAt: early, DryRun: true}
	if err := s.RecordAction(ctx, skipped); err != nil {
		t.Fatal(err)
	}
	before, err := s.EarliestActionAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A later decision must not move it either.
	later := &Action{ID: "obs2-" + sx, IncidentID: "inc-obs2-" + sx, Status: StatusSkipped,
		Approval: ApprovalNotRequired, CreatedAt: time.Now().UTC(), DryRun: true}
	if err := s.RecordAction(ctx, later); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune(ctx, time.Now().UTC().AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}
	after, err := s.EarliestActionAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.IsZero() || !after.Equal(before) {
		t.Fatalf("the observation start moved from %v to %v", before, after)
	}
}

// Issue 20 in SQL: an outage Chronicle acted on is not evidence.
func TestPostgresEvidenceExcludesSelfExecutedOutages(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	base, err := s.EvidenceSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shadow := proposal("ev1-"+sx, "c-ev1-"+sx, "redis", now)
	executed := proposal("ev2-"+sx, "c-ev2-"+sx, "worker", now)
	for _, a := range []*Action{shadow, executed} {
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET outcome='confirmed', status='expired' WHERE id=$1`, shadow.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET outcome='confirmed', status='succeeded', started_at=$2 WHERE id=$1`, executed.ID, now); err != nil {
		t.Fatal(err)
	}
	got, err := s.EvidenceSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Confirmed-base.Confirmed != 1 {
		t.Fatalf("expected exactly the shadow decision credited, got %d more", got.Confirmed-base.Confirmed)
	}
}

// Issues 16-17 end to end: the labeller reads the symptom and orders events.
func TestPostgresLabellingUsesTheSymptomAndOrder(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	decided := time.Now().UTC().Add(-2 * SettleWindow)
	a := proposal("lab-"+sx, "c-lab-"+sx, "redis", decided)
	a.Related = []string{"frontend", "redis"}
	a.IncidentID = "sym-" + sx
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	insert := func(id string, at time.Time, kind, name, typ, payload string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO events (id,occurred_at,ingested_at,source,namespace,entity_kind,entity_name,type,severity,title,payload)
			VALUES ($1,$2,$2,'test','default',$3,$4,$5,'info',$5,$6)`, id, at, kind, name, typ, payload); err != nil {
			t.Fatal(err)
		}
	}
	insert("sym-"+sx, decided.Add(-time.Minute), "Service", "frontend", "error_spike", `{}`)
	insert("e1-"+sx, decided.Add(time.Minute), "Pod", "billing-1", "became_ready", `{}`)
	insert("e2-"+sx, decided.Add(2*time.Minute), "Deployment", "redis", "scale", `{"old_replicas":0,"new_replicas":1}`)
	insert("e3-"+sx, decided.Add(3*time.Minute), "Service", "frontend", "error_spike_resolved", `{}`)
	if _, err := s.LabelOutcomes(ctx, time.Now().UTC(), 500); err != nil {
		t.Fatal(err)
	}
	var outcome, detail string
	if err := pool.QueryRow(ctx, `SELECT outcome, outcome_detail FROM heal_actions WHERE id=$1`, a.ID).Scan(&outcome, &detail); err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeConfirmed || !strings.Contains(detail, "frontend") {
		t.Fatalf("expected confirmed by frontend's recovery, got %s (%s)", outcome, detail)
	}
}

// The repair migration is idempotent and fixes rows the old code wrote.
func TestPostgresMigrationRepairsLegacyRows(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	sql, err := os.ReadFile("../../migrations/011_heal_safety.sql")
	if err != nil {
		t.Fatal(err)
	}
	legacy := []struct{ id, status, approval string }{
		{"leg-blocked-" + sx, StatusBlocked, ApprovalPending},
		{"leg-pending-" + sx, StatusWouldRun, ApprovalPending},
		{"leg-denied-" + sx, StatusBlocked, ApprovalDenied},
	}
	for _, l := range legacy {
		if _, err := pool.Exec(ctx, `INSERT INTO heal_actions (id, incident_id, rule, status, approval, created_at)
			VALUES ($1, $1, 'r', $2, $3, now())`, l.id, l.status, l.approval); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("migration run %d: %v", i+1, err)
		}
	}
	want := map[string][2]string{
		"leg-blocked-" + sx: {StatusBlocked, ApprovalNotRequired},
		"leg-pending-" + sx: {StatusExpired, ApprovalExpired},
		"leg-denied-" + sx:  {StatusDenied, ApprovalDenied},
	}
	for id, w := range want {
		got, err := s.GetAction(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != w[0] || got.Approval != w[1] {
			t.Errorf("%s: got %s/%s, want %s/%s", id, got.Status, got.Approval, w[0], w[1])
		}
	}
}

func runMigration(t *testing.T, pool *pgxpool.Pool, name string) {
	t.Helper()
	sql, err := os.ReadFile("../../migrations/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), string(sql)); err != nil {
		t.Fatalf("migration %s: %v", name, err)
	}
}

// Labels written by the old, namespace-wide labeller are evidence of what that
// labeller said, not of what happened. They are kept, re-judged by the current
// labeller, and a repeated migration changes nothing.
func TestPostgresLegacyLabelsArePreservedAndRejudged(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	runMigration(t, pool, "012_heal_outcome_history.sql") // adds the column and table

	decided := time.Now().UTC().Add(-2 * SettleWindow)
	a := proposal("rel-"+sx, "c-rel-"+sx, "redis", decided)
	a.Related = []string{"frontend", "redis"}
	a.IncidentID = "sym-rel-" + sx
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	// What the old labeller wrote, with no labeller recorded.
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET outcome='contradicted', outcome_at=$2, outcome_detail='the symptom recovered after a different change (billing: billing deployed)' WHERE id=$1`, a.ID, decided.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	insert := func(id string, at time.Time, kind, name, typ, payload string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO events (id,occurred_at,ingested_at,source,namespace,entity_kind,entity_name,type,severity,title,payload)
			VALUES ($1,$2,$2,'test','default',$3,$4,$5,'info',$5,$6)`, id, at, kind, name, typ, payload); err != nil {
			t.Fatal(err)
		}
	}
	insert("sym-rel-"+sx, decided.Add(-time.Minute), "Service", "frontend", "error_spike", `{}`)
	insert("e1-rel-"+sx, decided.Add(time.Minute), "Deployment", "billing", "deploy", `{}`) // unrelated: what fooled the old labeller
	insert("e2-rel-"+sx, decided.Add(2*time.Minute), "Deployment", "redis", "scale", `{"old_replicas":0,"new_replicas":1}`)
	insert("e3-rel-"+sx, decided.Add(3*time.Minute), "Service", "frontend", "error_spike_resolved", `{}`)

	for i := 0; i < 2; i++ { // twice: the second run must be a no-op
		runMigration(t, pool, "012_heal_outcome_history.sql")
		history, err := s.OutcomeHistory(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(history) != 1 || history[0].Outcome != "contradicted" || history[0].Labeller != "v1-namespace-wide" || history[0].SupersededBy != "v2-entity-ordered" {
			t.Fatalf("run %d: the old label was not preserved exactly once: %+v", i+1, history)
		}
	}
	var live *string
	if err := pool.QueryRow(ctx, `SELECT outcome FROM heal_actions WHERE id=$1`, a.ID).Scan(&live); err != nil || live != nil {
		t.Fatalf("the live label should be cleared for re-judging, got %v (%v)", live, err)
	}

	if _, err := s.LabelOutcomes(ctx, time.Now().UTC(), 500); err != nil {
		t.Fatal(err)
	}
	var outcome, labeller string
	if err := pool.QueryRow(ctx, `SELECT outcome, outcome_labeller FROM heal_actions WHERE id=$1`, a.ID).Scan(&outcome, &labeller); err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeConfirmed || labeller != LabellerVersion {
		t.Fatalf("re-judged as %s by %q; an unrelated deploy must not contradict the proposal", outcome, labeller)
	}

	// Once re-judged, the migration never touches it again.
	runMigration(t, pool, "012_heal_outcome_history.sql")
	history, _ := s.OutcomeHistory(ctx, a.ID)
	if err := pool.QueryRow(ctx, `SELECT outcome FROM heal_actions WHERE id=$1`, a.ID).Scan(&outcome); err != nil || outcome != OutcomeConfirmed || len(history) != 1 {
		t.Fatalf("a re-judged label was moved again: %s, %d history rows (%v)", outcome, len(history), err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM heal_outcome_history WHERE action_id LIKE $1`, "%"+sx)
	})
}

// Found by the worker integration test: a worker that lost the claim was told it
// had hit the cooldown the winner just started, and then blocked the decision it
// did not own. The loser must learn only that the decision is taken.
func TestPostgresALateClaimIsToldTheDecisionIsTakenNotThatALimitWasHit(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET started_at = started_at - interval '3 hours' WHERE started_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	a := proposal("late-"+sx, "c-late-"+sx, "late-"+sx, now)
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideAction(ctx, Decision{ID: a.ID, Approved: true, Now: now}); err != nil {
		t.Fatal(err)
	}
	req := ClaimRequest{ID: a.ID, Rule: a.Rule, Namespace: a.Namespace, Workload: a.Workload, Now: now, RuleMaxPerHour: 10, MaxPerHour: 10, Cooldown: 30 * time.Minute}
	if ok, refusal, err := s.ClaimExecution(ctx, req); !ok || refusal != "" || err != nil {
		t.Fatalf("the first claim should win: %v %q %v", ok, refusal, err)
	}
	ok, refusal, err := s.ClaimExecution(ctx, req)
	if ok || refusal != "" || err != nil {
		t.Fatalf("a late claim must get a plain 'not claimable', not a limit refusal: ok=%v refusal=%q err=%v", ok, refusal, err)
	}
	if ok, refusal, err := s.ClaimExecution(ctx, ClaimRequest{ID: "no-such-" + sx, Now: now}); ok || refusal != "" || err != nil {
		t.Fatalf("an unknown decision is not claimable and not an error: %v %q %v", ok, refusal, err)
	}
}

// A completion can only be written from the state it is made from: a refusal
// only over an approved decision, a result only over an executing one.
func TestPostgresACompletionCannotOverwriteAnotherWorkersState(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	plant := func(id string) *Action {
		a := proposal(id+"-"+sx, "c-"+id+"-"+sx, id+"-"+sx, now)
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DecideAction(ctx, Decision{ID: a.ID, Approved: true, Now: now}); err != nil {
			t.Fatal(err)
		}
		return a
	}
	status := func(id string) string {
		var st string
		if err := pool.QueryRow(ctx, `SELECT status FROM heal_actions WHERE id=$1`, id).Scan(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	executing := plant("exec")
	if _, err := pool.Exec(ctx, `UPDATE heal_actions SET status='executing', started_at=$2 WHERE id=$1`, executing.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteExecution(ctx, executing.ID, StatusBlocked, "cooldown", "cooldown", ""); err != nil || status(executing.ID) != StatusExecuting {
		t.Fatalf("a refusal overwrote a decision another worker is executing: %s (%v)", status(executing.ID), err)
	}
	if err := s.CompleteExecution(ctx, executing.ID, StatusSucceeded, "done", "", "ok"); err != nil || status(executing.ID) != StatusSucceeded {
		t.Fatalf("the executing worker could not record its result: %s (%v)", status(executing.ID), err)
	}
	if err := s.CompleteExecution(ctx, executing.ID, StatusBlocked, "late", "late", ""); err != nil || status(executing.ID) != StatusSucceeded {
		t.Fatalf("a late refusal overwrote a finished decision: %s (%v)", status(executing.ID), err)
	}
	if err := s.CompleteExecution(ctx, executing.ID, StatusFailed, "late", "late", ""); err != nil || status(executing.ID) != StatusSucceeded {
		t.Fatalf("a late failure overwrote a finished decision: %s (%v)", status(executing.ID), err)
	}
	approved := plant("appr")
	if err := s.CompleteExecution(ctx, approved.ID, StatusSucceeded, "x", "", "x"); err != nil || status(approved.ID) != StatusApproved {
		t.Fatalf("a result was recorded for a decision nobody claimed: %s (%v)", status(approved.ID), err)
	}
	if err := s.CompleteExecution(ctx, approved.ID, StatusBlocked, "gate", "gate", ""); err != nil || status(approved.ID) != StatusBlocked {
		t.Fatalf("a refusal of an unclaimed approved decision should be recorded: %s (%v)", status(approved.ID), err)
	}
}

// Found live: Kubernetes dates a change to the second while a decision is timed
// to the millisecond, so a fix applied right after the decision can carry an
// earlier timestamp. It must still be seen, and a later, unrelated incident must
// not be mistaken for the decision's own.
func TestPostgresLabellingToleratesSecondResolutionAndStopsAtTheIncidentsEnd(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	runMigration(t, pool, "012_heal_outcome_history.sql")
	decided := time.Now().UTC().Add(-2 * SettleWindow).Truncate(time.Second).Add(400 * time.Millisecond)
	a := proposal("skew-"+sx, "c-skew-"+sx, "redis", decided)
	a.Related = []string{"frontend", "redis", "api"}
	a.IncidentID = "sym-skew-" + sx
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	insert := func(id string, at time.Time, kind, name, typ, payload string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO events (id,occurred_at,ingested_at,source,namespace,entity_kind,entity_name,type,severity,title,payload)
			VALUES ($1,$2,$2,'test','default',$3,$4,$5,'info',$5,$6)`, id, at, kind, name, typ, payload); err != nil {
			t.Fatal(err)
		}
	}
	insert("sym-skew-"+sx, decided.Add(-time.Minute), "Service", "frontend", "error_spike", `{}`)
	// The restore, dated to the whole second the decision fell in: before it.
	insert("e1-skew-"+sx, decided.Truncate(time.Second), "Deployment", "redis", "scale", `{"old_replicas":0,"new_replicas":1}`)
	insert("e2-skew-"+sx, decided.Add(55*time.Second), "Service", "frontend", "error_spike_resolved", `{}`)
	// The next, unrelated fault four minutes later.
	insert("e3-skew-"+sx, decided.Add(4*time.Minute), "Deployment", "api", "config_change", `{}`)
	insert("e4-skew-"+sx, decided.Add(5*time.Minute), "Service", "frontend", "error_spike_resolved", `{}`)

	if _, err := s.LabelOutcomes(ctx, time.Now().UTC(), 500); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetAction(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != OutcomeConfirmed || got.OutcomeLabeller != LabellerVersion {
		t.Fatalf("expected confirmed (%s), got %q: %s", LabellerVersion, got.Outcome, got.OutcomeDetail)
	}
}

// The chain of labellers: a label v2 wrote is kept and re-judged by v3, a label
// v3 wrote is never touched, and repeating the migration changes nothing.
func TestPostgresV2LabelsArePreservedAndRejudgedByV3(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	runMigration(t, pool, "012_heal_outcome_history.sql")
	decided := time.Now().UTC().Add(-2 * SettleWindow)
	v2, v3 := proposal("v2-"+sx, "c-v2-"+sx, "redis", decided), proposal("v3-"+sx, "c-v3-"+sx, "redis", decided)
	for _, a := range []*Action{v2, v3} {
		a.IncidentID = "sym-" + a.ID
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	set := func(id, labeller, outcome string) {
		if _, err := pool.Exec(ctx, `UPDATE heal_actions SET outcome=$3, outcome_labeller=$2, outcome_at=now(), outcome_detail='written by '||$2 WHERE id=$1`, id, labeller, outcome); err != nil {
			t.Fatal(err)
		}
	}
	set(v2.ID, "v2-entity-ordered", "contradicted")
	set(v3.ID, LabellerVersion, "confirmed")
	for i := 0; i < 2; i++ {
		runMigration(t, pool, "013_heal_outcome_relabel_v3.sql")
	}
	history, err := s.OutcomeHistory(ctx, v2.ID)
	if err != nil || len(history) != 1 || history[0].Outcome != "contradicted" || history[0].Labeller != "v2-entity-ordered" || history[0].SupersededBy != LabellerVersion {
		t.Fatalf("the v2 label was not preserved exactly once: %+v (%v)", history, err)
	}
	if got, _ := s.GetAction(ctx, v2.ID); got.Outcome != "" || got.OutcomeLabeller != "" {
		t.Fatalf("the v2 label should be cleared for re-judging: %q by %q", got.Outcome, got.OutcomeLabeller)
	}
	got, _ := s.GetAction(ctx, v3.ID)
	if h, _ := s.OutcomeHistory(ctx, v3.ID); got.Outcome != "confirmed" || len(h) != 0 {
		t.Fatalf("a label the current labeller wrote was touched: %q, %d history rows", got.Outcome, len(h))
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM heal_outcome_history WHERE action_id LIKE $1`, "%"+sx)
	})
}

// Found live: both Chronicle pods run the migrations at start-up, and two of
// them copied the same labels into the history before either cleared them.
// The migration must be safe to run concurrently, by its lock alone.
func TestPostgresRelabelMigrationIsSafeToRunConcurrently(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	runMigration(t, pool, "012_heal_outcome_history.sql")
	// The uniqueness rule must not be what saves it: drop it for this test.
	if _, err := pool.Exec(ctx, `DROP INDEX IF EXISTS uniq_heal_outcome_history_label`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runMigration(t, pool, "014_heal_outcome_history_unique.sql") })

	ids := make([]string, 5)
	for i := range ids {
		a := proposal(fmt.Sprintf("conc%d-%s", i, sx), fmt.Sprintf("c-conc%d-%s", i, sx), "redis", time.Now().UTC())
		a.IncidentID = "sym-" + a.ID
		ids[i] = a.ID
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE heal_actions SET outcome='contradicted', outcome_labeller='v2-entity-ordered', outcome_at=now() WHERE id=$1`, a.ID); err != nil {
			t.Fatal(err)
		}
	}
	// Widen the window between copying a label and clearing it, so overlapping
	// runs collide every time instead of when the timing happens to line up.
	raw, err := os.ReadFile("../../migrations/013_heal_outcome_relabel_v3.sql")
	if err != nil {
		t.Fatal(err)
	}
	slow := strings.Replace(string(raw), "\nUPDATE heal_actions", "\nSELECT pg_sleep(0.25);\nUPDATE heal_actions", 1)
	if slow == string(raw) {
		t.Fatal("the migration no longer has the shape this test widens")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := pool.Exec(ctx, slow); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for _, id := range ids {
		if h, err := s.OutcomeHistory(ctx, id); err != nil || len(h) != 1 {
			t.Fatalf("%s: expected the label preserved exactly once, got %d copies (%v)", id, len(h), err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM heal_outcome_history WHERE action_id LIKE $1`, "%"+sx)
	})
}

// Migration 014 removes only the exact duplicate copies and makes "one label per
// decision per labeller" structural.
func TestPostgresHistoryDeduplicationKeepsEveryDistinctLabel(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx := context.Background()
	runMigration(t, pool, "012_heal_outcome_history.sql")
	if _, err := pool.Exec(ctx, `DROP INDEX IF EXISTS uniq_heal_outcome_history_label`); err != nil {
		t.Fatal(err)
	}
	id := "dedupe-" + sx
	a := proposal(id, "c-"+id, "redis", time.Now().UTC())
	a.IncidentID = "sym-" + id
	if err := s.RecordAction(ctx, a); err != nil {
		t.Fatal(err)
	}
	insert := func(labeller, outcome string) {
		if _, err := pool.Exec(ctx, `INSERT INTO heal_outcome_history (action_id, outcome, labeller, superseded_by) VALUES ($1,$2,$3,'next')`, id, outcome, labeller); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		insert("v1-namespace-wide", "contradicted") // the same label copied three times
	}
	insert("v2-entity-ordered", "unknown") // a distinct label from another labeller
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM heal_outcome_history WHERE action_id=$1`, id)
	})

	for i := 0; i < 2; i++ {
		runMigration(t, pool, "014_heal_outcome_history_unique.sql")
	}
	history, err := s.OutcomeHistory(ctx, id)
	if err != nil || len(history) != 2 {
		t.Fatalf("expected one row per labeller (2), got %d (%v)", len(history), err)
	}
	seen := map[string]string{}
	for _, h := range history {
		seen[h.Labeller] = h.Outcome
	}
	if seen["v1-namespace-wide"] != "contradicted" || seen["v2-entity-ordered"] != "unknown" {
		t.Fatalf("a distinct label was lost or changed: %+v", seen)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO heal_outcome_history (action_id, outcome, labeller, superseded_by) VALUES ($1,'x','v1-namespace-wide','next')`, id); err == nil {
		t.Fatal("the uniqueness rule is not enforced")
	}
}
