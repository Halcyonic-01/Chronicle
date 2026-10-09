package heal

// The execution worker, the real Postgres store and the real Kubernetes
// executor, running together. Kubernetes is a fake client, so nothing here can
// touch a cluster: this is the whole dry-run path, with "live" switched on only
// inside the test to see what the gates allow and refuse.
//
// Needs CHRONICLE_TEST_POSTGRES_URL (a scratch database with the migrations).

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/jackc/pgx/v5/pgxpool"
)

type workerEnv struct {
	t      *testing.T
	store  *PostgresAuditStore
	pool   *pgxpool.Pool
	client *fake.Clientset
	ctrl   *Controller
	sx     string
	now    time.Time
	mu     sync.Mutex
}

func (e *workerEnv) clock() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.now }
func (e *workerEnv) advance(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = e.now.Add(d)
}

func newWorkerEnv(t *testing.T, workloads map[string]int32) *workerEnv {
	t.Helper()
	store, pool, sx := healTestStore(t)
	// Earlier tests leave executions in the last hour; the global cap counts them.
	if _, err := pool.Exec(context.Background(), `UPDATE heal_actions SET started_at = started_at - interval '3 hours' WHERE started_at IS NOT NULL AND id NOT LIKE $1`, "%"+sx); err != nil {
		t.Fatal(err)
	}
	var objects []appsv1.Deployment
	for name, replicas := range workloads {
		r := replicas
		objects = append(objects, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: appsv1.DeploymentSpec{Replicas: &r}})
	}
	client := fake.NewSimpleClientset()
	for i := range objects {
		if _, err := client.AppsV1().Deployments("default").Create(context.Background(), &objects[i], metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	client.ClearActions()

	e := &workerEnv{t: t, store: store, pool: pool, client: client, sx: sx, now: time.Now().UTC()}
	targets := map[string]bool{}
	for name := range workloads {
		targets["default/"+name] = true
	}
	e.ctrl = &Controller{
		Store: store, Executor: &KubernetesExecutor{Client: client},
		Rules: append([]Rule(nil), defaultRules...), Now: e.clock,
		Policy: Policy{
			LiveEnabled: true, KillSwitch: false, Timeout: 5 * time.Second, VerifyTimeout: 5 * time.Second,
			ObservationSince:  time.Now().Add(-40 * 24 * time.Hour),
			AllowedActions:    map[string]bool{ActionRestoreReplicas: true},
			AllowedNamespaces: map[string]bool{"default": true},
			AllowedTargets:    targets,
			// The evidence gate is exercised in controller_test; here it would only
			// hide the gates under test.
			MinDecisive: 0, MinPrecision: 0,
			MaxExecutionsPerHour: 10, TargetCooldown: 30 * time.Minute,
		},
	}
	return e
}

// approved plants a proposal for a scale-to-zero of workload and approves it.
func (e *workerEnv) approved(id, cause, workload string) *Action {
	e.t.Helper()
	a := proposal(id+"-"+e.sx, cause+"-"+e.sx, workload, e.clock())
	a.Payload = []byte(`{"old_replicas":3,"new_replicas":0}`)
	if err := e.store.RecordAction(context.Background(), a); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.store.DecideAction(context.Background(), Decision{ID: a.ID, Approved: true, By: "test", Result: "APPROVED", Now: e.clock()}); err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *workerEnv) status(id string) *Action {
	e.t.Helper()
	a, err := e.store.GetAction(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *workerEnv) replicas(name string) int32 {
	d, err := e.client.AppsV1().Deployments("default").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		e.t.Fatal(err)
	}
	return *d.Spec.Replicas
}

// writes counts the Kubernetes mutations the executor made.
func (e *workerEnv) writes() int {
	n := 0
	for _, a := range e.client.Actions() {
		switch a.GetVerb() {
		case "update", "patch", "delete", "create":
			n++
		}
	}
	return n
}

func (e *workerEnv) tick() {
	e.t.Helper()
	if err := e.ctrl.Tick(context.Background()); err != nil {
		e.t.Fatal(err)
	}
}

// The whole path once: approved, claimed, executed, verified, recorded.
func TestWorkerExecutesAnApprovedDecisionEndToEnd(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	a := e.approved("ok", "c-ok", "cache")
	e.tick()
	got := e.status(a.ID)
	if got.Status != StatusSucceeded || got.Attempts != 1 || got.StartedAt == nil || got.FinishedAt == nil || got.Verification == "" {
		t.Fatalf("expected a recorded success, got status=%s attempts=%d verification=%q error=%q", got.Status, got.Attempts, got.Verification, got.Error)
	}
	if e.replicas("cache") != 3 || e.writes() != 1 {
		t.Fatalf("expected exactly one write restoring 3 replicas, got %d replicas and %d writes", e.replicas("cache"), e.writes())
	}
	// Ticking again must not repeat it.
	e.tick()
	e.tick()
	if e.writes() != 1 {
		t.Fatalf("a finished decision was executed again: %d writes", e.writes())
	}
}

// Live healing off: the worker only tidies. An approved decision never runs and
// expires unexecuted.
func TestWorkerNeverExecutesWhileLiveIsOff(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	e.ctrl.Policy.LiveEnabled = false
	a := e.approved("off", "c-off", "cache")
	e.tick()
	if got := e.status(a.ID); got.Status != StatusApproved || got.Attempts != 0 || e.writes() != 0 {
		t.Fatalf("executed with live off: status=%s attempts=%d writes=%d", got.Status, got.Attempts, e.writes())
	}
	e.advance(DefaultApprovalTTL + time.Minute)
	e.tick()
	if got := e.status(a.ID); got.Status != StatusExpired || e.writes() != 0 {
		t.Fatalf("an approved decision should expire unexecuted: status=%s writes=%d", got.Status, e.writes())
	}
}

// Kill switch: a decision it blocks is blocked for good. Turning the switch off
// later must not resurrect it.
func TestKillSwitchBlocksForGoodNotJustForNow(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	e.ctrl.Policy.KillSwitch = true
	a := e.approved("kill", "c-kill", "cache")
	e.tick()
	got := e.status(a.ID)
	if got.Status != StatusBlocked || !strings.Contains(got.Result, "kill switch") || e.writes() != 0 {
		t.Fatalf("the kill switch did not block: status=%s result=%q writes=%d", got.Status, got.Result, e.writes())
	}
	e.ctrl.Policy.KillSwitch = false
	e.tick()
	e.tick()
	if got := e.status(a.ID); got.Status != StatusBlocked || e.writes() != 0 {
		t.Fatalf("a blocked decision ran after the kill switch was lifted: status=%s writes=%d", got.Status, e.writes())
	}
}

// A stale approval: the deadline passes before the worker runs. It expires; the
// executor is never called.
func TestWorkerNeverExecutesAStaleApproval(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	a := e.approved("stale", "c-stale", "cache")
	e.advance(DefaultApprovalTTL + time.Second)
	e.tick()
	if got := e.status(a.ID); got.Status != StatusExpired || !strings.Contains(got.Result, "EXPIRED") || e.writes() != 0 {
		t.Fatalf("a stale approval was not expired unexecuted: status=%s result=%q writes=%d", got.Status, got.Result, e.writes())
	}
}

// The approval was fresh but the world moved on: somebody already restored the
// workload. The executor refuses and nothing is overwritten.
func TestWorkerRefusesWhenTheWorkloadChangedAfterApproval(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 5})
	a := e.approved("moved", "c-moved", "cache")
	e.tick()
	got := e.status(a.ID)
	if got.Status != StatusFailed || !strings.Contains(got.Error, "already at 5") {
		t.Fatalf("expected a refusal naming the current count, got status=%s error=%q", got.Status, got.Error)
	}
	if e.replicas("cache") != 5 || e.writes() != 0 {
		t.Fatalf("the newer replica count was overwritten: %d replicas, %d writes", e.replicas("cache"), e.writes())
	}
	e.tick()
	if e.writes() != 0 {
		t.Fatal("a refused decision was retried")
	}
}

// Two proposals for one outage (the engine prevents this; the claim must hold
// even if it did not): only the first acts.
func TestWorkerActsOnAnOutageOnlyOnce(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0, "cache2": 0})
	first := e.approved("dup1", "c-same", "cache")
	e.advance(time.Second)
	second := e.approved("dup2", "c-same", "cache2") // different workload, same cause
	e.tick()
	if got := e.status(first.ID); got.Status != StatusSucceeded {
		t.Fatalf("the first proposal should act: %s (%s)", got.Status, got.Error)
	}
	got := e.status(second.ID)
	if got.Status != StatusBlocked || !strings.Contains(got.Result, "already acted on") {
		t.Fatalf("the duplicate for the same outage was not blocked: status=%s result=%q", got.Status, got.Result)
	}
	if e.replicas("cache2") != 0 || e.writes() != 1 {
		t.Fatalf("the duplicate acted: cache2=%d writes=%d", e.replicas("cache2"), e.writes())
	}
}

// Per-workload cooldown and the global hourly cap hold across worker passes.
func TestWorkerHonoursCooldownAndTheGlobalCap(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"a": 0, "b": 0, "c": 0})
	e.ctrl.Policy.MaxExecutionsPerHour = 2
	for i := range e.ctrl.Rules {
		e.ctrl.Rules[i].MaxPerHour = 10
	}
	first := e.approved("cap-a1", "c-a1", "a")
	e.tick()
	e.advance(time.Second)
	again := e.approved("cap-a2", "c-a2", "a") // same workload, new outage
	b := e.approved("cap-b", "c-b", "b")
	c := e.approved("cap-c", "c-c", "c")
	e.tick()
	if e.status(first.ID).Status != StatusSucceeded {
		t.Fatal("the first action should have run")
	}
	if got := e.status(again.ID); got.Status != StatusBlocked || !strings.Contains(got.Result, "cooldown") {
		t.Fatalf("the cooldown did not hold: status=%s result=%q", got.Status, got.Result)
	}
	if got := e.status(b.ID); got.Status != StatusSucceeded {
		t.Fatalf("b should run (second of two allowed): %s %s", got.Status, got.Result)
	}
	if got := e.status(c.ID); got.Status != StatusBlocked || !strings.Contains(got.Result, "global limit") {
		t.Fatalf("the global cap did not hold: status=%s result=%q", got.Status, got.Result)
	}
	if e.writes() != 2 {
		t.Fatalf("expected exactly 2 writes, got %d", e.writes())
	}
}

// An interrupted run: the worker died after claiming. The decision is closed as
// failed -- whether the change landed is unknown -- and is never retried.
func TestWorkerClosesAnInterruptedRunAndNeverRetriesIt(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	a := e.approved("crash", "c-crash", "cache")
	// The worker claims it, then dies before recording anything.
	claimed, refusal, err := e.store.ClaimExecution(context.Background(), ClaimRequest{
		ID: a.ID, Rule: a.Rule, Namespace: a.Namespace, Workload: a.Workload, Now: e.clock(),
		RuleMaxPerHour: 10, MaxPerHour: 10, Cooldown: 30 * time.Minute})
	if err != nil || !claimed {
		t.Fatalf("setup: claim failed: %v %q", err, refusal)
	}
	// A pass inside the time budget leaves it alone: it may still be running.
	grace := 2 * (e.ctrl.Policy.Timeout + e.ctrl.Policy.VerifyTimeout)
	e.advance(grace / 2)
	e.tick()
	if got := e.status(a.ID); got.Status != StatusExecuting {
		t.Fatalf("a run still inside its time budget was closed early: %s", got.Status)
	}
	// Past twice (timeout + verify timeout) it is presumed dead.
	e.advance(grace)
	e.tick()
	got := e.status(a.ID)
	if got.Status != StatusFailed || !strings.Contains(got.Result, "not retried") || e.writes() != 0 {
		t.Fatalf("an interrupted run was not closed as failed unretried: status=%s result=%q writes=%d", got.Status, got.Result, e.writes())
	}
	for i := 0; i < 3; i++ {
		e.tick()
	}
	if e.writes() != 0 || e.status(a.ID).Status != StatusFailed {
		t.Fatalf("the interrupted decision was retried: writes=%d status=%s", e.writes(), e.status(a.ID).Status)
	}
}

// Several workers (leader hand-over, a slow pass) racing for one decision:
// exactly one executes.
func TestConcurrentWorkersExecuteADecisionExactlyOnce(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	a := e.approved("race", "c-race", "cache")
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := &Controller{Store: e.store, Executor: &KubernetesExecutor{Client: e.client}, Rules: e.ctrl.Rules, Policy: e.ctrl.Policy, Now: e.clock}
			_ = worker.Tick(context.Background())
		}()
	}
	wg.Wait()
	if got := e.status(a.ID); got.Status != StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("expected one successful attempt, got status=%s attempts=%d error=%q", got.Status, got.Attempts, got.Error)
	}
	if e.writes() != 1 || e.replicas("cache") != 3 {
		t.Fatalf("racing workers wrote %d times (replicas %d)", e.writes(), e.replicas("cache"))
	}
}

// Rejected and previously blocked decisions are never queued, whatever the clock.
func TestWorkerNeverExecutesDeniedOrBlockedDecisions(t *testing.T) {
	e := newWorkerEnv(t, map[string]int32{"cache": 0})
	denied := proposal("den-"+e.sx, "c-den-"+e.sx, "cache", e.clock())
	blocked := proposal("blk-"+e.sx, "c-blk-"+e.sx, "cache", e.clock())
	blocked.Status, blocked.Approval, blocked.Proposed, blocked.ExpiresAt = StatusBlocked, ApprovalNotRequired, false, nil
	for _, a := range []*Action{denied, blocked} {
		a.Payload = []byte(`{"old_replicas":3,"new_replicas":0}`)
		if err := e.store.RecordAction(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.store.DecideAction(context.Background(), Decision{ID: denied.ID, Approved: false, By: "test", Now: e.clock()}); err != nil {
		t.Fatal(err)
	}
	e.tick()
	e.tick()
	if e.writes() != 0 || e.status(denied.ID).Status != StatusDenied || e.status(blocked.ID).Status != StatusBlocked {
		t.Fatalf("a denied or blocked decision was touched: writes=%d denied=%s blocked=%s", e.writes(), e.status(denied.ID).Status, e.status(blocked.ID).Status)
	}
}
