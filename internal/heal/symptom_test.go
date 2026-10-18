package heal

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var (
	t0policy = SymptomPolicy{ImproveWithin: 2 * time.Minute, Hold: 5 * time.Minute}
	started  = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
)

func task() SymptomTask {
	return SymptomTask{ID: "a1", ActionType: ActionRestoreReplicas, Target: "redis", SymptomType: "error_spike",
		SymptomEntity: "frontend", Severity: "critical", StartedAt: started, DueAt: t0policy.Due(started)}
}

func at(d time.Duration) time.Time { return started.Add(d) }

var (
	healthy = Reading{Value: 0.01, Threshold: 0.05}
	firing  = Reading{Value: 0.40, Threshold: 0.05}
)

func TestTheDefaultsAreTwoMinutesToImproveAndFiveToHold(t *testing.T) {
	t.Setenv("SYMPTOM_IMPROVE_WITHIN", "")
	t.Setenv("SYMPTOM_HOLD", "")
	if p := SymptomPolicyFromEnv(); p.ImproveWithin != 2*time.Minute || p.Hold != 5*time.Minute {
		t.Fatalf("defaults %+v", p)
	}
	t.Setenv("SYMPTOM_HOLD", "90s")
	if p := SymptomPolicyFromEnv(); p.Hold != 90*time.Second {
		t.Fatalf("override %+v", p)
	}
}

// Recovery has to be seen to be believed: first improvement, then the whole hold.
func TestRecoveryIsSucceededOnlyAfterTheFullHold(t *testing.T) {
	j := judgeSymptom(task(), healthy, nil, at(10*time.Second), t0policy)
	if j.done || j.improvedAt == nil {
		t.Fatalf("the first healthy reading starts the hold, it does not finish it: %+v", j)
	}
	tk := task()
	tk.ImprovedAt = j.improvedAt
	if j := judgeSymptom(tk, healthy, nil, at(10*time.Second+4*time.Minute+59*time.Second), t0policy); j.done {
		t.Fatalf("finished one second early: %+v", j)
	}
	j = judgeSymptom(tk, healthy, nil, at(10*time.Second+5*time.Minute), t0policy)
	if !j.done || j.verdict != SymptomSucceeded {
		t.Fatalf("a full hold should succeed: %+v", j)
	}
}

func TestNoImprovementWithinTwoMinutesFails(t *testing.T) {
	if j := judgeSymptom(task(), firing, nil, at(119*time.Second), t0policy); j.done {
		t.Fatalf("still inside the improvement window: %+v", j)
	}
	j := judgeSymptom(task(), firing, nil, at(2*time.Minute), t0policy)
	if !j.done || j.verdict != SymptomFailed || !strings.Contains(j.detail, "no improvement") {
		t.Fatalf("got %+v", j)
	}
}

// Improving and then coming back is not recovery.
func TestTheSymptomReturningDuringTheHoldFails(t *testing.T) {
	tk := task()
	im := at(20 * time.Second)
	tk.ImprovedAt = &im
	j := judgeSymptom(tk, firing, nil, at(3*time.Minute), t0policy)
	if !j.done || j.verdict != SymptomFailed || !strings.Contains(j.detail, "came back") {
		t.Fatalf("got %+v", j)
	}
}

// No data is never recovery.
func TestMissingDataIsUnknownNeverSucceeded(t *testing.T) {
	cases := map[string]error{"no series": ErrNoSeries, "prometheus down": errors.New("connection refused")}
	for name, err := range cases {
		if j := judgeSymptom(task(), Reading{}, err, at(time.Minute), t0policy); j.done {
			t.Errorf("%s: gave up before the deadline: %+v", name, j)
		}
		j := judgeSymptom(task(), Reading{}, err, task().DueAt, t0policy)
		if !j.done || j.verdict != SymptomUnknown {
			t.Errorf("%s: got %+v at the deadline", name, j)
		}
	}
	// 0/0 (nothing served) reads as NaN, and NaN > threshold is false: it must not look healthy.
	j := judgeSymptom(task(), Reading{Value: math.NaN(), Threshold: 0.05}, nil, task().DueAt, t0policy)
	if j.verdict != SymptomUnknown || j.improvedAt != nil {
		t.Fatalf("NaN was treated as recovery: %+v", j)
	}
	// Data that disappears after an improvement is unknown, not success.
	tk := task()
	im := at(10 * time.Second)
	tk.ImprovedAt = &im
	if j := judgeSymptom(tk, Reading{}, ErrNoSeries, tk.DueAt, t0policy); j.verdict != SymptomUnknown {
		t.Fatalf("got %+v", j)
	}
}

func TestASymptomThatIsNotAMetricIsUnknownAtOnce(t *testing.T) {
	tk := task()
	tk.SymptomType = "log_error"
	j := judgeSymptom(tk, Reading{}, ErrNotMetric, at(time.Second), t0policy)
	if !j.done || j.verdict != SymptomUnknown || !strings.Contains(j.detail, "outcome labeller") {
		t.Fatalf("got %+v", j)
	}
}

// ---- the checker ----------------------------------------------------------

type fakeSymptomStore struct {
	tasks    []SymptomTask
	finished map[string]string
	improved map[string]time.Time
	action   *Action
}

func (f *fakeSymptomStore) BeginSymptomCheck(context.Context, string, time.Time, time.Time) error {
	return nil
}
func (f *fakeSymptomStore) PendingSymptomChecks(context.Context, int) ([]SymptomTask, error) {
	return f.tasks, nil
}
func (f *fakeSymptomStore) RecordSymptomImprovement(_ context.Context, id string, at time.Time) error {
	if f.improved == nil {
		f.improved = map[string]time.Time{}
	}
	f.improved[id] = at
	return nil
}
func (f *fakeSymptomStore) FinishSymptomCheck(_ context.Context, id, verdict, detail string, _ time.Time) (*Action, error) {
	if f.finished == nil {
		f.finished = map[string]string{}
	}
	if _, done := f.finished[id]; done {
		return nil, nil // already finished by another pass
	}
	f.finished[id] = verdict
	a := *f.action
	a.SymptomCheck, a.SymptomDetail = verdict, detail
	return &a, nil
}

type fixedProbe struct {
	r   Reading
	err error
}

func (p fixedProbe) Read(context.Context, string, string, string) (Reading, error) { return p.r, p.err }

type recNotifier struct{ n int }

func (r *recNotifier) NotifySymptom(context.Context, *Action) error { r.n++; return nil }

type recPager struct{ n int }

func (r *recPager) Page(context.Context, *Action) error { r.n++; return nil }

func newChecker(store *fakeSymptomStore, probe SymptomProbe, env string) (*SymptomChecker, *recNotifier, *recPager) {
	n, p := &recNotifier{}, &recPager{}
	return &SymptomChecker{Store: store, Probe: probe, Policy: t0policy, Notifier: n, Pager: p, Environment: env}, n, p
}

func TestAFailureIsAnnouncedOnceAndPagedOnlyWhenCriticalInProduction(t *testing.T) {
	for _, c := range []struct {
		env, severity string
		wantPage      int
	}{{"production", "critical", 1}, {"production", "warning", 0}, {"staging", "critical", 0}, {"", "critical", 0}} {
		tk := task()
		tk.Severity = c.severity
		store := &fakeSymptomStore{tasks: []SymptomTask{tk}, action: &Action{ID: "a1", ActionType: ActionRestoreReplicas, Target: "redis"}}
		checker, notifier, pager := newChecker(store, fixedProbe{r: firing}, c.env)
		if err := checker.Pass(context.Background(), at(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if store.finished["a1"] != SymptomFailed || notifier.n != 1 || pager.n != c.wantPage {
			t.Errorf("%s/%s: verdict %q, notified %d, paged %d (want page %d)", c.env, c.severity, store.finished["a1"], notifier.n, pager.n, c.wantPage)
		}
		// A second pass over the same task (another replica, a retry) finds it finished.
		_ = checker.Pass(context.Background(), at(2*time.Minute+5*time.Second))
		if notifier.n != 1 || pager.n != c.wantPage {
			t.Errorf("%s/%s: announced again on a repeat pass", c.env, c.severity)
		}
	}
}

func TestSuccessAndUnknownNeverNotifyOrPage(t *testing.T) {
	tk := task()
	im := at(time.Second)
	tk.ImprovedAt = &im
	store := &fakeSymptomStore{tasks: []SymptomTask{tk}, action: &Action{ID: "a1"}}
	checker, notifier, pager := newChecker(store, fixedProbe{r: healthy}, "production")
	_ = checker.Pass(context.Background(), at(time.Second+5*time.Minute))
	if store.finished["a1"] != SymptomSucceeded || notifier.n+pager.n != 0 {
		t.Fatalf("success: %v notified %d paged %d", store.finished, notifier.n, pager.n)
	}
	store = &fakeSymptomStore{tasks: []SymptomTask{task()}, action: &Action{ID: "a1"}}
	checker, notifier, pager = newChecker(store, fixedProbe{err: ErrNoSeries}, "production")
	_ = checker.Pass(context.Background(), task().DueAt)
	if store.finished["a1"] != SymptomUnknown || notifier.n+pager.n != 0 {
		t.Fatalf("unknown: %v notified %d paged %d", store.finished, notifier.n, pager.n)
	}
}

// Improvement is persisted the moment it is seen, so a new leader resumes the
// hold rather than starting it over.
func TestTheFirstHealthyReadingIsPersisted(t *testing.T) {
	store := &fakeSymptomStore{tasks: []SymptomTask{task()}, action: &Action{ID: "a1"}}
	checker, _, _ := newChecker(store, fixedProbe{r: healthy}, "")
	_ = checker.Pass(context.Background(), at(30*time.Second))
	if got, ok := store.improved["a1"]; !ok || !got.Equal(at(30*time.Second)) || len(store.finished) != 0 {
		t.Fatalf("improved=%v finished=%v", store.improved, store.finished)
	}
}

func TestWithoutAProbeEveryCheckIsUnknown(t *testing.T) {
	store := &fakeSymptomStore{tasks: []SymptomTask{task()}, action: &Action{ID: "a1"}}
	checker, _, _ := newChecker(store, nil, "")
	_ = checker.Pass(context.Background(), at(time.Second))
	if store.finished["a1"] != SymptomUnknown {
		t.Fatalf("got %v", store.finished)
	}
}

// A page is sent to its webhook; with no URL configured nothing is sent.
func TestThePagerPostsOnlyWhenConfigured(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { atomic.AddInt32(&hits, 1) }))
	defer srv.Close()
	t.Setenv("PAGER_WEBHOOK_URL", "")
	if p := NewPagerFromEnv(); p.Page(context.Background(), &Action{}) != nil || atomic.LoadInt32(&hits) != 0 {
		t.Fatal("paged without a configured webhook")
	}
	t.Setenv("PAGER_WEBHOOK_URL", srv.URL)
	if err := NewPagerFromEnv().Page(context.Background(), &Action{ID: "a1", SymptomDetail: "x"}); err != nil || atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("hits=%d err=%v", hits, err)
	}
}
