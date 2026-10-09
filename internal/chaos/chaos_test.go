package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
)

var t0 = time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)

// clock is a fake time that moves only when something sleeps.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
	return nil
}

// victim mirrors the victim application: five one-replica deployments with
// services, redis alone on its own node.
func victim(extra ...runtime.Object) []runtime.Object {
	var objs []runtime.Object
	for _, name := range []string{"frontend", "api", "worker", "redis", "postgres"} {
		one := int32(1)
		objs = append(objs,
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
				Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
					Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: "img:" + name, Env: []corev1.EnvVar{{Name: "MODE", Value: "ok"}}}}}},
					Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType}},
				Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
			},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec: corev1.ServiceSpec{Selector: map[string]string{"app": name}, Ports: []corev1.ServicePort{{Port: 8080}}}},
		)
		node := "kind-worker"
		if name == "redis" {
			node = "kind-worker2"
		}
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-7f9c", Namespace: "default", Labels: map[string]string{"app": name}, CreationTimestamp: metav1.NewTime(t0.Add(-time.Hour))},
			Spec:       corev1.PodSpec{NodeName: node}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	}
	return append(objs, extra...)
}

func testCluster(clk *clock, objs ...runtime.Object) (*Cluster, *fake.Clientset, *[]string) {
	client := fake.NewSimpleClientset(objs...)
	var mu sync.Mutex
	commands := &[]string{}
	c := &Cluster{Client: client, Namespace: "default", Context: "kind-test",
		Run: func(_ context.Context, name string, args ...string) ([]byte, error) {
			mu.Lock()
			defer mu.Unlock()
			*commands = append(*commands, name+" "+strings.Join(args, " "))
			return nil, nil
		},
		Sleep: clk.Sleep}
	return c, client, commands
}

func replicas(t *testing.T, client *fake.Clientset, dep string) int32 {
	t.Helper()
	d, err := client.AppsV1().Deployments("default").Get(context.Background(), dep, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return *d.Spec.Replicas
}

// --- actions and restore ------------------------------------------------------

func TestActionsChangeOnlyWhatTheySay(t *testing.T) {
	clk := &clock{t: t0}
	c, client, _ := testCluster(clk, victim()...)
	ctx := context.Background()
	for _, a := range []Action{Scale("redis", 0), SetEnv("api", "REDIS_URL", "typo:6379"), SetEnv("api", "MODE", "bad"),
		SetImage("api", "img:broken"), Recreate("api"), SetMemoryLimit("worker", "2Mi"), SetSelector("redis", map[string]string{"app": "nothing"})} {
		if err := a.Do(ctx, c); err != nil {
			t.Fatalf("%s: %v", a.Describe(), err)
		}
	}
	if replicas(t, client, "redis") != 0 {
		t.Error("redis was not scaled to zero")
	}
	api, _ := client.AppsV1().Deployments("default").Get(ctx, "api", metav1.GetOptions{})
	ct := api.Spec.Template.Spec.Containers[0]
	env := map[string]string{}
	for _, e := range ct.Env {
		env[e.Name] = e.Value
	}
	if env["REDIS_URL"] != "typo:6379" || env["MODE"] != "bad" || ct.Image != "img:broken" || api.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Errorf("api not changed as asked: env=%v image=%s strategy=%s", env, ct.Image, api.Spec.Strategy.Type)
	}
	worker, _ := client.AppsV1().Deployments("default").Get(ctx, "worker", metav1.GetOptions{})
	if got := worker.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String(); got != "2Mi" {
		t.Errorf("memory limit %s", got)
	}
	svc, _ := client.CoreV1().Services("default").Get(ctx, "redis", metav1.GetOptions{})
	if svc.Spec.Selector["app"] != "nothing" {
		t.Errorf("selector %v", svc.Spec.Selector)
	}
	if replicas(t, client, "frontend") != 1 {
		t.Error("an untouched deployment changed")
	}
}

// Every shipped experiment, injected and then restored, leaves the cluster as
// it found it: specs, policies and paused nodes.
func TestEveryExperimentIsUndoneByRestore(t *testing.T) {
	for _, exp := range Catalog() {
		t.Run(exp.Name, func(t *testing.T) {
			clk := &clock{t: t0}
			c, client, commands := testCluster(clk, victim()...)
			ctx := context.Background()
			before, err := c.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range append(append([]Action{}, exp.Setup...), exp.Inject...) {
				if err := a.Do(ctx, c); err != nil {
					t.Fatalf("%s: %v", a.Describe(), err)
				}
			}
			for _, a := range exp.Restore {
				if err := a.Do(ctx, c); err != nil {
					t.Fatalf("%s: %v", a.Describe(), err)
				}
			}
			if _, err := c.Restore(ctx, before); err != nil {
				t.Fatal(err)
			}
			after, _ := c.Snapshot(ctx)
			for name, spec := range before.Deployments {
				if !sameSpec(after.Deployments[name], spec) {
					t.Errorf("deployment %s not restored", name)
				}
			}
			for name, spec := range before.Services {
				if !equality.Semantic.DeepEqual(after.Services[name].Selector, spec.Selector) {
					t.Errorf("service %s not restored", name)
				}
			}
			policies, _ := client.NetworkingV1().NetworkPolicies("default").List(ctx, metav1.ListOptions{})
			if len(policies.Items) != 0 {
				t.Errorf("network policies left behind: %d", len(policies.Items))
			}
			paused, unpaused := 0, 0
			for _, cmd := range *commands {
				paused += strings.Count(cmd, "docker pause ")
				unpaused += strings.Count(cmd, "docker unpause ")
			}
			if paused != unpaused {
				t.Errorf("paused %d node(s), unpaused %d", paused, unpaused)
			}
		})
	}
}

func TestRestoreChangesNothingWhenNothingChanged(t *testing.T) {
	c, _, _ := testCluster(&clock{t: t0}, victim()...)
	snap, _ := c.Snapshot(context.Background())
	changed, err := c.Restore(context.Background(), snap)
	if err != nil || len(changed) != 0 {
		t.Fatalf("an unchanged cluster was touched: %v %v", changed, err)
	}
}

// A node fault is refused when the node runs anything but the target and
// per-node agents: it would blind Chronicle too.
func TestANodeFaultNeedsADedicatedNode(t *testing.T) {
	agent := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kindnet-x", Namespace: "kube-system", OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "kindnet"}}},
		Spec: corev1.PodSpec{NodeName: "kind-worker2"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	c, _, _ := testCluster(&clock{t: t0}, victim(agent)...)
	if err := c.NodeIsDedicated(context.Background(), "redis"); err != nil {
		t.Fatalf("a node with only redis and agents is dedicated: %v", err)
	}
	collector := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "chronicle-1", Namespace: "chronicle"},
		Spec: corev1.PodSpec{NodeName: "kind-worker2"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	c, _, _ = testCluster(&clock{t: t0}, victim(agent, collector)...)
	if err := c.NodeIsDedicated(context.Background(), "redis"); err == nil || !strings.Contains(err.Error(), "chronicle/chronicle-1") {
		t.Fatalf("a node that also runs Chronicle must be refused: %v", err)
	}
}

func TestOnlyKindContextsAreAllowed(t *testing.T) {
	for _, ok := range []string{"kind-chronicle", "kind-x"} {
		if CheckContext(ok, "") != nil {
			t.Errorf("%s should be allowed", ok)
		}
	}
	for _, bad := range []string{"", "prod-eu", "gke_project_zone_cluster", "kindchronicle"} {
		if CheckContext(bad, "") == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
	if CheckContext("staging", "staging") != nil || CheckContext("staging", "other") == nil {
		t.Error("only the exact context named in -allow-context may be overridden")
	}
}

func TestCatalogIsWellFormed(t *testing.T) {
	c, _, _ := testCluster(&clock{t: t0}, victim()...)
	seen := map[string]bool{}
	for _, e := range Catalog() {
		if seen[e.Name] || e.Hypothesis == "" || e.Truth == nil || len(e.Inject) == 0 {
			t.Errorf("%s: duplicate, or missing hypothesis, truth or injection", e.Name)
		}
		seen[e.Name] = true
		truth, err := e.Truth(context.Background(), c)
		if err != nil {
			t.Fatalf("%s: %v", e.Name, err)
		}
		switch e.Category {
		case Unobservable:
			if len(truth) != 0 {
				t.Errorf("%s: an unobservable cause has no recorded root", e.Name)
			}
		case ObservableChange, ObservableFailure:
			if len(truth) == 0 || truth[0].Entity == "" {
				t.Errorf("%s: an observable cause needs its root: %v", e.Name, truth)
			}
		default:
			t.Errorf("%s: unknown category %q", e.Name, e.Category)
		}
	}
}

func TestPlanIsSeededAndRunsEveryTrial(t *testing.T) {
	exps := Catalog()
	a, b := Plan(exps, 2, 42), Plan(exps, 2, 42)
	if len(a) != 2*len(exps) {
		t.Fatalf("planned %d runs", len(a))
	}
	count := map[string]int{}
	for i := range a {
		if a[i].Experiment.Name != b[i].Experiment.Name || a[i].Trial != b[i].Trial {
			t.Fatal("the same seed must give the same order")
		}
		count[a[i].Experiment.Name]++
	}
	for _, e := range exps {
		if count[e.Name] != 2 {
			t.Errorf("%s planned %d times", e.Name, count[e.Name])
		}
	}
}

// --- the runner, against a fake Chronicle ----------------------------------

// fakeChronicle serves signals once their time has come, and canned analyses.
type fakeChronicle struct {
	clk      *clock
	mu       sync.Mutex
	base     time.Time     // the injection time, set by the experiment
	lag      time.Duration // how long after its ingestion an event can be read
	signals  []rca.Signal
	results  map[string]rca.Result
	analysed map[string]time.Time
}

func (f *fakeChronicle) arm(at time.Time) { f.mu.Lock(); f.base = at; f.mu.Unlock() }

func (f *fakeChronicle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clk.Now()
	switch r.URL.Path {
	case "/api/events":
		var visible []rca.Signal
		if !f.base.IsZero() {
			for _, s := range f.signals {
				s.IngestedAt = f.base.Add(s.IngestedAt.Sub(time.Time{}))
				if !s.IngestedAt.Add(f.lag).After(now) {
					visible = append(visible, s)
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"events": visible})
	case "/api/analyze":
		id := r.URL.Query().Get("event_id")
		f.analysed[id] = now
		res := f.results[id]
		_ = json.NewEncoder(w).Encode(res)
	default:
		http.NotFound(w, r)
	}
}

func signal(id, typ, entity string, after time.Duration) rca.Signal {
	return rca.Signal{Event: event.Event{ID: id, Namespace: "default", EntityKind: "Service", EntityName: entity, Type: typ, Severity: "critical",
		IngestedAt: time.Time{}.Add(after)}}
}

func answer(verdict, typ, entity string, conf float64) rca.Result {
	return rca.Result{Verdict: verdict, Confidence: conf, Narrative: "n",
		Candidates: []rca.Candidate{{Event: event.Event{Type: typ, EntityName: entity}}}}
}

// steadyScript answers the steady-state check from a function of the clock.
type steadyScript func() (bool, string)

func (s steadyScript) Check(context.Context) (bool, string) { return s() }

type harness struct {
	clk     *clock
	cluster *Cluster
	client  *fake.Clientset
	api     *fakeChronicle
	out     *bytes.Buffer
	runner  *Runner
}

func newHarness(t *testing.T, steady Checker) *harness {
	clk := &clock{t: t0}
	c, client, _ := testCluster(clk, victim()...)
	api := &fakeChronicle{clk: clk, results: map[string]rca.Result{}, analysed: map[string]time.Time{}}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	h := &harness{clk: clk, cluster: c, client: client, api: api, out: &bytes.Buffer{}}
	if steady == nil {
		steady = steadyScript(func() (bool, string) { return true, "" })
	}
	cfg := DefaultConfig()
	h.runner = &Runner{Cluster: c, Chronicle: NewChronicle(srv.URL, ""), Steady: steady, Config: cfg, Out: h.out, Now: clk.Now}
	return h
}

// armed injects a fault that also tells the fake Chronicle the clock has started.
func (h *harness) experiment(truth []Root, inject ...Action) Experiment {
	arm := step{"start the clock", func(ctx context.Context, c *Cluster) error { h.api.arm(h.clk.Now()); return nil }}
	return Experiment{Name: "test-fault", Category: ObservableChange, Hypothesis: "h", Truth: fixed(truth...),
		Inject: append([]Action{arm}, inject...)}
}

func (h *harness) records(t *testing.T) []Record {
	t.Helper()
	recs, err := ReadRecords(bytes.NewReader(h.out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

// The truth is on record before the fault goes in; the answer is taken from the
// first alert after it settles, not from an earlier log line; the cluster is
// put back afterwards.
func TestARunPreRegistersAnalysesTheAlertAndRestores(t *testing.T) {
	h := newHarness(t, nil)
	h.api.signals = []rca.Signal{signal("log-1", "log_error", "api-7f9c", 10*time.Second), signal("alert-1", "error_spike", "frontend", 40*time.Second)}
	h.api.results["alert-1"] = answer(rca.VerdictRootCause, "scale", "redis", 0.82)
	h.api.results["log-1"] = answer(rca.VerdictRootCause, "scale", "redis", 0.7)
	registered := false
	check := step{"check registration", func(context.Context, *Cluster) error {
		registered = strings.Contains(h.out.String(), `"kind":"start"`) && strings.Contains(h.out.String(), `"type":"scale"`)
		return nil
	}}
	exp := h.experiment([]Root{{"scale", "redis"}}, check, Scale("redis", 0))
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{Seed: 1}); err != nil {
		t.Fatal(err)
	}
	if !registered {
		t.Fatal("the truth must be written before the fault is injected")
	}
	recs := h.records(t)
	if len(recs) != 3 || recs[0].Kind != "run" || recs[1].Kind != "start" || recs[2].Kind != "result" {
		t.Fatalf("records out of order: %+v", recs)
	}
	o := recs[2].Result
	if o.Status != StatusScored || o.Headline == nil || o.Headline.Symptom.ID != "alert-1" {
		t.Fatalf("the alert, not the earlier log line, is what pages: %+v", o)
	}
	if o.Row == nil || o.Row.Outcome != "correct" || !o.Row.Top1 {
		t.Fatalf("scored wrong: %+v", o.Row)
	}
	analysedAt := h.api.analysed["alert-1"]
	if settle := analysedAt.Sub(o.InjectedAt); settle < 40*time.Second+h.runner.Config.Settle {
		t.Errorf("analysed %.0fs after the fault, before the alert had settled", settle.Seconds())
	}
	if len(o.Others) != 1 || o.Others[0].Symptom.ID != "log-1" || o.Consistency != 1 {
		t.Errorf("the other symptom should be analysed for consistency: %+v", o.Others)
	}
	if replicas(t, h.client, "redis") != 1 || !o.Recovered {
		t.Fatal("redis was not restored")
	}
}

// An event can be raised promptly and still reach the store late; what the
// operator sees is when it can be read, so both are recorded.
func TestIngestionLagIsMeasuredNotHidden(t *testing.T) {
	h := newHarness(t, nil)
	h.api.lag = 90 * time.Second
	h.api.signals = []rca.Signal{signal("alert-1", "error_spike", "frontend", 40*time.Second)}
	h.api.results["alert-1"] = answer(rca.VerdictRootCause, "scale", "redis", 0.8)
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0))
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err != nil {
		t.Fatal(err)
	}
	o := h.records(t)[2].Result
	if o.AlertAfter != 40 || o.AlertSeenAfter < 130 || o.AlertSeenAfter > 136 {
		t.Fatalf("raised after %.0fs, readable after %.0fs; want 40 and about 130", o.AlertAfter, o.AlertSeenAfter)
	}
}

// Even the first fault waits for a full cooldown of steady state: a recovery
// shorter than Chronicle's flap window merges two incidents into one.
func TestAFaultWaitsForAFullCooldownOfSteadyState(t *testing.T) {
	var h *harness
	h = newHarness(t, steadyScript(func() (bool, string) {
		if h.clk.Now().Before(t0.Add(30 * time.Second)) {
			return false, "error ratio above 0.05"
		}
		return true, ""
	}))
	h.api.signals = []rca.Signal{signal("alert-1", "error_spike", "frontend", 30*time.Second)}
	h.api.results["alert-1"] = answer(rca.VerdictRootCause, "scale", "redis", 0.8)
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0))
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err != nil {
		t.Fatal(err)
	}
	injected := h.records(t)[2].Result.InjectedAt
	if quiet := injected.Sub(t0.Add(30 * time.Second)); quiet < h.runner.Config.Cooldown {
		t.Fatalf("injected after %.0fs of steady state, want at least %s", quiet.Seconds(), h.runner.Config.Cooldown)
	}
}

// A wrong answer is scored as wrong, and a confident one as false confident.
func TestAWrongAnswerIsScoredAsWrong(t *testing.T) {
	h := newHarness(t, nil)
	h.api.signals = []rca.Signal{signal("alert-1", "error_spike", "frontend", 30*time.Second)}
	h.api.results["alert-1"] = answer(rca.VerdictRootCause, "deploy", "frontend", 0.9)
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0))
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err != nil {
		t.Fatal(err)
	}
	o := h.records(t)[2].Result
	if o.Row.Outcome != "wrong-root" || !o.Row.FalseConfident {
		t.Fatalf("got %+v", o.Row)
	}
}

// With nothing raised, the run says whether the fault broke anything at all.
func TestNothingRaisedIsNotDetectedOrNoEffect(t *testing.T) {
	for _, broken := range []bool{false, true} {
		var injected bool
		var h *harness
		h = newHarness(t, steadyScript(func() (bool, string) {
			if broken && injected && replicas(t, h.client, "redis") == 0 {
				return false, "error ratio above 0.05"
			}
			return true, ""
		}))
		mark := step{"mark", func(context.Context, *Cluster) error { injected = true; return nil }}
		exp := h.experiment([]Root{{"scale", "redis"}}, mark, Scale("redis", 0))
		if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err != nil {
			t.Fatal(err)
		}
		o := h.records(t)[2].Result
		want := StatusNoEffect
		if broken {
			want = StatusNotDetected
		}
		if o.Status != want || o.Row != nil {
			t.Errorf("broken=%v: status %s (%s), want %s", broken, o.Status, o.Reason, want)
		}
		if replicas(t, h.client, "redis") != 1 {
			t.Errorf("broken=%v: not restored", broken)
		}
	}
}

// A failure half way through the injection still restores what was changed.
func TestAFailedInjectionIsStillRestored(t *testing.T) {
	h := newHarness(t, nil)
	boom := step{"fail", func(context.Context, *Cluster) error { return errors.New("boom") }}
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0), boom)
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err != nil {
		t.Fatal(err)
	}
	o := h.records(t)[2].Result
	if o.Status != StatusError || replicas(t, h.client, "redis") != 1 {
		t.Fatalf("status %s, redis replicas %d", o.Status, replicas(t, h.client, "redis"))
	}
}

// An interrupted run restores the cluster before it stops.
func TestAnInterruptedRunRestoresBeforeStopping(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	interrupt := step{"interrupt", func(context.Context, *Cluster) error { cancel(); return nil }}
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0), interrupt)
	second := h.experiment(nil, Scale("api", 0))
	err := h.runner.Run(ctx, []Planned{{Experiment: exp, Trial: 1}, {Experiment: second, Trial: 1}}, RunInfo{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("an interrupted run reports it: %v", err)
	}
	if replicas(t, h.client, "redis") != 1 || replicas(t, h.client, "api") != 1 {
		t.Fatal("the interrupted fault was not restored, or the next one ran")
	}
}

// When the system does not come back, nothing more is injected.
func TestARunStopsWhenTheSystemDoesNotRecover(t *testing.T) {
	var h *harness
	h = newHarness(t, steadyScript(func() (bool, string) {
		return h.api.base.IsZero(), "api 0/1 ready"
	}))
	h.runner.Config.SteadyTimeout = time.Minute
	first := h.experiment(nil, Scale("redis", 0))
	second := Experiment{Name: "never", Category: Unobservable, Hypothesis: "h", Truth: fixed(), Inject: []Action{Scale("api", 0)}}
	err := h.runner.Run(context.Background(), []Planned{{Experiment: first, Trial: 1}, {Experiment: second, Trial: 1}}, RunInfo{})
	if err == nil || !strings.Contains(err.Error(), "did not recover") {
		t.Fatalf("the run must stop: %v", err)
	}
	if replicas(t, h.client, "api") != 1 {
		t.Fatal("a fault was injected into a system that had not recovered")
	}
}

// Nothing is injected into a system that is not in its steady state.
func TestNothingIsInjectedIntoAnUnsteadySystem(t *testing.T) {
	h := newHarness(t, steadyScript(func() (bool, string) { return false, "worker 0/1 ready" }))
	h.runner.Config.SteadyTimeout = time.Minute
	exp := h.experiment([]Root{{"scale", "redis"}}, Scale("redis", 0))
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err == nil {
		t.Fatal("the run must refuse")
	}
	if replicas(t, h.client, "redis") != 1 {
		t.Fatal("injected anyway")
	}
}

func TestARefusedPreconditionIsSkipped(t *testing.T) {
	h := newHarness(t, nil)
	exp := h.experiment(nil, Scale("redis", 0))
	exp.Requires = func(context.Context, *Cluster) error { return errors.New("no dedicated node") }
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: exp, Trial: 1}}, RunInfo{}); err != nil {
		t.Fatal(err)
	}
	recs := h.records(t)
	if len(recs) != 2 || recs[1].Result.Status != StatusSkipped || replicas(t, h.client, "redis") != 1 {
		t.Fatalf("a refused experiment is recorded and touches nothing: %+v", recs)
	}
}

func TestTheReportListsEveryExperiment(t *testing.T) {
	h := newHarness(t, nil)
	h.api.signals = []rca.Signal{signal("alert-1", "error_spike", "frontend", 30*time.Second)}
	h.api.results["alert-1"] = answer(rca.VerdictNoRootCause, "log_error", "api-7f9c", 0.1)
	hang := h.experiment(nil, Scale("redis", 0))
	hang.Name, hang.Category = "test-hang", Unobservable
	skipped := h.experiment(nil, Scale("api", 0))
	skipped.Name, skipped.Requires = "test-skipped", func(context.Context, *Cluster) error { return errors.New("no") }
	if err := h.runner.Run(context.Background(), []Planned{{Experiment: hang, Trial: 1}, {Experiment: skipped, Trial: 1}}, RunInfo{Seed: 9}); err != nil {
		t.Fatal(err)
	}
	text := Report(h.records(t))
	for _, want := range []string{"seed 9", "scored 1", "skipped 1", "test-hang", "test-skipped", "declared-no-root-cause", "no-root-cause declared          : 1/1"} {
		if !strings.Contains(text, want) {
			t.Errorf("report should mention %q:\n%s", want, text)
		}
	}
}
