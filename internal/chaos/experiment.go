// Package chaos runs fault-injection experiments against a live cluster and
// scores Chronicle's root-cause analysis of the incidents they cause.
//
// Each experiment injects one real fault into the victim application, records
// its ground truth before injecting, lets Chronicle's own collectors observe
// what happens, asks Chronicle's API for its analysis, scores the answer with
// the same code as the controlled benchmarks, and always restores the cluster.
package chaos

import (
	"context"
	"fmt"
	"time"

	policyv1 "k8s.io/api/policy/v1"

	"github.com/Halcyonic-01/Chronicle/internal/heal"

	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata/eval"
)

// What Chronicle can know about an injected cause.
const (
	ObservableChange  = "observable-change"  // a Kubernetes change Chronicle records
	ObservableFailure = "observable-failure" // a recorded failure with no change behind it
	Unobservable      = "unobservable"       // nothing Chronicle records explains it
)

// Root names a cause by event type and entity, as the benchmarks do.
type Root struct {
	Type   string `json:"type"`
	Entity string `json:"entity"`
}

// Experiment is one fault, the diagnosis expected of it, and how to undo it.
type Experiment struct {
	Name       string
	Category   string
	Hypothesis string
	// Truth returns the real cause, resolved just before injection because pod
	// and node names are only known then. Empty: nothing recorded explains it.
	Truth func(ctx context.Context, c *Cluster) ([]Root, error)
	// Setup prepares without being the fault (a Recreate strategy, which
	// Chronicle does not record) and runs before the injection clock starts.
	Setup  []Action
	Inject []Action
	// Restore undoes what the snapshot cannot (a paused process). Deployments,
	// Services, NetworkPolicies and paused nodes are restored from the snapshot.
	Restore []Action
	// Requires refuses to run when the fault could hit more than its target.
	Requires func(ctx context.Context, c *Cluster) error
	// Heal, in heal mode, is how the proposal is decided and how it should end.
	Heal *HealExpectation
}

// Expected is the verdict the truth calls for.
func Expected(truth []Root) string {
	if len(truth) == 0 {
		return eval.NoRootCause
	}
	return eval.RootCause
}

// HealSuite is the RCA-to-remediation campaign, in a fixed order so steps on
// one workload stay apart. Each fault is decided by a real OIDC identity and
// carried out by Chronicle's worker; see deploy/e2e.
func HealSuite() []Experiment {
	byName := map[string]Experiment{}
	for _, e := range Catalog() {
		byName[e.Name] = e
	}
	with := func(e Experiment, decide, expect, why string) Experiment {
		e.Heal = &HealExpectation{Decide: decide, Expect: expect, Why: why}
		return e
	}
	because := func(e Experiment, reason string) Experiment {
		e.Heal.Reason = reason
		return e
	}
	// Steps on one workload are spaced past the 10-minute cooldown. (A repeat
	// outage of the same kind minutes later comes out inconclusive, so the
	// cooldown is not tested by a deliberate repeat; see internal/heal/README.md.)
	return []Experiment{
		with(byName["redis-scaled-to-zero"], "approve", heal.StatusSucceeded, "restore_replicas brings redis back"),
		with(byName["api-bad-release"], "approve", heal.StatusSucceeded, "rollback_deployment restores the previous pod template"),
		because(with(RedisDeadlockedStrictBudget(), "approve", heal.StatusFailed, "the disruption budget refuses to evict the unready pod"), "PodDisruptionBudget"),
		because(with(WorkerRestoreBlockedByQuota(), "approve", heal.StatusFailed, "the restore is applied but no pod can be created; verification fails"), "rollout"),
		with(byName["postgres-scaled-to-zero"], "deny", heal.StatusDenied, "a denied proposal never runs; the harness restores"),
		// Revised after the first live run: RCA names the first release, which
		// started the outage, and the executor refuses to act on evidence about
		// an image that is no longer running. Spaced past the api cooldown.
		because(with(byName["api-two-bad-releases"], "approve", heal.StatusFailed, "the decision is about release a; release b now runs, so the executor refuses"), "refusing to roll back"),
		with(byName["api-bad-config"], "approve", "none", "no rule acts on a config change, so nothing is proposed"),
		with(byName["redis-deadlocked"], "approve", heal.StatusSucceeded, "restart_pod evicts the unready pod; its replacement is ready"),
	}
}

// WorkerRestoreBlockedByQuota is a scale to zero Chronicle diagnoses correctly
// whose remedy cannot work: a pod quota, which Chronicle does not watch, stops
// the restored replica from being created. The restore is applied and recovery
// verification fails.
func WorkerRestoreBlockedByQuota() Experiment {
	return Experiment{
		Name: "worker-restore-blocked-by-quota", Category: ObservableChange,
		Hypothesis: "the scale of worker to zero is named; restoring it cannot succeed under the quota",
		Truth:      fixed(Root{"scale", "worker"}),
		Setup:      []Action{SetProgressDeadline("worker", 60)},
		Inject:     []Action{Scale("worker", 0), CapPods("chaos-pod-cap", "worker")},
		Restore:    []Action{RemoveQuota("chaos-pod-cap")},
	}
}

// redisDeadlocked stops redis answering anything, its readiness probe
// included, without crashing it: the pod turns unready and no probe restarts
// it. Nothing changed, so the failure itself is where it began.
func redisDeadlocked() Experiment {
	return Experiment{
		Name: "redis-deadlocked", Category: ObservableFailure,
		Hypothesis: "a redis that stopped answering is named, as its pod becoming unready",
		Truth: func(ctx context.Context, c *Cluster) ([]Root, error) {
			pod, err := c.Pod(ctx, "redis")
			if err != nil {
				return nil, err
			}
			return []Root{{"became_unready", pod.Name}}, nil
		},
		// ALL also holds back CLIENT UNPAUSE, so the restore replaces the pod.
		Inject:  []Action{Exec("redis", "redis", "redis-cli", "CLIENT", "PAUSE", "900000", "ALL")},
		Restore: []Action{DeletePodsOf("redis")},
	}
}

// RedisDeadlockedStrictBudget is redis-deadlocked under a disruption budget
// that refuses to evict an unready pod: a restart that cannot be carried out.
func RedisDeadlockedStrictBudget() Experiment {
	e := redisDeadlocked()
	e.Name = "redis-deadlocked-strict-budget"
	e.Hypothesis = "as redis-deadlocked; the budget makes the restart fail, and the failure is recorded"
	e.Setup = []Action{SetUnhealthyEviction("redis", policyv1.IfHealthyBudget)}
	e.Restore = []Action{SetUnhealthyEviction("redis", policyv1.AlwaysAllow), DeletePodsOf("redis")}
	return e
}

func fixed(roots ...Root) func(context.Context, *Cluster) ([]Root, error) {
	return func(context.Context, *Cluster) ([]Root, error) { return roots, nil }
}

// Catalog covers the ways a service goes down: a dependency removed, a bad
// change, a routing break, a lost node, a crash with no change, and faults
// Chronicle cannot see at all. Targets are the victim application.
func Catalog() []Experiment {
	scaleToZero := func(dep string) Experiment {
		return Experiment{
			Name: dep + "-scaled-to-zero", Category: ObservableChange,
			Hypothesis: fmt.Sprintf("the scale of %s to zero is named as the root cause", dep),
			Truth:      fixed(Root{"scale", dep}),
			Inject:     []Action{Scale(dep, 0)},
		}
	}
	return []Experiment{
		scaleToZero("redis"),
		scaleToZero("worker"),
		scaleToZero("api"),
		// The worker reaches postgres through a default in its code, so no edge
		// may lead there: this one tests the topology as much as the ranking.
		scaleToZero("postgres"),
		{
			Name: "api-bad-config", Category: ObservableChange,
			Hypothesis: "a typo in the api's REDIS_URL is named as the root cause",
			Truth:      fixed(Root{"config_change", "api"}),
			Inject:     []Action{SetEnv("api", "REDIS_URL", "redis-typo:6379")},
		},
		{
			Name: "worker-bad-config", Category: ObservableChange,
			Hypothesis: "a wrong POSTGRES_URL on the worker is named as the root cause",
			Truth:      fixed(Root{"config_change", "worker"}),
			Inject:     []Action{SetEnv("worker", "POSTGRES_URL", "postgres://postgres@postgres-typo:5432/postgres?sslmode=disable")},
		},
		{
			// With the default rolling update the old pod keeps serving and nothing
			// breaks; Recreate makes a bad release an outage, as it is in production.
			Name: "api-bad-release", Category: ObservableChange,
			Hypothesis: "a release whose image cannot start is named as the root cause",
			Truth:      fixed(Root{"deploy", "api"}),
			Setup:      []Action{Recreate("api")},
			Inject:     []Action{SetImage("api", "chronicle-victim:chaos-missing-release")},
		},
		{
			Name: "redis-memory-limit-cut", Category: ObservableChange,
			Hypothesis: "a memory limit too small for redis is named as the root cause",
			Truth:      fixed(Root{"resource_change", "redis"}),
			Setup:      []Action{Recreate("redis")},
			Inject:     []Action{SetMemoryLimit("redis", "2Mi")},
		},
		{
			// Established connections outlive a selector change (two live runs:
			// no effect), so redis drops its clients and they reconnect through
			// the broken Service, as they would after any restart or idle timeout.
			Name: "redis-selector-broken", Category: ObservableChange,
			Hypothesis: "a Service selector that matches no pods is named as the root cause",
			Truth:      fixed(Root{"service_change", "redis"}),
			Inject: []Action{SetSelector("redis", map[string]string{"app": "redis-typo"}),
				Try(Exec("redis", "redis", "redis-cli", "CLIENT", "KILL", "TYPE", "normal"))},
		},
		{
			Name: "redis-node-lost", Category: ObservableFailure,
			Hypothesis: "the loss of the node running redis is named as the root cause",
			Truth: func(ctx context.Context, c *Cluster) ([]Root, error) {
				node, err := c.NodeOf(ctx, "redis")
				return []Root{{"node_not_ready", node}}, err
			},
			Requires: func(ctx context.Context, c *Cluster) error { return c.NodeIsDedicated(ctx, "redis") },
			Inject:   []Action{PauseNodeOf("redis")},
		},
		{
			// A crash with no change behind it: the failure itself is where it began.
			Name: "redis-crash", Category: ObservableFailure,
			Hypothesis: "redis crashing on its own is named, as the restart of its pod",
			Truth: func(ctx context.Context, c *Cluster) ([]Root, error) {
				pod, err := c.Pod(ctx, "redis")
				if err != nil {
					return nil, err
				}
				return []Root{{"container_restart", pod.Name}}, nil
			},
			// The shutdown ends the exec session with it, so each is best effort;
			// repeating it pushes the kubelet into back-off, a real outage.
			Inject: []Action{
				Try(Exec("redis", "redis", "redis-cli", "shutdown", "nosave")), Wait(15 * time.Second),
				Try(Exec("redis", "redis", "redis-cli", "shutdown", "nosave")), Wait(15 * time.Second),
				Try(Exec("redis", "redis", "redis-cli", "shutdown", "nosave")),
			},
		},
		redisDeadlocked(),
		{
			// A rollback that lands on a revision just as broken: the remediation
			// runs and does not fix anything. Two bad releases, the second on top
			// of the first; a short progress deadline lets the Deployment
			// controller itself declare the rolled-back revision failed.
			Name: "api-two-bad-releases", Category: ObservableChange,
			Hypothesis: "the latest of two bad releases is named as the root cause",
			Truth:      fixed(Root{"deploy", "api"}),
			Setup:      []Action{Recreate("api"), SetProgressDeadline("api", 60)},
			Inject: []Action{SetImage("api", "chronicle-victim:chaos-missing-release-a"), Wait(30 * time.Second),
				SetImage("api", "chronicle-victim:chaos-missing-release-b")},
		},
		{
			// The api waits on redis with no timeout, so pausing writes hangs every
			// request (INCR is a write); no Kubernetes object changes or fails.
			// WRITE, not ALL, so CLIENT UNPAUSE itself is not held back.
			Name: "redis-hang", Category: Unobservable,
			Hypothesis: "a hung redis, which no recorded event explains, is declared as no root cause",
			Truth:      fixed(),
			Inject:     []Action{Exec("redis", "redis", "redis-cli", "CLIENT", "PAUSE", "240000", "WRITE")},
			Restore:    []Action{Try(Exec("redis", "redis", "redis-cli", "CLIENT", "UNPAUSE"))},
		},
		{
			// Chronicle does not watch NetworkPolicies. Existing connections survive a
			// new policy, so redis drops its clients to make them reconnect.
			Name: "redis-network-blocked", Category: Unobservable,
			Hypothesis: "traffic to redis cut by a NetworkPolicy, which Chronicle does not record, is declared as no root cause",
			Truth:      fixed(),
			Inject:     []Action{DenyIngress("redis"), Exec("redis", "redis", "redis-cli", "CLIENT", "KILL", "TYPE", "normal")},
		},
	}
}

// Select returns the named experiments, in catalog order; empty means all.
func Select(all []Experiment, names []string) ([]Experiment, error) {
	if len(names) == 0 {
		return all, nil
	}
	byName := map[string]Experiment{}
	for _, e := range all {
		byName[e.Name] = e
	}
	var out []Experiment
	for _, n := range names {
		e, ok := byName[n]
		if !ok {
			return nil, fmt.Errorf("unknown experiment %q (see 'chaos list')", n)
		}
		out = append(out, e)
	}
	return out, nil
}
