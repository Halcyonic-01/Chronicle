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
}

// Expected is the verdict the truth calls for.
func Expected(truth []Root) string {
	if len(truth) == 0 {
		return eval.NoRootCause
	}
	return eval.RootCause
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
