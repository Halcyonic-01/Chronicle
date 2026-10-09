package heal

import (
	"fmt"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/tidwall/gjson"
)

// Outcome labels for a dry-run decision, judged after the fact.
const (
	OutcomeConfirmed    = "confirmed"
	OutcomeContradicted = "contradicted"
	OutcomeUnknown      = "unknown"
)

// SettleWindow is how long a decision is left alone before judging it, and how
// far past it the evidence is read. Measured against real decisions, recovery
// signals arrive a median of ~23 minutes later, so a 15-minute window stopped
// watching before the answer arrived and labelled 40 of 42 decisions unknown.
// Wider would catch more, at the cost of mistaking an unrelated later change
// for the fix.
const SettleWindow = 45 * time.Minute

// EventClockSkew is how far before a decision an event may be dated and still
// count as after it. Kubernetes stamps a change at one-second resolution while a
// decision is timed to the millisecond, so a fix applied right after the
// decision can be dated up to a second before it.
const EventClockSkew = 2 * time.Second

// FieldManager is the name Chronicle's own writes carry, so they can be told
// apart from a human's in the events they produce.
const FieldManager = "chronicle-heal"

// SymptomRef is the failure a decision was about.
type SymptomRef struct {
	Namespace, Name string
}

// classifyOutcome judges one decision against what happened next.
//
// It is deliberately reluctant. A recovery that cannot be attributed is left
// unknown rather than counted, because a threshold calibrated on invented
// labels is worse than one calibrated on fewer honest ones. Only recoveries of
// the symptom or of the workload the action targets count, only after the
// change being credited, and only changes to entities the analysis weighed can
// contradict.
func classifyOutcome(a *Action, symptom SymptomRef, followUps []event.Event) (outcome, detail string) {
	workload := a.Workload
	if workload == "" {
		workload = workloadOf(a.ActionType, a.Target, a.Payload)
	}
	recovers := func(e event.Event) bool {
		return isRecoverySignal(e) && (sameWorkload(e.EntityName, symptom.Name) || sameWorkload(e.EntityName, workload))
	}
	// The window ends where this incident ended: the first recovery that held.
	// A change made after it cannot be what fixed it, and the 45 minutes ahead
	// otherwise reach into whatever fails next.
	followUps = withinIncident(followUps, recovers)
	recoveryAfter := func(t time.Time) *event.Event {
		for i := range followUps {
			if !followUps[i].OccurredAt.Before(t) && recovers(followUps[i]) {
				return &followUps[i]
			}
		}
		return nil
	}

	var applied, other *event.Event
	selfApplied := false
	for i := range followUps {
		e := &followUps[i]
		if remediates(a, *e) {
			if byChronicle(*e) {
				selfApplied = true
				continue
			}
			if applied == nil {
				applied = e
			}
			continue
		}
		if other == nil && isRemediationShaped(*e) && !byChronicle(*e) && relatedTo(a, symptom, workload, e.EntityName) {
			other = e
		}
	}

	switch {
	case applied != nil:
		rec := recoveryAfter(applied.OccurredAt)
		if rec == nil {
			if selfApplied {
				return OutcomeUnknown, "the change was made by Chronicle itself; its own action is not evidence"
			}
			return OutcomeUnknown, "the proposed action was performed but the symptom did not recover after it"
		}
		if other != nil && !other.OccurredAt.After(rec.OccurredAt) {
			return OutcomeUnknown, fmt.Sprintf("both the proposed action (%s) and another change (%s: %s) preceded the recovery", applied.Title, other.EntityName, other.Title)
		}
		return OutcomeConfirmed, fmt.Sprintf("the proposed action was performed (%s) and %s recovered after it", applied.Title, rec.EntityName)
	case selfApplied:
		return OutcomeUnknown, "the change was made by Chronicle itself; its own action is not evidence"
	case other != nil:
		if rec := recoveryAfter(other.OccurredAt); rec != nil {
			return OutcomeContradicted, fmt.Sprintf("the symptom recovered after a different change (%s: %s)", other.EntityName, other.Title)
		}
		return OutcomeUnknown, "another change was made but the symptom did not recover after it"
	}
	if rec := recoveryAfter(time.Time{}); rec != nil {
		return OutcomeUnknown, "the symptom recovered but no change explains it"
	}
	return OutcomeUnknown, "no recovery was observed in the window"
}

// withinIncident cuts follow-ups at the first recovery that held: the entity
// stayed free of failures for FlapWindow afterwards. With no such recovery the
// whole window stays, and the outcome is judged on what it holds.
func withinIncident(followUps []event.Event, recovers func(event.Event) bool) []event.Event {
	for i, e := range followUps {
		if !recovers(e) {
			continue
		}
		held := true
		for _, x := range followUps[i+1:] {
			if x.OccurredAt.Sub(e.OccurredAt) >= flapWindow {
				break
			}
			if isFailureSignal(x) && sameWorkload(x.EntityName, e.EntityName) {
				held = false
				break
			}
		}
		if held {
			return followUps[:i+1]
		}
	}
	return followUps
}

// flapWindow matches the RCA's: a recovery shorter than this is a flap.
const flapWindow = time.Minute

func isFailureSignal(e event.Event) bool {
	switch e.Type {
	case "error_spike", "latency_spike", "log_error", "became_unready", "container_restart",
		"oom_kill", "crash_loop", "node_not_ready", "application_unhealthy":
		return true
	}
	return false
}

// remediates reports whether an event is the action this decision proposed,
// actually carried out on the target it named.
func remediates(a *Action, e event.Event) bool {
	if !strings.EqualFold(e.Namespace, a.Namespace) {
		return false
	}
	switch a.ActionType {
	case ActionRestoreReplicas:
		if e.Type != "scale" || e.EntityName != a.Target {
			return false
		}
		// A restore is a scale away from zero, not any scale at all.
		return gjson.GetBytes(e.Payload, "old_replicas").Int() == 0 &&
			gjson.GetBytes(e.Payload, "new_replicas").Int() > 0
	case ActionRestartPod:
		return e.Type == "resource_deleted" && e.EntityKind == "Pod" && e.EntityName == a.Target
	case ActionRollbackDeployment:
		// A rollback returns to the image the bad deploy replaced; any other
		// deploy, including a roll-forward, is a different change.
		old := gjson.GetBytes(a.Payload, "old_image").String()
		return e.Type == "deploy" && e.EntityName == a.Target && old != "" &&
			gjson.GetBytes(e.Payload, "new_image").String() == old
	case ActionBumpMemory:
		owner := gjson.GetBytes(a.Payload, "owner").String()
		return e.Type == "resource_change" && owner != "" && e.EntityName == owner &&
			gjson.GetBytes(e.Payload, "new_mem_limit").Int() > gjson.GetBytes(e.Payload, "old_mem_limit").Int()
	}
	return false
}

// byChronicle reports a change Chronicle made itself. Counting it would let the
// engine's own actions confirm its own proposals.
func byChronicle(e event.Event) bool {
	return gjson.GetBytes(e.Payload, "changed_by").String() == FieldManager
}

// relatedTo limits contradicting changes to the symptom, the action's target
// and what the analysis weighed. A deploy of an unrelated service in the same
// namespace says nothing about whether this proposal was right.
func relatedTo(a *Action, symptom SymptomRef, workload, name string) bool {
	if sameWorkload(name, symptom.Name) || sameWorkload(name, workload) {
		return true
	}
	for _, r := range a.Related {
		if sameWorkload(name, r) {
			return true
		}
	}
	return false
}

// isRemediationShaped reports whether an event could plausibly be somebody
// fixing the problem. Two exclusions matter, because both produced false
// contradictions: a scale that reduces replicas is the kind of change that
// breaks things rather than repairs them, and a deleted Pod is usually churn --
// short-lived Pods come and go constantly. A deletion that is the proposed
// restart is matched by remediates instead.
func isRemediationShaped(e event.Event) bool {
	switch e.Type {
	case "deploy", "config_change", "resource_change", "service_change", "hpa_change":
		return true
	case "scale":
		return gjson.GetBytes(e.Payload, "new_replicas").Int() >
			gjson.GetBytes(e.Payload, "old_replicas").Int()
	}
	return false
}

func isRecoverySignal(e event.Event) bool {
	if strings.HasSuffix(e.Type, "_resolved") {
		return true
	}
	if e.Type == "became_ready" {
		return true
	}
	if e.Type == "resource_status" {
		phase := gjson.GetBytes(e.Payload, "phase").String()
		return phase == "Running" || phase == "Completed"
	}
	return false
}

// sameWorkload matches a Deployment against the Pods it owns, since a decision
// names the workload while the follow-up event may name either.
func sameWorkload(candidate, target string) bool {
	return target != "" && (candidate == target || strings.HasPrefix(candidate, target+"-"))
}
