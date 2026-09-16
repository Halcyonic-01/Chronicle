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

// classifyOutcome judges one decision against what happened next.
//
// It is deliberately reluctant. A recovery that cannot be attributed is left
// unknown rather than counted, because a threshold calibrated on invented
// labels is worse than one calibrated on fewer honest ones.
func classifyOutcome(a *Action, followUps []event.Event) (outcome, detail string) {
	var recovered bool
	var appliedFix, otherFix string

	for _, e := range followUps {
		if isRecoverySignal(e) {
			recovered = true
		}
		if remediates(a, e) {
			appliedFix = e.Title
			continue
		}
		if otherFix == "" && isRemediationShaped(e) {
			otherFix = e.EntityName + ": " + e.Title
		}
	}

	switch {
	case appliedFix != "" && recovered:
		return OutcomeConfirmed, fmt.Sprintf("the proposed action was performed (%s) and the symptom recovered", appliedFix)
	case recovered && otherFix != "":
		return OutcomeContradicted, fmt.Sprintf("the symptom recovered after a different change (%s)", otherFix)
	case appliedFix != "" && !recovered:
		return OutcomeUnknown, "the proposed action was performed but no recovery was observed in the window"
	case recovered:
		return OutcomeUnknown, "the symptom recovered but no change explains it"
	default:
		return OutcomeUnknown, "no recovery was observed in the window"
	}
}

// remediates reports whether an event is the action this decision proposed,
// actually carried out on the target it named.
func remediates(a *Action, e event.Event) bool {
	if !strings.EqualFold(e.Namespace, a.Namespace) {
		return false
	}
	switch a.ActionType {
	case ActionRestoreReplicas:
		if e.Type != "scale" || !sameWorkload(e.EntityName, a.Target) {
			return false
		}
		// A restore is a scale away from zero, not any scale at all.
		return gjson.GetBytes(e.Payload, "old_replicas").Int() == 0 &&
			gjson.GetBytes(e.Payload, "new_replicas").Int() > 0
	case ActionRestartPod:
		return e.Type == "resource_deleted" && e.EntityName == a.Target
	case ActionRollbackDeployment:
		return e.Type == "deploy" && sameWorkload(e.EntityName, a.Target)
	case ActionBumpMemory:
		owner := gjson.GetBytes(a.Payload, "owner").String()
		return e.Type == "resource_change" && (sameWorkload(e.EntityName, a.Target) || e.EntityName == owner)
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
	case "deploy", "config_change", "resource_change":
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
	return candidate == target || strings.HasPrefix(candidate, target+"-")
}
