package heal

import (
	"fmt"
	"os"
	"strings"
)

// Some actions are too broad for one person's say-so. Which ones is configuration
// (HEAL_TWO_APPROVER_ACTIONS, or approvers_required on a rule); the default is the
// two that rewrite a workload's pod template. A restart touches one pod, and a
// restore only brings back replicas somebody had.
var defaultTwoApproverActions = []string{ActionRollbackDeployment, ActionBumpMemory}

// TwoApproverActionsFromEnv lists the action types that need two approvers.
// Empty (or unset) is the default list; "none" turns the rule off.
func TwoApproverActionsFromEnv() map[string]bool {
	raw := strings.TrimSpace(os.Getenv("HEAL_TWO_APPROVER_ACTIONS"))
	switch {
	case raw == "":
		out := map[string]bool{}
		for _, a := range defaultTwoApproverActions {
			out[a] = true
		}
		return out
	case strings.EqualFold(raw, "none"):
		return map[string]bool{}
	}
	return csvSet(raw)
}

// approversFor is how many distinct people an action of this type needs: the
// rule's own number if it sets one, otherwise two for the configured types.
func approversFor(rule *Rule, actionType string, twoApprover map[string]bool) int {
	n := 1
	if twoApprover[actionType] {
		n = 2
	}
	if rule != nil && rule.Approvers > n {
		n = rule.Approvers
	}
	return n
}

// approvalsShort re-checks, at execution, that enough distinct people approved
// and that each is still permitted today. The need is the larger of what the
// decision was proposed under and what policy asks now, so tightening the policy
// reaches decisions already waiting. "" means the approvals stand.
func (c *Controller) approvalsShort(action *Action, rule *Rule) string {
	need := action.ApprovalsRequired
	if now := approversFor(rule, action.ActionType, c.Policy.TwoApprover); now > need {
		need = now
	}
	if need <= 1 {
		return ""
	}
	if got := DistinctApprovers(action.Approvals); got < need {
		return fmt.Sprintf("%d approvers are required and %d distinct people approved", need, got)
	}
	if c.ApproverStillPermitted != nil {
		for _, a := range action.Approvals {
			if !c.ApproverStillPermitted(a) {
				return fmt.Sprintf("an approver (%s) is no longer permitted to approve", a.Display)
			}
		}
	}
	return ""
}
