package heal

import (
	"strings"
	"testing"
)

func TestSlackSaysHowManyApproversAreNeededAndLinksToTheConsole(t *testing.T) {
	n := &SlackNotifier{consoleURL: "https://chronicle.example.com"}
	a := &Action{Status: StatusWouldRun, Approval: ApprovalPending, ActionType: ActionRollbackDeployment, Namespace: "default", Target: "api",
		Result: "WOULD HAVE RUN (approval required)", ApprovalsRequired: 2}
	got := n.message(a)
	for _, want := range []string{"rollback_deployment default/api", "needs 2 different approvers", "https://chronicle.example.com/#healing"} {
		if !strings.Contains(got, want) {
			t.Errorf("%q lacks %q", got, want)
		}
	}
	// One approver, no configured URL, or nothing left to decide: no extras.
	a.ApprovalsRequired = 1
	if got := (&SlackNotifier{}).message(a); strings.Contains(got, "needs") || strings.Contains(got, "http") {
		t.Errorf("unexpected extras: %q", got)
	}
	a.Approval, a.Status = ApprovalApproved, StatusSucceeded
	if got := n.message(a); strings.Contains(got, "#healing") {
		t.Errorf("a finished action links to a decision page: %q", got)
	}
}

func TestSlackReadsTheConsoleURLFromTheEnvironment(t *testing.T) {
	t.Setenv("SLACK_WEBHOOK_URL", "https://hooks.invalid/x")
	t.Setenv("CHRONICLE_PUBLIC_URL", " https://chronicle.example.com/ ")
	if got := NewSlackNotifierFromEnv().consoleURL; got != "https://chronicle.example.com" {
		t.Fatalf("got %q", got)
	}
}
