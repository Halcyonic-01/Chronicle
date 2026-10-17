package web

import (
	"os/exec"
	"testing"
)

// The console's sign-in code runs in the browser; this runs it under Node with
// a fake window and identity provider. Skipped where Node is absent.
func TestConsoleSignInFlow(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	out, err := exec.Command(node, "testdata/auth_test.js", "static/auth.js").CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}
