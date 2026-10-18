package startup

import (
	"strings"
	"testing"
)

func env(pairs ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return func(k string) string { return m[k] }
}

func TestALaptopMayStartWithNothingSet(t *testing.T) {
	if got := Problems(env()); len(got) != 0 {
		t.Fatalf("a development start was refused: %v", got)
	}
}

func TestAClusterNeverUsesTheBuiltInDatabasePassword(t *testing.T) {
	got := Problems(env("KUBERNETES_SERVICE_HOST", "10.96.0.1"))
	if len(got) != 1 || !strings.Contains(got[0], "POSTGRES_URL") {
		t.Fatalf("%v", got)
	}
	if got := Problems(env("KUBERNETES_SERVICE_HOST", "10.96.0.1", "POSTGRES_URL", "postgres://x")); len(got) != 0 {
		t.Fatalf("%v", got)
	}
}

func TestProductionNeedsAnAPITokenAndRealApproversForLiveHealing(t *testing.T) {
	base := []string{"CHRONICLE_ENVIRONMENT", "production", "POSTGRES_URL", "postgres://x"}
	got := Problems(env(base...))
	if len(got) != 1 || !strings.Contains(got[0], "CHRONICLE_API_TOKEN") {
		t.Fatalf("no token: %v", got)
	}
	with := append(base, "CHRONICLE_API_TOKEN", "t")
	if got := Problems(env(with...)); len(got) != 0 {
		t.Fatalf("dry-run production with a token: %v", got)
	}
	live := append(with, "HEAL_LIVE_ENABLED", "true")
	if got := Problems(env(live...)); len(got) != 1 || !strings.Contains(got[0], "OIDC_ISSUER_URL") {
		t.Fatalf("live without OIDC: %v", got)
	}
	if got := Problems(env(append(live, "OIDC_ISSUER_URL", "https://idp")...)); len(got) != 0 {
		t.Fatalf("live with OIDC: %v", got)
	}
	// Everything wrong is reported at once, not one fix at a time.
	if got := Problems(env("CHRONICLE_ENVIRONMENT", "production", "HEAL_LIVE_ENABLED", "true")); len(got) != 3 {
		t.Fatalf("%v", got)
	}
}
