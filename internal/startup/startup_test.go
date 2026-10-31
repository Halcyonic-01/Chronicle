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
	base := []string{"CHRONICLE_ENVIRONMENT", "production", "POSTGRES_URL", "postgres://x?sslmode=require"}
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

func TestProductionRefusesADatabaseConnectionThatCanFallBackToPlainText(t *testing.T) {
	prod := func(url string) []string {
		return Problems(env("CHRONICLE_ENVIRONMENT", "production", "POSTGRES_URL", url, "CHRONICLE_API_TOKEN", "t"))
	}
	for _, ok := range []string{"postgres://u:p@h/db?sslmode=require", "postgres://u:p@h/db?sslmode=verify-full", "postgres://u:p@h/db?sslmode=verify-ca"} {
		if got := prod(ok); len(got) != 0 {
			t.Errorf("%s was refused: %v", ok, got)
		}
	}
	for _, bad := range []string{"postgres://u:p@h/db", "postgres://u:p@h/db?sslmode=disable", "postgres://u:p@h/db?sslmode=prefer", "postgres://u:p@h/db?sslmode=allow"} {
		if got := prod(bad); len(got) != 1 || !strings.Contains(got[0], "sslmode") {
			t.Errorf("%s: %v", bad, got)
		}
	}
	// The message names the mode but never the password.
	if got := prod("postgres://u:hunter2@h/db?sslmode=disable"); len(got) != 1 || strings.Contains(got[0], "hunter2") {
		t.Errorf("a credential leaked into the message: %v", got)
	}
	// Outside production a laptop or a kind cluster may use plain text.
	if got := Problems(env("POSTGRES_URL", "postgres://u:p@localhost/db?sslmode=disable")); len(got) != 0 {
		t.Errorf("development was refused: %v", got)
	}
}

func TestASchemaNameMustBeAPlainIdentifier(t *testing.T) {
	for _, bad := range []string{`x"; drop schema public; --`, "Chronicle", "pg_catalog", "a b"} {
		if got := Problems(env("POSTGRES_SCHEMA", bad)); len(got) != 1 || !strings.Contains(got[0], "POSTGRES_SCHEMA") {
			t.Errorf("%q: %v", bad, got)
		}
	}
	if got := Problems(env("POSTGRES_SCHEMA", "chronicle")); len(got) != 0 {
		t.Errorf("%v", got)
	}
}

func TestWarningsNameTheSupabasePitfalls(t *testing.T) {
	direct := "postgresql://postgres:pw@db.abcdefgh.supabase.co:5432/postgres"
	session := "postgresql://postgres.abcdefgh:pw@aws-0-eu-west-1.pooler.supabase.com:5432/postgres"
	transaction := "postgresql://postgres.abcdefgh:pw@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"
	has := func(ws []string, want string) bool {
		for _, w := range ws {
			if strings.Contains(w, want) {
				return true
			}
		}
		return false
	}
	if ws := Warnings(env("POSTGRES_URL", direct)); !has(ws, "IPv6") || !has(ws, "POSTGRES_SCHEMA") {
		t.Errorf("direct host: %v", ws)
	}
	if ws := Warnings(env("POSTGRES_URL", transaction, "POSTGRES_SCHEMA", "chronicle")); len(ws) != 1 || !has(ws, "transaction pooler") {
		t.Errorf("transaction pooler: %v", ws)
	}
	if ws := Warnings(env("POSTGRES_URL", session, "POSTGRES_SCHEMA", "chronicle")); len(ws) != 0 {
		t.Errorf("the recommended setup produced warnings: %v", ws)
	}
	if ws := Warnings(env("POSTGRES_URL", "postgres://u:p@postgres.internal:5432/db")); len(ws) != 0 {
		t.Errorf("an ordinary database produced warnings: %v", ws)
	}
	for _, ws := range [][]string{Warnings(env("POSTGRES_URL", direct)), Warnings(env("POSTGRES_URL", transaction))} {
		for _, w := range ws {
			if strings.Contains(w, "pw@") || strings.Contains(w, ":pw") {
				t.Errorf("a credential leaked into a warning: %s", w)
			}
		}
	}
}
