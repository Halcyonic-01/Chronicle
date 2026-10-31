// Package startup refuses configurations that are unsafe to run, before anything
// is started. Chronicle defaults to what is convenient on a laptop; these checks
// are what stop that reaching a cluster that matters.
package startup

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Problems lists what is wrong with the environment, one sentence each. Empty
// means it may start. getenv is os.Getenv in production.
func Problems(getenv func(string) string) []string {
	production := getenv("CHRONICLE_ENVIRONMENT") == "production"
	inCluster := getenv("KUBERNETES_SERVICE_HOST") != ""
	var problems []string

	// The built-in fallbacks name a development database and its password. They
	// are for a laptop talking to a port-forward, and nowhere else.
	if getenv("POSTGRES_URL") == "" && (production || inCluster) {
		problems = append(problems, "POSTGRES_URL must be set when running in a cluster or in production: the built-in fallback carries a development password")
	}
	if schema := getenv("POSTGRES_SCHEMA"); schema != "" && (!schemaName.MatchString(schema) || strings.HasPrefix(schema, "pg_")) {
		problems = append(problems, fmt.Sprintf("POSTGRES_SCHEMA %q must be a plain lower-case identifier (letters, digits, underscore) and not start with pg_", schema))
	}
	if !production {
		return problems
	}
	// A database URL without sslmode lets the driver fall back to plain text if
	// the server does not offer TLS. In production the record crosses a network.
	if u, ok := databaseURL(getenv); ok {
		switch mode := u.Query().Get("sslmode"); mode {
		case "require", "verify-ca", "verify-full":
		default:
			if mode == "" {
				mode = "not set"
			}
			problems = append(problems, fmt.Sprintf("POSTGRES_URL must use sslmode=require (or verify-ca, verify-full) in production; it is %s", mode))
		}
	}
	// Anything that can reach the API can read the whole record of what Chronicle
	// saw and decided.
	if getenv("CHRONICLE_API_TOKEN") == "" {
		problems = append(problems, "CHRONICLE_API_TOKEN must be set in production: without it the API is open to anything that can reach it")
	}
	// The shared approval token cannot say who approved, and cannot satisfy the
	// two-approver rule: live healing in production needs real identities.
	if getenv("HEAL_LIVE_ENABLED") == "true" && getenv("OIDC_ISSUER_URL") == "" {
		problems = append(problems, "OIDC_ISSUER_URL must be set when live healing is enabled in production: approvals need an identity, not a shared token")
	}
	return problems
}

// Warnings are configurations that start but are likely to hurt: logged, never
// refused. They are about managed databases, where the same mistakes recur.
func Warnings(getenv func(string) string) []string {
	u, ok := databaseURL(getenv)
	if !ok {
		return nil
	}
	host, port := u.Hostname(), u.Port()
	var warnings []string
	supabase := strings.HasSuffix(host, ".supabase.co") || strings.HasSuffix(host, ".supabase.com")
	switch {
	case strings.HasPrefix(host, "db.") && strings.HasSuffix(host, ".supabase.co"):
		warnings = append(warnings, "POSTGRES_URL is a direct Supabase host, which has only an IPv6 address: pods and containers without IPv6 cannot reach it; use the session pooler (aws-…pooler.supabase.com:5432) unless you have the IPv4 add-on")
	case strings.HasSuffix(host, ".pooler.supabase.com") && port == "6543":
		warnings = append(warnings, "POSTGRES_URL uses Supabase's transaction pooler (port 6543), which does not keep session state: Chronicle's migration lock and its prepared statements need the session pooler (port 5432)")
	}
	if supabase && getenv("POSTGRES_SCHEMA") == "" {
		warnings = append(warnings, "POSTGRES_URL is a Supabase database and POSTGRES_SCHEMA is not set: tables created in public are exposed to the project's API roles (anon, authenticated) and are publishable through its REST API; set POSTGRES_SCHEMA=chronicle")
	}
	return warnings
}

func databaseURL(getenv func(string) string) (*url.URL, bool) {
	raw := getenv("POSTGRES_URL")
	if raw == "" {
		return nil, false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, false // pgx reports an unreadable URL itself
	}
	return u, true
}
