// Package startup refuses configurations that are unsafe to run, before anything
// is started. Chronicle defaults to what is convenient on a laptop; these checks
// are what stop that reaching a cluster that matters.
package startup

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
	if !production {
		return problems
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
