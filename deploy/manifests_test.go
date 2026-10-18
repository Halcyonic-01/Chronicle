// Package deploy holds checks on the shipped manifests: what they grant must
// match what the code claims, since the cluster enforces the manifests.
package deploy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/Halcyonic-01/Chronicle/internal/heal"
)

func documents(t *testing.T, path string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := k8syaml.NewYAMLReader(bufioReader(raw))
	var docs [][]byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return docs
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(bytes.TrimSpace(doc)) > 0 {
			docs = append(docs, doc)
		}
	}
}

type kinded struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
}

var writeVerbs = map[string]bool{"create": true, "update": true, "patch": true, "delete": true, "deletecollection": true, "*": true}

// Dry-run needs no mutation privileges: the default deployment may write only
// its own Lease and checkpoint ConfigMap, in its own namespace.
func TestDefaultRBACGrantsNoWritesOutsideChronicle(t *testing.T) {
	for _, doc := range documents(t, "chronicle/rbac.yaml") {
		var k kinded
		if err := yaml.Unmarshal(doc, &k); err != nil {
			t.Fatal(err)
		}
		if k.Kind != "ClusterRole" && k.Kind != "Role" {
			continue
		}
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal(doc, &role); err != nil {
			t.Fatal(err)
		}
		for _, rule := range role.Rules {
			for _, verb := range rule.Verbs {
				if !writeVerbs[verb] {
					continue
				}
				if k.Kind == "ClusterRole" {
					t.Errorf("ClusterRole %s grants %q on %v cluster-wide", k.Metadata.Name, verb, rule.Resources)
				} else if k.Metadata.Namespace != "chronicle" {
					t.Errorf("Role %s grants %q in %s", k.Metadata.Name, verb, k.Metadata.Namespace)
				}
				for _, r := range rule.Resources {
					if r != "leases" && r != "configmaps" {
						t.Errorf("%s %s grants %q on %s", k.Kind, k.Metadata.Name, verb, r)
					}
				}
			}
		}
	}
}

// Live healing's write access is namespaced and never cluster-wide.
func TestExecutorRBACIsNamespaced(t *testing.T) {
	for _, doc := range documents(t, "heal-executor/rbac.yaml") {
		var k kinded
		if err := yaml.Unmarshal(doc, &k); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(k.Kind, "Cluster") {
			t.Errorf("the executor overlay must not contain a %s", k.Kind)
		}
		if k.Metadata.Namespace == "" || k.Metadata.Namespace == "chronicle" {
			t.Errorf("%s %s must live in an allowlisted workload namespace, got %q", k.Kind, k.Metadata.Name, k.Metadata.Namespace)
		}
		if k.Kind != "Role" {
			continue
		}
		var role rbacv1.Role
		if err := yaml.Unmarshal(doc, &role); err != nil {
			t.Fatal(err)
		}
		// Restarts go through the Eviction API so a PodDisruptionBudget applies;
		// a plain pod delete would bypass it.
		for _, rule := range role.Rules {
			for _, res := range rule.Resources {
				for _, verb := range rule.Verbs {
					if res == "pods" && (verb == "delete" || verb == "deletecollection" || verb == "*") {
						t.Errorf("the executor may evict pods, never %s them", verb)
					}
				}
			}
		}
	}
	kustomization, err := os.ReadFile("chronicle/kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(kustomization), "heal-executor") {
		t.Fatal("the default deployment must not include the executor's write access")
	}
}

type validatingPolicy struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		ValidationActions []string `json:"validationActions"`
		FailurePolicy     string   `json:"failurePolicy"`
		Evaluation        struct {
			Background struct {
				Enabled *bool `json:"enabled"`
			} `json:"background"`
		} `json:"evaluation"`
		Autogen struct {
			PodControllers *struct {
				Controllers []string `json:"controllers"`
			} `json:"podControllers"`
		} `json:"autogen"`
		MatchConditions []struct {
			Expression string `json:"expression"`
		} `json:"matchConditions"`
		Validations []struct {
			Expression string `json:"expression"`
		} `json:"validations"`
	} `json:"spec"`
}

// What the CLI tests cannot see: configuration. Every policy starts in Audit,
// can never block the cluster by failing, is limited to Chronicle's service
// account, is not copied onto controllers, and agrees with the namespace
// allowlist Chronicle itself enforces.
func TestKyvernoGuardrailsAreAuditOnlyAndConsistent(t *testing.T) {
	docs := documents(t, "kyverno/chronicle-heal-guardrails.yaml")
	policies := 0
	allowed := allowedNamespaces(t)
	sawNamespaces := false
	for _, doc := range docs {
		var p validatingPolicy
		if err := yaml.Unmarshal(doc, &p); err != nil {
			t.Fatal(err)
		}
		name := p.Metadata.Name
		if p.Kind == "" {
			continue // the comment header before the first ---
		}
		policies++
		if p.Kind != "ValidatingPolicy" {
			t.Errorf("%s: kind %q; the deprecated ClusterPolicy must not come back", name, p.Kind)
		}
		if len(p.Spec.ValidationActions) != 1 || p.Spec.ValidationActions[0] != "Audit" {
			t.Errorf("%s: validationActions %v; every policy starts in Audit", name, p.Spec.ValidationActions)
		}
		if p.Spec.FailurePolicy != "Ignore" {
			t.Errorf("%s: failurePolicy %q; a policy fault must not block the cluster", name, p.Spec.FailurePolicy)
		}
		if bg := p.Spec.Evaluation.Background.Enabled; bg == nil || *bg {
			t.Errorf("%s: background scanning must be off; the rules read the requesting user", name)
		}
		if pc := p.Spec.Autogen.PodControllers; pc == nil || len(pc.Controllers) != 0 {
			t.Errorf("%s: autogen must be disabled so pod rules are not copied onto controllers", name)
		}
		if len(p.Spec.MatchConditions) != 1 || !strings.Contains(p.Spec.MatchConditions[0].Expression, "system:serviceaccount:chronicle:chronicle") {
			t.Errorf("%s: must be limited to the chronicle service account: %+v", name, p.Spec.MatchConditions)
		}
		if name == "chronicle-heal-namespaces" {
			sawNamespaces = true
			m := regexp.MustCompile(`in \[([^\]]*)\]`).FindStringSubmatch(p.Spec.Validations[0].Expression)
			if m == nil {
				t.Fatalf("%s: no namespace list in %q", name, p.Spec.Validations[0].Expression)
			}
			got := strings.NewReplacer("'", "", "\"", "", " ", "").Replace(m[1])
			if got != allowed {
				t.Errorf("policy namespaces %q disagree with HEAL_ALLOWED_NAMESPACES %q", got, allowed)
			}
		}
	}
	if policies != 6 {
		t.Errorf("expected the six guardrail policies, got %d", policies)
	}
	if !sawNamespaces {
		t.Error("the namespace policy is missing")
	}
}

func allowedNamespaces(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("chronicle/deployment.yaml")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) == "- name: HEAL_ALLOWED_NAMESPACES" && i+1 < len(lines) {
			return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(lines[i+1]), "value:"))
		}
	}
	t.Fatal("HEAL_ALLOWED_NAMESPACES not found in the deployment")
	return ""
}

func bufioReader(b []byte) *bufio.Reader { return bufio.NewReader(bytes.NewReader(b)) }

// Every replica applies every migration at start-up. They must do it one replica
// at a time, in one session that stops at the first error: running a psql per
// file let two pods copy the same outcome labels into the history twice.
func TestMigrationInitContainerSerializesAndStopsOnError(t *testing.T) {
	var found bool
	for _, doc := range documents(t, "chronicle/deployment.yaml") {
		var d struct {
			Kind string `json:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						InitContainers []struct {
							Name string   `json:"name"`
							Args []string `json:"args"`
						} `json:"initContainers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(doc, &d); err != nil || d.Kind != "Deployment" {
			continue
		}
		for _, c := range d.Spec.Template.Spec.InitContainers {
			if c.Name != "migrate" || len(c.Args) == 0 {
				continue
			}
			found = true
			script := c.Args[0]
			if !strings.Contains(script, "pg_advisory_lock(") {
				t.Error("the migration run must hold an advisory lock")
			}
			if n := strings.Count(script, "psql "); n != 1 {
				t.Errorf("one psql session must apply every migration so the lock covers them all; found %d invocations", n)
			}
			if !strings.Contains(script, "-v ON_ERROR_STOP=1") {
				t.Error("a failing migration must stop the run")
			}
			if strings.Contains(script, `-f "$migration"`) {
				t.Error("applying each file in its own psql session lets replicas interleave")
			}
		}
	}
	if !found {
		t.Fatal("the migrate init container was not found")
	}
}

// A half-configured sign-in must not ship: Chronicle refuses to start with an
// issuer but nobody allowed to approve, so the manifest has to stay consistent.
func TestOIDCSettingsAreConsistentAndHoldNoSecret(t *testing.T) {
	env := map[string]string{}
	for _, doc := range documents(t, "chronicle/deployment.yaml") {
		var d struct {
			Kind string `json:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name string `json:"name"`
							Env  []struct {
								Name      string         `json:"name"`
								Value     string         `json:"value"`
								ValueFrom map[string]any `json:"valueFrom"`
							} `json:"env"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(doc, &d); err != nil || d.Kind != "Deployment" {
			continue
		}
		for _, c := range d.Spec.Template.Spec.Containers {
			for _, e := range c.Env {
				if strings.HasPrefix(e.Name, "OIDC_") {
					env[e.Name] = e.Value
					if strings.Contains(e.Name, "SECRET") || e.ValueFrom != nil {
						t.Errorf("%s: the console is a public PKCE client; no OIDC value should be a secret", e.Name)
					}
				}
			}
		}
	}
	if _, ok := env["OIDC_ISSUER_URL"]; !ok {
		t.Fatal("OIDC_ISSUER_URL is not declared in the Deployment")
	}
	if env["OIDC_ISSUER_URL"] != "" {
		if !strings.HasPrefix(env["OIDC_ISSUER_URL"], "https://") {
			t.Errorf("issuer %q must be https", env["OIDC_ISSUER_URL"])
		}
		if env["OIDC_CLIENT_ID"] == "" || (env["OIDC_ALLOWED_GROUPS"] == "" && env["OIDC_ALLOWED_EMAILS"] == "") {
			t.Error("an issuer needs a client id and an allowlist, or the collector refuses to start")
		}
	}
}

// The database password is read from the PostgreSQL chart's Secret; no
// manifest carries it in a connection URL.
func TestNoManifestEmbedsTheDatabasePassword(t *testing.T) {
	inline := regexp.MustCompile(`postgres://[^:@/\s]+:([^@\s]+)@`)
	for _, path := range []string{"chronicle/deployment.yaml"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range inline.FindAllStringSubmatch(string(raw), -1) {
			if m[1] != "$(POSTGRES_PASSWORD)" {
				t.Errorf("%s embeds a database password in a URL", path)
			}
		}
		if !strings.Contains(string(raw), "key: postgres-password") {
			t.Errorf("%s does not read the password from the chart's Secret", path)
		}
	}
}

// The executor's restore ceiling and the Kyverno guardrail's are one number.
func TestRestoreCeilingMatchesTheGuardrail(t *testing.T) {
	raw, err := os.ReadFile("kyverno/chronicle-heal-guardrails.yaml")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("object.spec.replicas <= %d", heal.MaxRestoreReplicas)
	if !strings.Contains(string(raw), want) {
		t.Fatalf("the replica guardrail does not enforce %q, the executor's ceiling", want)
	}
}

// envOf reads one container's literal env values from a manifest.
func envOf(t *testing.T, path string) map[string]string {
	t.Helper()
	env := map[string]string{}
	for _, doc := range documents(t, path) {
		var d struct {
			Kind string `json:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Env []struct {
								Name  string `json:"name"`
								Value string `json:"value"`
							} `json:"env"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(doc, &d); err != nil || d.Kind != "Deployment" {
			continue
		}
		for _, c := range d.Spec.Template.Spec.Containers {
			for _, e := range c.Env {
				env[e.Name] = e.Value
			}
		}
	}
	return env
}

// The shipped manifest keeps healing dry-run; only the disposable e2e overlay
// turns it on.
func TestTheBaseDeploymentKeepsLiveHealingOff(t *testing.T) {
	env := envOf(t, "chronicle/deployment.yaml")
	if env["HEAL_LIVE_ENABLED"] != "false" || env["HEAL_KILL_SWITCH"] != "true" {
		t.Fatalf("base manifest: HEAL_LIVE_ENABLED=%q HEAL_KILL_SWITCH=%q", env["HEAL_LIVE_ENABLED"], env["HEAL_KILL_SWITCH"])
	}
	kustomization, err := os.ReadFile("chronicle/kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(kustomization), "e2e") {
		t.Fatal("the base kustomization must not include the e2e overlay")
	}
}

// The e2e overlay may raise hourly caps for the campaign, never lower a
// confidence floor or change what a rule does.
func TestTheE2EOverlayNeverLowersAConfidenceFloor(t *testing.T) {
	env := envOf(t, "e2e/chronicle/e2e-env.yaml")
	var rules []heal.Rule
	if err := json.Unmarshal([]byte(env["HEAL_RULES"]), &rules); err != nil {
		t.Fatalf("HEAL_RULES: %v", err)
	}
	t.Setenv("HEAL_RULES", "")
	defaults := map[string]heal.Rule{}
	for _, r := range heal.RulesFromEnv() {
		defaults[r.Name] = r
	}
	if len(rules) != len(defaults) {
		t.Fatalf("overlay has %d rules, defaults %d", len(rules), len(defaults))
	}
	for _, r := range rules {
		d, ok := defaults[r.Name]
		if !ok || r.CauseType != d.CauseType || r.ActionType != d.ActionType || r.MinConfidence != d.MinConfidence {
			t.Errorf("rule %s differs from the built-in one beyond its hourly cap: %+v vs %+v", r.Name, r, d)
		}
	}
}

// .env.example lists every variable the code reads, and nothing it does not.
func TestEnvExampleMatchesTheCode(t *testing.T) {
	read := regexp.MustCompile(`(?:os\.Getenv|valueOrEnv|intFromEnv|floatFromEnv|durationFromEnv|durationEnv|intEnv|floatEnv|timeEnv)\("([A-Z0-9_]+)"`)
	// Set by the platform, or only for tests, benchmarks and the e2e harness.
	ignore := regexp.MustCompile(`^(HOME|KUBECONFIG|KUBERNETES_SERVICE_HOST|RCA_BENCH_.*|RCA_HOLDOUT_OUT|RCA_INDEPENDENT.*|RCA_SENSITIVITY.*|E2E_.*)$`)
	code := map[string]bool{}
	for _, dir := range []string{"../cmd", "../internal"} {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(path)
			for _, m := range read.FindAllStringSubmatch(string(raw), -1) {
				if !ignore.MatchString(m[1]) {
					code[m[1]] = true
				}
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^#? ?([A-Z0-9_]+)=`).FindAllStringSubmatch(string(raw), -1) {
		listed[m[1]] = true
	}
	for name := range code {
		if !listed[name] {
			t.Errorf("%s is read by the code but missing from .env.example", name)
		}
	}
	for name := range listed {
		if !code[name] && !strings.HasPrefix(name, "CHRONICLE_TEST_") && !strings.HasPrefix(name, "HEAL_TEST_") {
			t.Errorf("%s is in .env.example but nothing reads it", name)
		}
	}
}

// Every script that runs kubectl pins its context: either it sources the
// kind-only guard or it passes --context itself. A bare kubectl would act on
// whichever cluster the kubeconfig currently points at, EKS included.
func TestEveryClusterScriptPinsItsContext(t *testing.T) {
	scripts, err := filepath.Glob("../scripts/*.sh")
	if err != nil || len(scripts) == 0 {
		t.Fatalf("no scripts found: %v", err)
	}
	for _, path := range scripts {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if !strings.Contains(text, "kubectl") || strings.HasSuffix(path, "kube-context.sh") {
			continue
		}
		if !strings.Contains(text, "kube-context.sh") && !strings.Contains(text, "--context") {
			t.Errorf("%s runs kubectl without pinning a context", filepath.Base(path))
		}
	}
	makefile, err := os.ReadFile("../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	for i, line := range strings.Split(string(makefile), "\n") {
		if strings.Contains(line, "kubectl ") && !strings.Contains(line, "--context") && !strings.HasPrefix(strings.TrimSpace(line), "#") {
			t.Errorf("Makefile:%d runs kubectl without --context: %s", i+1, strings.TrimSpace(line))
		}
	}
}
