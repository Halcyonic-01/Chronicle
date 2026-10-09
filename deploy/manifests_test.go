// Package deploy holds checks on the shipped manifests: what they grant must
// match what the code claims, since the cluster enforces the manifests.
package deploy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
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
