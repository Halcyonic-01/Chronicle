package gitops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// gitServer is a real Git smart-HTTP server (git http-backend) with basic auth,
// and a fake pull-request API in the shape GitHub, Gitea and Forgejo share.
type gitServer struct {
	t       *testing.T
	root    string
	bare    string
	srv     *httptest.Server
	mu      sync.Mutex
	pulls   []map[string]any
	merged  map[int]bool
	closed  map[int]bool
	nextNum int
}

const token = "test-token"

func newGitServer(t *testing.T) *gitServer {
	t.Helper()
	gitBin, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed; the Git server for this test needs git http-backend")
	}
	root := t.TempDir()
	g := &gitServer{t: t, root: root, bare: filepath.Join(root, "acme", "gitops.git"), merged: map[int]bool{}, closed: map[int]bool{}, nextNum: 1}
	g.git(root, "init", "--bare", "--initial-branch=main", g.bare)
	g.git(g.bare, "config", "http.receivepack", "true")

	backend := &cgi.Handler{Path: gitBin, Args: []string{"http-backend"}, Root: "/",
		Env: []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1"}}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/repos/acme/gitops/pulls", g.listOrCreate)
	mux.HandleFunc("/api/v1/repos/acme/gitops/pulls/", g.getPull)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "chronicle" || p != token {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		backend.ServeHTTP(w, r)
	})
	g.srv = httptest.NewServer(mux)
	t.Cleanup(g.srv.Close)
	return g
}

func (g *gitServer) url() string { return g.srv.URL + "/acme/gitops.git" }

func (g *gitServer) git(dir string, args ...string) string {
	g.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	out, err := cmd.CombinedOutput()
	if err != nil {
		g.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commit writes files (nil content deletes) to main through a scratch clone and
// returns the new commit.
func (g *gitServer) commit(msg string, files map[string]*string) string {
	g.t.Helper()
	work := filepath.Join(g.t.TempDir(), "w")
	g.git(g.root, "clone", "--quiet", g.bare, work)
	for name, body := range files {
		p := filepath.Join(work, name)
		if body == nil {
			_ = os.Remove(p)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			g.t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(*body), 0o644); err != nil {
			g.t.Fatal(err)
		}
	}
	g.git(work, "add", "-A")
	g.git(work, "commit", "--quiet", "-m", msg)
	g.git(work, "push", "--quiet", "origin", "HEAD:main")
	return g.git(work, "rev-parse", "HEAD")
}

func (g *gitServer) show(ref, file string) string {
	cmd := exec.Command("git", "--git-dir", g.bare, "show", ref+":"+file)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "<missing>"
	}
	return string(out)
}

func (g *gitServer) listOrCreate(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "token "+token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.Method == http.MethodGet {
		var open []map[string]any
		for _, p := range g.pulls {
			if p["state"] == "open" {
				open = append(open, p)
			}
		}
		_ = json.NewEncoder(w).Encode(open)
		return
	}
	var in map[string]string
	_ = json.NewDecoder(r.Body).Decode(&in)
	p := map[string]any{"number": g.nextNum, "html_url": fmt.Sprintf("%s/acme/gitops/pulls/%d", g.srv.URL, g.nextNum),
		"state": "open", "head": map[string]any{"ref": in["head"]}, "base": in["base"], "title": in["title"], "body": in["body"]}
	g.nextNum++
	g.pulls = append(g.pulls, p)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(p)
}

func (g *gitServer) getPull(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var n int
	fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/api/v1/repos/acme/gitops/pulls/"), "%d", &n)
	for _, p := range g.pulls {
		if p["number"] == n {
			out := map[string]any{"number": n, "state": p["state"], "merged": g.merged[n]}
			if g.merged[n] {
				out["state"], out["merge_commit_sha"], out["merged_at"] = "closed", "abc123", "2026-10-31T12:00:00Z"
			} else if g.closed[n] {
				out["state"] = "closed"
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func str(s string) *string { return &s }

func (g *gitServer) remediator() *Remediator {
	return New(Config{Token: token, Username: "chronicle", APIURL: g.srv.URL + "/api/v1", AllowedRepos: []string{g.url()},
		AuthorName: "Chronicle", AuthorEmail: "chronicle@test"})
}

// history: a good release, a bad one, and an unrelated change elsewhere.
func (g *gitServer) history() (good, bad string) {
	good = g.commit("good", map[string]*string{
		"apps/api/deploy.yaml": str("image: api:v1\n"),
		"apps/api/config.yaml": str("redis: redis:6379\n"),
		"apps/web/deploy.yaml": str("image: web:v1\n"),
	})
	bad = g.commit("bad release", map[string]*string{
		"apps/api/deploy.yaml": str("image: api:v2-broken\n"),
		"apps/api/extra.yaml":  str("added by the bad release\n"),
		"apps/web/deploy.yaml": str("image: web:v2\n"), // another app, not ours to touch
	})
	g.commit("unrelated", map[string]*string{"docs/readme.md": str("hello\n")})
	return good, bad
}

func request(g *gitServer, good, bad string) Request {
	return Request{DecisionID: "01JABCDEF", App: "argocd:api", RepoURL: g.url(), Branch: "main", Path: "apps/api",
		BadRevision: bad, GoodRevision: good, Summary: "api went down after the release", Evidence: []string{"deploy:api, confidence 0.95"},
		Approvers: []string{"ana@example.com"}}
}

func TestARevertOpensAPullRequestThatRestoresOnlyTheAppsFiles(t *testing.T) {
	g := newGitServer(t)
	good, bad := g.history()
	ch, err := g.remediator().Revert(context.Background(), request(g, good, bad))
	if err != nil {
		t.Fatal(err)
	}
	if ch.Number != 1 || ch.Branch != "chronicle/revert-argocd-api-01jabcdef" || ch.Reused {
		t.Fatalf("change %+v", ch)
	}
	if got := strings.Join(ch.Files, ","); got != "apps/api/deploy.yaml,apps/api/extra.yaml" {
		t.Fatalf("files %s", got)
	}
	// On the branch: our app restored, the file the bad release added removed,
	// the other app and the later unrelated commit untouched.
	if got := g.show(ch.Branch, "apps/api/deploy.yaml"); got != "image: api:v1\n" {
		t.Errorf("deploy.yaml on the branch: %q", got)
	}
	if got := g.show(ch.Branch, "apps/api/extra.yaml"); got != "<missing>" {
		t.Errorf("a file the bad release added was kept: %q", got)
	}
	if got := g.show(ch.Branch, "apps/web/deploy.yaml"); got != "image: web:v2\n" {
		t.Errorf("another app was touched: %q", got)
	}
	if got := g.show(ch.Branch, "docs/readme.md"); got != "hello\n" {
		t.Errorf("a later unrelated commit was lost: %q", got)
	}
	// The watched branch is never written to.
	if got := g.show("main", "apps/api/deploy.yaml"); got != "image: api:v2-broken\n" {
		t.Errorf("main was changed: %q", got)
	}
	msg := g.git(g.root, "--git-dir", g.bare, "log", "-1", "--format=%B", ch.Branch)
	if !strings.Contains(msg, "Chronicle-Decision: 01JABCDEF") || !strings.Contains(msg, "Chronicle-Approved-By: ana@example.com") {
		t.Errorf("the commit does not carry the audit trail: %q", msg)
	}
	body := g.pulls[0]["body"].(string)
	for _, want := range []string{"01JABCDEF", good, bad, "apps/api/deploy.yaml", "ana@example.com", "Nothing changes in the cluster until this is merged"} {
		if !strings.Contains(body, want) {
			t.Errorf("the pull request body lacks %q", want)
		}
	}
	if g.pulls[0]["base"] != "main" {
		t.Errorf("base %v", g.pulls[0]["base"])
	}
}

func TestARetryReturnsTheOpenPullRequestInsteadOfOpeningAnother(t *testing.T) {
	g := newGitServer(t)
	good, bad := g.history()
	r := g.remediator()
	first, err := r.Revert(context.Background(), request(g, good, bad))
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.Revert(context.Background(), request(g, good, bad))
	if err != nil || !again.Reused || again.Number != first.Number || len(g.pulls) != 1 {
		t.Fatalf("again %+v, err %v, pulls %d", again, err, len(g.pulls))
	}
}

func TestARevertIsRefusedWhenGitNoLongerMatchesTheEvidence(t *testing.T) {
	refused := func(t *testing.T, err error, want string) {
		t.Helper()
		var r *Refusal
		if !errors.As(err, &r) || !strings.Contains(r.Reason, want) {
			t.Fatalf("want a refusal mentioning %q, got %v", want, err)
		}
	}
	t.Run("changed since", func(t *testing.T) {
		g := newGitServer(t)
		good, bad := g.history()
		g.commit("someone is fixing it", map[string]*string{"apps/api/deploy.yaml": str("image: api:v3\n")})
		_, err := g.remediator().Revert(context.Background(), request(g, good, bad))
		refused(t, err, "changed on main after revision")
	})
	t.Run("already reverted", func(t *testing.T) {
		g := newGitServer(t)
		good, bad := g.history()
		g.commit("manual revert", map[string]*string{"apps/api/deploy.yaml": str("image: api:v1\n"), "apps/api/extra.yaml": nil})
		_, err := g.remediator().Revert(context.Background(), request(g, good, bad))
		refused(t, err, "no change")
	})
	t.Run("not on the branch", func(t *testing.T) {
		g := newGitServer(t)
		good, _ := g.history()
		_, err := g.remediator().Revert(context.Background(), request(g, good, strings.Repeat("a", 40)))
		refused(t, err, "is not in the repository")
	})
	t.Run("good is newer than bad", func(t *testing.T) {
		g := newGitServer(t)
		good, bad := g.history()
		_, err := g.remediator().Revert(context.Background(), request(g, bad, good))
		refused(t, err, "is not an ancestor")
	})
	t.Run("nothing to revert under the path", func(t *testing.T) {
		g := newGitServer(t)
		good, bad := g.history()
		req := request(g, good, bad)
		req.Path = "docs"
		_, err := g.remediator().Revert(context.Background(), req)
		refused(t, err, "nothing to revert")
	})
	t.Run("repository not allowed", func(t *testing.T) {
		g := newGitServer(t)
		r := New(Config{Token: token, Username: "chronicle", APIURL: g.srv.URL + "/api/v1"})
		_, err := r.Revert(context.Background(), request(g, strings.Repeat("a", 40), strings.Repeat("b", 40)))
		refused(t, err, "not in HEAL_GIT_ALLOWED_REPOS")
		if len(g.pulls) != 0 {
			t.Fatal("a pull request was opened for a repository that is not allowed")
		}
	})
}

func TestPullStatusReadsOpenMergedAndClosed(t *testing.T) {
	g := newGitServer(t)
	good, bad := g.history()
	r := g.remediator()
	ch, err := r.Revert(context.Background(), request(g, good, bad))
	if err != nil {
		t.Fatal(err)
	}
	if s, err := r.PullStatus(context.Background(), g.url(), ch.Number); err != nil || s.State != "open" {
		t.Fatalf("%+v %v", s, err)
	}
	g.merged[ch.Number] = true
	if s, err := r.PullStatus(context.Background(), g.url(), ch.Number); err != nil || s.State != "merged" || s.MergeCommit != "abc123" || s.MergedAt.IsZero() {
		t.Fatalf("%+v %v", s, err)
	}
	g.merged[ch.Number], g.closed[ch.Number] = false, true
	if s, _ := r.PullStatus(context.Background(), g.url(), ch.Number); s.State != "closed" {
		t.Fatalf("%+v", s)
	}
}

func TestRepositoryURLsCompareRegardlessOfSpelling(t *testing.T) {
	r := New(Config{AllowedRepos: []string{"https://GitHub.com/Acme/GitOps.git/"}})
	for _, u := range []string{"https://github.com/acme/gitops", "https://github.com/acme/gitops.git", "HTTPS://github.com/ACME/gitops/"} {
		if !r.Allowed(u) {
			t.Errorf("%s should match", u)
		}
	}
	if r.Allowed("https://github.com/acme/other") {
		t.Error("another repository matched")
	}
	if o, n, err := ownerRepo("https://gitea.local:3000/acme/gitops.git"); err != nil || o != "acme" || n != "gitops" {
		t.Errorf("%s %s %v", o, n, err)
	}
}
